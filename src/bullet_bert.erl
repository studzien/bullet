-module(bullet_bert).

-behaviour(gen_server).

%% API
-export([start_link/4]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         code_change/3]).

%% bullet_handler callbacks
-export([init/4,
         stream/3,
         info/3]).

%% common callback
-export([terminate/2]).

-record(state, {handler, handler_state, timestamp=0,
                timer, clientid, transports=[]}).
-record(bullet_state, {clientid}).

-define(TIMEOUT, 90000).

%%%===================================================================
%%% Callbacks definitions
%%%===================================================================
-type state() :: any().

-callback init(list())
    -> {ok, state()}
    | {stop, any()}.
-callback handle_call(any(), state())
    -> {reply, any(), state()}.
-callback handle_cast(any(), state())
    -> {noreply, state()}.
-callback handle_info(any(), state())
    -> {reply, any(), state()}
    | {noreply, state()}.
-callback terminate(state())
    -> any().

%%%===================================================================
%%% API
%%%===================================================================

start_link(ClientId, Handler, Args, Transport) ->
    gen_server:start_link(?MODULE, [ClientId, Handler, Args, Transport], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([ClientId, Handler, Args, Transport]) ->
    case Handler:init(Args) of
        {ok, HandlerState} ->
            Timer = erlang:send_after(?TIMEOUT, self(), timeout),
            State = #state{handler=Handler,
                           handler_state=HandlerState,
                           clientid=ClientId,
                           transports=[Transport],
                           timer=Timer},
            {ok, State};
        {stop, Reason} ->
            {stop, Reason}
    end.

handle_call({call, Timestamp, Term}, _From,
            #state{handler=Handler, handler_state=HandlerState}=State) ->
    State1 = bump_timestamp(State, Timestamp),
    case Handler:handle_call(Term, HandlerState) of
        {reply, Reply, NewHandlerState} ->
            {reply, {reply, {reply, Timestamp, Reply}},
             State1#state{handler_state=NewHandlerState}}
    end;
handle_call({cast, Timestamp, Term}, _From,
            #state{handler=Handler, handler_state=HandlerState}=State) ->
    State1 = bump_timestamp(State, Timestamp),
    case Handler:handle_cast(Term, HandlerState) of
        {noreply, NewHandlerState} ->
            {reply, noreply, State1#state{handler_state=NewHandlerState}}
    end;
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(ping, State) ->
    State1 = reset_timer(State),
    {noreply, State1};
handle_cast({register, Pid}, #state{transports=Pids}=State) ->
    State1 = State#state{transports=[Pid|Pids]},
    State2 = reset_timer(State1),
    {noreply, State2};
handle_cast({unregister, Pid}, #state{transports=Pids}=State) ->
    State1 = State#state{transports=lists:delete(Pid, Pids)},
    {noreply, State1};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(timeout, State) ->
    {stop, shutdown, State};
handle_info(Info, State=#state{handler=Handler,
                               handler_state=HandlerState,
                               transports=Pids}) ->
    #state{timestamp=Timestamp} = State1 = bump_timestamp(State),
    case Handler:handle_info(Info, HandlerState) of
        {noreply, NewHandlerState} ->
            {noreply, State1#state{handler_state=NewHandlerState}};
        {reply, Reply, NewHandlerState} ->
            [Pid ! {info, Timestamp, Reply} || Pid <- Pids],
            {noreply, State1#state{handler_state=NewHandlerState}}
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Bullet handler callbacks
%%%===================================================================
init(_Transport, Req, Opts, _Active) ->
    {callbacks, Handler} = lists:keyfind(callbacks, 1, Opts),
    {args, Args} = lists:keyfind(args, 1, Opts),
    {ClientId, Req1} = cowboy_req:binding(clientid, Req),
    case ets:lookup(bullet_clients, ClientId) of
        [] ->
            {ok, Pid} = supervisor:start_child(bullet_sup,
                                              [ClientId, Handler,
                                               Args, self()]),
            ets:insert(bullet_clients, {ClientId, Pid});
        [{ClientId, Pid}] ->
            gen_server:cast(Pid, {register, self()})
    end,
    {ok, Req1, #bullet_state{clientid=ClientId}}.

stream(<<"ping">>, Req, #bullet_state{clientid=ClientId}=State) ->
    Pid = client_pid(ClientId),
    gen_server:cast(Pid, ping),
    {reply, <<"pong">>, Req, State};
stream(Data, Req, State) ->
    try
        Binary = base64:decode(Data),
        handle_stream(bert:decode(Binary), Req, State)
    catch _:_ ->
        {ok, Req, State}
    end.

info(Info, Req, State) ->
    handle_reply(Info, Req, State).

%%%===================================================================
%%% terminate/2 is common callback - differentiate by state record
%%%===================================================================
terminate(_Reason, #state{clientid=ClientId}) ->
    ets:delete(bullet_clients, ClientId),
    ok;
terminate(_Req, #bullet_state{clientid=ClientId}) ->
    Pid = client_pid(ClientId),
    gen_server:cast(Pid, {unregister, self()}),
    ok.

%%%===================================================================
%%% gen_server internal functions
%%%===================================================================
reset_timer(#state{timer=Timer}=State) ->
    erlang:cancel_timer(Timer),
    Timer1 = erlang:send_after(?TIMEOUT, self(), timeout),
    State#state{timer=Timer1}.

bump_timestamp(#state{timestamp=LocalTS}=State, RemoteTS) ->
    NewTS = erlang:max(LocalTS+1, RemoteTS),
    State#state{timestamp=NewTS}.

bump_timestamp(#state{timestamp=LocalTS}=State) ->
    State#state{timestamp=LocalTS+1}.

%%%===================================================================
%%% bullet_handler internal functions
%%%===================================================================
handle_stream(Term, Req, #bullet_state{clientid=ClientId}=State) ->
    Pid = client_pid(ClientId),
    case gen_server:call(Pid, Term) of
        {reply, Reply} ->
            handle_reply(Reply, Req, State);
        noreply ->
            {ok, Req, State}
    end;
handle_stream(_, Req, State) ->
    {ok, Req, State}.

handle_reply(HandlerReply, Req, State) ->
    Reply = bert:encode(HandlerReply),
    {reply, base64:encode(Reply), Req, State}. 

client_pid(ClientId) ->
    [{ClientId, Pid}] = ets:lookup(bullet_clients, ClientId),
    Pid. 
