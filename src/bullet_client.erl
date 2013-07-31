-module(bullet_client).

-behaviour(gen_server).

%% API
-export([start_link/3]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {handler, handler_state, timestamp=0, clientid, transports=[]}).

-define(TIMEOUT, 90000).

%%--------------------------------------------------------------------
%% Callbacks definitions
%%--------------------------------------------------------------------
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

start_link(ClientId, Handler, Args) ->
    gen_server:start_link(?MODULE, [ClientId, Handler, Args], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([ClientId, Handler, Args]) ->
    case Handler:init(Args) of
        {ok, HandlerState} ->
            State = #state{handler=Handler,
                           handler_state=HandlerState,
                           clientid=ClientId},
            {ok, State, ?TIMEOUT};
        {stop, Reason} ->
            {stop, Reason}
    end.

handle_call({call, Timestamp, Term}, _From,
            #state{handler=Handler, handler_state=HandlerState}=State) ->
    State1 = bump_timestamp(State, Timestamp),
    case Handler:handle_call(Term, HandlerState) of
        {reply, Reply, NewHandlerState} ->
            {reply, {reply, {reply, Timestamp, Reply}},
             State1#state{handler_state=NewHandlerState}, ?TIMEOUT}
    end;
handle_call({cast, Timestamp, Term}, _From,
            #state{handler=Handler, handler_state=HandlerState}=State) ->
    State1 = bump_timestamp(State, Timestamp),
    case Handler:handle_cast(Term, HandlerState) of
        {noreply, NewHandlerState} ->
            {reply, noreply,
             State1#state{handler_state=NewHandlerState}, ?TIMEOUT}
    end;
handle_call(_Request, _From, State) ->
    {reply, ok, State, ?TIMEOUT}.

handle_cast(ping, State) ->
    {noreply, State, ?TIMEOUT};
handle_cast({register, Pid}, #state{transports=Pids}=State) ->
    State1 = State#state{transports=[Pid|Pids]},
    {noreply, State1, ?TIMEOUT};
handle_cast({unregister, Pid}, #state{transports=Pids}=State) ->
    State1 = State#state{transports=lists:delete(Pid, Pids)},
    {noreply, State1, ?TIMEOUT};
handle_cast(_Msg, State) ->
    {noreply, State, ?TIMEOUT}.

handle_info(timeout, State) ->
    error_logger:info_msg("Timeout~n",[]),
    {stop, shutdown, State};
handle_info(Info, State=#state{handler=Handler,
                               handler_state=HandlerState,
                               transports=Pids}) ->
    #state{timestamp=Timestamp} = State1 = bump_timestamp(State),
    case Handler:handle_info(Info, HandlerState) of
        {noreply, NewHandlerState} ->
            {noreply, State1#state{handler_state=NewHandlerState}, ?TIMEOUT};
        {reply, Reply, NewHandlerState} ->
            [Pid ! {info, Timestamp, Reply} || Pid <- Pids],
            {noreply, State1#state{handler_state=NewHandlerState}, ?TIMEOUT}
    end.

terminate(_Reason, #state{clientid=ClientId}) ->
    ets:delete(bullet_clients, ClientId),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
bump_timestamp(#state{timestamp=LocalTS}=State, RemoteTS) ->
    NewTS = erlang:max(LocalTS+1, RemoteTS),
    State#state{timestamp=NewTS}.

bump_timestamp(#state{timestamp=LocalTS}=State) ->
    State#state{timestamp=LocalTS+1}.
