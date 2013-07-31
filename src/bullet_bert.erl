-module(bullet_bert).

-record(state, {handler, handler_state, clientid, timer}).

-export([init/4, stream/3, info/3, terminate/2]).

-type state() :: any().

-define(TIMEOUT, 90000).

%%--------------------------------------------------------------------
%% Callbacks definitions
%%--------------------------------------------------------------------
-callback init(list())
    -> {ok, state()}
    | {shutdown, any()}.
-callback init(list(), state())
    -> {ok, state()}.
-callback handle_call(any(), state())
    -> {reply, any(), state()}.
-callback handle_cast(any(), state())
    -> {noreply, state()}.
-callback handle_info(any(), state())
    -> {reply, any(), state()}
    | {noreply, state()}.
-callback terminate(state())
    -> any().

%%--------------------------------------------------------------------
%% Bullet handler callbacks
%%--------------------------------------------------------------------
init(_Transport, Req, Opts, _Active) ->
    {callbacks, Handler} = lists:keyfind(callbacks, 1, Opts),
    {args, Args} = lists:keyfind(args, 1, Opts),
    {ClientId, Req1} = cowboy_req:binding(clientid, Req),
    case ets:lookup(bullet_clients, ClientId) of
        [] ->
            State = #state{handler=Handler, clientid=ClientId},
            handle_init1(Handler:init(Args), Req1, State);
        [{ClientId, State=#state{handler_state=HandlerState}, _}] ->
            handle_init2(Handler:init(Args, HandlerState), Req1, State)
    end.

stream(<<"ping">>, Req, State) ->
    State1 = reset_timer(State),
    {reply, <<"pong">>, Req, State1};
stream(Data, Req, State) ->
    try
        Binary = base64:decode(Data),
        handle_stream(bert:decode(Binary), Req, State)
    catch _:_ ->
        {ok, Req, State}
    end.

info(Info, Req, #state{handler=Handler, handler_state=HandlerState}=State) ->
    Timestamp = bump_timestamp(State),
    case Handler:handle_info(Info, HandlerState) of
        {noreply, NewHandlerState} ->
            {ok, Req, State#state{handler_state=NewHandlerState}};
        {reply, Reply, NewHandlerState} ->
            handle_reply(info, Reply, Timestamp, Req,
                         State#state{handler_state=NewHandlerState})
    end.

terminate(_Req, State=#state{handler=Handler,
                             handler_state=HandlerState,
                             clientid=ClientId}) ->
    State1 = reset_timer(State),
    ets:update_element(bullet_clients, ClientId, {2, State1}),
    Handler:terminate(HandlerState).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
handle_init1({ok, HandlerState}, Req, State=#state{clientid=ClientId}) ->
    State1 = State#state{handler_state=HandlerState,
                         timer=set_timer(ClientId)},
    ets:insert(bullet_clients, {ClientId, State, 0}),
    {ok, Req, State1};
handle_init1({stop, HandlerState}, _Req, State) ->
    {shutdown, State#state{handler_state=HandlerState}}.

handle_init2({ok, NewHandlerState}, Req, State) ->
    State1 = State#state{handler_state=NewHandlerState},
    {ok, Req, State1}.

handle_stream({call, Timestamp, Term}, Req,
              #state{handler=Handler, handler_state=HandlerState}=State) ->
    bump_timestamp(State, Timestamp),
    case Handler:handle_call(Term, HandlerState) of
        {reply, Reply, NewHandlerState} ->
            handle_reply(reply, Reply, Timestamp, Req,
                         State#state{handler_state=NewHandlerState})
    end;
handle_stream({cast, Timestamp, Term}, Req,
              #state{handler=Handler, handler_state=HandlerState}=State) ->
    bump_timestamp(State, Timestamp),
    case Handler:handle_cast(Term, HandlerState) of
        {noreply, NewHandlerState} ->
            {ok, Req, State#state{handler_state=NewHandlerState}}
    end;
handle_stream(_, Req, State) ->
    {ok, Req, State}.

handle_reply(Tag, HandlerReply, Timestamp, Req, State) ->
    Reply = bert:encode({Tag, Timestamp, HandlerReply}),
    {reply, base64:encode(Reply), Req, State}. 

bump_timestamp(#state{clientid=ClientId}) ->
    ets:update_counter(bullet_clients, ClientId, {3,1}).

bump_timestamp(#state{clientid=ClientId}, RemoteTS) ->
    [{_,_,LocalTS}] = ets:lookup(bullet_clients, ClientId),
    NewTS = erlang:max(LocalTS+1, RemoteTS),
    ets:update_element(bullet_clients, ClientId, {3,NewTS}),
    NewTS.

reset_timer(#state{timer=Timer, clientid=ClientId}=State) ->
    error_logger:info_msg("Timer ~p canceled for ~p~n", [Timer, ClientId]),
    erlang:cancel_timer(Timer),
    State#state{timer=set_timer(ClientId)}.

set_timer(ClientId) ->
    T = erlang:send_after(?TIMEOUT, bullet_cleaner, {clean, ClientId}),
    error_logger:info_msg("Timer ~p set for ~p~n", [T, ClientId]),
    T.
