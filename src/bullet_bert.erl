-module(bullet_bert).

-record(state, {handler, handler_state, timestamp=0}).

-export([init/4, stream/3, info/3, terminate/2]).

-type state() :: any().

%%--------------------------------------------------------------------
%% Callbacks definitions
%%--------------------------------------------------------------------
-callback init(list())
    -> {ok, state()}
    | {shutdown, any()}.
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
init(_Transport, Req, [{handler,_},{callbacks,Handler},{args,Args}], _Active) ->
    State = #state{handler=Handler},
    case Handler:init(Args) of
        {ok, HandlerState} ->
            {ok, Req, State#state{handler_state=HandlerState}};
        {stop, HandlerState} ->
            {shutdown, State#state{handler_state=HandlerState}}
    end.

stream(<<"ping">>, Req, State) ->
    {reply, <<"pong">>, Req, State};
stream(Data, Req, State) ->
    try
        handle_stream(bert:decode(Data), Req, State)
    catch _:_ ->
        {ok, Req, State}
    end.

info(Info, Req, #state{handler=Handler, handler_state=HandlerState}=State) ->
    #state{timestamp=Timestamp} = State1 = bump_timestamp(State),
    case Handler:handle_info(Info, HandlerState) of
        {noreply, NewHandlerState} ->
            {ok, Req, State1#state{handler_state=NewHandlerState}};
        {reply, Reply, NewHandlerState} ->
            handle_reply(info, Reply, Timestamp, Req,
                         State1#state{handler_state=NewHandlerState})
    end.

terminate(_Req, #state{handler=Handler, handler_state=HandlerState}) ->
    Handler:terminate(HandlerState).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
handle_stream({call, Timestamp, Term}, Req,
              #state{handler=Handler, handler_state=HandlerState}=State) ->
    State1 = bump_timestamp(State, Timestamp),
    case Handler:handle_call(Term, HandlerState) of
        {reply, Reply, NewHandlerState} ->
            handle_reply(reply, Reply, Timestamp, Req,
                         State1#state{handler_state=NewHandlerState})
    end;
handle_stream({cast, Timestamp, Term}, Req,
              #state{handler=Handler, handler_state=HandlerState}=State) ->
    State1 = bump_timestamp(State, Timestamp),
    case Handler:handle_cast(Term, HandlerState) of
        {noreply, NewHandlerState} ->
            {ok, Req, State1#state{handler_state=NewHandlerState}}
    end;
handle_stream(_, Req, State) ->
    {ok, Req, State}.

handle_reply(Tag, HandlerReply, Timestamp, Req, State) ->
    Reply = bert:encode({Tag, Timestamp, HandlerReply}),
    {reply, {binary, Reply}, Req, State}. 

bump_timestamp(#state{timestamp=LocalTS}=State) ->
    State#state{timestamp=LocalTS+1}.

bump_timestamp(#state{timestamp=LocalTS}=State, RemoteTS) ->
    State#state{timestamp=erlang:max(LocalTS+1, RemoteTS)}.
