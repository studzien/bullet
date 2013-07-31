-module(bullet_bert).

-record(state, {clientid}).

-export([init/4, stream/3, info/3, terminate/2]).

%%--------------------------------------------------------------------
%% Bullet handler callbacks
%%--------------------------------------------------------------------
init(_Transport, Req, Opts, _Active) ->
    {callbacks, Handler} = lists:keyfind(callbacks, 1, Opts),
    {args, Args} = lists:keyfind(args, 1, Opts),
    {ClientId, Req1} = cowboy_req:binding(clientid, Req),
    Client = case ets:lookup(bullet_clients, ClientId) of
        [] ->
            {ok, Pid} = supervisor:start_child(bullet_sup,
                                               [ClientId, Handler, Args]),
            ets:insert(bullet_clients, {ClientId, Pid}),
            Pid;
        [{ClientId, Pid}] ->
            Pid
    end,
    gen_server:cast(Client, {register, self()}),
    {ok, Req1, #state{clientid=ClientId}}.

stream(<<"ping">>, Req, #state{clientid=ClientId}=State) ->
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

terminate(_Req, #state{clientid=ClientId}) ->
    Pid = client_pid(ClientId),
    gen_server:cast(Pid, {unregister, self()}).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
handle_stream(Term, Req, #state{clientid=ClientId}=State) ->
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
