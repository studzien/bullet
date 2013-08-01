
-module(bullet_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    ets:new(bullet_clients, [public, named_table, {read_concurrency, true}]),
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    {ok, { {simple_one_for_one, 5, 10}, [
                {bullet_bert,
                 {bullet_bert, start_link, []},
                 transient,
                 5000,
                 worker,
                 dynamic} 
                ]} }.

