%% Copyright 2012 K2Informatics GmbH, Root Längenbold, Switzerland
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(oci_logger).
-behaviour(gen_server).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-export([log/1]).

log(Msg) ->
    case [R || R <- erlang:registered(), R =:= ?MODULE] of
        [] ->
            gen_server:start_link({local, ?MODULE}, ?MODULE, [], []),
            log(Msg);
        _ -> gen_server:cast(?MODULE, Msg)
    end.

init(_) ->
    io:format(user, "_-_-_-_-_- O  C  I    L O G G E R _-_-_-_-~n", []),
    {ok, undefined}.

handle_cast(Msg, State) ->
    io:format(user, Msg, []),
    {noreply, State}.

handle_info(Msg, State) ->
    io:format(user, "~p unsupported handle_info ~p", [{?MODULE, ?LINE}, Msg]),
    {noreply, State}.

handle_call(Msg, _From, State) ->
    io:format(user, "~p unsupported handle_call ~p", [{?MODULE, ?LINE}, Msg]),
    {reply, ok, State}.

terminate(Reason, _) -> io:format(user, "~p Terminating ~p", [{?MODULE, ?LINE}, Reason]).
code_change(_OldVsn, State, _Extra) -> {ok, State}.
