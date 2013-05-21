%% Copyright 2012 K2Informatics GmbH, Root Laengenbold, Switzerland
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

-module(oci_port_empty_table_mock).
-compile([export_all]).

-include("oci.hrl").
-include_lib("stdlib/include/qlc.hrl").

-define(SessionId, 112233).
-define(Statement, 223344).

open_port({spawn_executable, _Executable}, _Options) ->
    Port = fun
        (F) ->
            receive
                {port_cmd, Pid, {?R_DEBUG_MSG, 0}} ->
                    reply(Pid, F, {?R_DEBUG_MSG, ok, log_disabled});
                {port_cmd, Pid, {?R_DEBUG_MSG, 1}} ->
                    reply(Pid, F, {?R_DEBUG_MSG, ok, log_enabled});
                {port_cmd, Pid, {?CREATE_SESSION_POOL,_,_,_,_}} ->
                    reply(Pid, F, {?CREATE_SESSION_POOL, ok, "mock_pool"});
                {port_cmd, Pid, {?RELEASE_SESSION_POOL}} ->
                    reply(Pid, F, {?RELEASE_SESSION_POOL, ok});
                {port_cmd, Pid, {?GET_SESSION}} ->
                    reply(Pid, F, {?GET_SESSION, ok, ?SessionId});
                {port_cmd, Pid, {?RELEASE_SESSION, ?SessionId}} ->
                    reply(Pid, F, {?RELEASE_SESSION, ?SessionId, {ok}});
                {port_cmd, Pid, {?RELEASE_SESSION, _}} ->
                    reply(Pid, F, {?RELEASE_SESSION, ?SessionId, {error, mock_error_wrong_session}});
                {port_cmd, Pid, {?EXEC_SQL, ?SessionId, _, []}} ->
                    reply(Pid, F, {?EXEC_SQL, ?SessionId, {columns, ?Statement, columns()}});
                {port_cmd, Pid, {?EXEC_SQL, ?SessionId, _, _}} ->
                    reply(Pid, F, {?EXEC_SQL, ?SessionId, {executed, "Batman"}});
                {port_cmd, Pid, {?FETCH_ROWS, ?SessionId, ?Statement}} ->
                    reply(Pid, F, {?FETCH_ROWS, ?SessionId, {{rows, []}, done}});
                {port_stop, Pid} ->
                    Pid ! ok;
                M ->
                    io:format("unrecognized port command ~p~n", [M]),
                    F(F)
            end
    end,
    spawn(fun() -> Port(Port) end).

port_close(Port) ->
    Port ! {port_stop, self()}.

port_info(_Port) ->
    [{name, "erlocimock"}, {id, 1234}, {mode, mock}].

port_command(Port, Msg) ->
    Port ! {port_cmd, self(), binary_to_term(Msg)},
    true.

reply(Pid, F, Msg) ->
    Pid ! {self(), {data, term_to_binary(Msg)}},
    F(F).

columns() ->
    [
        {"PKEY", integer, 100},
        {"PUBLISHER", string, 100},
        {"RANK", integer, 2},
        {"HERO", string, 100},
        {"REAL", string, 100},
        {"VOTES", integer, 4},
        {"VOTES_FIRST_RANK", integer, 4}
    ].
