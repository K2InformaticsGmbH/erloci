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

-module(erloci_session).
-behaviour(gen_server).

-include("erloci.hrl").

%% API
-export([
    start_link/5,
    exec_sql/3,
    get_rows/2
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-record(state, {
    ociSess
}).

% 
% external APIs
%

start_link(Tns,Usr,Pswd,Opts,PortOptions) ->
    StartRes = gen_server:start_link(?MODULE, [Tns,Usr,Pswd,Opts,PortOptions], []),
    case StartRes of
        {ok, Pid} -> {?MODULE, Pid};
        Error -> throw({error, Error})
    end.

exec_sql(Sql, Opts, {?MODULE, ErlOciSession}) when is_binary(Sql); is_list(Opts) ->
    gen_server:call(ErlOciSession, {exec_sql, Sql, Opts}, ?PORT_TIMEOUT).

get_rows(Count, {?MODULE, ErlOciSession}) ->
    gen_server:call(ErlOciSession, {get_rows, Count}, ?PORT_TIMEOUT).

%
% gen_server interfaces
%

init([Tns,Usr,Pswd,Opts,PortOptions]) ->
    OciPort = oci_port:start_link(PortOptions),
    OciPool = OciPort:create_sess_pool(Tns,Usr,Pswd,Opts),
    throw_if_error(OciPool, "pool creation failed"),
    OciSession = OciPort:get_session(),
    throw_if_error(OciSession, "get session failed"),
    {ok, #state{ociSess=OciSession}}.

handle_call({exec_sql, Sql, Opts}, _From, #state{ociSess=OciSession} = State) ->
    Resp = case OciSession:exec_sql(Sql, Opts) of
        {{?MODULE, _, StmtId}, Clms} ->
            Cols = [
                #stmtCol {
                    alias = N,
                    type = T,
                    len = L
                }
            || {N,T,L} <- Clms],
        #stmtResult{stmtRef = StmtId, stmtCols = lists:reverse(Cols)};
        R -> R
    end,
    {reply, Resp, State};
handle_call({get_rows, Count, StmtId}, _From, #state{ociSess=OciSession} = State) ->
    {Mod,PortPid,_} = OciSession,
    Statement = {Mod, PortPid, StmtId},
    {{rows, Rows}, F} = Statement:get_rows(Count),
    {reply, {flip(Rows), F}, State}.

handle_cast(Msg, State) ->
    error_logger:error_report("ERLOCI: received unexpected cast: ~p~n", [Msg]),
    {noreply, State}.

handle_info(Info, State) ->
    error_logger:error_report("ERLOCI: received unexpected info: ~p~n", [Info]),
    {noreply, State}.

terminate(Reason, _State) ->
    ?Error("Terminating ~p", [Reason]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%
% helper functions
%

throw_if_error({error, Error}, Msg) -> throw({Msg, Error});
throw_if_error(_,_) -> ok.

flip(Rows) -> flip(Rows,[]).
flip([],Acc) -> Acc;
flip([Row|Rows],Acc) -> flip(Rows, [lists:reverse(Row)|Acc]).
