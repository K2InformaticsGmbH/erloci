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

% TODO convert to eunit
-export([run/2, run0/0]).

%% API
-export([
    start_link/4,
    exec_sql/3,
    get_rows/3
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

start_link(Tns,Usr,Pswd,PortOptions) ->
    StartRes = gen_server:start_link(?MODULE, [Tns,Usr,Pswd,PortOptions], []),
    case StartRes of
        {ok, Pid} -> {?MODULE, Pid};
        Error -> throw({error, Error})
    end.

exec_sql(Sql, Opts, {?MODULE, ErlOciSession}) when is_binary(Sql); is_list(Opts) ->
    gen_server:call(ErlOciSession, {exec_sql, Sql, Opts}, ?PORT_TIMEOUT).

get_rows(Count, Stmt, {?MODULE, ErlOciSession}) ->
    gen_server:call(ErlOciSession, {get_rows, Count, Stmt}, ?PORT_TIMEOUT).

%
% gen_server interfaces
%

init([Tns,Usr,Pswd,PortOptions]) ->
    OciPort = oci_port:start_link(PortOptions),
    OciSession = OciPort:get_session(Tns,Usr,Pswd),
    throw_if_error(OciSession, "get session failed"),
    {ok, #state{ociSess=OciSession}}.

handle_call({exec_sql, Sql, Opts}, _From, #state{ociSess=OciSession} = State) ->
    Resp = case OciSession:exec_sql(Sql, Opts) of
        {{_, _, StmtId}, Clms} ->
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
handle_call({get_rows, Count, #stmtResult{stmtRef = StmtId}}, _From, #state{ociSess=OciSession} = State) ->
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


%
% Eunit tests
%

-include_lib("eunit/include/eunit.hrl").
-define(NowMs, (fun() -> {M,S,Ms} = erlang:now(), ((M*1000000 + S)*1000000) + Ms end)()).

run0() ->
    ErlOciSession = connect_db(),
    Res0 = ErlOciSession:exec_sql(<<"drop table bikram_test">>, []),
    io:format(user, "drop bikram_test ~p~n", [Res0]),
    Res1 = ErlOciSession:exec_sql(list_to_binary(["create table bikram_test(pkey number,
                                       publisher varchar2(100),
                                       rank number,
                                       hero varchar2(100),
                                       real varchar2(100),
                                       votes number,
                                       createdate date default sysdate,
                                       createtime timestamp default systimestamp,
                                       votes_first_rank number)"]), []),
    io:format(user, "create bikram_test ~p~n", [Res1]),
    Res2 = ErlOciSession:exec_sql(<<"insert into bikram_test (pkey,publisher,rank,hero,real,votes,votes_first_rank) values (:1,:2,:3,:4,:5,:6,:7)">>, []),
    io:format("insert statement ~p~n", [Res2]).

run(Threads, InsertCount) when is_integer(Threads), is_integer(InsertCount) ->
    ErlOciSession = connect_db(),
    This = self(),
    [(fun(Idx) ->
        Table = "erloci_table_"++Idx,
        try
            create_table(ErlOciSession, Table),
            spawn(fun() -> insert_select(ErlOciSession, Table, InsertCount, This) end)
        catch
            Class:Reason -> oci_logger:log(lists:flatten(io_lib:format("~p:~p~n", [Class,Reason])))
        end
    end)(integer_to_list(I))
    || I <- lists:seq(1,Threads)],
    receive_all(Threads).

receive_all(Count) -> receive_all(Count, []).
receive_all(0, Acc) ->
    [(fun(Table, InsertCount, InsertTime, SelectCount, SelectTime) ->
        InsRate = erlang:trunc(InsertCount / InsertTime),
        SelRate = erlang:trunc(SelectCount / SelectTime),
        io:format(user, "_[~s]_ insert ~p, ~p sec, ~p rows/sec    select ~p, ~p sec, ~p rows/sec~n", [Table,InsertCount, InsertTime, InsRate,SelectCount, SelectTime, SelRate])
    end)(T, Ic, It, Sc, St)
    || {T, Ic, It, Sc, St} <- Acc];
receive_all(Count, Acc) ->
    receive
        {T, Ic, It, Sc, St} ->
            receive_all(Count-1, [{T, Ic, It, Sc, St}|Acc])
    after
        (1000*1000) ->
            io:format(user, ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> ~p~n", [timeout]),
            receive_all(0, Acc)
    end.

connect_db() ->
    erloci_session:start_link(
                <<"(DESCRIPTION=
                    (ADDRESS_LIST=
                        (ADDRESS=(PROTOCOL=tcp)
                            (HOST=80.67.144.206)
                            (PORT=1521)
                        )
                    )
                    (CONNECT_DATA=(SERVICE_NAME=XE))
                )">>,
                <<"bikram">>,
                <<"abcd123">>,
                [{logging, true}]).
    %erloci_session:start_link(
    %            <<"(DESCRIPTION=
    %                (ADDRESS=(PROTOCOL=tcp)
    %                    (HOST=192.168.1.69)
    %                    (PORT=1521)
    %                )
    %                (CONNECT_DATA=(SERVICE_NAME=SBS0.k2informatics.ch))
    %            )">>,
    %            <<"SBS0">>,
    %            <<"sbs0sbs0_4dev">>,
    %            <<>>,
    %            [{logging, true}]).

create_table(ErlOciSession, Table) ->
    Res = ErlOciSession:exec_sql(list_to_binary(["drop table ",Table]), []),
    print_if_error(Res, "drop failed"),
    oci_logger:log(lists:flatten(io_lib:format("___________----- OCI drop ~p~n", [Res]))),
    Res0 = ErlOciSession:exec_sql(list_to_binary(["create table ",Table,"(pkey number,
                                       publisher varchar2(100),
                                       rank number,
                                       hero varchar2(100),
                                       real varchar2(100),
                                       votes number,
                                       createdate date default sysdate,
                                       createtime timestamp default systimestamp,
                                       votes_first_rank number)"]), []),
    throw_if_error(undefined, Res0, "create "++Table++" failed"),
    oci_logger:log(lists:flatten(io_lib:format("___________----- OCI create ~p~n", [Res0]))).

insert_select(ErlOciSession, Table, InsertCount, Parent) ->
    try
        InsertStart = ?NowMs,
        [(fun(I) ->
            Qry = erlang:list_to_binary([
                    "insert into ", Table, " (pkey,publisher,rank,hero,real,votes,votes_first_rank) values (",
                    I,
                    ", 'publisher"++I++"',",
                    I,
                    ", 'hero"++I++"'",
                    ", 'real"++I++"',",
                    I,
                    ",",
                    I,
                    ")"]),
            Res = ErlOciSession:exec_sql(Qry, []),
            oci_logger:log(lists:flatten(io_lib:format("_[~s]_ ~s -> ~p~n", [Table,I,Res]))),
            throw_if_error(Parent, Res, "insert "++Table++" failed"),
            if {executed, no_ret} =/= Res -> oci_logger:log(lists:flatten(io_lib:format("_[~s]_ ~p~n", [Table,Res]))); true -> ok end
          end)(integer_to_list(Idx))
        || Idx <- lists:seq(1, InsertCount)],
        InsertEnd = ?NowMs,
        #stmtResult{stmtCols = Cols} = Stmt = ErlOciSession:exec_sql(list_to_binary(["select * from ", Table]), []),
        throw_if_error(Parent, Stmt, "select "++Table++" failed"),
        oci_logger:log(lists:flatten(io_lib:format("_[~s]_ columns ~p~n", [Table,Cols]))),
        Rows = get_all_rows(ErlOciSession, Stmt, 10, []),
        oci_logger:log(lists:flatten(io_lib:format("_[~s]_ rows ~p~n", [Table, Rows]))),
        SelectEnd = ?NowMs,
        InsertTime = (InsertEnd - InsertStart)/1000000,
        SelectTime = (SelectEnd - InsertEnd)/1000000,
        Parent ! {Table, InsertCount, InsertTime, length(Rows), SelectTime}
    catch
        Class:Reason ->
            if is_pid(Parent) -> Parent ! {{Table,Class,Reason}, 0, 1, 0, 1}; true -> ok end,
            oci_logger:log(lists:flatten(io_lib:format(" ~p:~p~n", [Class,Reason])))
    end.

get_all_rows(ErlOciSession, Stmt, Chunk, Rows) ->
    case ErlOciSession:get_rows(Chunk, Stmt) of
        {Rs, false} -> get_all_rows(ErlOciSession, Stmt, Chunk, Rows ++ Rs);
        {Rs, true} -> Rows ++ Rs
    end.

print_if_error({error, Error}, Msg) -> oci_logger:log(lists:flatten(io_lib:format("___________----- continue after ~p ~p~n", [Msg,Error])));
print_if_error(_, _) -> ok.

throw_if_error(Parent, {error, Error}, Msg) ->
    if is_pid(Parent) -> Parent ! {{Msg, Error}, 0, 1, 0, 1}; true -> ok end,
    throw({Msg, Error});
throw_if_error(_,_,_) -> ok.
