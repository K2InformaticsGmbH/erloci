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

-module(oci_test).
-behavior(gen_server).

%-ifdef(TEST).

-include("oci_test.hrl").

-export([ start/2
        , setup/0
        , teardown/1
        , bigtable_test/1
        , bigtab_setup/0
        , bigtab_load/2
        , bigtab_access/1
        ]).

%gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3]).

-define(TablePrefix, "erloci_table_").
-define(ToMs(__Now), begin {__M,__S,__Ms} = __Now, (((__M*1000000 + __S)*1000000) + __Ms) end).
-define(NowMs, ?ToMs(erlang:now())).
-define(Log(__Fmt,__Args),
(fun(__F,__A) ->
    {{__Y,__M,__D},{__H,__Min,__S}} = calendar:now_to_datetime(erlang:now()),
    io:format(user, "~4..0B-~2..0B-~2..0B ~2..0B:~2..0B:~2..0B ~p "++__F, [__Y,__M,__D,__H,__Min,__S,{?MODULE, ?LINE} | __A])
end)(__Fmt,__Args)).
-define(Log(__F), ?Log(__F,[])).

-record(state,  { port
                , session
                , stats = []
                , monitorrefs
                , threads
                , rowcount
                , statTref
       }).

start(Threads, RowCount) ->
    application:start(erloci),
    {ok, {Tns,User,Pswd}} = application:get_env(erloci, default_connect_param),
    start(Tns,User,Pswd,Threads,RowCount).

start(Tns,User,Pswd,Threads,RowCount) ->
    application:start(erloci),
    gen_server:start_link(?MODULE, [Tns,User,Pswd,Threads,RowCount], []).

init([Tns,User,Pswd,Threads,RowCount]) ->
    OciPort = oci_port:start_link([{logging, true}]),
    OciSession = setupi(OciPort,Tns,User,Pswd),
    self() ! {start,Threads,RowCount},
    StatTRef = erlang:send_after(1000, self(), stats),
    {ok, #state{ port       = OciPort
               , session    = OciSession
               , threads    = Threads
               , rowcount   = RowCount
               , statTref   = StatTRef}}.

handle_call(Msg, From, State) ->
    {stop,normal,{unknown_call, Msg, From},State}.

handle_cast({log, Str}, State) ->
    ?Log("~s", [Str]),
    {noreply,State};
handle_cast({Type, Table, Start, End}, #state{stats=Stats} = State) ->
    NewStats = case lists:keyfind(Table, 1, Stats) of
        false -> [{Table, [{Type, Start, End}]} | Stats];
        {Table, TabStats} -> lists:keyreplace(Table, 1, Stats, {Table, [{Type, Start, End} | TabStats]})
    end,
    {noreply,State#state{stats=NewStats}};
handle_cast(Request, State) ->
    ?Log("Unknown cast ~p from~p", [Request]),
    {stop, normal, State}.

handle_info(stats, #state{stats = Stats, rowcount = RowCount} = State) ->
    ?Log("------------------------------------------------------------------------~n"),
    print_stats(Stats,RowCount),
    StatTRef = erlang:send_after(1000, self(), stats),
    {noreply,State#state{statTref = StatTRef}};
handle_info({start,Threads,RowCount}, #state{session=OciSession} = State) ->
    ?Log("========================================================================~n"),
    ?Log("Starting tests processes ~p rows/process ~p~n", [Threads,RowCount]),
    ?Log("========================================================================~n"),
    Self = self(),
    MonitorRefs = [{erlang:monitor(process, P), Tab}
                  || {Tab,P} <- [begin
                                    Table = ?TablePrefix++integer_to_list(T),
                                    Pid = spawn(fun() ->
                                        run_test(OciSession, Table, RowCount, Self)
                                    end),
                                    {Table,Pid}
                                 end
                                || T <- lists:seq(1,Threads)]],
    {noreply, State#state{monitorrefs=MonitorRefs}};
handle_info({'DOWN', MonRef, _, Pid, Info}, #state{ monitorrefs = MonitorRefs
                                                  , stats       = Stats
                                                  , rowcount    = RowCount
                                                  , statTref    = StatTRef} = State) ->
    {Table, NewMonitorRefs} = case lists:keytake(MonRef, 1, MonitorRefs) of
        {value, {MonRef, Tab}, NewMonRefs} -> {Tab, NewMonRefs};
        false -> {"", MonitorRefs}
    end,
    if Info =/= normal -> ?Log("Test table ~p process ~p died reason ~p~n", [Table, Pid, Info]); true -> ok end,
    NewStatTRef = if NewMonitorRefs =:= [] ->
        erlang:cancel_timer(StatTRef),
        ?Log("========================================================================~n"),
        print_stats(Stats,RowCount),
        ?Log("========================================================================~n"),
        undefined;
        true -> StatTRef
    end,
    {noreply, State#state{monitorrefs=NewMonitorRefs, statTref = NewStatTRef}};
handle_info(Info, State) ->
    ?Log("Unknown info ~p from~p", [Info]),
    {stop, normal, State}.

print_stats(Stats, _RowCount) when Stats =:= [] -> ok;
print_stats(Stats, RowCount) ->
    try
        [begin
            FormatStats = [begin
                        StartMs = (((element(1,Start)*1000000 + element(2,Start))*1000000) + element(3,Start)),
                        EndMs = (((element(1,End)*1000000 + element(2,End))*1000000) + element(3,End)),
                        ExecTime = (EndMs - StartMs) / 1000,
                        RowPerS = RowCount / ExecTime,
                        {Op, ExecTime, RowPerS}
                    end || {Op,Start,End} <- TableStats],
            OpsHeader = lists:flatten([io_lib:format("~*s ", [10,atom_to_list(Op)]) || {Op, _, _} <- FormatStats]),
            ?Log("~s ~s rows ~p~n", [Tab, OpsHeader, RowCount]),
            Times = lists:flatten([io_lib:format("~*.*f ", [10,4,ExecTime]) || {_, ExecTime, _} <- FormatStats]),
            Rates = lists:flatten([io_lib:format("~*.*f ", [10,5,RowPerS]) || {_, _, RowPerS} <- FormatStats]),
            ?Log("  Time(s):     ~s~n", [Times]),
            ?Log("Rates(/s):     ~s~n", [Rates])
        end || {Tab, TableStats} <- Stats]
    catch
        _:Error ->
            ?Log("Error printing stats: ~p~n", [Error])
    end.

terminate(Reason, #state{session = Session, port = OciPort}) ->
    if Session =/= undefined -> Session:close(); true -> ok end,
    if OciPort =/= undefined -> OciPort:close(); true -> ok end,
    ?Log("Shutdown for ~p", [Reason]).
    
code_change(OldVsn, State, Extra) ->
    ?Log("Code change old vsn ~p Extra ~p", [OldVsn, Extra]),
    {ok, State}.

setup() ->
    application:start(erloci),
    OciPort = oci_port:start_link([{logging, true}]),
    {ok, {Tns,User,Pswd}} = application:get_env(erloci, default_connect_param),
    {OciPort, setupi(OciPort,Tns,User,Pswd)}.

setupi(OciPort,Tns,User,Pswd) ->
    OciSession = OciPort:get_session(Tns, User, Pswd),
    throw_if_error(undefined, "", OciSession, "session failed"),
    ?Log("[OCI Session] ~p~n", [OciSession]),
    OciSession.

teardown({OciPort, OciSession}) ->
    OciSession:close(),
    OciPort:close(),
    application:stop(erloci).

-define(RLog(__Target, __Fmt, __Vars), gen_server:cast(__Target, {log, lists:flatten(io_lib:format(__Fmt, __Vars))})).
run_test(OciSession, Table, RowCount, Master) ->
    ?RLog(Master, "[~s,~p] start >>>>~n", [Table,self()]),

    % Drop
    StartDrop = erlang:now(),
    StmtDrop = OciSession:prep_sql(list_to_binary(["drop table ",Table])),
    print_if_error(Master, Table, StmtDrop, "drop prep for "++Table++" failed"),
    print_if_error(Master, Table, StmtDrop:exec_stmt(), "drop exec for "++Table++" failed"),
    print_if_error(Master, Table, StmtDrop:close(), "drop close for "++Table++" failed"),
    EndDrop = erlang:now(),
    ok = gen_server:cast(Master, {drop, Table, StartDrop, EndDrop}),
    ?RLog(Master, "[~s] drop~n", [Table]),

    % Create
    StartCreate = erlang:now(),
    StmtCreate = OciSession:prep_sql(list_to_binary(["create table ",Table,"(pkey number,
                                   publisher varchar2(30),
                                   rank float,
                                   hero varchar2(30),
                                   reality varchar2(30),
                                   votes number,
                                   createdate date default sysdate,
                                   votes_first_rank number)"])),
    throw_if_error(Master, Table, StmtCreate, "create pref for "++Table++" failed"),
    throw_if_error(Master, Table, StmtCreate:exec_stmt(), "create exec for "++Table++" failed"),
    throw_if_error(Master, Table, StmtCreate:close(), "create close for "++Table++" failed"),
    EndCreate = erlang:now(),
    ok = gen_server:cast(Master, {create, Table, StartCreate, EndCreate}),
    ?RLog(Master, "[~s] create~n", [Table]),

    % Insert
    InsertRows = [{ I
                  , list_to_binary(["_publisher_",integer_to_list(I),"_"])
                  , I+I/2
                  , list_to_binary(["_hero_",integer_to_list(I),"_"])
                  , list_to_binary(["_reality_",integer_to_list(I),"_"])
                  , I
                  , oci_util:edatetime_to_ora(erlang:now())
                  , I
                  } || I <- lists:seq(1, RowCount)],
    StartInsert = erlang:now(),
    StmtInsert = OciSession:prep_sql(list_to_binary([
                "insert into ", Table,
                " (pkey,publisher,rank,hero,reality,votes,createdate,votes_first_rank)",
                " values (",
                "  :pkey",
                ", :publisher",
                ", :rank",
                ", :hero",
                ", :reality",
                ", :votes",
                ", :createdate"
                ", :votes_first_rank)"])),
    throw_if_error(Master, Table, StmtInsert, "insert prep for "++Table++" failed"),
    throw_if_error(Master, Table, StmtInsert:bind_vars([ {<<":pkey">>, 'SQLT_INT'}
                                                    , {<<":publisher">>, 'SQLT_CHR'}
                                                    , {<<":rank">>, 'SQLT_FLT'}
                                                    , {<<":hero">>, 'SQLT_CHR'}
                                                    , {<<":reality">>, 'SQLT_CHR'}
                                                    , {<<":votes">>, 'SQLT_INT'}
                                                    , {<<":createdate">>, 'SQLT_DAT'}
                                                    , {<<":votes_first_rank">>, 'SQLT_INT'}
                                                    ])
                            , "insert bind for "++Table++" failed"),
    InsertResult = StmtInsert:exec_stmt(InsertRows),
    throw_if_error(Master, Table, InsertResult, "insert exec for "++Table++" failed"),
    throw_if_error(Master, Table, StmtInsert:close(), "insert close for "++Table++" failed"),
    EndInsert = erlang:now(),
    ok = gen_server:cast(Master, {insert, Table, StartInsert, EndInsert}),
    ?RLog(Master, "[~s] insert~n", [Table]),
    
    % Update
    {rowids, AllKeys} = InsertResult,
    UpdateRows = [{ I
                  , list_to_binary(["_Publisher_",integer_to_list(I),"_"])
                  , I+I/3
                  , list_to_binary(["_Hero_",integer_to_list(I),"_"])
                  , list_to_binary(["_Reality_",integer_to_list(I),"_"])
                  , I+1
                  , oci_util:edatetime_to_ora(erlang:now())
                  , I+1
                  , Key
                  } || {Key, I} <- lists:zip(AllKeys, lists:seq(1, length(AllKeys)))],
    StartUpdate = erlang:now(),
    StmtUpdate = OciSession:prep_sql(list_to_binary([
                "update ", Table, " set ",
                "pkey = :pkey",
                ", publisher = :publisher",
                ", rank = :rank",
                ", hero = :hero",
                ", reality = :reality",
                ", votes = :votes",
                ", createdate = :createdate"
                ", votes_first_rank = :votes_first_rank where ", Table, ".rowid = :pri_rowid1"])),
    throw_if_error(Master, Table, StmtUpdate, "update prep for "++Table++" failed"),
    throw_if_error(Master, Table, StmtUpdate:bind_vars([ {<<":pkey">>, 'SQLT_INT'}
                                                    , {<<":publisher">>, 'SQLT_CHR'}
                                                    , {<<":rank">>, 'SQLT_FLT'}
                                                    , {<<":hero">>, 'SQLT_CHR'}
                                                    , {<<":reality">>, 'SQLT_CHR'}
                                                    , {<<":votes">>, 'SQLT_INT'}
                                                    , {<<":createdate">>, 'SQLT_DAT'}
                                                    , {<<":votes_first_rank">>, 'SQLT_INT'}
                                                    , {<<":pri_rowid1">>, 'SQLT_STR'}
                                                    ])
                            , "update bind for "++Table++" failed"),
    UpdateResult = StmtUpdate:exec_stmt(UpdateRows),
    throw_if_error(Master, Table, UpdateResult, "update exec for "++Table++" failed"),
    throw_if_error(Master, Table, StmtUpdate:close(), "update close for "++Table++" failed"),
    EndUpdate = erlang:now(),
    ok = gen_server:cast(Master, {update, Table, StartUpdate, EndUpdate}),
    ?RLog(Master, "[~s] update~n", [Table]),

    % Select
    StartSelect = erlang:now(),
    StmtSelect = OciSession:prep_sql(list_to_binary(["select ",Table,".rowid, ",Table,".* from ", Table])),
    throw_if_error(Master, Table, StmtSelect, "select prep for "++Table++" failed"),
    throw_if_error(Master, Table, StmtSelect:exec_stmt(), "select exec for "++Table++" failed"),
    fetch_all_rows(Master, Table, StmtSelect, RowCount),
    EndSelect = erlang:now(),
    ok = gen_server:cast(Master, {select, Table, StartSelect, EndSelect}),

    ?RLog(Master, "[~s] select~n", [Table]),
    %?RLog(Master, "[~s] end <<<<~n", [Table]),
    ok.

fetch_all_rows(Master, Table, Statement, RowCount) -> fetch_all_rows(Master, Table, Statement, RowCount, []).
fetch_all_rows(Master, Table, Statement, RowCount, Rows) ->
    {{rows, NewRows}, Finished} = Statement:fetch_rows(RowCount),
    case Finished of
        false -> fetch_all_rows(Master, Table, Statement, RowCount, Rows++NewRows);
        true ->
            throw_if_error(Master, Table, Statement:close(), "select close for "++Table++" failed"),
            Rows++NewRows
    end.

throw_if_error(Parent,Table,{error,Error},Msg) ->
    if is_pid(Parent) ->
        ?RLog(Parent, "[~s] error "++Msg++": ~p~n", [Table, Error]);
        true -> ok
    end,
    throw({Msg, Error});
throw_if_error(_,_,_,_) -> ok.

print_if_error(Parent,Table,{error, Error},Msg) ->
    if is_pid(Parent) ->
        %?RLog(Parent, "[~s] warning "++Msg++": ~p~n", [Table, Error]);
        ?Log("[~s] warning "++Msg++": ~p~n", [Table, Error]);
        true -> ok
    end;
print_if_error(_,_,_,_) -> ok.

bigtable_test(RowCount) ->
    ?ELog("------------------------------------------------------------------"),
    ?ELog("|                       bigtable_test                            |"),
    ?ELog("------------------------------------------------------------------"),
    {OciPort, OciSession} = oci_test:bigtab_setup(),
    ok = oci_test:bigtab_load(OciSession, RowCount),
    oci_test:bigtab_access(OciSession),
    ok = OciSession:cose(),
    ok = OciPort:close().

bigtab_setup() ->
    ?ELog("Creating ~p", [?TESTTABLE]),
    {OciPort, OciSession} = setup(),
    DropStmt = OciSession:prep_sql(?DROP),
    {oci_port, statement, _, _, _} = DropStmt,
    case DropStmt:exec_stmt() of
        {error, _} -> ok; 
        _ -> ok = DropStmt:close()
    end,
    ?ELog("creating table ~s", [?TESTTABLE]),
    StmtCreate = OciSession:prep_sql(?CREATE),
    {oci_port, statement, _, _, _} = StmtCreate,
    {executed, 0} = StmtCreate:exec_stmt(),
    ok = StmtCreate:close(),
    {OciPort, OciSession}.

bigtab_load(OciSession, RowCount) ->
    ?ELog("Loading ~p with ~p rows", [?TESTTABLE, RowCount]),
    ?ELog("inserting into table ~s", [?TESTTABLE]),
    BoundInsStmt = OciSession:prep_sql(?INSERT),
    {oci_port, statement, _, _, _} = BoundInsStmt,
    BoundInsStmtRes = BoundInsStmt:bind_vars(?BIND_LIST),
    ok = BoundInsStmtRes,
    {rowids, RowIds} = BoundInsStmt:exec_stmt(
        [{ (fun() -> io:format(user, ".", []), I end)()
         , list_to_binary(["_publisher_",integer_to_list(I),"_"])
         , I+I/2
         , list_to_binary(["_hero_",integer_to_list(I),"_"])
         , list_to_binary(["_reality_",integer_to_list(I),"_"])
         , I
         , oci_util:edatetime_to_ora(erlang:now())
         , I
         } || I <- lists:seq(1, RowCount)]),
    ?ELog("", []),
    ?ELog("inserted into table ~s ~p rows", [?TESTTABLE, length(RowIds)]),
    RowCount = length(RowIds),
    ok = BoundInsStmt:close(),
    ok.

bigtab_access(OciSession) ->
    ?ELog("selecting from table ~s", [?TESTTABLE]),
    SelStmt = OciSession:prep_sql(?SELECT_WITH_ROWID),
    {oci_port, statement, _, _, _} = SelStmt,
    {cols, Cols} = SelStmt:exec_stmt(),
    ?ELog("selected columns ~p from table ~s", [Cols, ?TESTTABLE]),
    10 = length(Cols),
    load_rows_to_end(SelStmt:fetch_rows(100), SelStmt),
    ok = SelStmt:close(),
    ok.

load_rows_to_end({{rows, Rows}, true}, _) -> ?ELog("Loaded ~p rows - Finished", [length(Rows)]);
load_rows_to_end({{rows, Rows}, false}, SelStmt) ->
    ?ELog("Loaded ~p rows", [length(Rows)]),
    load_rows_to_end(SelStmt:fetch_rows(100), SelStmt).

%-endif.
