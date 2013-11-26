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

-export([ start/2
        , setup/0
        , teardown/1]).

%gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3]).

-define(TablePrefix, "erloci_table_").
-define(NowMs, (fun() -> {M,S,Ms} = erlang:now(), ((M*1000000 + S)*1000000) + Ms end)()).
-define(Log(__Fmt,__Args),
(fun(__F,__A) ->
    {{__Y,__M,__D},{__H,__Min,__S}} = calendar:now_to_datetime(erlang:now()),
    io:format(user, "~4..0B-~2..0B-~2..0B ~2..0B:~2..0B:~2..0B ~p "++__F, [__Y,__M,__D,__H,__Min,__S,{?MODULE, ?LINE} | __A])
end)(__Fmt,__Args)).
-define(Log(__F), ?Log(__F,[])).

-record(state,  { port
                , session
                , stats = []
       }).

start(Threads, InsertCount) ->
    application:start(erloci),
    {ok, {Tns,User,Pswd}} = application:get_env(erloci, default_connect_param),
    start(Tns,User,Pswd,Threads,InsertCount).

start(Tns,User,Pswd,Threads,InsertCount) ->
    application:start(erloci),
    gen_server:start_link(?MODULE, [Tns,User,Pswd,Threads,InsertCount], []).

init([Tns,User,Pswd]) ->
    OciPort = oci_port:start_link([{logging, true}]),
    Session = setupi(OciPort,Tns,User,Pswd),
    {ok, #state{port=OciPort, session=Session}}.

%run(Threads, InsertCount) ->
%    OciSession = setup(),
%    run_test(OciSession, Threads, InsertCount),
%    teardown(OciSession).

handle_call({create_tables, -1}, From, State) ->
    {reply,ok,State};
handle_call({create_tables, N}, From, {session = OciSession} = State) ->
    Table = lists:flatten([?TablePrefix, integer_to_list(N)]),
    try
        StartCreate = erlang:now(),
        StmtDrop = OciSession:prep_sql(list_to_binary(["drop table ",Table])),
        print_if_error(StmtDrop, "drop prep failed"),
        print_if_error(StmtDrop:exec_stmt(), "drop exec failed"),
        print_if_error(StmtDrop:close(), "drop close failed"),
        Stmt0 = OciSession:prep_sql(list_to_binary(["create table ",Table,"(pkey number,
                                       publisher varchar2(30),
                                       rank float,
                                       hero varchar2(30),
                                       reality varchar2(30),
                                       votes number,
                                       createdate date default sysdate,
                                       votes_first_rank number)"])),
        throw_if_error(undefined, Stmt0, "create "++Table++" prep failed"),
        throw_if_error(undefined, Stmt0:exec_stmt(), "create "++Table++" exec failed"),
        throw_if_error(undefined, Stmt0:close(), "close statement for "++Table++" failed"),
        EndCreate = erlang:now(),
        gen_server:cast(self(), {create, Table, {StartCreate, EndCreate}}),
        handle_call({create_tables, N-1}, From, State)
    catch
        _:Error -> ?Log("[OCI drop ~s] failed ~p~n", [Table, Error]),
        {reply, {error, Error}, State}
    end;
handle_call(Msg, From, State) ->
    {stop,normal,{unknown_call, Msg, From},State}.

handle_cast(Request, State) ->
    ?Log("Unknown cast ~p from~p", [Request]),
    {stop, normal, State}.

handle_info(Info, State) ->
    ?Log("Unknown info ~p from~p", [Info]),
    {stop, normal, State}.

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
    setupi(OciPort,Tns,User,Pswd).

setupi(OciPort,Tns,User,Pswd) ->
    OciSession = OciPort:get_session(Tns, User, Pswd),
    throw_if_error(undefined, OciSession, "session failed"),
    ?Log("[OCI Session] ~p~n", [OciSession]),
    OciSession.

teardown(_OciSession) ->
    application:stop(erloci).

run_test(OciSession, Threads, InsertCount) ->
    This = self(),
    [(fun(Idx) ->
        Table = "erloci_table_"++Idx,
        try
            create_table(OciSession, Table),            
            spawn(fun() -> insert_select(OciSession, Table, InsertCount, This) end)
        catch
            Class:Reason -> ?Log("~p:~p~n", [Class,Reason])
        end
    end)(integer_to_list(I))
    || I <- lists:seq(1,Threads)],
    receive_all(OciSession, Threads).

throw_if_error(Parent, {error, Error}, Msg) ->
    if is_pid(Parent) -> Parent ! {{Msg, Error}, 0, 1, 0, 1, 1}; true -> ok end,
    throw({Msg, Error});
throw_if_error(_,_,_) -> ok.

create_table(OciSession, Table) ->
    try
        Stmt = OciSession:prep_sql(list_to_binary(["drop table ",Table])),
        print_if_error(Stmt, "drop prep failed"),
        Res0 = Stmt:exec_stmt(),
        print_if_error(Res0, "drop exec failed"),
        Stmt:close(),
        ?Log("[OCI drop ~s] ~p~n", [Table, Res0])
    catch
        _:_ -> ok % errors due to table doesn't exists bypassed
    end,
    Stmt0 = OciSession:prep_sql(list_to_binary(["create table ",Table,"(pkey number,
                                       publisher varchar2(30),
                                       rank float,
                                       hero varchar2(30),
                                       reality varchar2(30),
                                       votes number,
                                       createdate date default sysdate,
                                       votes_first_rank number)"])),
%                                       createtime timestamp default systimestamp,
    throw_if_error(undefined, Stmt0, "create "++Table++" prep failed"),
    Res = Stmt0:exec_stmt(),
    throw_if_error(undefined, Res, "create "++Table++" exec failed"),
    Stmt0:close(),
    ?Log("[OCI create ~s] ~p~n", [Table, Res]).

receive_all(OciSession, Count) -> receive_all(OciSession, Count, []).
receive_all(OciSession, 0, Acc) ->
    OciSession:close(),
    [(fun(Table, InsertCount, InsertTime, SelectCount, SelectTime, UpdateTime) ->
        InsRate = erlang:trunc(InsertCount / InsertTime),
        SelRate = erlang:trunc(SelectCount / SelectTime),
        UdpRate = erlang:trunc(SelectCount / UpdateTime),
        io:format(user, "~p insert ~p(~p sec) ~p rows/sec;"
                        " select ~p(~p sec) ~p rows/sec;"
                        " update ~p(~p sec) sec ~p rows/sec~n", [Table, InsertCount, InsertTime, InsRate
                                                                      , SelectCount, SelectTime, SelRate
                                                                      , SelectCount, UpdateTime, UdpRate])
    end)(T, Ic, It, Sc, St, Ut)
    || {T, Ic, It, Sc, St, Ut} <- Acc];
receive_all(OciSession, Count, Acc) ->
    receive
        {T, Ic, It, Sc, St, Ut} ->
            receive_all(OciSession, Count-1, [{T, Ic, It, Sc, St, Ut}|Acc]);
        {keep_alive, Op, Table} ->
            io:format(user, ">>>> ~p on table ~s~n", [Op, Table]),
            receive_all(OciSession, Count, Acc)
    after
        (100*1000) ->
            io:format(user, ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> ~p~n", [timeout]),
            receive_all(OciSession, 0, Acc)
    end.

insert_select(OciSession, Table, InsertCount, Parent) ->
    try
        Parent ! {keep_alive, insert_qry_prepare, Table},
        InsertStart = ?NowMs,
        BindInsQry = erlang:list_to_binary([
                "insert into ", Table, " (pkey,publisher,rank,hero,reality,votes,createdate,votes_first_rank) values (",
                ":pkey",
                ", :publisher",
                ", :rank",
                ", :hero",
                ", :reality",
                ", :votes",
                ", :createdate"
                ", :votes_first_rank)"]),
        ?Log("_[~p]_ ~p~n", [Table,BindInsQry]),
        BoundInsStmt = OciSession:prep_sql(BindInsQry),
        Parent ! {keep_alive, insert_qry_preped, Table},
        throw_if_error(Parent, BoundInsStmt, binary_to_list(BindInsQry)),
        Res0 = BoundInsStmt:bind_vars([ {<<":pkey">>, 'SQLT_INT'}
                            , {<<":publisher">>, 'SQLT_CHR'}
                            , {<<":rank">>, 'SQLT_FLT'}
                            , {<<":hero">>, 'SQLT_CHR'}
                            , {<<":reality">>, 'SQLT_CHR'}
                            , {<<":votes">>, 'SQLT_INT'}
                            , {<<":createdate">>, 'SQLT_DAT'}
                            , {<<":votes_first_rank">>, 'SQLT_INT'}
                            ]),
        throw_if_error(Parent, Res0, "bind_vars_insert"),
        Parent ! {keep_alive, insert_qry_var_bound, Table},
        Res1 = BoundInsStmt:exec_stmt([{ I
                            , list_to_binary(["_publisher_",integer_to_list(I),"_"])
                            , I+I/2
                            , list_to_binary(["_hero_",integer_to_list(I),"_"])
                            , list_to_binary(["_reality_",integer_to_list(I),"_"])
                            , I
                            , oci_util:edatetime_to_ora(erlang:now())
                            , I
                            } || I <- lists:seq(1, InsertCount)]),
        Parent ! {keep_alive, insert_qry_executed, Table},
        case Res1 of
            {error, ErrorIns} -> ?Log("[OCI insert ~s] ~p~n", [Table, ErrorIns]);
            _ -> ok
        end,
        BoundInsStmt:close(),
        Parent ! {keep_alive, insert_qry_closed, Table},
        InsertEnd = ?NowMs,
        Parent ! {keep_alive, select_qry_prepare, Table},
        Statement = OciSession:prep_sql(list_to_binary(["select ",Table,".rowid, ",Table,".* from ", Table])),
        Parent ! {keep_alive, select_qry_prepared, Table},
        throw_if_error(Parent, Statement, "select "++Table++" prep failed"),
        Cols = Statement:exec_stmt(),
        Parent ! {keep_alive, select_qry_exec, Table},
        throw_if_error(Parent, Cols, "select "++Table++" exec failed"),
        ?Log("_[~p]_ columns~n~p~n", [Table,Cols]),
        F = fun(Self,Rows) ->
            {{rows, NewRows}, Finished} = Statement:fetch_rows(100),
            %?Log("...[~p]... OCI select rows ~p finished ~p~n", [Table,length(Rows)+length(NewRows), Finished]),
            case Finished of
                false -> Self(Self,Rows++NewRows);
                true ->
                    Parent ! {keep_alive, select_qry_closed, Table},
                    Statement:close(),
                    Rows++NewRows
            end
        end,
        Parent ! {keep_alive, select_qry_fetch, Table},
        AllRows = F(F,[]),
        Parent ! {keep_alive, select_qry_fetch_over, Table},
        SelectEnd = ?NowMs,
        AllKeys = [lists:last(R) || R <- AllRows],
        UpdateStart = ?NowMs,
        Parent ! {keep_alive, update_qry_prepare, Table},
        BindUpdQry = erlang:list_to_binary([
                "update ", Table, " set ",
                "pkey = :pkey",
                ", publisher = :publisher",
                ", rank = :rank",
                ", hero = :hero",
                ", reality = :reality",
                ", votes = :votes",
                ", createdate = :createdate"
                ", votes_first_rank = :votes_first_rank where ", Table, ".rowid = :pri_rowid1"]),
        ?Log("_[~p]_ ~p~n", [Table,BindUpdQry]),
        BoundUpdStmt = OciSession:prep_sql(BindUpdQry),
        Parent ! {keep_alive, update_qry_prepared, Table},
        throw_if_error(Parent, BoundUpdStmt, binary_to_list(BindUpdQry)),
        Parent ! {keep_alive, update_qry_var_bind, Table},
        ResUdpBind = BoundUpdStmt:bind_vars([
                              {<<":pkey">>, 'SQLT_INT'}
                            , {<<":publisher">>, 'SQLT_CHR'}
                            , {<<":rank">>, 'SQLT_FLT'}
                            , {<<":hero">>, 'SQLT_CHR'}
                            , {<<":reality">>, 'SQLT_CHR'}
                            , {<<":votes">>, 'SQLT_INT'}
                            , {<<":createdate">>, 'SQLT_DAT'}
                            , {<<":votes_first_rank">>, 'SQLT_INT'}
                            , {<<":pri_rowid1">>, 'SQLT_STR'}
                            ]),
        throw_if_error(Parent, ResUdpBind, "bind_vars_update"),
        Parent ! {keep_alive, update_qry_var_bound, Table},
        ResUdpBind1 = BoundUpdStmt:exec_stmt([{ I
                            , list_to_binary(["_Publisher_",integer_to_list(I),"_"])
                            , I+I/3
                            , list_to_binary(["_Hero_",integer_to_list(I),"_"])
                            , list_to_binary(["_Reality_",integer_to_list(I),"_"])
                            , I+1
                            , oci_util:edatetime_to_ora(erlang:now())
                            , I+1
                            , Key
                            } || {Key, I} <- lists:zip(AllKeys, lists:seq(1, length(AllKeys)))]),
        case ResUdpBind1 of
            {error, ErrorUpdate} -> ?Log("[OCI update ~s] ~p~n", [Table, ErrorUpdate]);
            _ -> ok
        end,
        Parent ! {keep_alive, update_qry_exec, Table},
        BoundUpdStmt:close(),
        Parent ! {keep_alive, update_qry_closed, Table},
        UpdateEnd = ?NowMs,
        TotalRowCount = length(AllRows),
        InsertTime = (InsertEnd - InsertStart)/1000000,
        SelectTime = (SelectEnd - InsertEnd)/1000000,
        UpdateTime = (UpdateEnd - UpdateStart)/1000000,
        Parent ! {Table, InsertCount, InsertTime, TotalRowCount, SelectTime, UpdateTime}
    catch
        Class:Reason ->
            if is_pid(Parent) -> Parent ! {{Table,Class,Reason}, 0, 1, 0, 1}; true -> ok end,
            ?Log(" ~p:~p~n", [Class,Reason])
    end.

print_if_error({error, Error}, Msg) -> ?Log("___________----- continue after ~p ~p~n", [Msg,Error]);
print_if_error(_, _) -> ok.
