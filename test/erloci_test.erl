-module(erloci_test).

-include_lib("eunit/include/eunit.hrl").
-define(PORT_MODULE, oci_port).

-define(ELog(__F), ?ELog(__F,[])).
-define(ELog(__Fmt,__Args),
(fun(__F,__A) ->
    {_,_,__McS} = __Now = erlang:now(),
    {_,{_,__Min,__S}} = calendar:now_to_datetime(__Now),
    ok = io:format(user, "~2..0B:~2..0B.~6..0B ~p "++__F++"~n", [__Min,__S,__McS rem 1000000,{?MODULE,?LINE} | __A])
end)(__Fmt,__Args)).

-define(TESTTABLE, "erloci_test_1").
-define(TESTFUNCTION, "ERLOCI_TEST_FUNCTION").
-define(DROP,   <<"drop table "?TESTTABLE>>).
-define(CREATE, <<"create table "?TESTTABLE" (pkey integer,"
                  "publisher varchar2(30),"
                  "rank float,"
                  "hero varchar2(30),"
                  "reality varchar2(30),"
                  "votes number(1,-10),"
                  "createdate date default sysdate,"
                  "chapters int,"
                  "votes_first_rank number)">>).
-define(INSERT, <<"insert into "?TESTTABLE
                  " (pkey,publisher,rank,hero,reality,votes,createdate,votes_first_rank) values ("
                  ":pkey"
                  ", :publisher"
                  ", :rank"
                  ", :hero"
                  ", :reality"
                  ", :votes"
                  ", :createdate"
                  ", :votes_first_rank)">>).
-define(SELECT_WITH_ROWID, <<"select "?TESTTABLE".rowid, "?TESTTABLE".* from "?TESTTABLE>>).
-define(SELECT_ROWID_ASC, <<"select rowid from ", ?TESTTABLE, " order by pkey">>).
-define(SELECT_ROWID_DESC, <<"select rowid from ", ?TESTTABLE, " order by pkey desc">>).
-define(BIND_LIST, [ {<<":pkey">>, 'SQLT_INT'}
                   , {<<":publisher">>, 'SQLT_CHR'}
                   , {<<":rank">>, 'SQLT_FLT'}
                   , {<<":hero">>, 'SQLT_CHR'}
                   , {<<":reality">>, 'SQLT_CHR'}
                   , {<<":votes">>, 'SQLT_INT'}
                   , {<<":createdate">>, 'SQLT_DAT'}
                   , {<<":votes_first_rank">>, 'SQLT_INT'}
                   ]).
-define(UPDATE, <<"update "?TESTTABLE" set "
                  "pkey = :pkey"
                  ", publisher = :publisher"
                  ", rank = :rank"
                  ", hero = :hero"
                  ", reality = :reality"
                  ", votes = :votes"
                  ", createdate = :createdate"
                  ", votes_first_rank = :votes_first_rank"
                  " where "?TESTTABLE".rowid = :pri_rowid1">>).
-define(UPDATE_BIND_LIST, [ {<<":pkey">>, 'SQLT_INT'}
                          , {<<":publisher">>, 'SQLT_CHR'}
                          , {<<":rank">>, 'SQLT_FLT'}
                          , {<<":hero">>, 'SQLT_CHR'}
                          , {<<":reality">>, 'SQLT_CHR'}
                          , {<<":votes">>, 'SQLT_STR'}
                          , {<<":createdate">>, 'SQLT_DAT'}
                          , {<<":votes_first_rank">>, 'SQLT_INT'}
                          , {<<":pri_rowid1">>, 'SQLT_STR'}
                          ]).

%%-----------------------------------------------------------------------------
%% db_negative_test_
%%-----------------------------------------------------------------------------
db_negative_test_() ->
    {timeout, 60, {
        setup,
        fun setup/0,
        fun teardown/1,
        {with, [
            fun echo/1,
            fun bad_password/1
        ]}
    }}.

setup() ->
    application:start(erloci),
    OciPort = oci_port:start_link([{logging, true}]),
    timer:sleep(1000),
    OciPort.

teardown(OciPort) ->
    OciPort:close(),
    application:stop(erloci).

bad_password(OciPort) ->
    ?ELog("+----------------------------------------------------------------+"),
    ?ELog("|                       bad_password                             |"),
    ?ELog("+----------------------------------------------------------------+"),
    ?ELog("get_session with wrong password", []),
    {ok, {Tns,User,Pswd}} = application:get_env(erloci, default_connect_param),
    ?assertMatch({error, {1017,_}}, OciPort:get_session(Tns, User, list_to_binary([Pswd,"_bad"]))).

echo(OciPort) ->
    ?ELog("+----------------------------------------------------------------+"),
    ?ELog("|                           echo                                 |"),
    ?ELog("+----------------------------------------------------------------+"),
    ?ELog("echo back erlang terms", []),
    ?assertEqual(1, OciPort:echo(1)),
    ?assertEqual(1.2, OciPort:echo(1.2)),
    ?assertEqual(atom, OciPort:echo(atom)),
    ?assertEqual(self(), OciPort:echo(self())),
    ?assertEqual(node(), OciPort:echo(node())),
    Ref = make_ref(),
    ?assertEqual(Ref, OciPort:echo(Ref)),
    % Load the ref cache to generate long ref
    _Refs = [make_ref() || _I <- lists:seq(1,1000000)],    
    Ref1 = make_ref(),
    ?assertEqual(Ref1, OciPort:echo(Ref1)),
    %Fun = fun() -> ok end, % Not Supported
    %?assertEqual(Fun, OciPort:echo(Fun)),
    ?assertEqual("", OciPort:echo("")),
    ?assertEqual(<<>>, OciPort:echo(<<>>)),
    ?assertEqual([], OciPort:echo([])),
    ?assertEqual({}, OciPort:echo({})),
    ?assertEqual("string", OciPort:echo("string")),
    ?assertEqual(<<"binary">>, OciPort:echo(<<"binary">>)),
    ?assertEqual({1,'Atom',1.2,"string"}, OciPort:echo({1,'Atom',1.2,"string"})),
    ?assertEqual([1, atom, 1.2,"string"], OciPort:echo([1,atom,1.2,"string"])).

%%-----------------------------------------------------------------------------
%% db_test_
%%-----------------------------------------------------------------------------
db_test_() ->
    {timeout, 60, {
        setup,
        fun setup_conn/0,
        fun teardown_conn/1,
        {with, [
            fun drop_create/1
            , fun insert_select_update/1
            , fun auto_rollback_test/1
            , fun commit_rollback_test/1
            , fun asc_desc_test/1
            , fun describe_test/1
            , fun function_test/1
            , fun lob_test/1
        ]}
    }}.

setup_conn() ->
    application:start(erloci),
    OciPort = oci_port:start_link([{logging, true}]),
    {ok, {Tns,User,Pswd}} = application:get_env(erloci, default_connect_param),
    OciSession = OciPort:get_session(Tns, User, Pswd),
    {OciPort, OciSession}.

teardown_conn({OciPort, OciSession}) ->
    DropStmt = OciSession:prep_sql(?DROP),
    DropStmt:exec_stmt(),
    DropStmt:close(),
    OciSession:close(),
    OciPort:close(),
    application:stop(erloci).



flush_table(OciSession) ->
    ?ELog("creating (drop if exists) table ~s", [?TESTTABLE]),
    DropStmt = OciSession:prep_sql(?DROP),
    ?assertMatch({?PORT_MODULE, statement, _, _, _}, DropStmt),
    % If table doesn't exists the handle isn't valid
    % Any error is ignored anyway
    case DropStmt:exec_stmt() of
        {error, _} -> ok; 
        _ -> ?assertEqual(ok, DropStmt:close())
    end,
    ?ELog("creating table ~s", [?TESTTABLE]),
    StmtCreate = OciSession:prep_sql(?CREATE),
    ?assertMatch({?PORT_MODULE, statement, _, _, _}, StmtCreate),
    ?assertEqual({executed, 0}, StmtCreate:exec_stmt()),
    ?assertEqual(ok, StmtCreate:close()).

lob_test({_, OciSession}) ->
    ?ELog("+----------------------------------------------------------------+"),
    ?ELog("|                              lob_test                          |"),
    ?ELog("+----------------------------------------------------------------+"),

    RowCount = 5,
    
    StmtCreate = OciSession:prep_sql(<<"create table lobs(clobd clob, blobd blob, nclobd nclob)">>),
    ?assertMatch({?PORT_MODULE, statement, _, _, _}, StmtCreate),
    case StmtCreate:exec_stmt() of
        {executed, 0} ->
            ?ELog("creating table lobs", []),
            ?assertEqual(ok, StmtCreate:close());
        _ ->
            StmtTruncate = OciSession:prep_sql(<<"truncate table lobs">>),
            ?assertMatch({?PORT_MODULE, statement, _, _, _}, StmtTruncate),
            ?assertEqual({executed, 0}, StmtTruncate:exec_stmt()),
            ?ELog("truncated table lobs", []),
            ?assertEqual(ok, StmtTruncate:close())
    end,

    [begin
        StmtInsert = OciSession:prep_sql(list_to_binary(["insert into lobs values("
            "to_clob('clobd0'),"
            "hextoraw('453d7a30'),"
            "to_nclob('nclobd0'))"])),
        ?assertMatch({?PORT_MODULE, statement, _, _, _}, StmtInsert),
        ?assertMatch({rowids, [_]}, StmtInsert:exec_stmt()),
        ?assertEqual(ok, StmtInsert:close())
     end
     || _R <- lists:seq(1,RowCount)],
    ?ELog("inserted ~p rows into lobs", [RowCount]),

    StmtSelect = OciSession:prep_sql(<<"select * from lobs">>),
    ?assertMatch({?PORT_MODULE, statement, _, _, _}, StmtSelect),
    ?assertMatch({cols, _}, StmtSelect:exec_stmt()),
    {{rows, Rows}, true} = StmtSelect:fetch_rows(RowCount+1),
    ?assertEqual(RowCount, length(Rows)),
    ?ELog("rows from lobs ~p", [Rows]),

    ?assertEqual(ok, StmtSelect:close()),

    StmtDrop = OciSession:prep_sql(<<"drop table lobs">>),
    ?assertMatch({?PORT_MODULE, statement, _, _, _}, StmtDrop),
    ?assertEqual({executed, 0}, StmtDrop:exec_stmt()),
    ?assertEqual(ok, StmtDrop:close()).

drop_create({_, OciSession}) ->
    ?ELog("+----------------------------------------------------------------+"),
    ?ELog("|                            drop_create                         |"),
    ?ELog("+----------------------------------------------------------------+"),

    ?ELog("creating (drop if exists) table ~s", [?TESTTABLE]),
    TmpDropStmt = OciSession:prep_sql(?DROP),
    ?assertMatch({?PORT_MODULE, statement, _, _, _}, TmpDropStmt),
    case TmpDropStmt:exec_stmt() of
        {error, _} -> ok; % If table doesn't exists the handle isn't valid
        _ -> ?assertEqual(ok, TmpDropStmt:close())
    end,
    StmtCreate = OciSession:prep_sql(?CREATE),
    ?assertMatch({?PORT_MODULE, statement, _, _, _}, StmtCreate),
    ?assertEqual({executed, 0}, StmtCreate:exec_stmt()),
    ?assertEqual(ok, StmtCreate:close()),

    ?ELog("dropping table ~s", [?TESTTABLE]),
    DropStmt = OciSession:prep_sql(?DROP),
    ?assertMatch({?PORT_MODULE, statement, _, _, _}, DropStmt),
    ?assertEqual({executed,0}, DropStmt:exec_stmt()),
    ?assertEqual(ok, DropStmt:close()).

insert_select_update({_, OciSession}) ->
    ?ELog("+----------------------------------------------------------------+"),
    ?ELog("|                      insert_select_update                      |"),
    ?ELog("+----------------------------------------------------------------+"),
    RowCount = 5,

    flush_table(OciSession),

    ?ELog("~s", [binary_to_list(?INSERT)]),
    BoundInsStmt = OciSession:prep_sql(?INSERT),
    ?assertMatch({?PORT_MODULE, statement, _, _, _}, BoundInsStmt),
    BoundInsStmtRes = BoundInsStmt:bind_vars(?BIND_LIST),
    ?assertMatch(ok, BoundInsStmtRes),
    {rowids, RowIds} = BoundInsStmt:exec_stmt(
        [{ I
         , list_to_binary(["_publisher_",integer_to_list(I),"_"])
         , I+I/2
         , list_to_binary(["_hero_",integer_to_list(I),"_"])
         , list_to_binary(["_reality_",integer_to_list(I),"_"])
         , I
         , oci_util:edatetime_to_ora(erlang:now())
         , I
         } || I <- lists:seq(1, RowCount)]),
    ?assertMatch(RowCount, length(RowIds)),
    ?assertEqual(ok, BoundInsStmt:close()),

    ?ELog("~s", [binary_to_list(?SELECT_WITH_ROWID)]),
    SelStmt = OciSession:prep_sql(?SELECT_WITH_ROWID),
    ?assertMatch({?PORT_MODULE, statement, _, _, _}, SelStmt),
    {cols, Cols} = SelStmt:exec_stmt(),
    ?ELog("selected columns ~p from table ~s", [Cols, ?TESTTABLE]),
    ?assertEqual(10, length(Cols)),
    {{rows, Rows0}, false} = SelStmt:fetch_rows(2),
    {{rows, Rows1}, false} = SelStmt:fetch_rows(2),
    {{rows, Rows2}, true} = SelStmt:fetch_rows(2),
    ?assertEqual(ok, SelStmt:close()),

    Rows = Rows0 ++ Rows1 ++ Rows2,

    %?ELog("Got rows~n~p", [
    %    [
    %        begin
    %        [Rowid
    %        , Pkey
    %        , Publisher
    %        , Rank
    %        , Hero
    %        , Reality
    %        , Votes
    %        , Createdate
    %        , Chapters
    %        , Votes_first_rank] = R,
    %        [Rowid
    %        , oci_util:oranumber_decode(Pkey)
    %        , Publisher
    %        , oci_util:oranumber_decode(Rank)
    %        , Hero
    %        , Reality
    %        , oci_util:oranumber_decode(Votes)
    %        , oci_util:oradate_to_str(Createdate)
    %        , oci_util:oranumber_decode(Chapters)
    %        , oci_util:oranumber_decode(Votes_first_rank)]
    %        end
    %    || R <- Rows]
    %]),
    RowIDs = [R || [R|_] <- Rows],

    ?ELog("RowIds ~p", [RowIds]),
    ?ELog("~s", [binary_to_list(?UPDATE)]),
    BoundUpdStmt = OciSession:prep_sql(?UPDATE),
    ?assertMatch({?PORT_MODULE, statement, _, _, _}, BoundUpdStmt),
    BoundUpdStmtRes = BoundUpdStmt:bind_vars(lists:keyreplace(<<":votes">>, 1, ?UPDATE_BIND_LIST, {<<":votes">>, 'SQLT_INT'})),
    ?assertMatch(ok, BoundUpdStmtRes),
    ?assertMatch({rowids, _}, BoundUpdStmt:exec_stmt([{ I
                            , list_to_binary(["_Publisher_",integer_to_list(I),"_"])
                            , I+I/3
                            , list_to_binary(["_Hero_",integer_to_list(I),"_"])
                            , <<>> % deleting
                            , I+1
                            , oci_util:edatetime_to_ora(erlang:now())
                            , I+1
                            , Key
                            } || {Key, I} <- lists:zip(RowIDs, lists:seq(1, length(RowIDs)))])),
    ?assertEqual(ok, BoundUpdStmt:close()).

auto_rollback_test({_, OciSession}) ->
    ?ELog("+----------------------------------------------------------------+"),
    ?ELog("|                      auto_rollback_test                        |"),
    ?ELog("+----------------------------------------------------------------+"),
    RowCount = 3,

    flush_table(OciSession),

    ?ELog("inserting into table ~s", [?TESTTABLE]),
    BoundInsStmt = OciSession:prep_sql(?INSERT),
    ?assertMatch({?PORT_MODULE, statement, _, _, _}, BoundInsStmt),
    BoundInsStmtRes = BoundInsStmt:bind_vars(?BIND_LIST),
    ?assertMatch(ok, BoundInsStmtRes),
    ?assertMatch({rowids, _},
    BoundInsStmt:exec_stmt([{ I
            , list_to_binary(["_publisher_",integer_to_list(I),"_"])
            , I+I/2
            , list_to_binary(["_hero_",integer_to_list(I),"_"])
            , list_to_binary(["_reality_",integer_to_list(I),"_"])
            , I
            , oci_util:edatetime_to_ora(erlang:now())
            , I
            } || I <- lists:seq(1, RowCount)], 1)),
    ?assertEqual(ok, BoundInsStmt:close()),

    ?ELog("selecting from table ~s", [?TESTTABLE]),
    SelStmt = OciSession:prep_sql(?SELECT_WITH_ROWID),
    ?assertMatch({?PORT_MODULE, statement, _, _, _}, SelStmt),
    {cols, Cols} = SelStmt:exec_stmt(),
    ?assertEqual(10, length(Cols)),
    {{rows, Rows}, false} = SelStmt:fetch_rows(RowCount),
    ?assertEqual(ok, SelStmt:close()),

    ?ELog("update in table ~s", [?TESTTABLE]),
    RowIDs = [R || [R|_] <- Rows],
    BoundUpdStmt = OciSession:prep_sql(?UPDATE),
    ?assertMatch({?PORT_MODULE, statement, _, _, _}, BoundUpdStmt),
    BoundUpdStmtRes = BoundUpdStmt:bind_vars(?UPDATE_BIND_LIST),
    ?assertMatch(ok, BoundUpdStmtRes),
    % Expected Invalid number Error (1722)
    ?assertMatch({error,{1722,_}}, BoundUpdStmt:exec_stmt([{ I
                            , list_to_binary(["_Publisher_",integer_to_list(I),"_"])
                            , I+I/3
                            , list_to_binary(["_Hero_",integer_to_list(I),"_"])
                            , list_to_binary(["_Reality_",integer_to_list(I),"_"])
                            , if I > (RowCount-2) -> <<"error">>; true -> integer_to_binary(I+1) end
                            , oci_util:edatetime_to_ora(erlang:now())
                            , I+1
                            , Key
                            } || {Key, I} <- lists:zip(RowIDs, lists:seq(1, length(RowIDs)))], 1)),

    ?ELog("testing rollback table ~s", [?TESTTABLE]),
    SelStmt1 = OciSession:prep_sql(?SELECT_WITH_ROWID),
    ?assertMatch({?PORT_MODULE, statement, _, _, _}, SelStmt1),
    ?assertEqual({cols, Cols}, SelStmt1:exec_stmt()),
    ?assertEqual({{rows, Rows}, false}, SelStmt1:fetch_rows(RowCount)),
    ?assertEqual(ok, SelStmt1:close()).

commit_rollback_test({_, OciSession}) ->
    ?ELog("+----------------------------------------------------------------+"),
    ?ELog("|                      commit_rollback_test                      |"),
    ?ELog("+----------------------------------------------------------------+"),
    RowCount = 3,

    flush_table(OciSession),

    ?ELog("inserting into table ~s", [?TESTTABLE]),
    BoundInsStmt = OciSession:prep_sql(?INSERT),
    ?assertMatch({?PORT_MODULE, statement, _, _, _}, BoundInsStmt),
    BoundInsStmtRes = BoundInsStmt:bind_vars(?BIND_LIST),
    ?assertMatch(ok, BoundInsStmtRes),
    ?assertMatch({rowids, _},
    BoundInsStmt:exec_stmt([{ I
                            , list_to_binary(["_publisher_",integer_to_list(I),"_"])
                            , I+I/2
                            , list_to_binary(["_hero_",integer_to_list(I),"_"])
                            , list_to_binary(["_reality_",integer_to_list(I),"_"])
                            , I
                            , oci_util:edatetime_to_ora(erlang:now())
                            , I
                            } || I <- lists:seq(1, RowCount)], 1)),
    ?assertEqual(ok, BoundInsStmt:close()),

    ?ELog("selecting from table ~s", [?TESTTABLE]),
    SelStmt = OciSession:prep_sql(?SELECT_WITH_ROWID),
    ?assertMatch({?PORT_MODULE, statement, _, _, _}, SelStmt),
    {cols, Cols} = SelStmt:exec_stmt(),
    ?assertEqual(10, length(Cols)),
    {{rows, Rows}, false} = SelStmt:fetch_rows(RowCount),
    ?assertEqual(RowCount, length(Rows)),
    ?assertEqual(ok, SelStmt:close()),

    ?ELog("update in table ~s", [?TESTTABLE]),
    RowIDs = [R || [R|_] <- Rows],
    ?ELog("rowids ~p", [RowIDs]),
    BoundUpdStmt = OciSession:prep_sql(?UPDATE),
    ?assertMatch({?PORT_MODULE, statement, _, _, _}, BoundUpdStmt),
    BoundUpdStmtRes = BoundUpdStmt:bind_vars(?UPDATE_BIND_LIST),
    ?assertMatch(ok, BoundUpdStmtRes),
    ?assertMatch({rowids, _},
    BoundUpdStmt:exec_stmt([{ I
                            , list_to_binary(["_Publisher_",integer_to_list(I),"_"])
                            , I+I/3
                            , list_to_binary(["_Hero_",integer_to_list(I),"_"])
                            , list_to_binary(["_Reality_",integer_to_list(I),"_"])
                            , integer_to_binary(I+1)
                            , oci_util:edatetime_to_ora(erlang:now())
                            , I+1
                            , Key
                            } || {Key, I} <- lists:zip(RowIDs, lists:seq(1, length(RowIDs)))], -1)),

    ?assertMatch(ok, BoundUpdStmt:close()),

    ?ELog("testing rollback table ~s", [?TESTTABLE]),
    ?assertEqual(ok, OciSession:rollback()),
    SelStmt1 = OciSession:prep_sql(?SELECT_WITH_ROWID),
    ?assertMatch({?PORT_MODULE, statement, _, _, _}, SelStmt1),
    ?assertEqual({cols, Cols}, SelStmt1:exec_stmt()),
    {{rows, NewRows}, false} = SelStmt1:fetch_rows(RowCount),
    ?assertEqual(lists:sort(Rows), lists:sort(NewRows)),
    ?assertEqual(ok, SelStmt1:close()).

asc_desc_test({_, OciSession}) ->
    ?ELog("+----------------------------------------------------------------+"),
    ?ELog("|                          asc_desc_test                         |"),
    ?ELog("+----------------------------------------------------------------+"),
    RowCount = 10,

    flush_table(OciSession),

    ?ELog("inserting into table ~s", [?TESTTABLE]),
    BoundInsStmt = OciSession:prep_sql(?INSERT),
    ?assertMatch({?PORT_MODULE, statement, _, _, _}, BoundInsStmt),
    ?assertMatch(ok, BoundInsStmt:bind_vars(?BIND_LIST)),
    ?assertMatch({rowids, _},
    BoundInsStmt:exec_stmt([{ I
                            , list_to_binary(["_publisher_",integer_to_list(I),"_"])
                            , I+I/2
                            , list_to_binary(["_hero_",integer_to_list(I),"_"])
                            , list_to_binary(["_reality_",integer_to_list(I),"_"])
                            , I
                            , oci_util:edatetime_to_ora(erlang:now())
                            , I
                            } || I <- lists:seq(1, RowCount)], 1)),
    ?assertEqual(ok, BoundInsStmt:close()),

    ?ELog("selecting from table ~s", [?TESTTABLE]),
    SelStmt1 = OciSession:prep_sql(?SELECT_ROWID_ASC),
    ?assertMatch({?PORT_MODULE, statement, _, _, _}, SelStmt1),
    SelStmt2 = OciSession:prep_sql(?SELECT_ROWID_DESC),
    ?assertMatch({?PORT_MODULE, statement, _, _, _}, SelStmt2),
    ?assertEqual(SelStmt1:exec_stmt(), SelStmt2:exec_stmt()),

    {{rows, Rows11}, false} = SelStmt1:fetch_rows(5),
    {{rows, Rows12}, false} = SelStmt1:fetch_rows(5),
    {{rows, []}, true} = SelStmt1:fetch_rows(1),
    Rows1 = Rows11++Rows12,
    ?assertEqual(RowCount, length(Rows1)),

    {{rows, Rows21}, false} = SelStmt2:fetch_rows(5),
    {{rows, Rows22}, false} = SelStmt2:fetch_rows(5),
    {{rows, []}, true} = SelStmt2:fetch_rows(1),
    Rows2 = Rows21++Rows22,
    ?assertEqual(RowCount, length(Rows2)),

    ?ELog("Got rows asc ~p~n desc ~p", [Rows1, Rows2]),

    ?assertEqual(Rows1, lists:reverse(Rows2)),

    ?assertEqual(ok, SelStmt1:close()),
    ?assertEqual(ok, SelStmt2:close()).

describe_test({_, OciSession}) ->
    ?ELog("+----------------------------------------------------------------+"),
    ?ELog("|                         describe_test                          |"),
    ?ELog("+----------------------------------------------------------------+"),

    flush_table(OciSession),

    ?ELog("describing table ~s", [?TESTTABLE]),
    {ok, Descs} = OciSession:describe(list_to_binary(?TESTTABLE), 'OCI_PTYPE_TABLE'),
    ?assertEqual(9, length(Descs)),
    ?ELog("table ~s has ~p", [?TESTTABLE, Descs]).

function_test({_, OciSession}) ->
    ?ELog("+----------------------------------------------------------------+"),
    ?ELog("|                        function_test                           |"),
    ?ELog("+----------------------------------------------------------------+"),

    CreateFunction = OciSession:prep_sql(<<"
        create or replace function "
        ?TESTFUNCTION
        "(sal in number, com in number)
            return number is
        begin
            return ((sal*12)+(sal*12*nvl(com,0)));
        end;
    ">>),
    ?assertMatch({?PORT_MODULE, statement, _, _, _}, CreateFunction),
    ?assertEqual({executed, 0}, CreateFunction:exec_stmt()),
    ?assertEqual(ok, CreateFunction:close()),

    SelectStmt = OciSession:prep_sql(<<"select "?TESTFUNCTION"(10,30) from dual">>),
    ?assertMatch({?PORT_MODULE, statement, _, _, _}, SelectStmt),
    {cols, [Col|_]} = SelectStmt:exec_stmt(),
    ?assertEqual(<<?TESTFUNCTION"(10,30)">>, element(1, Col)),
    {{rows, [[F|_]|_]}, true} = SelectStmt:fetch_rows(2),
    ?assertEqual(<<3,194,38,21,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0>>, F),
    ?assertEqual(ok, SelectStmt:close()),

    SelectBoundStmt = OciSession:prep_sql(<<"select "?TESTFUNCTION"(:SAL,:COM) from dual">>),
    ?assertMatch({?PORT_MODULE, statement, _, _, _}, SelectBoundStmt),
    ?assertMatch(ok, SelectBoundStmt:bind_vars([{<<":SAL">>, 'SQLT_INT'}, {<<":COM">>, 'SQLT_INT'}])),
    {cols, [Col2|_]} = SelectBoundStmt:exec_stmt([{10, 30}], 1),
    ?assertEqual(<<?TESTFUNCTION"(:SAL,:COM)">>, element(1, Col2)),
    ?assertMatch({{rows, [[F|_]|_]}, true}, SelectBoundStmt:fetch_rows(2)),
    ?ELog("Col ~p", [Col]),
    ?assertEqual(ok, SelectBoundStmt:close()),

    % Drop function
    DropFunStmt = OciSession:prep_sql(<<"drop function "?TESTFUNCTION>>),
    ?assertEqual({executed, 0}, DropFunStmt:exec_stmt()),
    ?assertEqual(ok, DropFunStmt:close()).
