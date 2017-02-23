-module(erloci_SUITE).
-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([load/1]).

-include_lib("common_test/include/ct.hrl").
-include("test_common.hrl").

-define(value(Key, Config), proplists:get_value(Key, Config)).
-define(TAB, "erloci_load").

%10 10 100
%select table_name from all_tables where table_name like 'ERL%';
-define(CONNECTIONS, 100).
-define(STATEMENTS, 100).
-define(ROWS_PER_TABLE, 5).

-define(CONNIDLIST, lists:seq(1, ?CONNECTIONS)).
-define(STMTIDLIST, lists:seq(1, ?STATEMENTS)).

all() -> [load].

init_per_suite(ConfigData) ->
    io:format(user, "---~p---~n", [?LINE]),
    application:start(erloci),
    io:format(user, "---~p---~n", [?LINE]),
    {Tns, User, Pswd, NlsLang} = (fun() ->
        ConnectConfigFile =
            filename:join(lists:reverse(["connect.config" |
                case lists:reverse(filename:split(?config(data_dir, ConfigData))) of
                    ["erloci_SUITE_data" | Rest] -> Rest;
                    Error ->
                        ?ELog("~p", [Error]),
                        error(Error)
                end
            ])),
        case file:consult(ConnectConfigFile) of
            {ok, Params} -> {
                proplists:get_value(tns, Params),
                proplists:get_value(user, Params),
                proplists:get_value(password, Params),
                proplists:get_value(nls_lang, Params)};
            {error, Reason} ->
                ?ELog("~p", [Reason]),
                error(Reason)
        end
                                  end)(),
    io:format(user, "---~p---~n", [?LINE]),
    Tables = [{C,
        [lists:flatten([?TAB, "_", integer_to_list(C), "_", integer_to_list(S)])
            || S <- ?STMTIDLIST]}
        || C <- ?CONNIDLIST],
    ct:pal(info, "Building ~p rows to bind for ~p tables", [?ROWS_PER_TABLE, length(Tables)]),
    Binds = [{I
        , list_to_binary(["_publisher_", integer_to_list(I), "_"])
        , I + I / 2
        , list_to_binary(["_hero_", integer_to_list(I), "_"])
        , list_to_binary(["_reality_", integer_to_list(I), "_"])
        , I
        , oci_util:edatetime_to_ora(erlang:now())
        , I
    } || I <- lists:seq(1, ?ROWS_PER_TABLE)],
    ct:pal(info, "Starting ~p processes", [length(Tables)]),
    [{tables, Tables}, {binds, Binds}, {config, {Tns, User, Pswd, NlsLang}} | ConfigData].

end_per_suite(ConfigData) ->
    Tables = lists:merge([Tabs || {_, Tabs} <- ?value(tables, ConfigData)]),
    {Tns, User, Pswd, NlsLang} = ?value(config, ConfigData),
    OciPort = erloci:new(
        [{logging, true},
            {env, [{"NLS_LANG", NlsLang}]}
        ]),
    OciSession = OciPort:get_session(Tns, User, Pswd),
    [tab_drop(OciSession, Table) || Table <- Tables],
    ct:pal(info, "Finishing...", []).

load(ConfigData) ->
    Tables = ?value(tables, ConfigData),
    ct:pal(info, "tables: ~p", [tables]),
    Binds = ?value(binds, ConfigData),
    {Tns, User, Pswd, NlsLang} = ?value(config, ConfigData),
    RowsPerProcess = length(Binds),
    ct:pal(info, "Starting ~p connection processes with ~p", [?CONNECTIONS, Tables]),
    % OciPort = oci_port:start_link([{logging, true}]),
    OciPort = erloci:new([
        {logging, true},
        {env, [{"NLS_LANG", NlsLang}]}
    ]),
    This = self(),
    [spawn(fun() ->
        connection(OciPort, C, proplists:get_value(C, Tables, []), Tns, User, Pswd, This, RowsPerProcess, Binds)
           end)
        || C <- ?CONNIDLIST],
    collect_processes(lists:sort(Tables), []),
    ct:pal(info, "Closing port ~p", [OciPort]),
    ok = OciPort:close().

connection(OciPort, Cid, Tables, Tns, User, Pswd, Master, RowsPerProcess, Binds) ->
    OciSession = OciPort:get_session(Tns, User, Pswd),
    ct:pal(info, "Got session ~p", [OciSession]),
    [begin
         tab_drop(OciSession, Table),
         tab_create(OciSession, Table)
     end
        || Table <- Tables],
    This = self(),
    [spawn(fun() ->
        table(OciSession, Cid, Tid, This, RowsPerProcess, Binds)
           end)
        || Tid <- ?STMTIDLIST],
    collect_processes(lists:sort(Tables), []),
    ct:pal(info, "Closing session ~p", [OciSession]),
    ok = OciSession:close(),
    Master ! {Cid, Tables}.

table(OciSession, Cid, Tid, Master, RowsPerProcess, Binds) ->
    Table = lists:flatten([?TAB, "_", integer_to_list(Cid), "_", integer_to_list(Tid)]),
    tab_load(OciSession, Table, RowsPerProcess, Binds),
    tab_access(OciSession, Table, 10),
    tab_drop(OciSession, Table),
    Master ! Table.

collect_processes(Tables, Acc) ->
    receive
        Table ->
            case lists:sort([Table | Acc]) of
                Tables -> ok;
                NewAcc ->
                    ct:pal(info, "Expecting ~p", [Tables -- NewAcc]),
                    timer:sleep(1000),
                    collect_processes(Tables, NewAcc)
            end
    end.

-define(B(__L), list_to_binary(__L)).
-define(CREATE(__T), ?B([
    "create table "
    , __T
    , " (pkey integer,"
    , "publisher varchar2(30),"
    , "rank float,"
    , "hero varchar2(30),"
    , "reality varchar2(30),"
    , "votes number(1,-10),"
    , "createdate date default sysdate,"
    , "chapters int,"
    , "votes_first_rank number)"])
).
-define(INSERT(__T), ?B([
    "insert into "
    , __T
    , " (pkey,publisher,rank,hero,reality,votes,createdate,votes_first_rank) values ("
    , ":pkey"
    , ", :publisher"
    , ", :rank"
    , ", :hero"
    , ", :reality"
    , ", :votes"
    , ", :createdate"
    , ", :votes_first_rank)"])
).
-define(BIND_LIST, [
    {<<":pkey">>, 'SQLT_INT'}
    , {<<":publisher">>, 'SQLT_CHR'}
    , {<<":rank">>, 'SQLT_FLT'}
    , {<<":hero">>, 'SQLT_CHR'}
    , {<<":reality">>, 'SQLT_CHR'}
    , {<<":votes">>, 'SQLT_INT'}
    , {<<":createdate">>, 'SQLT_DAT'}
    , {<<":votes_first_rank">>, 'SQLT_INT'}
]
).
-define(SELECT_WITH_ROWID(__T), ?B([
    "select ", __T, ".rowid, ", __T, ".* from ", __T])
).

tab_drop(OciSession, Table) when is_list(Table) ->
    DropStmt = OciSession:prep_sql(?B(["drop table ", Table])),
    {oci_port, statement, _, _, _} = DropStmt,
    case DropStmt:exec_stmt() of
        {error, _} -> ok;
        _ ->
            %ct:pal(info, "[~s] Droped!", [Table]),
            ok = DropStmt:close()
    end.

tab_create(OciSession, Table) when is_list(Table) ->
    StmtCreate = OciSession:prep_sql(?CREATE(Table)),
    {oci_port, statement, _, _, _} = StmtCreate,
    {executed, 0} = StmtCreate:exec_stmt(),
    ok = StmtCreate:close(),
    ct:pal(info, "[~s] Created", [Table]).

tab_load(OciSession, Table, RowCount, Binds) ->
    BoundInsStmt = OciSession:prep_sql(?INSERT(Table)),
    {oci_port, statement, _, _, _} = BoundInsStmt,
    BoundInsStmtRes = BoundInsStmt:bind_vars(?BIND_LIST),
    ok = BoundInsStmtRes,
    {rowids, RowIds} = BoundInsStmt:exec_stmt(Binds),
    RowCount = length(RowIds),
    ok = BoundInsStmt:close(),
    ct:pal(info, "[~s] Loaded ~p rows", [Table, RowCount]).

tab_access(OciSession, Table, Count) ->
    ct:pal(info, "[~s]  Loading rows @ ~p per fetch", [Table, Count]),
    SelStmt = OciSession:prep_sql(?SELECT_WITH_ROWID(Table)),
    {oci_port, statement, _, _, _} = SelStmt,
    {cols, Cols} = SelStmt:exec_stmt(),
    ct:pal(info, "[~s] Selected columns ~p", [Table, Cols]),
    10 = length(Cols),
    load_rows_to_end(Table, SelStmt:fetch_rows(Count), SelStmt, Count, 0),
    ok = SelStmt:close(),
    ok.

load_rows_to_end(Table, {{rows, Rows}, true}, _, _, Total) ->
    Loaded = length(Rows),
    ct:pal(info, "[~s] Loaded ~p / ~p rows - Finished", [Table, Loaded, Total + Loaded]);
load_rows_to_end(Table, {error, Error}, SelStmt, Count, Total) ->
    ct:pal(info, "[~s] Loaded ~p error - ~p", [Table, Total, Error]),
    load_rows_to_end(Table, SelStmt:fetch_rows(Count), SelStmt, Count, Total);
load_rows_to_end(Table, {{rows, Rows}, false}, SelStmt, Count, Total) ->
    Loaded = length(Rows),
    ct:pal(info, "[~s] Loaded ~p / ~p", [Table, Loaded, Total + Loaded]),
    load_rows_to_end(Table, SelStmt:fetch_rows(Count), SelStmt, Count, Total + Loaded).
