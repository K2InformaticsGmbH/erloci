-module(k2_test).

-include("oci.hrl").

-export([ run1/0
        , run2/0
        ]).

run1() ->
    try
        OciPort = oci_port:start_link([{logging, true}]),
        timer:sleep(10000),
        OciPool = OciPort:create_sess_pool(
                <<"(DESCRIPTION=
                    (ADDRESS=(PROTOCOL=tcp)
                        (HOST=192.168.1.69)
                        (PORT=1521)
                    )
                    (CONNECT_DATA=(SERVICE_NAME=SBS0.k2informatics.ch))
                )">>,
                <<"SBS0">>,
                <<"sbs0sbs0_4dev">>,
                <<>>),
        throw_if_error(OciPool, "pool creation failed"),
        oci_logger:log(lists:flatten(io_lib:format("___________----- OCI Pool ~p~n", [OciPool]))),

        OciSession = OciPort:get_session(),
        throw_if_error(OciSession, "get session failed"),
        oci_logger:log(lists:flatten(io_lib:format("___________----- OCI Session ~p~n", [OciSession]))),

%        timer:sleep(10000),
        Stmt0 = OciSession:exec_sql(<<"drop table test_erloci">>, []),
%        print_if_error(Stmt0, "drop failed"),
        oci_logger:log(lists:flatten(io_lib:format("___________----- OCI drop ~p~n", [Stmt0]))),

        Stmt1 = OciSession:exec_sql(<<"create table test_erloci(pkey number,
                                       publisher varchar2(100),
                                       rank number,
                                       hero varchar2(100),
                                       real varchar2(100),
                                       votes number,
                                       votes_first_rank number)">>, []),
        throw_if_error(Stmt1, "create failed"),
        oci_logger:log(lists:flatten(io_lib:format("___________----- OCI create ~p~n", [Stmt1]))),

        Stmt2 = OciSession:exec_sql(<<"insert into test_erloci values (1,
                                       'publisher1',
                                       1,
                                       'hero1',
                                       'real',
                                       1,
                                       1)">>, []),
        throw_if_error(Stmt2, "insert failed"),
        oci_logger:log(lists:flatten(io_lib:format("___________----- OCI insert ~p~n", [Stmt2]))),
%        timer:sleep(10000),
        Stmt3 = OciSession:exec_sql(<<"select * from test_erloci">>, []),
        throw_if_error(Stmt3, "select failed"),
        oci_logger:log(lists:flatten(io_lib:format("___________----- OCI select ~p~n", [Stmt3])))
    catch
        Class:Reason -> oci_logger:log(lists:flatten(io_lib:format("___________----- ~p:~p~n", [Class,Reason])))
    end.

print_if_error({error, Error}, Msg) -> oci_logger:log(lists:flatten(io_lib:format("___________----- continue after ~p ~p~n", [Msg,Error])));
print_if_error(_, _) -> ok.

throw_if_error({error, Error}, Msg) -> throw({Msg, Error});
throw_if_error(_, _) -> ok.

run2() ->
    S = connect(),
    test1(S).

connect() ->
    {ok, SP} = oci_session_pool:start_link("192.168.1.69",
                                           1521,
                                           {service, "SBS0.k2informatics.ch"},
                                           "SBS0",
                                           "sbs0sbs0_4dev",
                                           [{port_options, [{logging, true}]}]),
    oci_session_pool:get_session(SP).

test1(S) ->
    S:execute_sql("drop table test_erloci", [], 10),
    S:execute_sql("create table test_erloci(pkey number,
                                            publisher varchar2(100),
                                            rank number,
                                            hero varchar2(100),
                                            real varchar2(100),
                                            votes number,
                                            votes_first_rank number)", [], 10),
    S:execute_sql("insert into test_erloci values (1,
                                                  'publisher1',
                                                  1,
                                                  'hero1',
                                                  'real',
                                                  1,
                                                  1)", [], 10),
    {statement, Stmt} = S:execute_sql("select * from test_erloci", [], 10),
    Cols = Stmt:get_columns(),
    io:format(user, "[TEST] Columns ~p~n",[Cols]),
    Rows = Stmt:next_rows(),
    io:format(user, "[TEST] Rows ~p~n", [Rows]),
    ok.
