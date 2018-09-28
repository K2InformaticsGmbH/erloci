#!/usr/bin/env escript
%% -*- erlang -*-
%%! -smp enable -pa _build/default/lib/erloci/ebin

-define(L(__F, __A), io:format(user, ">     > [~p:~p] "__F, [?FUNCTION_NAME, ?LINE | __A])).
-define(L(__F), ?L(__F, [])).

main(_) ->
    {ok, Apps} = application:ensure_all_started(erloci),
    ?L("started ~p~n", [Apps]),

    {ok, [#{lang := Lang, logging := Logging, password := Pswd, tns := Tns,
            user := User}]} = file:consult("test/connect.config"),
    {oci_port, _} = OciPort = erloci:new([{logging, Logging}, {env, [{"NLS_LANG", Lang}]}]),
    OciSession = OciPort:get_session(Tns, User, Pswd),

    % Drop testclob
    {oci_port, statement, _, _, _} = StmtDrop
        = OciSession:prep_sql(<<"drop table testclob">>),
    case StmtDrop:exec_stmt() of
        {executed, 0} -> ok;
        DropResult -> ?L("drop testclob : ~p~n", [DropResult])
    end,
    ok = StmtDrop:close(),

    % Create testclob
    {oci_port, statement, _, _, _} = StmtCreate
        = OciSession:prep_sql(<<"create table testclob (id int, someclobtest clob)">>),
    {executed, 0} = StmtCreate:exec_stmt(),
    ok = StmtCreate:close(),
    ?L("created table testclob~n"),

    % Insert testclob (2KB)
    Chars = lists:seq($a, $z) ++ lists:seq($0, $9) ++ lists:seq($A, $Z),
    ClobData = << <<(lists:nth(rand:uniform(length(Chars)), Chars))>>
                 || _ <- lists:seq(1, 1024 * 2)>>,
    {oci_port, statement, _, _, _} = StmtInsert
        = OciSession:prep_sql(<<"insert into testclob values(1, to_clob('",ClobData/binary,"'))">>),
    {rowids, RowIds} = StmtInsert:exec_stmt(),
    ok = StmtInsert:close(),
    ?L("RowIds ~p~n", [RowIds]),

    % Select 1 testclob
    {oci_port, statement, _, _, _} = StmtSelect = OciSession:prep_sql(<<"select * from testclob">>),
    {cols, Cols} = StmtSelect:exec_stmt(),
    ?L("Cols ~p~n", [Cols]),
    {{rows, [[_, {LidClobd, ClobdLen}]]}, true} = StmtSelect:fetch_rows(10),
    {lob, ClobDVal} = StmtSelect:lob(LidClobd, 1, ClobdLen - 1024),
    ?L("ClobDVal(~p of ~p) ~p~n", [byte_size(ClobDVal), ClobdLen, ClobDVal]),

    % Insert testclob NULL CLOB
    {oci_port, statement, _, _, _} = StmtInsert1
        = OciSession:prep_sql(<<"insert into testclob values(1, NULL)">>),
    {rowids, RowIds1} = StmtInsert1:exec_stmt(),
    ok = StmtInsert1:close(),
    ?L("RowIds1 ~p~n", [RowIds1]),

    % Select 2 testclob
    {oci_port, statement, _, _, _} = StmtSelect1 = OciSession:prep_sql(<<"select * from testclob">>),
    {cols, Cols1} = StmtSelect1:exec_stmt(),
    ?L("Cols1 ~p~n", [Cols1]),
    {{rows, Rows1}, true} = StmtSelect1:fetch_rows(10),
    ?L("Rows1 ~p~n", [Rows1]),

    %- % Insert bind testclob
    %- {oci_port, statement, _, _, _} = StmtInsert2
    %-     = OciSession:prep_sql(<<"insert into testclob values(:SQLT_INT_1, :SQLT_CLOB_2)">>),
	%- ok = StmtInsert2:bind_vars([{<<":SQLT_INT_1">>, 'SQLT_INT'}, {<<":SQLT_CLOB_2">>, 'SQLT_CLOB'}]),
	%- ?L("sleeping for 10 seconds....~n"),
	%- timer:sleep(10000),
    %- {rowids, RowIds2} = StmtInsert2:exec_stmt([{3, <<>>}, {4, <<"this is a test">>}], 1),
    %- ok = StmtInsert2:close(),
    %- ?L("RowIds2 ~p~n", [RowIds2]),

    %- % Select 3 testclob
    %- {oci_port, statement, _, _, _} = StmtSelect2 = OciSession:prep_sql(<<"select * from testclob">>),
    %- {cols, Cols2} = StmtSelect2:exec_stmt(),
    %- ?L("Cols2 ~p~n", [Cols2]),
    %- {{rows, Rows2}, true} = StmtSelect2:fetch_rows(10),
    %- ?L("Rows2 ~p~n", [Rows2]),

    ?L("DONE!!~n").
