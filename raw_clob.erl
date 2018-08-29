-module(raw_clob).
-ifdef(CONSOLE).

werl.exe -pa _build/default/lib/erloci/ebin -s erloci &

Tns = <<"(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=tcp)(HOST=127.0.0.1)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=XE)))">>.
%Tns = <<"(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=tcp)(HOST=192.168.1.43)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=XE)))">>.
Pswd= <<"regit">>.
User= <<"scott">>.

f(OciPort).
OciPort = erloci:new([{logging, true}, {env, []}]).
f(OciSession).
OciSession = OciPort:get_session(Tns, User, Pswd).

StmtDrop = OciSession:prep_sql(<<"drop table raw_clob">>).
{executed, 0} = StmtDrop:exec_stmt().
ok = StmtDrop:close().
f(StmtDrop).

StmtCreate = OciSession:prep_sql(<<"create table raw_clob(col_clob clob, col_raw raw(2000))">>).
{executed, 0} = StmtCreate:exec_stmt().
ok = StmtCreate:close().
f(StmtCreate).

StmtInsert = OciSession:prep_sql(<<"insert into raw_clob (col_raw) values (:col_raw)">>).
ok = StmtInsert:bind_vars([{<<":col_raw">>, 'SQLT_BIN'}]).
f(Data).
%Data = << <<(rand:uniform(255))>> || _ <- lists:duplicate(1999, 0) >>.
{rowids, _} = StmtInsert:exec_stmt([{<<1,2,3,4,0,0,0,5,6,7,8>>},{<<2,2,4,0,0,5,7,8>>}]).
ok = StmtInsert:close().
f(StmtInsert).

% Works
f(SelStmt).
SelStmt = OciSession:prep_sql("select CKO, JPO from TPAC_UMJTT").
{cols, _} = SelStmt:exec_stmt().
%{{rows, [[Data]]}, true} = SelStmt:fetch_rows(100).
{{rows, _}, true} = SelStmt:fetch_rows(100).
SelStmt:close().

f(SelStmt1).
SelStmt1 = OciSession:prep_sql("select * from TPAC_UMJTT where cko = :cko_raw").
ok = SelStmt1:bind_vars([{<<":cko_raw">>, 'SQLT_BIN'}]).
{cols, _} = SelStmt1:exec_stmt([{<<17,17,2,2>>}]).
f(Rows).
{{rows, Rows}, _} = SelStmt1:fetch_rows(100).
SelStmt1:close().

f(SelStmt1).
SelStmt1 = OciSession:prep_sql("select cko from TPAC_UMJTT where cko > :cko_raw").
ok = SelStmt1:bind_vars([{<<":cko_raw">>, 'SQLT_BIN'}]).
{cols, _} = SelStmt1:exec_stmt([{<<17,17,2,2>>}]).
f(Rows).
{{rows, Rows}, _} = SelStmt1:fetch_rows(1).
SelStmt1:close().

OciSession:close().
OciPort:close().

-endif.