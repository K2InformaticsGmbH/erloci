Tns = <<"(DESCRIPTION=(ADDRESS_LIST=(ADDRESS="
         "(PROTOCOL=tcp)(HOST=80.67.144.206)(PORT=5437)))"
         "(CONNECT_DATA=(SERVICE_NAME=XE)))">>.
User = <<"scott">>.
Pswd = <<"regit">>.
SESSSQL = <<"select '' || s.sid || ',' || s.serial# "
            "from gv$session s join "
            "gv$process p on p.addr = s.paddr and "
            "p.inst_id = s.inst_id "
            "where s.type != 'BACKGROUND' and s.program like 'ocierl%'">>.

f(OciPort).
OciPort = erloci:new([{logging, true}, {env, [{"NLS_LANG", "GERMAN_SWITZERLAND.AL32UTF8"}]}]).

f(OciSess).
OciSess = OciPort:get_session(Tns, User, Pswd).

f(Stmt).
Stmt = OciSess:prep_sql(SESSSQL).
{cols, _} = Stmt:exec_stmt().
f(SessionsBefore).
{{rows, SessionsBefore}, true} = Stmt:fetch_rows(10000).
ok = Stmt:close().

f(PingOciPort).
PingOciPort = erloci:new([{logging, true}, {ping_timeout, 1000},
                          {env, [{"NLS_LANG", "GERMAN_SWITZERLAND.AL32UTF8"}]}]).
f(PingOciSession).
PingOciSession = PingOciPort:get_session(Tns, User, Pswd).

f(Stmt).
Stmt = OciSess:prep_sql(SESSSQL).
{cols, _} = Stmt:exec_stmt().
f(SessionsAfter).
{{rows, SessionsAfter}, true} = Stmt:fetch_rows(10000).
ok = Stmt:close().

f(PingSession).
[PingSession | _] = lists:flatten(SessionsAfter) -- lists:flatten(SessionsBefore).
pong = PingOciSession:ping().


f(Stmt).
Stmt = OciSess:prep_sql(
           <<"alter system kill session '", PingSession/binary,
             "' immediate">>).
case Stmt:exec_stmt() of
    {error,{30, _}} -> ok;
    {error,{31, _}} -> ok;
    {executed, 0} -> io:format("~p closed~n", [PingSession])
end,
ok = Stmt:close().

timer:sleep(2000).
pang = PingOciSession:ping().
PingOciPort:close().

OciSess:close().
OciPort:close().

