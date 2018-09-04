-module(psf).

-compile(export_all).

%%
% werl.exe -pa _build/default/lib/erloci/ebin -s erloci &
% erl -pa _build/default/lib/erloci/ebin -s erloci
% f().
% c(psf).
% Ctx = psf:open().
% Stmt = psf:select(Ctx).
% spawn(psf, fetch_loop, [Stmt]).
%

open() ->
    Tns = <<"(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=tcp)(HOST=127.0.0.1)"
            "(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=XE)))">>,
    Pswd= <<"regit">>,
    User= <<"scott">>,
    OciPort = erloci:new([{logging, true}, {env, []}]),
    OciSession = OciPort:get_session(Tns, User, Pswd),
    {OciPort, OciSession}.

create_type({_OciPort, OciSession}) ->
    CreateType = OciSession:prep_sql(
                   <<"CREATE OR REPLACE TYPE T_TEXT_ROW AS OBJECT (text_line VARCHAR2(4000));">>
                 ),
    {executed, 0} = CreateType:exec_stmt(),
    ok = CreateType:close(),
    CreateType = OciSession:prep_sql(<<"CREATE OR REPLACE TYPE T_TEXT_TAB IS TABLE OF T_TEXT_ROW;">>),
    {executed, 0} = CreateType:exec_stmt(),
    ok = CreateType:close().

create_function({_OciPort, OciSession}) ->
    CreateFunction = OciSession:prep_sql(<<"
     create or replace function SLOW_TEXT_TABLE (
         SQLT_STR_TEXT  IN Varchar2,
         SQLT_INT_ROWS  IN INTEGER,
         SQLT_VNR_DELAY IN NUMBER
     ) RETURN T_TEXT_TAB PIPELINED
     is
     begin
         FOR i IN 1..SQLT_INT_ROWS LOOP
             PIPE ROW(T_TEXT_ROW(SQLT_STR_TEXT || i));
             sys.dbms_lock.sleep(SQLT_VNR_DELAY);
         END LOOP;
     end SLOW_TEXT_TABLE;
    ">>),
    {executed, 0} = CreateFunction:exec_stmt(),
    ok = CreateFunction:close().

select({_OciPort, OciSession}) ->
    SelectSlow = OciSession:prep_sql(
    <<"SELECT *
            FROM TABLE(
                SLOW_TEXT_TABLE (
                    :SQLT_STR_TEXT,
                    :SQLT_INT_ROWS,
                    :SQLT_INT_DELAY
                )
            )">>
    ),
    ok = SelectSlow:bind_vars([
        {<<":SQLT_STR_TEXT">>, 'SQLT_STR'},
        {<<":SQLT_INT_ROWS">>, 'SQLT_INT'},
        {<<":SQLT_INT_DELAY">>, 'SQLT_INT'}]),

    {cols, Cols} = SelectSlow:exec_stmt([{<<"FOO_BAR_">>, 10, 2}], 1),
    io:format("Cols : ~p~n", [Cols]),
    SelectSlow.

fetch_loop(SelectSlow) ->
    case timer:tc(fun() -> SelectSlow:fetch_rows(1) end)of
        {Time, {{rows, []}, true}} -> io:format("[~8.3.0f ms] EOT~n", [Time / 1000]);
        {Time, {{rows, Rows}, true}} ->
             io:format("[~8.3.0f ms] Last ~p~n", [Time / 1000, Rows]);
        {Time, {{rows, Rows}, _}} ->
             io:format("[~8.3.0f ms] Rows ~p~n", [Time / 1000, Rows]),
             receive die -> 
                SelectSlow:close()
             after 1000 ->
                fetch_loop(SelectSlow)
             end
    end.

close({OciPort, OciSession}) ->
    OciSession:close(),
    OciPort:close().
