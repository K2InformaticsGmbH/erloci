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

-module(oci_port).
-behaviour(gen_server).

-include("oci.hrl").

%% API
-export([
    start_link/1,
    start_link/2,
    stop/1,
    logging/2,
    get_session/4,
    describe/3,
    prep_sql/2,
    commit/1,
    rollback/1,
    bind_vars/2,
    exec_stmt/1,
    exec_stmt/2,
    exec_stmt/3,
    fetch_rows/2,
    close/1
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
    port,
    logging = ?DBG_FLAG_OFF
}).

-define(log(__Flag, __Format, __Args), if __Flag == ?DBG_FLAG_ON -> ?Info(__Format, __Args); true -> ok end).

%% External API
start_link(Options) ->
    start_link(Options, fun(B) ->
        case re:run(B, <<"\n">>) of
            nomatch -> oci_logger:log(binary_to_list(B));
            _ ->
                [(fun
                    ("") -> ok;
                    (Txt) ->
                        oci_logger:log(?T++" "++Txt++"~n")
                end)(Log) || Log <- re:split(B, <<"\n">>, [{return, list}])]
        end
    end).
start_link(Options,LogFun) ->
    {ok, LSock} = gen_tcp:listen(0, [binary, {packet, 0}, {active, false}]),
    {ok, ListenPort} = inet:port(LSock),
    ?Info("listening at ~p for log connections...", [ListenPort]),
    spawn(fun() -> server(LSock,LogFun) end),
    case Options of
        undefined ->
            gen_server:start_link(?MODULE, [false, ListenPort], []);
        Options when is_list(Options)->
            Logging = proplists:get_value(logging, Options, false),
            StartRes = gen_server:start_link(?MODULE, [Logging, ListenPort], []),
            case StartRes of
                {ok, Pid} -> {?MODULE, Pid};
                Error -> throw({error, Error})
            end
    end.

stop(PortPid) ->
    gen_server:call(PortPid, stop).

logging(enable, {?MODULE, PortPid}) ->
    gen_server:call(PortPid, {port_call, [?RMOTE_MSG, ?DBG_FLAG_ON]}, ?PORT_TIMEOUT);
logging(disable, {?MODULE, PortPid}) ->
    gen_server:call(PortPid, {port_call, [?RMOTE_MSG, ?DBG_FLAG_OFF]}, ?PORT_TIMEOUT).

get_session(Tns, Usr, Pswd, {?MODULE, PortPid})
when is_binary(Tns); is_binary(Usr); is_binary(Pswd) ->
    case gen_server:call(PortPid, {port_call, [?GET_SESSN, Tns, Usr, Pswd]}, ?PORT_TIMEOUT) of
        {error, Error} -> {error, Error};
        SessionId -> {?MODULE, PortPid, SessionId}
    end.

close({?MODULE, PortPid, SessionId}) ->
    gen_server:call(PortPid, {port_call, [?PUT_SESSN, SessionId]}, ?PORT_TIMEOUT),
    gen_server:call(PortPid, close, ?PORT_TIMEOUT);
close({?MODULE, statement, PortPid, StmtId}) ->
    gen_server:call(PortPid, {port_call, [?CLSE_STMT, StmtId]}, ?PORT_TIMEOUT).

bind_vars(BindVars, {?MODULE, statement, PortPid, StmtId}) when is_list(BindVars) ->
    TranslatedBindVars = [{K, ?CT(V)} || {K,V} <- BindVars],
    R = gen_server:call(PortPid, {port_call, [?BIND_ARGS, StmtId, TranslatedBindVars]}, ?PORT_TIMEOUT),
    timer:sleep(100), % Port driver breaks on faster pipe access
    case R of
        ok -> ok;
        R -> R
    end.

commit({?MODULE, PortPid, SessionId}) ->
    gen_server:call(PortPid, {port_call, [?CMT_SESSN, SessionId]}, ?PORT_TIMEOUT).

rollback({?MODULE, PortPid, SessionId}) ->
    gen_server:call(PortPid, {port_call, [?RBK_SESSN, SessionId]}, ?PORT_TIMEOUT).

describe(Object, Type, {?MODULE, PortPid, SessionId})
when is_binary(Object); is_atom(Type) ->
    R = gen_server:call(PortPid, {port_call, [?CMD_DSCRB, SessionId, Object, ?DT(Type)]}, ?PORT_TIMEOUT),
    case R of
        {desc, Descs} -> {ok, lists:reverse([{N,?CS(T),Sz} || {N,T,Sz} <- Descs])};
        R -> R
    end.

prep_sql(Sql, {?MODULE, PortPid, SessionId}) when is_list(Sql) ->
    prep_sql(iolist_to_binary(Sql), {?MODULE, PortPid, SessionId});
prep_sql(Sql, {?MODULE, PortPid, SessionId}) when is_binary(Sql) ->
    R = gen_server:call(PortPid, {port_call, [?PREP_STMT, SessionId, Sql]}, ?PORT_TIMEOUT),
    timer:sleep(100), % Port driver breaks on faster pipe access
    case R of
        {stmt,StmtId} -> {?MODULE, statement, PortPid, StmtId};
        R -> R
    end.

% AutoCommit is default set to true
exec_stmt({?MODULE, statement, PortPid, StmtId}) ->
    exec_stmt([], 1, {?MODULE, statement, PortPid, StmtId}).
exec_stmt(BindVars, {?MODULE, statement, PortPid, StmtId}) ->
    exec_stmt(BindVars, 1, {?MODULE, statement, PortPid, StmtId}).
exec_stmt(BindVars, AutoCommit, {?MODULE, statement, PortPid, StmtId}) ->
    R = gen_server:call(PortPid, {port_call, [?EXEC_STMT, StmtId, BindVars, AutoCommit]}, ?PORT_TIMEOUT),
    timer:sleep(100), % Port driver breaks on faster pipe access
    case R of
        {cols, Clms} -> {ok, lists:reverse([{N,?CS(T),Sz,P,Sc} || {N,T,Sz,P,Sc} <- Clms])};
        R -> R
    end.

fetch_rows(Count, {?MODULE, statement, PortPid, StmtId}) ->
    case gen_server:call(PortPid, {port_call, [?FTCH_ROWS, StmtId, Count]}, ?PORT_TIMEOUT) of
        {{rows, Rows}, Completed} -> {{rows, lists:reverse(Rows)}, Completed};
        Other -> Other
    end.


%% Callbacks
init([Logging, ListenPort]) ->
    PrivDir = case code:priv_dir(erloci) of
        {error,_} -> "./priv/";
        PDir -> PDir
    end,
    case os:find_executable(?EXE_NAME, PrivDir) of
        false ->
            case os:find_executable(?EXE_NAME, "./deps/erloci/priv/") of
                false -> {stop, bad_executable};
                Executable -> start_exe(Executable, Logging, ListenPort)
            end;
        Executable -> start_exe(Executable, Logging, ListenPort)
    end.

start_exe(Executable, Logging, ListenPort) ->
    PrivDir = case code:priv_dir(erloci) of
        {error,_} -> "./priv/";
        PDir -> PDir
    end,
    LibPath = case os:type() of
	    {unix,darwin}   -> "DYLD_LIBRARY_PATH";
	    _               -> "LD_LIBRARY_PATH"
    end,
    NewLibPath = case os:getenv(LibPath) of
        false -> "";
        LdLibPath -> LdLibPath ++ ":"
    end ++ PrivDir,
    ?Info("New ~s path: ~s", [LibPath, NewLibPath]),
    PortOptions = [ {packet, 4}
                  , binary
                  , exit_status
                  , use_stdio
                  , {args, ["true", integer_to_list(ListenPort)]}
                  , {env, [{LibPath, NewLibPath}]}
                  ],
    case (catch open_port({spawn_executable, Executable}, PortOptions)) of
        {'EXIT', Reason} ->
            ?Error("oci could not open port: ~p", [Reason]),
            {stop, Reason};
        Port ->
            %% TODO -- Logging is turned after port creation for the integration tests to run
            case Logging of
                true ->
                    port_command(Port, term_to_binary({undefined, ?RMOTE_MSG, ?DBG_FLAG_ON})),
                    ?Info("started log enabled new port:~n~p", [erlang:port_info(Port)]),
                    {ok, #state{port=Port, logging=?DBG_FLAG_ON}};
                false ->
                    port_command(Port, term_to_binary({undefined, ?RMOTE_MSG, ?DBG_FLAG_OFF})),
                    ?Info("started log disabled new port:~n~p", [erlang:port_info(Port)]),
                    {ok, #state{port=Port, logging=?DBG_FLAG_OFF}}
            end
    end.

server(LSock,Fun) ->
    ?Info("~p waiting for log connections...", [LSock]),
    case gen_tcp:accept(LSock) of
        {ok, Sock} ->
            ?Info("logger connected at ~p, now waiting to receive in tight loop and log", [Sock]),
            log(Sock,Fun),
            ?Info("closing tcp connection"),
            ok = gen_tcp:close(Sock),
            ok = gen_tcp:close(LSock);
        {error, Error} ->
            ?Error("listener failed ~p!", [Error])
    end.

log(Sock,Fun) ->
    case gen_tcp:recv(Sock, 0) of
        {ok, B} ->
            Fun(B),
            log(Sock,Fun);
        {error, closed} -> ok
    end.

handle_call(close, _From, #state{port=Port} = State) ->
    try
        erlang:port_close(Port)
    catch
        _:R -> error_logger:error_report("Port close failed with reason: ~p~n", [R])
    end,
    {stop, normal, ok, State};
handle_call({port_call, Msg}, From, #state{port=Port} = State) ->
    Cmd = [From | Msg],
    %CmdBin = term_to_binary(Cmd),
    %?Debug("TX ~p bytes", [byte_size(CmdBin)]),
    %?Info(">>>>>>>>>>>> TX ~p", [Cmd]),
    %case Cmd of
    %    [_,C,S|Args] -> ?Info("PORT CMD : ~s ~p\n~p", [?CMDSTR(C),S,Args]);
    %    [_,C|Args] -> ?Info("PORT CMD : ~s\n~p", [?CMDSTR(C),Args])
    %end,
    true = port_command(Port, term_to_binary(list_to_tuple(Cmd))),
    %io:format(user, "_____________________________________ request ~p_____________________________________ ~n", [From]),
    {noreply, State}. %% we will reply inside handle_info_result

handle_cast(Msg, State) ->
    error_logger:error_report("ORA: received unexpected cast: ~p~n", [Msg]),
    {noreply, State}.

%% We got a reply from a previously sent command to the Port.  Relay it to the caller.
handle_info({Port, {data, Data}}, #state{port=Port} = State) when is_binary(Data) ->
    Resp = binary_to_term(Data),
    %?Info("RX ~p bytes", [byte_size(Data)]),
    %?Info("<<<<<<<<<<<< RX ~p", [Resp]),
    case handle_result(State#state.logging, Resp) of
        {undefined, Result} ->
            ?Info("no reply for ~p", [Result]);
        {From, {error, Reason}} ->
            ?Error("~p", [Reason]), % Just in case its ignored later
            gen_server:reply(From, {error, Reason});
        {From, Result} ->
            %io:format(user, "_____________________________________ reply ~p_____________________________________ ~n", [From]),
            gen_server:reply(From, Result) % regular reply
    end,
    {noreply, State};
handle_info({Port, {exit_status, Status}}, #state{port = Port} = State) ->
    ?log(State#state.logging, "port ~p exited with status ~p", [Port, Status]),
    case Status of
        0 ->
            {stop, normal, State};
        Other ->
            {stop, {port_exit, signal_str(Other-128)}, State}
    end;
%% Catch all - throws away unknown messages (This could happen by "accident"
%% so we do not want to crash, but we make a log entry as it is an
%% unwanted behaviour.)
handle_info(Info, State) ->
    error_logger:error_report("ORA: received unexpected info: ~p~n", [Info]),
    {noreply, State}.

terminate(Reason, #state{port=Port}) ->
    ?Error("Terminating ~p", [Reason]),
    catch port_close(Port),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% log on and off -- mostly called from an undefined context so logged the status here
handle_result(L, {Ref, ?RMOTE_MSG, En} = _Resp) ->
    ?log(L, "Remote ~p", [En]),
    {Ref, En};

% port error handling
handle_result(L, {Ref, Cmd, error, Reason}) ->
    ?log(L, "RX: ~p ~s error ~p", [Ref,?CMDSTR(Cmd), Reason]),
    {Ref, {error, Reason}};

% generic command handling
handle_result(_L, {Ref, _Cmd, Result}) ->
    %?log(_L, "RX: ~s -> ~p", [?CMDSTR(_Cmd), Result]),
    {Ref, Result}.


% port return signals
signal_str(1)  -> {'SIGHUP',        exit,   "Hangup"};
signal_str(2)  -> {'SIGINT',        exit,   "Interrupt"};
signal_str(3)  -> {'SIGQUIT',       core,   "Quit"};
signal_str(4)  -> {'SIGILL',        core,   "Illegal Instruction"};
signal_str(5)  -> {'SIGTRAP',       core,   "Trace/Breakpoint Trap"};
signal_str(6)  -> {'SIGABRT',       core,   "Abort"};
signal_str(7)  -> {'SIGEMT',        core,   "Emulation Trap"};
signal_str(8)  -> {'SIGFPE',        core,   "Arithmetic Exception"};
signal_str(9)  -> {'SIGKILL',       exit,   "Killed"};
signal_str(10) -> {'SIGBUS',        core,   "Bus Error"};
signal_str(11) -> {'SIGSEGV',       core,   "Segmentation Fault"};
signal_str(12) -> {'SIGSYS',        core,   "Bad System Call"};
signal_str(13) -> {'SIGPIPE',       exit,   "Broken Pipe"};
signal_str(14) -> {'SIGALRM',       exit,   "Alarm Clock"};
signal_str(15) -> {'SIGTERM',       exit,   "Terminated"};
signal_str(16) -> {'SIGUSR1',       exit,   "User Signal 1"};
signal_str(17) -> {'SIGUSR2',       exit,   "User Signal 2"};
signal_str(18) -> {'SIGCHLD',       ignore, "Child Status"};
signal_str(19) -> {'SIGPWR',        ignore, "Power Fail/Restart"};
signal_str(20) -> {'SIGWINCH',      ignore, "Window Size Change"};
signal_str(21) -> {'SIGURG',        ignore, "Urgent Socket Condition"};
signal_str(22) -> {'SIGPOLL',       ignore, "Socket I/O Possible"};
signal_str(23) -> {'SIGSTOP',       stop,   "Stopped (signal)"};
signal_str(24) -> {'SIGTSTP',       stop,   "Stopped (user)"};
signal_str(25) -> {'SIGCONT',       ignore, "Continued"};
signal_str(26) -> {'SIGTTIN',       stop,   "Stopped (tty input)"};
signal_str(27) -> {'SIGTTOU',       stop,   "Stopped (tty output)"};
signal_str(28) -> {'SIGVTALRM',     exit,   "Virtual Timer Expired"};
signal_str(29) -> {'SIGPROF',       exit,   "Profiling Timer Expired"};
signal_str(30) -> {'SIGXCPU',       core,   "CPU time limit exceeded"};
signal_str(31) -> {'SIGXFSZ',       core,   "File size limit exceeded"};
signal_str(32) -> {'SIGWAITING',    ignore, "All LWPs blocked"};
signal_str(33) -> {'SIGLWP',        ignore, "Virtual Interprocessor Interrupt for Threads Library"};
signal_str(34) -> {'SIGAIO',        ignore, "Asynchronous I/O"};
signal_str(N)  -> {udefined,        ignore, N}.

%
% Eunit tests
%
-ifdef(TEST).

-define(ELog(__F), ?ELog(__F,[])).
-define(ELog(__F,__A), io:format(user, "[~p:~p] "__F"~n", [?MODULE,?LINE|__A])).
-include_lib("eunit/include/eunit.hrl").

-define(TESTTABLE, "erloci_test_1").
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
-define(SELECT_WITH_ORDER, <<"select pkey from ", ?TESTTABLE, " order by pkey desc">>).
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

flush_table(OciSession) ->
    ?ELog("creating (drop if exists) table ~s", [?TESTTABLE]),
    DropStmt = OciSession:prep_sql(?DROP),
    ?assertMatch({?MODULE, statement, _, _}, DropStmt),
    DropStmt:exec_stmt(),
    ?assertEqual(ok, DropStmt:close()),
    ?ELog("creating table ~s", [?TESTTABLE]),
    StmtCreate = OciSession:prep_sql(?CREATE),
    ?assertMatch({?MODULE, statement, _, _}, StmtCreate),
    ?assertEqual({executed, 0}, StmtCreate:exec_stmt()),
    ?assertEqual(ok, StmtCreate:close()).

setup() ->
    application:start(erloci),
    OciPort = oci_port:start_link([{logging, true}]),
    timer:sleep(1000),
    OciPort.

teardown(_OciPort) ->
    application:stop(erloci).

db_negative_test_() ->
    {timeout, 60, {
        setup,
        fun setup/0,
        fun teardown/1,
        {with, [
            fun bad_password/1
        ]}
    }}.

bad_password(OciPort) ->
    ?ELog("------------------------------------------------------------------"),
    ?ELog("|                       bad_password                             |"),
    ?ELog("------------------------------------------------------------------"),
    ?ELog("get_session with wrong password", []),
    {ok, {Tns,User,Pswd}} = application:get_env(erloci, default_connect_param),
    ?assertMatch({error, {1017,_}}, OciPort:get_session(Tns, User, list_to_binary([Pswd,"_bad"]))).

db_test_() ->
    {timeout, 60, {
        setup,
        fun oci_test:setup/0,
        fun oci_test:teardown/1,
        {with, [
            fun drop_create/1
            , fun insert_select_update/1
            , fun auto_rollback_test/1
            , fun commit_rollback_test/1
            , fun asc_desc_test/1
            , fun describe_test/1
        ]}
    }}.

drop_create(OciSession) ->
    ?ELog("------------------------------------------------------------------"),
    ?ELog("|                            drop_create                         |"),
    ?ELog("------------------------------------------------------------------"),

    ?ELog("creating (drop if exists) table ~s", [?TESTTABLE]),
    DropStmt = OciSession:prep_sql(?DROP),
    ?assertMatch({?MODULE, statement, _, _}, DropStmt),
    DropStmt:exec_stmt(),
    ?assertEqual(ok, DropStmt:close()),
    StmtCreate = OciSession:prep_sql(?CREATE),
    ?assertMatch({?MODULE, statement, _, _}, StmtCreate),
    ?assertEqual({executed, 0}, StmtCreate:exec_stmt()),
    ?assertEqual(ok, StmtCreate:close()),

    ?ELog("dropping table ~s", [?TESTTABLE]),
    DropStmt = OciSession:prep_sql(?DROP),
    ?assertMatch({?MODULE, statement, _, _}, DropStmt),
    ?assertEqual({executed,0}, DropStmt:exec_stmt()),
    ?assertEqual(ok, DropStmt:close()).

insert_select_update(OciSession) ->
    ?ELog("------------------------------------------------------------------"),
    ?ELog("|                      insert_select_update                      |"),
    ?ELog("------------------------------------------------------------------"),
    RowCount = 5,

    flush_table(OciSession),

    ?ELog("inserting into table ~s", [?TESTTABLE]),
    BoundInsStmt = OciSession:prep_sql(?INSERT),
    ?assertMatch({?MODULE, statement, _, _}, BoundInsStmt),
    BoundInsStmtRes = BoundInsStmt:bind_vars(?BIND_LIST),
    ?assertMatch(ok, BoundInsStmtRes),
    {rowids, RowIds} = BoundInsStmt:exec_stmt(
        [{ I
         , list_to_binary(["_publisher_",integer_to_list(I),"_"])
         , I+I/2
         , list_to_binary(["_hero_",integer_to_list(I),"_"])
         , list_to_binary(["_reality_",integer_to_list(I),"_"])
         , I
         , oci_test:edatetime_to_ora(erlang:now())
         , I
         } || I <- lists:seq(1, RowCount)]),
    ?assertMatch(RowCount, length(RowIds)),
    ?assertEqual(ok, BoundInsStmt:close()),

    ?ELog("selecting from table ~s", [?TESTTABLE]),
    SelStmt = OciSession:prep_sql(?SELECT_WITH_ROWID),
    ?assertMatch({?MODULE, statement, _, _}, SelStmt),
    {ok, Cols} = SelStmt:exec_stmt(),
    ?ELog("selected columns ~p from table ~s", [Cols, ?TESTTABLE]),
    ?assertEqual(10, length(Cols)),
    {{rows, Rows0}, false} = SelStmt:fetch_rows(2),
    {{rows, Rows1}, false} = SelStmt:fetch_rows(2),
    {{rows, Rows2}, true} = SelStmt:fetch_rows(2),
    ?assertEqual(ok, SelStmt:close()),

    ?ELog("update in table ~s", [?TESTTABLE]),
    Rows = Rows0 ++ Rows1 ++ Rows2,
    RowIDs = [lists:last(R) || R <- Rows],
    BoundUpdStmt = OciSession:prep_sql(?UPDATE),
    ?assertMatch({?MODULE, statement, _, _}, BoundUpdStmt),
    BoundUpdStmtRes = BoundUpdStmt:bind_vars(lists:keyreplace(<<":votes">>, 1, ?UPDATE_BIND_LIST, {<<":votes">>, 'SQLT_INT'})),
    ?assertMatch(ok, BoundUpdStmtRes),
    ?assertMatch({rowids, _}, BoundUpdStmt:exec_stmt([{ I
                            , list_to_binary(["_Publisher_",integer_to_list(I),"_"])
                            , I+I/3
                            , list_to_binary(["_Hero_",integer_to_list(I),"_"])
                            , list_to_binary(["_Reality_",integer_to_list(I),"_"])
                            , I+1
                            , oci_test:edatetime_to_ora(erlang:now())
                            , I+1
                            , Key
                            } || {Key, I} <- lists:zip(RowIDs, lists:seq(1, length(RowIDs)))])),
    ?assertEqual(ok, BoundUpdStmt:close()).

auto_rollback_test(OciSession) ->
    ?ELog("------------------------------------------------------------------"),
    ?ELog("|                         auto_rollback                          |"),
    ?ELog("------------------------------------------------------------------"),
    RowCount = 3,

    flush_table(OciSession),

    ?ELog("inserting into table ~s", [?TESTTABLE]),
    BoundInsStmt = OciSession:prep_sql(?INSERT),
    ?assertMatch({?MODULE, statement, _, _}, BoundInsStmt),
    BoundInsStmtRes = BoundInsStmt:bind_vars(?BIND_LIST),
    ?assertMatch(ok, BoundInsStmtRes),
    ?assertMatch({rowids, _},
    BoundInsStmt:exec_stmt([{ I
            , list_to_binary(["_publisher_",integer_to_list(I),"_"])
            , I+I/2
            , list_to_binary(["_hero_",integer_to_list(I),"_"])
            , list_to_binary(["_reality_",integer_to_list(I),"_"])
            , I
            , oci_test:edatetime_to_ora(erlang:now())
            , I
            } || I <- lists:seq(1, RowCount)], 1)),
    ?assertEqual(ok, BoundInsStmt:close()),

    ?ELog("selecting from table ~s", [?TESTTABLE]),
    SelStmt = OciSession:prep_sql(?SELECT_WITH_ROWID),
    ?assertMatch({?MODULE, statement, _, _}, SelStmt),
    {ok, Cols} = SelStmt:exec_stmt(),
    ?assertEqual(10, length(Cols)),
    {{rows, Rows}, false} = SelStmt:fetch_rows(RowCount),
    ?assertEqual(ok, SelStmt:close()),

    ?ELog("update in table ~s", [?TESTTABLE]),
    RowIDs = [lists:last(R) || R <- Rows],
    BoundUpdStmt = OciSession:prep_sql(?UPDATE),
    ?assertMatch({?MODULE, statement, _, _}, BoundUpdStmt),
    BoundUpdStmtRes = BoundUpdStmt:bind_vars(?UPDATE_BIND_LIST),
    ?assertMatch(ok, BoundUpdStmtRes),
    ?assertMatch({error, _}, BoundUpdStmt:exec_stmt([{ I
                            , list_to_binary(["_Publisher_",integer_to_list(I),"_"])
                            , I+I/3
                            , list_to_binary(["_Hero_",integer_to_list(I),"_"])
                            , list_to_binary(["_Reality_",integer_to_list(I),"_"])
                            , if I > (RowCount-2) -> <<"error">>; true -> integer_to_binary(I+1) end
                            , oci_test:edatetime_to_ora(erlang:now())
                            , I+1
                            , Key
                            } || {Key, I} <- lists:zip(RowIDs, lists:seq(1, length(RowIDs)))], 1)),
    ?assertMatch(ok, BoundUpdStmt:close()),

    ?ELog("testing rollback table ~s", [?TESTTABLE]),
    SelStmt1 = OciSession:prep_sql(?SELECT_WITH_ROWID),
    ?assertMatch({?MODULE, statement, _, _}, SelStmt1),
    ?assertEqual({ok, Cols}, SelStmt1:exec_stmt()),
    ?assertEqual({{rows, Rows}, false}, SelStmt1:fetch_rows(RowCount)),
    ?assertEqual(ok, SelStmt1:close()).

commit_rollback_test(OciSession) ->
    ?ELog("------------------------------------------------------------------"),
    ?ELog("|                      commit_rollback_test                      |"),
    ?ELog("------------------------------------------------------------------"),
    RowCount = 3,

    flush_table(OciSession),

    ?ELog("inserting into table ~s", [?TESTTABLE]),
    BoundInsStmt = OciSession:prep_sql(?INSERT),
    ?assertMatch({?MODULE, statement, _, _}, BoundInsStmt),
    BoundInsStmtRes = BoundInsStmt:bind_vars(?BIND_LIST),
    ?assertMatch(ok, BoundInsStmtRes),
    ?assertMatch({rowids, _},
    BoundInsStmt:exec_stmt([{ I
                            , list_to_binary(["_publisher_",integer_to_list(I),"_"])
                            , I+I/2
                            , list_to_binary(["_hero_",integer_to_list(I),"_"])
                            , list_to_binary(["_reality_",integer_to_list(I),"_"])
                            , I
                            , oci_test:edatetime_to_ora(erlang:now())
                            , I
                            } || I <- lists:seq(1, RowCount)], 1)),
    ?assertEqual(ok, BoundInsStmt:close()),

    ?ELog("selecting from table ~s", [?TESTTABLE]),
    SelStmt = OciSession:prep_sql(?SELECT_WITH_ROWID),
    ?assertMatch({?MODULE, statement, _, _}, SelStmt),
    {ok, Cols} = SelStmt:exec_stmt(),
    ?assertEqual(10, length(Cols)),
    {{rows, Rows}, false} = SelStmt:fetch_rows(RowCount),
    ?assertEqual(ok, SelStmt:close()),

    ?ELog("update in table ~s", [?TESTTABLE]),
    RowIDs = [lists:last(R) || R <- Rows],
    ?ELog("rowids ~p", [RowIDs]),
    BoundUpdStmt = OciSession:prep_sql(?UPDATE),
    ?assertMatch({?MODULE, statement, _, _}, BoundUpdStmt),
    BoundUpdStmtRes = BoundUpdStmt:bind_vars(?UPDATE_BIND_LIST),
    ?assertMatch(ok, BoundUpdStmtRes),
    ?assertMatch({rowids, _},
    BoundUpdStmt:exec_stmt([{ I
                            , list_to_binary(["_Publisher_",integer_to_list(I),"_"])
                            , I+I/3
                            , list_to_binary(["_Hero_",integer_to_list(I),"_"])
                            , list_to_binary(["_Reality_",integer_to_list(I),"_"])
                            , integer_to_binary(I+1)
                            , oci_test:edatetime_to_ora(erlang:now())
                            , I+1
                            , Key
                            } || {Key, I} <- lists:zip(RowIDs, lists:seq(1, length(RowIDs)))], -1)),

    ?assertMatch(ok, BoundUpdStmt:close()),

    ?ELog("testing rollback table ~s", [?TESTTABLE]),
    ?assertEqual(ok, OciSession:rollback()),
    SelStmt1 = OciSession:prep_sql(?SELECT_WITH_ROWID),
    ?assertMatch({?MODULE, statement, _, _}, SelStmt1),
    ?assertEqual({ok, Cols}, SelStmt1:exec_stmt()),
    {{rows, NewRows}, false} = SelStmt1:fetch_rows(RowCount),
    ?assertEqual(lists:sort(Rows), lists:sort(NewRows)),
    ?assertEqual(ok, SelStmt1:close()).

asc_desc_test(OciSession) ->
    ?ELog("------------------------------------------------------------------"),
    ?ELog("|                          asc_desc_test                         |"),
    ?ELog("------------------------------------------------------------------"),
    RowCount = 10,

    flush_table(OciSession),

    ?ELog("inserting into table ~s", [?TESTTABLE]),
    BoundInsStmt = OciSession:prep_sql(?INSERT),
    ?assertMatch({?MODULE, statement, _, _}, BoundInsStmt),
    BoundInsStmtRes = BoundInsStmt:bind_vars(?BIND_LIST),
    ?assertMatch(ok, BoundInsStmtRes),
    ?assertMatch({rowids, _},
    BoundInsStmt:exec_stmt([{ I
                            , list_to_binary(["_publisher_",integer_to_list(I),"_"])
                            , I+I/2
                            , list_to_binary(["_hero_",integer_to_list(I),"_"])
                            , list_to_binary(["_reality_",integer_to_list(I),"_"])
                            , I
                            , oci_test:edatetime_to_ora(erlang:now())
                            , I
                            } || I <- lists:seq(1, RowCount)], 1)),
    ?assertEqual(ok, BoundInsStmt:close()),

    ?ELog("selecting from table ~s", [?TESTTABLE]),
    SelStmt = OciSession:prep_sql(?SELECT_WITH_ORDER),
    ?assertMatch({?MODULE, statement, _, _}, SelStmt),
    {ok, Cols} = SelStmt:exec_stmt(),
    ?assertEqual(1, length(Cols)),

    {{rows, Rows1}, false} = SelStmt:fetch_rows(5),
    {{rows, Rows2}, false} = SelStmt:fetch_rows(5),
    {{rows, []}, true} = SelStmt:fetch_rows(1),

    ?assertEqual([binary_to_integer(R) || [R] <- Rows1++Rows2], lists:reverse(lists:seq(1,10))),

    ?assertEqual(ok, SelStmt:close()).

describe_test(OciSession) ->
    ?ELog("------------------------------------------------------------------"),
    ?ELog("|                         describe_test                          |"),
    ?ELog("------------------------------------------------------------------"),

    flush_table(OciSession),

    ?ELog("describing table ~s", [?TESTTABLE]),
    {ok, Descs} = OciSession:describe(list_to_binary(?TESTTABLE), 'OCI_PTYPE_TABLE'),
    ?assertEqual(9, length(Descs)),
    ?ELog("table ~s has ~p", [?TESTTABLE, Descs]).

-endif.
