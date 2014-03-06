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
%-define(WITH_VALGRIND, 1).

%% API
-export([
    start/1,
    start/2,
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
    keep_alive/2,
    close/1,
    close/2,
    echo/2
]).

-export([
    split_binds/2
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
    waiting_resp = false,
    logging = ?DBG_FLAG_OFF,
    logger,
    lastcmd
}).

-define(log(__Lgr,__Flag, __Format, __Args), if __Flag == ?DBG_FLAG_ON -> ?Info(__Lgr, __Format, __Args); true -> ok end).
% Port driver breaks on faster pipe access
%-define(DriverSleep, timer:sleep(100)).
-define(DriverSleep, ok).

-ifdef(TEST).
-define(LOGFUN,
    fun
        ({Lvl, File, Fun, Line, Msg}) -> io:format(user, ?T++"[~p] {~s,~s,~p} ~s~n", [Lvl, File, Fun, Line, Msg]);
        ({Lvl, File, Fun, Line, Msg, Term}) ->
            STerm = case Term of
                "" -> "";
                Term when is_list(Term) -> Term;
                _ -> lists:flatten(io_lib:format("~p", [Term]))
            end,
            io:format(user, ?T++"[~p] {~s,~s,~p} ~s : ~s~n", [Lvl, File, Fun, Line, Msg, STerm]);
        (Log) when is_list(Log) -> io:format(user, "~s", [Log]);
        (Log) -> io:format(user, ?T++".... ~p~n", [Log])
    end).
-else.
-define(LOGFUN,
    fun
        ({Lvl, File, Fun, Line, Msg}) -> io:format(user, ?T++"[~p] ["++?LOG_TAG++"] {~s,~s,~p} ~s~n", [Lvl, File, Fun, Line, Msg]);
        ({Lvl, File, Fun, Line, Msg, Term}) ->
            STerm = case Term of
                "" -> "";
                Term when is_list(Term) -> Term;
                _ -> lists:flatten(io_lib:format("~p", [Term]))
            end,
            io:format(user, ?T++"[~p] ["++?LOG_TAG++"] {~s,~s,~p} ~s : ~s~n", [Lvl, File, Fun, Line, Msg, STerm]);
        (Log) when is_list(Log) -> io:format(user, ?T++"["++?LOG_TAG++"] ~s~n", [Log]);
        (Log) -> io:format(user, ?T++"["++?LOG_TAG++"] ~p~n", [Log])
    end).
-endif.

%% External API
start(Options) -> start_link(Options, ?LOGFUN).
start(Options,LogFun) ->
    start(start,Options,LogFun).

start_link(Options) -> start_link(Options, ?LOGFUN).
start_link(Options,LogFun) ->
    start(start_link,Options,LogFun).

start(Type,Options,LogFun) ->
    {ok, LSock} = gen_tcp:listen(0, [binary, {packet, 0}, {active, false}, {ip, {127,0,0,1}}]),
    {ok, ListenPort} = inet:port(LSock),
    AcceptLogFun = fun(Logger) -> Logger:accept(LSock, LogFun) end,
    StartRes = case Options of
        undefined ->
            gen_server:Type(?MODULE, [false, ListenPort, AcceptLogFun], []);
        Options when is_list(Options)->
            Logging = proplists:get_value(logging, Options, false),
            gen_server:Type(?MODULE, [Logging, ListenPort, AcceptLogFun], [])
    end,
    case StartRes of
        {ok, Pid} -> {?MODULE, Pid};
        Error -> throw({error, Error})
    end.

stop(PortPid) ->
    gen_server:call(PortPid, stop).

logging(enable, {?MODULE, PortPid}) ->
    gen_server:call(PortPid, {port_call, [?RMOTE_MSG, ?DBG_FLAG_ON]}, ?PORT_TIMEOUT);
logging(disable, {?MODULE, PortPid}) ->
    gen_server:call(PortPid, {port_call, [?RMOTE_MSG, ?DBG_FLAG_OFF]}, ?PORT_TIMEOUT).

keep_alive(KeepAlive, {?MODULE, PortPid}) ->
    gen_server:call(PortPid, {keep_alive, KeepAlive}, ?PORT_TIMEOUT).

echo(Term, {?MODULE, PortPid}) ->
    case gen_server:call(PortPid, {port_call, [?CMD_ECHOT, Term]}, ?PORT_TIMEOUT) of
        {error, Error} -> {error, Error};
        Return -> Return
    end.

get_session(Tns, Usr, Pswd, {?MODULE, PortPid})
when is_binary(Tns); is_binary(Usr); is_binary(Pswd) ->
    case gen_server:call(PortPid, {port_call, [?GET_SESSN, Tns, Usr, Pswd]}, ?PORT_TIMEOUT) of
        {error, Error} -> {error, Error};
        SessionId -> {?MODULE, PortPid, SessionId}
    end.

close({?MODULE, statement, _, _, _} = Ctx)  -> close(ignore_port, Ctx);
close({?MODULE, _, _} = Ctx)                -> close(ignore_port, Ctx);
close({?MODULE, PortPid}) ->
    io:format(user, "port close ~n", []),
    gen_server:call(PortPid, close, ?PORT_TIMEOUT).

close(port_close, {?MODULE, statement, PortPid, _SessionId, _StmtId}) ->
    close({?MODULE, PortPid});
close(port_close, {?MODULE, PortPid, _SessionId}) ->
    close({?MODULE, PortPid});
close(_, {?MODULE, statement, PortPid, SessionId, StmtId}) ->
    gen_server:call(PortPid, {port_call, [?CLSE_STMT, SessionId, StmtId]}, ?PORT_TIMEOUT);
close(_, {?MODULE, PortPid, SessionId}) ->
    gen_server:call(PortPid, {port_call, [?PUT_SESSN, SessionId]}, ?PORT_TIMEOUT).

bind_vars(BindVars, {?MODULE, statement, PortPid, SessionId, StmtId}) when is_list(BindVars) ->
    TranslatedBindVars = [{K, ?CT(V)} || {K,V} <- BindVars],
    R = gen_server:call(PortPid, {port_call, [?BIND_ARGS, SessionId, StmtId, TranslatedBindVars]}, ?PORT_TIMEOUT),
    ?DriverSleep,
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
        {desc, Descs} -> {ok, [{N,?CS(T),Sz} || {N,T,Sz} <- Descs]};
        R -> R
    end.

prep_sql(Sql, {?MODULE, PortPid, SessionId}) when is_list(Sql) ->
    prep_sql(iolist_to_binary(Sql), {?MODULE, PortPid, SessionId});
prep_sql(Sql, {?MODULE, PortPid, SessionId}) when is_binary(Sql) ->
    R = gen_server:call(PortPid, {port_call, [?PREP_STMT, SessionId, Sql]}, ?PORT_TIMEOUT),
    ?DriverSleep,
    case R of
        {stmt,StmtId} -> {?MODULE, statement, PortPid, SessionId, StmtId};
        R -> R
    end.

% AutoCommit is default set to true
exec_stmt({?MODULE, statement, PortPid, SessionId, StmtId}) ->
    exec_stmt([], 1, {?MODULE, statement, PortPid, SessionId, StmtId}).
exec_stmt(BindVars, {?MODULE, statement, PortPid, SessionId, StmtId}) ->
    exec_stmt(BindVars, 1, {?MODULE, statement, PortPid, SessionId, StmtId}).
exec_stmt(BindVars, AutoCommit, {?MODULE, statement, PortPid, SessionId, StmtId}) ->
    GroupedBindVars = split_binds(BindVars,?MAX_REQ_SIZE),
    collect_grouped_bind_request(GroupedBindVars, PortPid, SessionId, StmtId, AutoCommit, []).

collect_grouped_bind_request([], _, _, _, _, Acc) ->
    UniqueResponses = sets:to_list(sets:from_list(Acc)),
    Results = lists:foldl(fun({K, Vs}, Res) ->
                                  case lists:keyfind(K, 1, Res) of
                                      {K, Vals} -> lists:keyreplace(K, 1, Res, {K, Vals++Vs});
                                      false -> [{K, Vs} | Res]
                                  end
                          end,
                          [],
                          UniqueResponses),
    case Results of
        [Result] -> Result;
        _ -> Results
    end;
collect_grouped_bind_request([BindVars|GroupedBindVars], PortPid, SessionId, StmtId, AutoCommit, Acc) ->
    NewAutoCommit = if length(GroupedBindVars) > 0 -> 0; true -> AutoCommit end,
    %if length(BindVars) > 0 -> io:format(user,"TX rows ~p~n", [length(BindVars)]); true -> ok end,
    R = gen_server:call(PortPid, {port_call, [?EXEC_STMT, SessionId, StmtId, BindVars, NewAutoCommit]}, ?PORT_TIMEOUT),
    ?DriverSleep,
    case R of
        {error, Error}  -> {error, Error};
        {cols, Clms}    -> collect_grouped_bind_request( GroupedBindVars, PortPid, SessionId, StmtId, AutoCommit
                                                       , [{cols, [{N,?CS(T),Sz,P,Sc} || {N,T,Sz,P,Sc} <- Clms]} | Acc]);
        R               -> collect_grouped_bind_request(GroupedBindVars, PortPid, SessionId, StmtId, AutoCommit, [R | Acc])
    end.

split_binds(BindVars,MaxReqSize)    -> split_binds(BindVars, MaxReqSize, length(BindVars), []).
split_binds(BindVars, _, 0, [])     -> [BindVars];
split_binds(BindVars, _, 0, Acc)    -> lists:reverse([B || B <- [BindVars | Acc], length(B) > 0]);
split_binds([], _, _, Acc)          -> lists:reverse([B || B <- Acc, length(B) > 0]);
split_binds(BindVars, MaxReqSize, At, Acc) when is_list(BindVars) ->
    {Head,Tail} = lists:split(At, BindVars),
    ReqSize = byte_size(term_to_binary(Head)),
    if
        ReqSize > MaxReqSize ->
            NewAt = round(At / (ReqSize / MaxReqSize)),
            %io:format(user,"req size ~p, max ~p -- BindVar(~p) splitting at ~p~n", [ReqSize, MaxReqSize, length(BindVars), NewAt]),
            split_binds(BindVars, MaxReqSize, NewAt, Acc);
        true ->
            split_binds(Tail, MaxReqSize, length(Tail), [lists:reverse(Head)|Acc])
    end.

fetch_rows(Count, {?MODULE, statement, PortPid, SessionId, StmtId}) ->
    case gen_server:call(PortPid, {port_call, [?FTCH_ROWS, SessionId, StmtId, Count]}, ?PORT_TIMEOUT) of
        %%{{rows, Rows}, Completed} -> {{rows, lists:reverse(Rows)}, Completed};
        {{rows, Rows}, Completed} -> {{rows, Rows}, Completed};
        Other -> Other
    end.

%% Callbacks
init([Logging, ListenPort, AcceptLogFun]) ->
    PortLogger = oci_logger:start_link(),
    PrivDir = case code:priv_dir(erloci) of
        {error,_} -> "./priv/";
        PDir -> PDir
    end,
    case os:find_executable(?EXE_NAME, PrivDir) of
        false ->
            case os:find_executable(?EXE_NAME, "./deps/erloci/priv/") of
                false -> {stop, bad_executable};
                Executable ->
                    Ret = start_exe(Executable, Logging, ListenPort, PortLogger),
                    AcceptLogFun(PortLogger),
                    Ret
            end;
        Executable ->
            Ret = start_exe(Executable, Logging, ListenPort, PortLogger),
            AcceptLogFun(PortLogger),
            Ret
    end.

start_exe(Executable, Logging, ListenPort, PortLogger) ->
    OciLibs = case os:type() of
	    {unix,darwin}   -> ["libocci.dylib"];
        {win32,nt}      -> ["oci.dll"];
	    _               -> ["libocci.so"]
    end,
    {ok, OciDir} = case os:getenv("INSTANT_CLIENT_LIB_PATH") of
        false -> {error, "INSTANT_CLIENT_LIB_PATH not defined"};
        OCIRuntimeLibraryPath ->
            case
               lists:any(fun(F) ->
                            case filelib:is_file(filename:join([OCIRuntimeLibraryPath, F])) of
                                false -> true;
                                _ -> false
                            end
                         end,
                        OciLibs) of
                true -> {error, "Some required runtime libraries missing at "++OCIRuntimeLibraryPath};
                _ -> {ok, OCIRuntimeLibraryPath}
            end
    end,
    {LibPath, PathSepStr} = case os:type() of
	    {unix,darwin}   -> {"DYLD_LIBRARY_PATH", ":"};
        {win32,nt}      -> {"PATH", ";"};
	    _               -> {"LD_LIBRARY_PATH", ":"}
    end,
    NewLibPath = case os:getenv(LibPath) of
        false -> "";
        LdLibPath -> LdLibPath ++ PathSepStr
    end ++ OciDir,

    LibPathVal = lists:last(re:split(re:replace(NewLibPath, "~", "~~", [global, {return, list}]), "["++PathSepStr++"]", [{return, list}])),
    ?Info(PortLogger, "~s = ...~s", [LibPath, LibPathVal]),
    PortOptions = [ {packet, 4}
                  , binary
                  , exit_status
                  , use_stdio
                  , {args, [ integer_to_list(?MAX_REQ_SIZE)
                           , "true"
                           , integer_to_list(ListenPort)]}
                  , {env, [{LibPath, NewLibPath}]}
                  ],
    io:format(user, "Executable ~p~nOptions :~p~n", [Executable, PortOptions]),
    case (catch portstart(Executable, PortOptions)) of
        {'EXIT', Reason} ->
            ?Error(PortLogger, "oci could not open port: ~p", [Reason]),
            {stop, Reason};
        Port ->
%            timer:sleep(10000),
            %% TODO -- Logging is turned after port creation for the integration tests to run
            case Logging of
                true ->
                    port_command(Port, term_to_binary({undefined, ?RMOTE_MSG, ?DBG_FLAG_ON})),
                    ?Info(PortLogger, "started log enabled new port:~n~p", [erlang:port_info(Port)]),
                    {ok, #state{port=Port, logging=?DBG_FLAG_ON, logger=PortLogger}};
                false ->
                    port_command(Port, term_to_binary({undefined, ?RMOTE_MSG, ?DBG_FLAG_OFF})),
                    ?Info(PortLogger, "started log disabled new port:~n~p", [erlang:port_info(Port)]),
                    {ok, #state{port=Port, logging=?DBG_FLAG_OFF, logger=PortLogger}}
            end
    end.

-ifdef(WITH_VALGRIND).
portstart(Executable, PortOptions) ->
    Args = proplists:get_value(args, PortOptions),
    NewArgs = ["--leak-check=yes", Executable | Args],
    NewExecutable = "/usr/bin/valgrind",
    NewPortOptions = lists:keyreplace(args, 1, PortOptions, {args, NewArgs}),
    io:format(user, "NewExecutable ~p, NewPortOptions = ~p~n", [NewExecutable, NewPortOptions]),
    open_port({spawn_executable, NewExecutable}, NewPortOptions).
-else.
portstart(Executable, PortOptions) ->
    open_port({spawn_executable, Executable}, PortOptions).
-endif.

handle_call(close, _From, #state{port=Port} = State) ->
    try
        true = erlang:port_close(Port)
    catch
        _:R -> error_logger:error_report("Port close failed with reason: ~p~n", [R])
    end,
    {stop, normal, ok, State};
handle_call({port_call, Msg}, From, #state{port=Port, logger=_PortLogger} = State) ->
    Cmd = [if From /= undefined -> term_to_binary(From); true -> From end | Msg],
    CmdTuple = list_to_tuple(Cmd),
    BTerm = term_to_binary(CmdTuple),
    %?Debug(_PortLogger, "TX (~p):~n---~n~p~n---~n~w~n---", [byte_size(BTerm), Cmd, BTerm]),
    %?Debug(_PortLogger, "TX (~p):~n---~n~p~n---~n~s~n---", [byte_size(BTerm), Cmd, oci_logger:bin2str(BTerm)]),
    %?Debug(_PortLogger, "TX (~p)", [integer_to_list(byte_size(BTerm),16)]),
    true = port_command(Port, BTerm),
    {noreply, State#state{waiting_resp=true, lastcmd=CmdTuple}}.

handle_cast(Msg, State) ->
    error_logger:error_report("ORA: received unexpected cast: ~p~n", [Msg]),
    {noreply, State}.

%% We got a reply from a previously sent command to the Port.  Relay it to the caller.
handle_info({Port, {data, Data}}, #state{port=Port, logger=L} = State) when is_binary(Data) andalso (byte_size(Data) > 0) ->    
    Resp = binary_to_term(Data),
    case handle_result(State#state.logging, Resp, L) of
        {undefined, Result} -> ?Info(L,"no reply for ~p", [Result]);
        {From, {error, Reason}} ->
            ?Error(L, "~p", [Reason]), % Just in case its ignored later
            gen_server:reply(binary_to_term(From), {error, Reason});
        {From, Result} ->
            gen_server:reply(binary_to_term(From), Result) % regular reply
    end,
    {noreply, State#state{waiting_resp=false}};
handle_info({Port, {exit_status, Status}}, #state{port = Port, logger = Logger} = State) ->
    ?log(Logger, State#state.logging, "port ~p exited with status ~p", [Port, Status]),
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

terminate(Reason, #state{port=Port, logger=L, lastcmd=LastCmd}) ->
    ?Error(L, "Terminating ~p: last ~p", [Reason, LastCmd]),
    catch port_close(Port),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% log on and off -- mostly called from an undefined context so logged the status here
handle_result(L, {Ref, ?RMOTE_MSG, En} = _Resp, Lgr) ->
    ?log(Lgr, L, "Remote ~p", [En]),
    {Ref, En};

% port error handling
handle_result(L, {Ref, Cmd, error, Reason}, Lgr) ->
    ?log(Lgr, L, "RX: ~p ~s error ~p", [Ref,?CMDSTR(Cmd), Reason]),
    {Ref, {error, Reason}};

% generic command handling
handle_result(_L, {Ref, _Cmd, Result}, _Lgr) ->
    %?log(_Lgr, _L, "RX: ~s -> ~p", [?CMDSTR(_Cmd), Result]),
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
signal_str(N)  -> {undefined,       ignore, N}.

%
% Eunit tests
%
-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-include("oci_test.hrl").

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
    ?assertMatch({?MODULE, statement, _, _, _}, DropStmt),
    % If table doesn't exists the handle isn't valid
    % Any error is ignored anyway
    case DropStmt:exec_stmt() of
        {error, _} -> ok; 
        _ -> ?assertEqual(ok, DropStmt:close())
    end,
    ?ELog("creating table ~s", [?TESTTABLE]),
    StmtCreate = OciSession:prep_sql(?CREATE),
    ?assertMatch({?MODULE, statement, _, _, _}, StmtCreate),
    ?assertEqual({executed, 0}, StmtCreate:exec_stmt()),
    ?assertEqual(ok, StmtCreate:close()).

drop_create({_, OciSession}) ->
    ?ELog("+----------------------------------------------------------------+"),
    ?ELog("|                            drop_create                         |"),
    ?ELog("+----------------------------------------------------------------+"),

    ?ELog("creating (drop if exists) table ~s", [?TESTTABLE]),
    TmpDropStmt = OciSession:prep_sql(?DROP),
    ?assertMatch({?MODULE, statement, _, _, _}, TmpDropStmt),
    case TmpDropStmt:exec_stmt() of
        {error, _} -> ok; % If table doesn't exists the handle isn't valid
        _ -> ?assertEqual(ok, TmpDropStmt:close())
    end,
    StmtCreate = OciSession:prep_sql(?CREATE),
    ?assertMatch({?MODULE, statement, _, _, _}, StmtCreate),
    ?assertEqual({executed, 0}, StmtCreate:exec_stmt()),
    ?assertEqual(ok, StmtCreate:close()),

    ?ELog("dropping table ~s", [?TESTTABLE]),
    DropStmt = OciSession:prep_sql(?DROP),
    ?assertMatch({?MODULE, statement, _, _, _}, DropStmt),
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
    ?assertMatch({?MODULE, statement, _, _, _}, BoundInsStmt),
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
    ?assertMatch({?MODULE, statement, _, _, _}, SelStmt),
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
    ?assertMatch({?MODULE, statement, _, _, _}, BoundUpdStmt),
    BoundUpdStmtRes = BoundUpdStmt:bind_vars(lists:keyreplace(<<":votes">>, 1, ?UPDATE_BIND_LIST, {<<":votes">>, 'SQLT_INT'})),
    ?assertMatch(ok, BoundUpdStmtRes),
    ?assertMatch({rowids, _}, BoundUpdStmt:exec_stmt([{ I
                            , list_to_binary(["_Publisher_",integer_to_list(I),"_"])
                            , I+I/3
                            , list_to_binary(["_Hero_",integer_to_list(I),"_"])
                            , list_to_binary(["_Reality_",integer_to_list(I),"_"])
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
    ?assertMatch({?MODULE, statement, _, _, _}, BoundInsStmt),
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
    ?assertMatch({?MODULE, statement, _, _, _}, SelStmt),
    {cols, Cols} = SelStmt:exec_stmt(),
    ?assertEqual(10, length(Cols)),
    {{rows, Rows}, false} = SelStmt:fetch_rows(RowCount),
    ?assertEqual(ok, SelStmt:close()),

    ?ELog("update in table ~s", [?TESTTABLE]),
    RowIDs = [R || [R|_] <- Rows],
    BoundUpdStmt = OciSession:prep_sql(?UPDATE),
    ?assertMatch({?MODULE, statement, _, _, _}, BoundUpdStmt),
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
    ?assertMatch({?MODULE, statement, _, _, _}, SelStmt1),
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
    ?assertMatch({?MODULE, statement, _, _, _}, BoundInsStmt),
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
    ?assertMatch({?MODULE, statement, _, _, _}, SelStmt),
    {cols, Cols} = SelStmt:exec_stmt(),
    ?assertEqual(10, length(Cols)),
    {{rows, Rows}, false} = SelStmt:fetch_rows(RowCount),
    ?assertEqual(RowCount, length(Rows)),
    ?assertEqual(ok, SelStmt:close()),

    ?ELog("update in table ~s", [?TESTTABLE]),
    RowIDs = [R || [R|_] <- Rows],
    ?ELog("rowids ~p", [RowIDs]),
    BoundUpdStmt = OciSession:prep_sql(?UPDATE),
    ?assertMatch({?MODULE, statement, _, _, _}, BoundUpdStmt),
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
    ?assertMatch({?MODULE, statement, _, _, _}, SelStmt1),
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
    ?assertMatch({?MODULE, statement, _, _, _}, BoundInsStmt),
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
    ?assertMatch({?MODULE, statement, _, _, _}, SelStmt1),
    SelStmt2 = OciSession:prep_sql(?SELECT_ROWID_DESC),
    ?assertMatch({?MODULE, statement, _, _, _}, SelStmt2),
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
    ?assertMatch({?MODULE, statement, _, _, _}, CreateFunction),
    ?assertEqual({executed, 0}, CreateFunction:exec_stmt()),
    ?assertEqual(ok, CreateFunction:close()),

    SelectStmt = OciSession:prep_sql(<<"select "?TESTFUNCTION"(10,30) from dual">>),
    ?assertMatch({?MODULE, statement, _, _, _}, SelectStmt),
    {cols, [Col|_]} = SelectStmt:exec_stmt(),
    ?assertEqual(<<?TESTFUNCTION"(10,30)">>, element(1, Col)),
    {{rows, [[F|_]|_]}, true} = SelectStmt:fetch_rows(2),
    ?assertEqual(<<3,194,38,21,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0>>, F),
    ?assertEqual(ok, SelectStmt:close()),

    SelectBoundStmt = OciSession:prep_sql(<<"select "?TESTFUNCTION"(:SAL,:COM) from dual">>),
    ?assertMatch({?MODULE, statement, _, _, _}, SelectBoundStmt),
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

-endif.
