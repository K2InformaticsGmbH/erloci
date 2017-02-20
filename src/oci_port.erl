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
    start_link/2,
    stop/1,
    logging/2,
    get_session/4,
    describe/3,
    prep_sql/2,
    ping/1,
    commit/1,
    rollback/1,
    bind_vars/2,
    lob/4,
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
    lastcmd,
    ping_timeout,
    ping_tref = undefined
}).

-define(log(__Lgr,__Flag, __Format, __Args), if __Flag == ?DBG_FLAG_ON -> ?Info(__Lgr, __Format, __Args); true -> ok end).
% Port driver breaks on faster pipe access
%-define(DriverSleep, timer:sleep(100)).
-define(DriverSleep, ok).

start_link(Options,LogFun) ->
    {ok, LSock} = gen_tcp:listen(0, [binary, {packet, 0}, {active, false}, {ip, {127,0,0,1}}]),
    {ok, ListenPort} = inet:port(LSock),
    case Options of
        undefined ->
            gen_server:start_link(?MODULE, [false, ListenPort, LSock, LogFun, []], []);
        Options when is_list(Options)->
            Logging = proplists:get_value(logging, Options, false),
            gen_server:start_link(?MODULE, [Logging, ListenPort, LSock, LogFun, Options], [])
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
        SessionId ->
            PortPid ! {check_sess, SessionId},
            {?MODULE, PortPid, SessionId}
    end.

close({?MODULE, statement, _, _, _} = Ctx)  -> close(ignore_port, Ctx);
close({?MODULE, _, _} = Ctx)                -> close(ignore_port, Ctx);
close({?MODULE, PortPid}) ->
    gen_server:call(PortPid, close, ?PORT_TIMEOUT).

close(port_close, {?MODULE, statement, PortPid, _SessionId, _StmtId}) ->
    close({?MODULE, PortPid});
close(port_close, {?MODULE, PortPid, _SessionId}) ->
    close({?MODULE, PortPid});
close(_, {?MODULE, statement, PortPid, SessionId, StmtId}) ->
    gen_server:call(PortPid, {port_call, [?CLSE_STMT, SessionId, StmtId]}, ?PORT_TIMEOUT);
close(_, {?MODULE, PortPid, SessionId}) ->
    gen_server:call(PortPid, {port_call, [?PUT_SESSN, SessionId]}, ?PORT_TIMEOUT).

lob(LobHandle, Offset, Length, {?MODULE, statement, PortPid, SessionId, StmtId})
  when is_integer(LobHandle)
       andalso (Length > 0)
       andalso (Offset > 0) ->
    R = gen_server:call(PortPid, {port_call, [?GET_LOBDA, SessionId, StmtId, LobHandle, Offset, Length]}, ?PORT_TIMEOUT),
    ?DriverSleep,
    case R of
        ok -> ok;
        R -> R
    end.

bind_vars(BindVars, {?MODULE, statement, PortPid, SessionId, StmtId}) when is_list(BindVars) ->
    TranslatedBindVars = [case BV of
                              {K,V}     -> {K, ?AD(in), ?CT(V)};
                              {K,D,V}   -> {K, ?AD(D),  ?CT(V)}
                          end || BV <- BindVars],
    R = gen_server:call(PortPid, {port_call, [?BIND_ARGS, SessionId, StmtId, TranslatedBindVars]}, ?PORT_TIMEOUT),
    ?DriverSleep,
    case R of
        ok -> ok;
        R -> R
    end.

ping({?MODULE, PortPid, SessionId}) ->
    case catch gen_server:call(PortPid, {port_call, [?SESN_PING, SessionId]}, ?PORT_TIMEOUT) of
        ok -> pong;
        _  -> pang
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
        {executed, _} -> R;
        {executed, C, OutVars} ->
            {executed, C, [
                case OV of
                    {N,{cursor, SessionId, NewStmtId}} -> {N, {?MODULE, statement, PortPid, SessionId, NewStmtId}};
                    Other -> Other
                end
             || OV <- OutVars]};
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
init([Logging, ListenPort, LSock, LogFun, Options]) ->
    PortLogger = oci_logger:start_link(LSock, LogFun, Options),
    PrivDir = case code:priv_dir(erloci) of
        {error,_} -> "./priv/";
        PDir -> PDir
    end,
    case os:find_executable(?EXE_NAME, PrivDir) of
        false ->
            case os:find_executable(?EXE_NAME, "./deps/erloci/priv/") of
                false -> {stop, bad_executable};
                Executable ->
                    Ret = start_exe(Executable, Logging, ListenPort, PortLogger, Options),
                    PortLogger:accept(),
                    Ret
            end;
        Executable ->
            Ret = start_exe(Executable, Logging, ListenPort, PortLogger, Options),
            PortLogger:accept(),
            Ret
    end.

verify_runtime_lib_path([],_OciLibs) ->
    error("Some required runtime libraries not found");
verify_runtime_lib_path([Path|Paths],OciLibs) ->
    case
        lists:any(
          fun(F) ->
                  Fname = filename:join([Path, F]),
                  case {file:read_link(Fname), filelib:is_file(Fname)} of
                      {{error, _}, false} -> true;  %% not symlink not file
                      _ -> false
                  end
          end,
          OciLibs) of
        true ->
            verify_runtime_lib_path(Paths,OciLibs);
        _ ->
            {ok, Path}
    end.

start_exe(Executable, Logging, ListenPort, PortLogger, Options) ->
    OciLibs = case os:type() of
	    {unix,darwin}   -> ["libocci.dylib"];
        {win32,nt}      -> ["oci.dll","oraons.dll","oraociei12.dll"];
	    _               -> ["libocci.so"]
    end,
    ExePath = filename:dirname(Executable),
    {ok, OciDir} = verify_runtime_lib_path(
                     [ExePath | case os:getenv("INSTANT_CLIENT_LIB_PATH") of
                                    false -> [];
                                    InstClientLibPath -> [InstClientLibPath]
                                end], OciLibs),
    {LibPath, PathSepStr} = case os:type() of
	    {unix,darwin}   -> {"DYLD_LIBRARY_PATH", ":"};
        {win32,nt}      -> {"PATH", ";"};
	    _               -> {"LD_LIBRARY_PATH", ":"}
    end,
    NewLibPath = case {OciDir, os:type()} of
                     {ExePath, {win32,nt}}  -> "";
                     {OciDir, _} ->
                         case os:getenv(LibPath) of
                             false -> "";
                             LdLibPath -> LdLibPath ++ PathSepStr
                         end ++ OciDir
                 end,
    LibPathVal = lists:last(
                   re:split(
                     re:replace(NewLibPath, "~", "~~", [global, {return, list}]),
                     "["++PathSepStr++"]", [{return, list}])),
    ?Debug(PortLogger, "~s = ...~s", [LibPath, LibPathVal]),
    Envs = proplists:get_value(env, Options, []),
    PingTimeout = proplists:get_value(ping_timeout, Options, 0),
    case proplists:get_value(pstate, Options, '$none') of
        ProcessState when is_map(ProcessState) ->
            maps:map(fun(K, V) -> put(K, V), V end, ProcessState);
        _ -> ok
    end,
    ?Debug(PortLogger, "Extra Env :~p", [Envs]),
    PortOptions = [ {packet, 4}
                  , binary
                  , exit_status
                  , use_stdio
                  , {args, [ integer_to_list(?MAX_REQ_SIZE)
                           , "true"
                           , integer_to_list(ListenPort)]}
                  , {env, [{LibPath, NewLibPath}|Envs]}
                  ],
    ?Debug(PortLogger, "Executable ~p", [Executable]),
    ?Debug(PortLogger, "Options :~p", [PortOptions]),
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
                    ?Debug(PortLogger, "started log enabled new port:~n~p", [erlang:port_info(Port)]),
                    {ok, #state{port=Port, logging=?DBG_FLAG_ON, logger=PortLogger, ping_timeout = PingTimeout}};
                false ->
                    port_command(Port, term_to_binary({undefined, ?RMOTE_MSG, ?DBG_FLAG_OFF})),
                    ?Debug(PortLogger, "started log disabled new port:~n~p", [erlang:port_info(Port)]),
                    {ok, #state{port=Port, logging=?DBG_FLAG_OFF, logger=PortLogger, ping_timeout = PingTimeout}}
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
    erloci:del(self()),
    {reply, ok, State};
handle_call({port_call, Msg}, From, #state{ping_timeout = PingInterval, ping_tref = PTref,
                                           port=Port, logger=_PortLogger} = State) ->
    Cmd = [if From /= undefined -> term_to_binary(From); true -> From end | Msg],
    CmdTuple = list_to_tuple(Cmd),
    BTerm = term_to_binary(CmdTuple),
    %?Debug(_PortLogger, "TX (~p):~n---~n~p~n---~n~w~n---", [byte_size(BTerm), Cmd, BTerm]),
    %?Debug(_PortLogger, "TX (~p):~n---~n~p~n---~n~s~n---", [byte_size(BTerm), Cmd, oci_logger:bin2str(BTerm)]),
    %?Debug(_PortLogger, "TX (~p)", [integer_to_list(byte_size(BTerm),16)]),
    true = port_command(Port, BTerm),
    NewPTref = case Msg of
                   [_, SessionId | _] when is_integer(SessionId) ->
                       reset_ping_timer(PingInterval, PTref, SessionId);
                   _ -> PTref
               end,
    {noreply, State#state{waiting_resp=true, lastcmd=CmdTuple, ping_tref = NewPTref}}.

handle_cast(Msg, State) ->
    error_logger:error_report("ORA: received unexpected cast: ~p~n", [Msg]),
    {noreply, State}.

%% We got a reply from a previously sent command to the Port.  Relay it to the caller.
handle_info({Port, {data, Data}}, #state{port=Port, logger=L, ping_tref = PTref} = State) when is_binary(Data) andalso (byte_size(Data) > 0) ->
    Resp = binary_to_term(Data),
    NewPTref =
    case handle_result(State#state.logging, Resp, L) of
        {undefined, Result} ->
            ?Debug(L,"no reply for ~p", [Result]),
            PTref;
        {Info, Result} ->
            case {binary_to_term(Info), Result} of
                {{ping, SessionId}, ok} ->
                    erlang:send_after(State#state.ping_timeout, self(), {check_sess, SessionId});
                {{ping, SessionId}, {error, _Reason}} ->
                    try
                        true = erlang:port_close(Port)
                    catch
                        _:R -> error_logger:error_report("Port close failed with reason: ~p~n", [R])
                    end,
                    erloci:del(self()),
                    PTref;
                {From, {error, Reason}} ->
                    gen_server:reply(From, {error, Reason}),
                    PTref;
                {From, Result} ->
                    gen_server:reply(From, Result), % regular reply
                    PTref
            end
    end,
    {noreply, State#state{waiting_resp=false, lastcmd=undefined, ping_tref = NewPTref}};
handle_info({Port, {exit_status, Status}}, #state{port = Port, logger = L} = State) ->
    case Status of
        0 ->
            ?log(L, State#state.logging, "port ~p exited with status ~p", [Port, Status]),
            erloci:del(self()),
            {noreply, State};
        Other ->
           ?Error(L, "~p abnormal termination ~p", [Port, signal_str(Other-128)]),
            erloci:del(self()),
           {noreply, State}
    end;
handle_info({check_sess, _}, #state{ping_timeout = 0} = State) ->
    {noreply, State};
handle_info({check_sess, SessionId}, #state{port = Port} = State) ->
    CmdTuple = list_to_tuple([term_to_binary({ping, SessionId}), ?SESN_PING, SessionId]),
    BTerm = term_to_binary(CmdTuple),
    true = port_command(Port, BTerm),
    {noreply, State#state{waiting_resp=true, lastcmd=CmdTuple}};
%% Catch all - throws away unknown messages (This could happen by "accident"
%% so we do not want to crash, but we make a log entry as it is an
%% unwanted behaviour.)
handle_info(Info, State) ->
    error_logger:error_report("ORA: received unexpected info: ~p~n", [Info]),
    {noreply, State}.

terminate(Reason, #state{port=Port, logger=L, lastcmd=LastCmd}) ->
    Cmd = lists:flatten(if
        LastCmd /= undefined -> io_lib:format(" with Command on flight ~p", [LastCmd]);
        true -> ""
    end),
    if Reason == normal orelse Reason == shutdown ->
           ?Info(L, "~p terminate ~p with COF ~s", [Port, Reason, Cmd]);
       true ->
           ?Error(L, "Abnormal termination of ~p~s", [Reason, Cmd])
    end,
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

reset_ping_timer(0, _, _) -> undefined;
reset_ping_timer(PingInterval, TRef, SessionId) when is_reference(TRef) ->
    erlang:cancel_timer(TRef),
    reset_ping_timer(PingInterval, SessionId);
reset_ping_timer(PingInterval, _TRef, SessionId) ->
    reset_ping_timer(PingInterval, SessionId).

reset_ping_timer(PingInterval, SessionId) ->
    erlang:send_after(PingInterval, self(), {check_sess, SessionId}).

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
