%% Copyright 2012 K2Informatics GmbH, Root Laengenbold, Switzerland
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
    stop/1,
    logging/2,
    get_session/4,
    prep_sql/2,
    exec_stmt/1,
    fetch_rows/2,
    close/1,
    inject_rowid/1
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
    {ok, LSock} = gen_tcp:listen(0, [binary, {packet, 0}, {active, false}]),
    {ok, ListenPort} = inet:port(LSock),
    spawn(fun() -> server(LSock) end),
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
    gen_server:call(PortPid, {port_call, {?RMOTE_MSG, ?DBG_FLAG_ON}}, ?PORT_TIMEOUT);
logging(disable, {?MODULE, PortPid}) ->
    gen_server:call(PortPid, {port_call, {?RMOTE_MSG, ?DBG_FLAG_OFF}}, ?PORT_TIMEOUT).

get_session(Tns, Usr, Pswd, {?MODULE, PortPid})
when is_binary(Tns); is_binary(Usr); is_binary(Pswd) ->
    case gen_server:call(PortPid, {port_call, {?GET_SESSN, Tns, Usr, Pswd}}, ?PORT_TIMEOUT) of
        {error, Error} -> {error, Error};
        SessionId -> {?MODULE, PortPid, SessionId}
    end.

close({?MODULE, PortPid, SessionId}) ->
    gen_server:call(PortPid, {port_call, {?PUT_SESSN, SessionId}}, ?PORT_TIMEOUT);
close({?MODULE, statement, PortPid, StmtId}) ->
    gen_server:call(PortPid, {port_call, {?CLSE_STMT, StmtId}}, ?PORT_TIMEOUT).

inject_rowid(Sql) ->
    {ok,{[{PT,_}],_}} = sqlparse:parsetree(Sql),
    {NewSql, NewPT} = case PT of
        {select, Args} ->
            {fields, Flds} = lists:keyfind(fields, 1, Args),
            {from, [FirstTable|_]=Forms} = lists:keyfind(from, 1, Args),
            NewFields =
                [list_to_binary(case FirstTable of
                    {as, _, Alias} -> [Alias, ".ROWID"];
                    Tab -> [Tab, ".ROWID"]
                end) | lists:flatten([case F of
                                        <<"*">> ->
                                        lists:reverse(lists:foldl(
                                            fun(T, AFields) ->
                                                case T of
                                                    {as, _, Alias} -> [list_to_binary([Alias,".*"]) | AFields];
                                                    Tab -> [list_to_binary([Tab,".*"]) | AFields]
                                                end
                                            end,
                                            [],
                                            Forms));
                                        _ -> F
                                     end
                                     || F <- Flds]
                )],
            NewArgs = lists:keyreplace(fields, 1, Args, {fields, NewFields}),
            NPT = {select, NewArgs},
            {sqlparse:fold(NPT), NPT};
        _ -> {Sql, PT}
    end,
io:format(user, "~n________________________~nSQL ~p~n", [NewSql]),
io:format(user, "Old SQL ~p~n", [Sql]),
io:format(user, "Old parse tree ~p~n", [PT]),
io:format(user, "New parse tree ~p~n________________________~n", [NewPT]),
    NewSql.

prep_sql(Sql, {?MODULE, PortPid, SessionId}) when is_binary(Sql) ->
    NewSql = inject_rowid(Sql),
    R = gen_server:call(PortPid, {port_call, {?PREP_STMT, SessionId, NewSql}}, ?PORT_TIMEOUT),
    timer:sleep(100), % Port driver breaks on faster pipe access
    case R of
        {stmt,StmtId} -> {?MODULE, statement, PortPid, StmtId};
        R -> R
    end.

exec_stmt({?MODULE, statement, PortPid, StmtId}) ->
    R = gen_server:call(PortPid, {port_call, {?EXEC_STMT, StmtId}}, ?PORT_TIMEOUT),
    timer:sleep(100), % Port driver breaks on faster pipe access
    case R of
        {cols, Clms} -> {ok, Clms};
        R -> R
    end.

fetch_rows(Count, {?MODULE, statement, PortPid, StmtId}) ->
    gen_server:call(PortPid, {port_call, {?FTCH_ROWS, StmtId, Count}}, ?PORT_TIMEOUT).


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
    NewPath = case os:getenv("LD_LIBRARY_PATH") of
        false -> "";
        LdLibPath -> LdLibPath
    end ++ ":./c_src/lib/instantclient/",
    PortOptions = [ {packet, 4}
                  , binary
                  , exit_status
                  , use_stdio
                  , {args, ["true", integer_to_list(ListenPort)]}
                  , {env, [{"LD_LIBRARY_PATH", NewPath}]}
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

server(LSock) ->
    ?Info("~p waiting for log connections...", [LSock]),
    case gen_tcp:accept(LSock) of
        {ok, Sock} ->
            ?Info("logger connected at ~p, now waiting to receive in tight loop and log", [Sock]),
            log(Sock),
            ?Info("closing tcp connection"),
            ok = gen_tcp:close(Sock),
            ok = gen_tcp:close(LSock);
        {error, Error} ->
            ?Error("listener failed ~p!", [Error])
    end.

log(Sock) ->
    case gen_tcp:recv(Sock, 0) of
        {ok, B} ->
            case re:run(B, <<"\n">>) of
                nomatch -> oci_logger:log(binary_to_list(B));
                _ ->
                    [(fun
                        ("") -> ok;
                        (Txt) ->
                            oci_logger:log(?T++" "++Txt++"~n")
                    end)(Log) || Log <- re:split(B, <<"\n">>, [{return, list}])]
            end,
            log(Sock);
        {error, closed} -> ok
    end.

handle_call({port_call, Msg}, From, #state{port=Port} = State) ->
    Cmd = list_to_tuple([From|tuple_to_list(Msg)]),
    %CmdBin = term_to_binary(Cmd),
    %?Debug("TX ~p bytes", [byte_size(CmdBin)]),
    %?Debug(" ~p", [Cmd]),
    true = port_command(Port, term_to_binary(Cmd)),
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

-include_lib("eunit/include/eunit.hrl").

-define(NowMs, (fun() -> {M,S,Ms} = erlang:now(), ((M*1000000 + S)*1000000) + Ms end)()).

setup() ->
    OciPort = oci_port:start_link([{logging, true}]),
    OciSession = OciPort:get_session(
                <<"(DESCRIPTION="
                  "  (ADDRESS_LIST="
                  "      (ADDRESS=(PROTOCOL=tcp)"
                  "          (HOST=127.0.0.1)"
                  "          (PORT=1521)"
                  "      )"
                  "  )"
                  "  (CONNECT_DATA=(SERVICE_NAME=XE))"
                  ")">>,
                <<"bikram">>,
                <<"abcd123">>),
    throw_if_error(undefined, OciSession, "session get failed"),
    oci_logger:log(lists:flatten(io_lib:format("___________----- OCI Session ~p~n", [OciSession]))),
    OciSession.

teardown(_OciSession) -> ok.

db_test_() ->
    {timeout, 60, {
        setup,
        fun setup/0,
        fun teardown/1,
        {with, [
            fun db_perf/1
        ]}
    }}.

db_perf(OciSession) ->
    Threads = 1,
    InsertCount = 1,
    This = self(),
    [(fun(Idx) ->
        Table = "erloci_table_"++Idx,
        try
            create_table(OciSession, Table),            
            spawn(fun() -> insert_select(OciSession, Table, InsertCount, This) end)
        catch
            Class:Reason -> oci_logger:log(lists:flatten(io_lib:format("~p:~p~n", [Class,Reason])))
        end
    end)(integer_to_list(I))
    || I <- lists:seq(1,Threads)],
    receive_all(OciSession, Threads).

receive_all(OciSession, Count) -> receive_all(OciSession, Count, []).
receive_all(OciSession, 0, Acc) ->
    OciSession:close(),
    [(fun(Table, InsertCount, InsertTime, SelectCount, SelectTime) ->
        InsRate = erlang:trunc(InsertCount / InsertTime),
        SelRate = erlang:trunc(SelectCount / SelectTime),
        io:format(user, "~p insert ~p, ~p sec, ~p rows/sec    select ~p, ~p sec, ~p rows/sec~n", [Table,InsertCount, InsertTime, InsRate,SelectCount, SelectTime, SelRate])
    end)(T, Ic, It, Sc, St)
    || {T, Ic, It, Sc, St} <- Acc];
receive_all(OciSession, Count, Acc) ->
    receive
        {T, Ic, It, Sc, St} ->
            receive_all(OciSession, Count-1, [{T, Ic, It, Sc, St}|Acc])
    after
        (100*1000) ->
            io:format(user, ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> ~p~n", [timeout]),
            receive_all(OciSession, 0, Acc)
    end.

create_table(OciSession, Table) ->
    Stmt = OciSession:prep_sql(list_to_binary(["drop table ",Table])),
    print_if_error(Stmt, "drop prep failed"),
    Res = Stmt:exec_stmt(),
    print_if_error(Res, "drop exec failed"),
    Stmt:close(),
    oci_logger:log(lists:flatten(io_lib:format("___________----- OCI drop ~p~n", [Res]))),
    Stmt0 = OciSession:prep_sql(list_to_binary(["create table ",Table,"(pkey number,
                                       publisher varchar2(100),
                                       rank number,
                                       hero varchar2(100),
                                       reality varchar2(100),
                                       votes number,
                                       createdate date default sysdate,
                                       votes_first_rank number)"])),
%                                       createtime timestamp default systimestamp,
    throw_if_error(undefined, Stmt0, "create "++Table++" prep failed"),
    Res = Stmt0:exec_stmt(),
    throw_if_error(undefined, Res, "create "++Table++" exec failed"),
    Stmt0:close(),
    oci_logger:log(lists:flatten(io_lib:format("___________----- OCI create ~p~n", [Res]))).

insert_select(OciSession, Table, InsertCount, Parent) ->
    try
        InsertStart = ?NowMs,
        [(fun(I) ->
            Qry = erlang:list_to_binary([
                    "insert into ", Table, " (pkey,publisher,rank,hero,reality,votes,votes_first_rank) values (",
                    I,
                    ", 'publisher"++I++"',",
                    I,
                    ", 'hero"++I++"'",
                    ", 'reality"++I++"',",
                    I,
                    ",",
                    I,
                    ")"]),
            oci_logger:log(lists:flatten(io_lib:format("_[~p]_ ~p~n", [Table,Qry]))),
            Stmt = OciSession:prep_sql(Qry),
            throw_if_error(Parent, Stmt, "insert "++Table++" prep failed"),
            Res = Stmt:exec_stmt(),
            throw_if_error(undefined, Res, "insert "++Table++" exec failed"),
            Stmt:close(),
            if {executed, no_ret} =/= Res -> oci_logger:log(lists:flatten(io_lib:format("_[~p]_ ~p~n", [Table,Res]))); true -> ok end
          end)(integer_to_list(Idx))
        || Idx <- lists:seq(1, InsertCount)],
        InsertEnd = ?NowMs,
io:format(user, "-----------------~n", []),
        timer:sleep(5000),
        Statement = OciSession:prep_sql(list_to_binary(["select * from ", Table])),
        throw_if_error(Parent, Statement, "select "++Table++" prep failed"),
io:format(user, ".................~n", []),
        Cols = Statement:exec_stmt(),
        throw_if_error(Parent, Cols, "select "++Table++" exec failed"),
        oci_logger:log(lists:flatten(io_lib:format("_[~p]_ columns ~p~n", [Table,Cols]))),
        {{rows, Rows}, _} = _RowResp = Statement:fetch_rows(100),
        Statement:close(),
        oci_logger:log(lists:flatten(io_lib:format("...[~p]... OCI select rows ~p~n", [Table,_RowResp]))),
        SelectEnd = ?NowMs,
        InsertTime = (InsertEnd - InsertStart)/1000000,
        SelectTime = (SelectEnd - InsertEnd)/1000000,
        Parent ! {Table, InsertCount, InsertTime, length(Rows), SelectTime}
    catch
        Class:Reason ->
            if is_pid(Parent) -> Parent ! {{Table,Class,Reason}, 0, 1, 0, 1}; true -> ok end,
            oci_logger:log(lists:flatten(io_lib:format(" ~p:~p~n", [Class,Reason])))
    end.

print_if_error({error, Error}, Msg) -> oci_logger:log(lists:flatten(io_lib:format("___________----- continue after ~p ~p~n", [Msg,Error])));
print_if_error(_, _) -> ok.

throw_if_error(Parent, {error, Error}, Msg) ->
    if is_pid(Parent) -> Parent ! {{Msg, Error}, 0, 1, 0, 1}; true -> ok end,
    throw({Msg, Error});
throw_if_error(_,_,_) -> ok.

-endif.
