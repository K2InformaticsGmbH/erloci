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

-module(oci_session_pool).
-behaviour(gen_server).
-include_lib("eunit/include/eunit.hrl").
-include("oci.hrl").

%% API
-export([
    start_link/6,
    start_link/4,
    stop/1,
    enable_log/1,
    disable_log/1,
    get_port/1,
    get_poolname/1,
    get_session/1]).

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
        pool_name
    }).

%% External API
%% @doc see create_session_pool/4
-spec start_link(Host::string(),
    Port :: integer(),
    Service :: {sid, ServiceId::string()} | {service, Service::string()},
    Username::string(),
    Password :: string(),
    Options :: [{atom(), boolean()|integer()}]
) -> {ok, SessionPoolName::string()} | {error, Reason::ora_error()}.
start_link(Host, Port, {sid, Sid}, UserName, Password, Options)
        when is_list(Host), is_integer(Port), is_list(Sid), is_list(UserName), is_list(Password), is_list(Options) ->
    TnsNameStr =
    "(DESCRIPTION=" ++
        "(ADDRESS=(PROTOCOL=tcp)" ++
                 "(HOST=" ++ Host ++ ")" ++
                 "(PORT=" ++ integer_to_list(Port) ++ ")"++
        ")"++
        "(CONNECT_DATA=(SID=" ++ Sid ++ "))"++
    ")",
    start_link(TnsNameStr, UserName, Password, Options);
start_link(Host, Port, {service, Service}, UserName, Password, Options)
        when is_list(Host), is_integer(Port), is_list(Service), is_list(UserName), is_list(Password), is_list(Options) ->
    TnsNameStr =
    "(DESCRIPTION=" ++
        "(ADDRESS_LIST=" ++
            "(ADDRESS=(PROTOCOL=tcp)"++
                     "(HOST=" ++ Host ++ ")"++
                     "(PORT=" ++ integer_to_list(Port) ++ ")"++
            ")"++
        ")"++
        "(CONNECT_DATA=(SERVICE_NAME=" ++ Service ++ "))"++
    ")",
    start_link(TnsNameStr, UserName, Password, Options).

%% @doc Spawns an erlang control process that will open a port
%%      to a c-process that uses the ORACLE OCI API to open a connection
%%      to the database.  ``ConnectionStr'' can contain
%%      "user/password@database" connection string. Following options are
%%      currently supported:
%%          {autocommit, boolean()}
%%          {max_rows, integer()}
%%          {query_cache_size, integer()}
%%
%%      ``autocommit'' will automatically commit the query, ``max_rows'' option limits
%%      the max number of elements in the list of rows returned by a select
%%      statement.  ``query_cache_size'' controls the size of the driver's cache
%%      that stores prepared query statements.  Its default value is 50.
%%      The port program can be started in debug mode by specifying
%%      {mod, {ora, [{debug, true}]}} option in the ora.app file.
-spec start_link(ConnectionStr::string(),
    Username::string(),
    Password :: string(),
    Options :: [{atom(), boolean()|integer()}]
) -> {ok, SessionPoolPid::pid()} | {error, Error::term()}.
start_link(TnsNameStr, UserName, Password, Options) when is_list(TnsNameStr) and is_list(Options) ->
    gen_server:start_link(?MODULE, [TnsNameStr, UserName, Password, Options], []).

%% @doc stops the connection pool process
-spec stop(SessionPoolPid::pid()) -> ok.
stop(SessionPoolPid) ->
    gen_server:call(SessionPoolPid, stop).

%% @doc get a new Session from the SessionPool specified. We return you a parameterized module
-spec get_session(SessionPoolPid::pid()) -> Session::session().
get_session(SessionPoolPid) ->
    gen_server:call(SessionPoolPid, get_session, ?PORT_TIMEOUT).

%% @doc enable log output from the port
-spec enable_log(SessionPoolPid::pid()) -> ok.
enable_log(SessionPoolPid) ->
    gen_server:call(SessionPoolPid, enable_log, ?PORT_TIMEOUT).

%% @doc disable log output from the port
-spec disable_log(SessionPoolPid::pid()) -> ok.
disable_log(SessionPoolPid) ->
    gen_server:call(SessionPoolPid, disable_log, ?PORT_TIMEOUT).

%% @doc returns the port
-spec get_port(SessionPoolPid::pid()) -> port().
get_port(SessionPoolPid) ->
    gen_server:call(SessionPoolPid, get_port).

%% @doc returns the poolname
-spec get_poolname(SessionPoolPid::pid()) -> string().
get_poolname(SessionPoolPid) ->
    gen_server:call(SessionPoolPid, get_poolname).

%% Callbacks
init([TnsNameStr, UserName, Password, Options]) ->
    PortOptions = proplists:get_value(port_options, Options),
    PoolOptions = proplists:delete(port_options, Options),
    {ok, PortPid} = oci_port:start_link(PortOptions),
    {ok, PoolName} = oci_port:call(PortPid, {
            ?CREATE_SESSION_POOL,
            binary:list_to_bin(TnsNameStr),
            binary:list_to_bin(UserName),
            binary:list_to_bin(Password),
            binary:list_to_bin(PoolOptions)}),
    {ok, #state{port=PortPid, pool_name=PoolName}}.

handle_call(stop, _From, State) ->
    {stop, normal, stopped, State};

handle_call(get_port, _From, #state{port=PortPid} = State) ->
    {reply, {ok, PortPid}, State};
handle_call(get_poolname, _From, #state{pool_name=Poolname} = State) ->
    {reply, {ok, Poolname}, State};
handle_call(get_session, _From, #state{port=PortPid} = State) ->
    {ok, SessionPid} = oci_session:open(PortPid, self()),
    %% we return a parameterized module here, we don't use
    %% oci_session:new() though, we use the tuple notation
    %% directly. this lets you call every function in oci_session
    %% by giving the SessionPid as a last parameter, therefore:
    %% Session:next_rows() == oci_session:next_rows(SessionPid)
    {reply, {oci_session, SessionPid}, State};

handle_call(enable_log, _From, #state{port=PortPid} = State) ->
    oci_port:enable_logging(PortPid),
    {reply, ok, State};
handle_call(disable_log, _From, #state{port=PortPid} = State) ->
    oci_port:disable_logging(PortPid),
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{port=PortPid}) ->
    case is_process_alive(PortPid) of
        true ->
            oci_port:call(PortPid, {?RELEASE_SESSION_POOL});
        _ ->
            ok
    end,
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal Functions

%% Test
integration_test(Host, Port, Service, UserName, Password) ->
    {ok, Pool} = oci_session_pool:start_link(Host, Port, Service, UserName, Password,""),
    Session = oci_session_pool:get_session(Pool),
    Session:execute_sql("drop table test_erloci", [], 10),
    ok = Session:execute_sql("create table test_erloci(pkey number,  publisher varchar2(100), rank number, hero varchar2(100), real varchar2(100), votes number, votes_first_rank number)", [], 10),
    insert_rows(Session),
    ok = Session:execute_sql("select * from test_erloci", [], 10),
    [true] = sets:to_list(sets:from_list([Session:next_rows() /= [] || _ <- lists:seq(1,10)])),
    [] = Session:next_rows(),
    stopped = Session:close(),
    stopped = oci_session_pool:stop(Pool).

mock_suite_test_() ->
    {
        setup, fun()->ok end,
        [
            fun() -> mock_test() end,
            fun() -> mock_empty_table_test() end
        ]
    }.

mock_test() ->
    {ok, Pool} = oci_session_pool:start_link("127.0.0.1", 1521, {service, "db.local"}, "dba", "supersecret", [{port_options, [{mock_port, oci_port_mock}]}]),
    Session = oci_session_pool:get_session(Pool),

    %% no cache
    {statement, Statement1} = Session:execute_sql("select * from test_erloci", [], 10),
    [true] = sets:to_list(sets:from_list([Statement1:next_rows() /= [] || _ <- lists:seq(1,10)])),
    [] = Statement1:next_rows(),
    ok = Statement1:close(),

    %% no cache, odd max_num
    {statement, Statement2} = Session:execute_sql("select * from test_erloci", [], 3),
    [true] = sets:to_list(sets:from_list([Statement2:next_rows() /= [] || _ <- lists:seq(1,34)])),
    [] = Statement2:next_rows(),
    ok = Statement2:close(),

    %% use cache
    {statement, Statement3} = Session:execute_sql("select * from test_erloci", [], 10, true),
    [true] = sets:to_list(sets:from_list([Statement3:next_rows() /= [] || _ <- lists:seq(1,10)])),
    [] = Statement3:next_rows(),
    ok = Statement3:close(),

    %% use cache , odd max_num
    {statement, Statement4} = Session:execute_sql("select * from test_erloci", [], 3, true),
    [true] = sets:to_list(sets:from_list([Statement4:next_rows() /= [] || _ <- lists:seq(1,34)])),
    [] = Statement4:next_rows(),
    ok = Statement4:close(),

    %% use cache with prev
    {statement, Statement5} = Session:execute_sql("select * from test_erloci", [], 10, true),
    [true] = sets:to_list(sets:from_list([Statement5:next_rows() /= [] || _ <- lists:seq(1,10)])),
    [] = Statement5:next_rows(),
    [true] = sets:to_list(sets:from_list([Statement5:prev_rows() /= [] || _ <- lists:seq(1,9)])),
    [] = Statement5:prev_rows(),
    ok = Statement5:close(),

    %% use cache with prev
    {statement, Statement6} = Session:execute_sql("select * from test_erloci", [], 3, true),
    [true] = sets:to_list(sets:from_list([Statement6:next_rows() /= [] || _ <- lists:seq(1,34)])),
    [] = Statement6:next_rows(),
    [true] = sets:to_list(sets:from_list([Statement6:prev_rows() /= [] || _ <- lists:seq(1,33)])),
    [] = Statement6:prev_rows(),
    ok = Statement6:close(),

    stopped = Session:close(),
    stopped = oci_session_pool:stop(Pool).

mock_empty_table_test() ->
    {ok, Pool} = oci_session_pool:start_link("127.0.0.1", 1521, {service, "db.local"}, "dba", "supersecret", [{port_options, [{mock_port, oci_port_empty_table_mock}]}]),
    Session = oci_session_pool:get_session(Pool),

    {statement, Statement1} = Session:execute_sql("select * from test_erloci", [], 10),
    [] = Statement1:next_rows(),
    ok = Statement1:close().

insert_rows(Session) ->
    [
        ok =
        Session:execute_sql(
            binary_to_list(iolist_to_binary([
                        "insert into test_erloci values(",
                        integer_to_list(PKey), ",",
                        "'", Publisher, "',",
                        integer_to_list(Rank), ",",
                        "'", Hero, "',",
                        "'", Real, "',",
                        integer_to_list(Votes), ",",
                        integer_to_list(VotesFirstRank),
                        ")"])), [], 10)
        || [PKey, Publisher, Rank, Hero, Real, Votes, VotesFirstRank] <- port_mock:rows()].
