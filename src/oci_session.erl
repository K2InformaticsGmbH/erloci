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

-module(oci_session).
-behaviour(gen_server).

-include("oci.hrl").

%% API
-export([
    execute_sql/4,
    execute_sql/5,
    next_rows/1,
    prev_rows/1,
    rows_from/2,
    get_buffer_max/1,
    open/2,
    close/1,
    get_columns/1]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-record(state, {
    status=closed,
    port,
    pool,
    session_id,
    statements = []
}).
-record(statement, {
    ref,
    handle,
    max_rows,
    row_top = 0,
    row_bottom = 0,
    columns,
    results,
    use_cache = false,
    port_row_fetch_status = more,
    fetch_activity
}).

%% External API

%% @doc open new session
-spec open(PortPid::pid(), PoolPid::pid()) -> {ok, SessionPid::pid()} | {error, Error::term()}.
open(Port, Pool) ->
    gen_server:start(?MODULE, [Port, Pool], []).

%% @doc closes a session, can be used in a parameterized fashion Session:close() or Statement:close()
-spec close(session() | statement()) -> stopped|ok.
close({?MODULE, Pid}) ->
    gen_server:call(Pid, stop, ?PORT_TIMEOUT);
close({?MODULE, StatementRef, Pid}) ->
    gen_server:call(Pid, {close_statement, StatementRef}, ?PORT_TIMEOUT).

%% @doc Executes an UPDATE/DELETE/INSERT SQL query, can be used in a parameterized fashion Session:execute_sql(...)
-spec execute_sql(Query :: string()|binary(),
    Params :: [{
            Type :: atom(),
            Dir :: atom(),
            Value :: integer() | float() | string()
        }],
    MaxRows :: integer(),
    Session :: session()) ->
    {statement, statement()} |
    {ok, RetArgs :: [integer()|float()|string()]} |
    {error, Reason :: ora_error()}.
execute_sql(Query, Params, MaxRows, {?MODULE, Pid}) when is_list(Query) ->
    execute_sql(Query, Params, MaxRows, false, {?MODULE, Pid}).
execute_sql(Query, Params, MaxRows, UseCache, {?MODULE, Pid}) when is_list(Query) ->
    gen_server:call(Pid, {execute_sql, Query, Params, MaxRows, UseCache}, ?PORT_TIMEOUT).

%% @doc Fetches the resulting rows of a previously executed sql statement, can be used in a parameterized fashion Session:get_rows()
-spec prev_rows(Statement::statement()) ->  [Row::tuple()] | {error, Reason::ora_error()}.
prev_rows({?MODULE, StatementRef, Pid}) ->
    gen_server:call(Pid, {prev_rows, StatementRef}, ?PORT_TIMEOUT).

next_rows({?MODULE, StatementRef, Pid}) ->
    ok = gen_server:call(Pid, {next_rows, StatementRef}, ?PORT_TIMEOUT),
    get_rows(Pid, StatementRef).

rows_from(RowNum, {?MODULE, StatementRef, Pid}) ->
    ok = gen_server:call(Pid, {rows_from, StatementRef, RowNum}, ?PORT_TIMEOUT),
    get_rows(Pid, StatementRef).

get_rows(Pid, StatementRef) ->
    case gen_server:call(Pid, {get_buffer_max, StatementRef}, ?PORT_TIMEOUT) of
        {ok, working, _} ->
            timer:sleep(1),
            get_rows(Pid, StatementRef);
        _ ->
            gen_server:call(Pid, {get_rows, StatementRef}, ?PORT_TIMEOUT)
    end.

get_buffer_max({?MODULE, StatementRef, Pid}) ->
    case gen_server:call(Pid, {get_buffer_max, StatementRef}, ?PORT_TIMEOUT) of
        {ok,working,CacheSize} -> {ok, false, CacheSize};
        {ok,_,CacheSize} ->       {ok, true,  CacheSize}
    end.

%% @doc Fetches the column information of the last executed query, can be used in a parameterized fashion Session:get_columns()
-spec get_columns(Statement::statement()) -> [{ColumnName::string(), Type::atom(), Length::integer()}].
get_columns({?MODULE, StatementRef, Pid}) ->
    gen_server:call(Pid, {get_columns, StatementRef}, ?PORT_TIMEOUT).

%% Callbacks
init([Port, Pool]) ->
    case oci_port:call(Port, {?GET_SESSION}) of
        {ok, SessionId} ->
            {ok, #state{port=Port, pool=Pool, session_id=SessionId, status=open}};
        {error, Error} -> {stop, Error}
    end.

handle_call({get_columns, StatementRef}, _From, #state{statements=Statements} = State) ->
    S = proplists:get_value(StatementRef, Statements),
    {reply, {ok, S#statement.columns}, State};

handle_call({execute_sql, Query, Params, MaxRows, UseCache}, _From, #state{session_id=SessionId, port=Port} = State) ->
    CorrectedParams =
    case re:run(Query, "([:])", [global]) of
        {match, ArgList} ->
            if length(Params) /= length (ArgList) ->
                    {error, badarg};
                true ->
                    correct_params(Params, [])
            end;
        nomatch ->
            []
    end,
    exec(State, Port, SessionId, Query, CorrectedParams, MaxRows, UseCache);

handle_call({prev_rows, StatementRef}, _From, #state{statements=Statements} = State) ->
    Statement = proplists:get_value(StatementRef, Statements),
    #statement{
        results=Results,
        use_cache=UseCache,
        max_rows=MaxRows,
        row_top=RowTop,
        port_row_fetch_status=PortRowFetchStatus,
        columns=Columns
    } = Statement,

    CacheSize = ets:info(Results, size),
    NewRowTop =
        if (RowTop - MaxRows - 1) < 1 -> 1;
           true -> (RowTop - MaxRows - 1)
        end,
    NewRowBottom =
        if (NewRowTop + MaxRows) > CacheSize -> CacheSize;
           true -> (NewRowTop + MaxRows)
        end,
%?Info("prev_rows >>>> CacheSize ~p, RowTop ~p, RowBottom ~p", [CacheSize, NewRowTop, NewRowBottom]),
    case UseCache of
        true ->
            NewStatement = Statement#statement{
                row_top = NewRowTop,
                row_bottom = NewRowBottom
            },
            NewStatements = proplists:delete(StatementRef, Statements) ++ [{StatementRef, NewStatement}],
            RetRows = get_rows_from_ets(NewRowTop, NewRowBottom, Results, Columns),
            %Keys = lists:seq(NewRowTop, NewRowBottom),
            %Rows =[ets:lookup(Results, K1)||K1<-Keys],
            %RetRows = [[integer_to_list(I)|R] || [{I,R}|_] <-Rows],
            {reply, {RetRows, PortRowFetchStatus, CacheSize}, State#state{statements=NewStatements}};
        _ ->
            {reply, {[], PortRowFetchStatus, 0}, State}
    end;

handle_call({rows_from, StatementRef, RowNum}, _From, #state{statements=Statements} = State) ->
    Statement = proplists:get_value(StatementRef, Statements),
    #statement{
        results=Results,
        use_cache=UseCache,
        max_rows=MaxRows
    } = Statement,
    gen_server:cast(self(), {fetch_until, StatementRef, RowNum}),
    CacheSize = ets:info(Results, size),
    NewRowBottom =
        if (RowNum > CacheSize) or (RowNum + MaxRows > CacheSize) -> CacheSize;
           true -> RowNum + MaxRows
        end,
    NewRowTop =
        if (NewRowBottom - MaxRows) < 1 -> 1;
           true -> (NewRowBottom - MaxRows)
        end,
?Info("rows_from >> ~p >> CacheSize ~p, RowTop ~p, RowBottom ~p", [RowNum, CacheSize, NewRowTop, NewRowBottom]),

    %% delete results if no_cache
    case UseCache of
        false ->
            [ets:delete(Results, I) || {I,_} <- [ets:lookup(Results, K1)||K1<-lists:seq(NewRowTop, NewRowBottom)]];
        true ->
            ok
    end,
    NewStatements = proplists:delete(StatementRef, Statements) ++ [{StatementRef, Statement#statement{fetch_activity=working}}],
    {reply, ok, State#state{statements=NewStatements}};

handle_call({next_rows, StatementRef}, _From, #state{statements=Statements} = State) ->
    Statement = proplists:get_value(StatementRef, Statements),
    #statement{
        results=Results,
        use_cache=UseCache,
        max_rows=MaxRows,
        row_bottom=RowBottom
    } = Statement,
    CacheSize = ets:info(Results, size),
    NewRowBottom =
        if (RowBottom + 1 + MaxRows) > CacheSize -> CacheSize;
            true -> (RowBottom + 1 + MaxRows)
        end,
    NewRowTop =
        if (NewRowBottom - MaxRows) < 1 -> 1;
            true -> (NewRowBottom - MaxRows)
        end,
    gen_server:cast(self(), {fetch_until, StatementRef, NewRowTop}),
?Info("next_rows >>>> CacheSize ~p, RowTop ~p, RowBottom ~p", [CacheSize, NewRowTop, NewRowBottom]),

    case UseCache of
        false ->
            [ets:delete(Results, I) || {I,_} <- [ets:lookup(Results, K1)||K1<-lists:seq(NewRowTop, NewRowBottom)]];
        true ->
            ok
    end,
    NewStatements = proplists:delete(StatementRef, Statements) ++ [{StatementRef, Statement#statement{fetch_activity=working}}],
    {reply, ok, State#state{statements=NewStatements}};

handle_call({get_rows, StatementRef}, _From, #state{statements=Statements} = State) ->
    Statement = proplists:get_value(StatementRef, Statements),
    #statement{
        results=Results,
        port_row_fetch_status=PortRowFetchStatus,
        row_top = RowTop,
        row_bottom=RowBottom,
        columns=Columns
    } = Statement,
    CacheSize = ets:info(Results, size),
    RetRows = get_rows_from_ets(RowTop, RowBottom, Results, Columns),
    %Keys = lists:seq(RowTop, RowBottom),
    %Rows =[ets:lookup(Results, K1)||K1<-Keys],
    %RetRows = [[integer_to_list(I)|R] || [{I,R}|_] <-Rows],
%?Info("get_rows >>>> RowTop ~p, RowBottom ~p from Results ~p", [RowTop, RowBottom, Results]),
    {reply, {RetRows, PortRowFetchStatus, CacheSize}, State};

handle_call({get_buffer_max, StatementRef}, _From, #state{statements=Statements} = State) ->
    Statement = proplists:get_value(StatementRef, Statements),
    #statement{
        results=Results,
        fetch_activity=FetchAct
    } = Statement,
    CacheSize = ets:info(Results, size),
%?Info("\t\tget_buffer_max >>>> CacheSize ~p", [CacheSize]),
    {reply, {ok, FetchAct, CacheSize}, State};

handle_call({close_statement, StatementRef}, _From, #state{statements=Statements} = State) ->
    Statement = proplists:get_value(StatementRef, Statements),
    #statement{
        results=Results
    } = Statement,
    ets:delete(Results),
    NewStatements = proplists:delete(StatementRef, Statements),
    {reply, ok, State#state{statements=NewStatements}};

handle_call(stop, _From, State) ->
    {stop, normal, stopped, State};

handle_call(Request, _Froqm, State) ->
    ?Error("unknown Request ~p",[{Request, State}]),
    {reply, ok, State}.

handle_cast({fetch_until, StatementRef, RowNum}, #state{port=Port, session_id=SessionId, statements=Statements} = State) ->
    Statement = proplists:get_value(StatementRef, Statements),
    #statement{
        handle=StatementHandle,
        results=Results,
        use_cache=UseCache,
        max_rows=MaxRows,
        port_row_fetch_status=PortRowFetchStatus
    } = Statement,
    Cs = ets:info(Results, size),
    NewPortRowFetchStatus =
        if (PortRowFetchStatus == more) and (RowNum + MaxRows >= Cs) ->
                case fetch(Port, SessionId, StatementHandle, Results) of
                    {ok, NewStatus} -> NewStatus;
                    _ -> PortRowFetchStatus
                end;
            true -> PortRowFetchStatus
        end,
    CacheSize = ets:info(Results, size),
    NewRowBottom =
        if (RowNum > CacheSize) or (RowNum + MaxRows > CacheSize) -> CacheSize;
           true -> RowNum + MaxRows
        end,
    NewRowTop =
        if (NewRowBottom - MaxRows) < 1 -> 1;
           true -> (NewRowBottom - MaxRows)
        end,
%?Info("\tfetch_until >>>> CacheSize ~p, RowTop ~p, RowBottom ~p", [CacheSize, NewRowTop, NewRowBottom]),
    %% delete results if no_cache
    case UseCache of
        false ->
            [ets:delete(Results, I) || {I,_} <- [ets:lookup(Results, K1)||K1<-lists:seq(NewRowTop, NewRowBottom)]];
        true ->
            ok
    end,
    NewStatement = if (NewPortRowFetchStatus == more) and (RowNum >= CacheSize) ->
            gen_server:cast(self(), {fetch_until, StatementRef, RowNum}),
             Statement#statement{
                port_row_fetch_status=NewPortRowFetchStatus,
                row_top = NewRowTop,
                row_bottom = NewRowBottom,
                fetch_activity=working
            };
        true ->
            Statement#statement{
                port_row_fetch_status=NewPortRowFetchStatus,
                row_top = NewRowTop,
                row_bottom = NewRowBottom,
                fetch_activity=finished
            }
    end,
    NewStatements = proplists:delete(StatementRef, Statements) ++ [{StatementRef, NewStatement}],
    {noreply, State#state{statements=NewStatements}};

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{port = Port, session_id = SessionId}) ->
    case oci_port:call(Port, SessionId, {?RELEASE_SESSION, SessionId}) of
        {ok} ->
            ok;
        {error, _Reason} ->
            ok
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% internal
correct_params([], Acc) ->
    Acc;
correct_params([{Type,Dir,Val}|Params], Acc0) when is_atom(Type), is_atom(Dir), is_list(Acc0)->
    Acc1 = correct_params(Params, Acc0),
    NewType =
    case Type of
        sql_number ->
            ?NUMBER;
        sql_string ->
            ?STRING
    end,
    NewDir =
    case Dir of
        in ->
            ?ARG_DIR_IN;
        out ->
            ?ARG_DIR_OUT;
        inout ->
            ?ARG_DIR_INOUT
    end,
    NewVal = if is_list(Val) -> list_to_binary(Val);
        true -> Val
    end,
    [{NewType,NewDir,NewVal} | Acc1].

exec(State, Port, SessionId, Query, Params, MaxRows, UseCache) ->
    case oci_port:call(Port, SessionId, {?EXEC_SQL, SessionId, binary:list_to_bin(Query), Params}) of
        {executed, no_ret} ->
            {reply, ok, State};
        {executed, RetArgs} ->
            {reply, {ok, RetArgs}, State};
        {columns, StatementHandle, Columns} ->
            StatementRef = make_ref(),
            Results = ets:new(results, [ordered_set, public]),
            Statement = #statement{
                ref=StatementRef,
                handle=StatementHandle,
                columns=Columns,
                max_rows=MaxRows,
                results=Results,
                use_cache=UseCache
            },
            Statements = State#state.statements ++ [{StatementRef, Statement}],
            {reply, {statement, {?MODULE, StatementRef, self()}}, State#state{statements=Statements}};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end.

get_rows_from_ets(RowStart, RowEnd, TableId, Columns) ->
    Keys = lists:seq(RowStart, RowEnd),
    Rows =[ets:lookup(TableId, K1)||K1<-Keys],
    [[integer_to_list(I)|format_row(Columns,R)]
    || [{I,R}|_] <-Rows].

format_row(Cols,Rows) -> format_row(Cols,Rows,[]).
format_row([],[], Acc) -> Acc;
format_row([{_,date,_}|Columns],[R|Rows], Acc) ->
    <<Y:32, Mon:16, D:16, H:16, M:16, S:16>> = list_to_binary(R),
    Date = binary_to_list(list_to_binary([<<D:16>>, ".", <<Mon:16>>, ".", <<Y:32>>, " ", <<H:16>>, ":", <<M:16>>, ":", <<S:16>>])),
    format_row(Columns, Rows, Acc ++ [Date]);
format_row([_|Columns],[R|Rows], Acc) -> format_row(Columns, Rows, Acc ++ [R]).

fetch(Port, SessionId, StatementHandle, ResultsTid) ->
    case oci_port:call(Port, SessionId, {?FETCH_ROWS, SessionId, StatementHandle}) of
        {{rows, Rows}, NewStatus} ->
            NrOfRows = length(Rows),
            CacheSize = ets:info(ResultsTid, size),
            ets:insert(ResultsTid, [{I, R}||{I,R}<-lists:zip(lists:seq(CacheSize+1, CacheSize+NrOfRows), lists:reverse(Rows))]),
            {ok, NewStatus};
        {error, Reason} ->
            %% maybe we should close the statement here (and gc resuls)
            {error, Reason}
    end.
