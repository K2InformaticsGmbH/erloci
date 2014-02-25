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

-module(oci_logger).
-behaviour(gen_server).

-include("log.hrl").

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-export([start_link/0, log/2,accept/3, bin2str/1]).

-record(state, {
          sock,
          logfun,
          buf = <<>>
         }).

bin2str(Bin) when is_binary(Bin) ->
    BinStr = [lists:flatten(io_lib:format("~2.16.0B",[X])) || <<X:8>> <= Bin],
    string:join(lists:reverse(lists:foldl(fun(L,Acc)-> [string:join(L, " ")|Acc] end,
        [],
        split_list(BinStr, 16, []))),
    "\n").

split_list(List, Size, Acc) ->
    {L1, L2} = lists:split(16, List),
    if (length(L2) > 16) -> split_list(L2, Size, [L1|Acc]);
       true -> lists:reverse([L2,L1|Acc])
    end.

start_link() ->
    case gen_server:start_link(?MODULE, [], []) of
        {ok, Pid} -> {?MODULE, Pid};
        Error -> throw({error, Error})
    end.

accept(LSock, LogFun, {?MODULE, Pid}) when is_function(LogFun, 1) ->
    gen_server:call(Pid, {accept, LSock, LogFun}, infinity).

-ifdef(TEST).
log({Lvl, _Tag, File, Func, Line, Msg}, Mod) ->
    log(lists:flatten(io_lib:format(?T++"[~p] {~s,~s,~p} ~s", [Lvl, File, Func, Line, Msg])), Mod);
log(Msg, {?MODULE, Pid}) -> gen_server:cast(Pid, Msg).
-else.
log({Lvl, Tag, File, Func, Line, Msg}, Mod) ->
    log(lists:flatten(io_lib:format(?T++"[~p] ["++Tag++"] {~s,~s,~p} ~s", [Lvl, File, Func, Line, Msg])), Mod);
log(Msg, {?MODULE, Pid}) -> gen_server:cast(Pid, Msg).
-endif.

init(_) ->
    io:format(user, "---- ERLOCI PORT PROCESS LOGGER ----~n", []),
    {ok, #state{}}.

handle_cast(Msg, #state{logfun = LogFun} = State) ->
    try
        LogFun(Msg)
    catch
        _:_ -> io:format(user, Msg, [])
    end,
    {noreply, State}.

handle_info({tcp, Socket, Data}, #state{sock = Socket, logfun = LogFun} = State) ->
    << Size:32/integer, Payload/binary >> = NewBuf = list_to_binary([State#state.buf, Data]),
    inet:setopts(Socket,[{active,once}]),
    if Size > byte_size(Payload) ->
        io:format(user, "~p TCP RX expected ~p received so far ~p~n", [{?MODULE, ?LINE}, Size, byte_size(Payload)]),
        {noreply, State#state{buf = NewBuf}};
    true ->
        case binary_to_term(Payload) of
        Log when is_tuple(Log) ->
            [Lvl|Rest] = tuple_to_list(Log),
            try
                LogFun(list_to_tuple([?LLVL(Lvl) | Rest]))
            catch
                _:_ ->                   
                   [File, Func, Line, Msg | R] = Rest,
                   io:format(user, ?T++" [~p] {~s,~s,~p} ~s ~p~n", [?LLVL(Lvl), File, Func, Line, Msg, R])
            end;
        Other ->
            io:format(user, "~p Unknown log format ~p~n", [{?MODULE, ?LINE}, Other])
        end,
        {noreply, State#state{buf = <<>>}}
    end;
handle_info({tcp_closed, Socket}, #state{sock = Socket} = State) ->
    io:format(user, "~p TCP closed~n", [{?MODULE, ?LINE}]),
    {stop, normal, State};
handle_info(Msg, State) ->
    io:format(user, "~p unsupported handle_info ~p", [{?MODULE, ?LINE}, Msg]),
    {noreply, State}.

handle_call({accept, LSock, LogFun}, _From, State) ->
    {ok, {_,LPort}} = inet:sockname(LSock),
    io:format(user, ?T++"[debug] [_OCI_] Waiting for peer to connect on ~p~n", [LPort]),
    case gen_tcp:accept(LSock) of
        {ok, Sock} ->
            inet:setopts(Sock,[{active,once}]),
            {ok, {_,RemPort}} = inet:peername(Sock),
            {ok, {_,LclPort}} = inet:sockname(Sock),
            io:format(user, ?T++" [debug] [_OCI_] Connection from ~p to ~p~n", [RemPort, LclPort]),
            {reply, ok, State#state{sock = Sock, logfun = LogFun}};
        {error, Error} ->
            {reply, {error, {accept_failed, Error}}, State}
    end;
handle_call(Msg, _From, State) ->
    io:format(user, "~p unsupported handle_call ~p", [{?MODULE, ?LINE}, Msg]),
    {reply, ok, State}.

terminate(Reason, _) ->
   io:format(user, "~p Terminating ~p~n", [{?MODULE, ?LINE}, Reason]),
   ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
