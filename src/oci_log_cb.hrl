-ifndef(_OCI_LOG_CB_).
-define(_OCI_LOG_CB_, true).

-export([log/1]).

-ifdef(TEST).

log({Lvl, File, Fun, Line, Msg}) -> io:format(user, ?T++"[~p] {~s,~s,~p} ~s~n", [Lvl, File, Fun, Line, Msg]);
log({Lvl, File, Fun, Line, Msg, Term}) ->
    STerm = case Term of
                "" -> "";
                Term when is_list(Term) -> Term;
                _ -> lists:flatten(io_lib:format("~p", [Term]))
            end,
    io:format(user, ?T++"[~p] {~s,~s,~p} ~s : ~s~n", [Lvl, File, Fun, Line, Msg, STerm]);
log(Log) when is_list(Log) -> io:format(user, "~s", [Log]);
log(Log) -> io:format(user, ?T++".... ~p~n", [Log]).

-else.

log({Lvl, File, Fun, Line, Msg}) ->
    io:format(user, ?T++"[~p] ["++?LOG_TAG++"] {~s,~s,~p} ~s~n", [Lvl, File, Fun, Line, Msg]);
log({Lvl, File, Fun, Line, Msg, Term}) ->
    STerm = case Term of
                "" -> "";
                Term when is_list(Term) -> Term;
                _ -> lists:flatten(io_lib:format("~p", [Term]))
            end,
    io:format(user, ?T++"[~p] ["++?LOG_TAG++"] {~s,~s,~p} ~s : ~s~n", [Lvl, File, Fun, Line, Msg, STerm]);
log(Log) when is_list(Log) -> io:format(user, ?T++"["++?LOG_TAG++"] ~s~n", [Log]);
log(Log) -> io:format(user, ?T++"["++?LOG_TAG++"] ~p~n", [Log]).

-endif. % TEST

-define(LOGFUN, fun ?MODULE:log/1).

-endif. % _OCI_LOG_CB_
