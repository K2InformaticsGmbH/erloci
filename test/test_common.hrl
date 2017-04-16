
-define(CONN_CONF,
(fun() ->
         case file:get_cwd() of
             {ok, Cwd} ->
                 ConnectConfigFile =
                 filename:join(
                   lists:reverse(
                     ["connect.config", "test" | lists:reverse(filename:split(Cwd))])),
                 case file:consult(ConnectConfigFile) of
                     {ok, [Params]} when is_map(Params) -> Params;
                     {ok, Params} ->
                         ?ELog("bad config (expected map) ~p", [Params]),
                         error(badconfig);
                     {error, Reason} ->
                         ?ELog("~p", [Reason]),
                         error(Reason)
                 end;
             {error, Reason} ->
                 ?ELog("~p", [Reason]),
                 error(Reason)
         end
 end)()).

-define(CONN_CONF_CT,
    (fun() ->
        ConnectConfigFile =
            filename:join(
                lists:reverse(
                    ["connect.config", "../../lib/erloci/test"])),
        case file:consult(ConnectConfigFile) of
            {ok, [Params]} when is_map(Params) -> Params;
            {ok, Params} ->
                ?ELog("bad config (expected map) ~p", [Params]),
                error(badconfig);
            {error, Reason} ->
                ?ELog("~p", [Reason]),
                error(Reason)
        end
     end)()).

-ifdef(debugFmt).
    -define(ELog(__Fmt,__Args),
    (fun(__F,__A) ->
        {_,_,__McS} = __Now = os:timestamp(),
        {_,{_,__Min,__S}} = calendar:now_to_datetime(__Now),
        ok = ?debugFmt("~2..0B:~2..0B.~6..0B "++__F, [__Min,__S,__McS rem 1000000 | __A])
    end)(__Fmt,__Args)).
-else.
    -define(ELog(__Fmt,__Args),
    (fun(__A) ->
        {_,_,__McS} = __Now = os:timestamp(),
        {_,{_,__Min,__S}} = calendar:now_to_datetime(__Now),
        io:format(user, "~2..0B:~2..0B.~6..0B "__Fmt"~n", [__Min,__S,__McS rem 1000000 | __A])
    end)(__Args)).
-endif.

-define(ELog(__F), ?ELog(__F,[])).
