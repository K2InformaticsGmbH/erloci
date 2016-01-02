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

-ifndef(LOG_HRL).
-define(LOG_HRL, true).

-ifdef(TEST).
-define(T,
(fun() ->
    {_,_,__McS} = __Now = os:timestamp(),
    {_,{_,__M,__S}} = calendar:now_to_local_time(__Now),
    lists:flatten(io_lib:format("~2..0B:~2..0B.~6..0B ", [__M,__S,__McS rem 1000000]))
end)()).
-else.
-define(T,
(fun() ->
    {_,_,__McS} = __Now = os:timestamp(),
    {{__YYYY,__MM,__DD},{__H,__M,__S}} = calendar:now_to_local_time(__Now),
    lists:flatten(io_lib:format("~2..0B.~2..0B.~4..0B ~2..0B:~2..0B:~2..0B.~6..0B ", [__DD,__MM,__YYYY,__H,__M,__S,__McS rem 1000000]))
end)()).
-endif.

-define(LLVL(__N), case __N of
                       0 -> debug;
                       1 -> info;
                       2 -> notice;
                       3 -> error;
                       4 -> warn;
                       5 -> critical;
                       6 -> fatal;
                       _ -> unknown
                   end).

-define(LOG(__LMod,__T,__L,__F,__A),
(fun() ->
    __LMod:log({?LLVL(__L), __T, atom_to_list(?MODULE), "", ?LINE, lists:flatten(io_lib:format(__F, __A))})
end)()
).

-endif. % LOG_HRL
