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

-module(erloci).
-behaviour(application).
-behaviour(supervisor).

-include("oci.hrl").

% application start/stop interface
-export([start/0, stop/0]).

% Application callbacks
-export([start/2, stop/1]).

% Supervisor callback
-export([init/1]).

% create port interface
-export([new/1, new/2, del/1, bind_arg_types/0]).

-include("oci.hrl").
-include("oci_log_cb.hrl").

start() -> application:start(?MODULE).
start(_Type, _Args) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

stop() -> application:stop(?MODULE).
stop(_State) ->
    ok.

%% @doc Supervisor Callback
%% @hidden
init(_) ->
    {ok, {{simple_one_for_one,5,10},
          [{oci_port,{oci_port,start_link,[]},
            permanent, 5000, worker, [oci_port]}]}}.

new(Options) -> new(Options, ?LOGFUN).
new(Options, undefined) -> new(Options, ?LOGFUN);
new(Options, LogFun) ->
    case proplists:is_defined(pid, erlang:fun_info(LogFun)) of
        true -> error({badarg, "only external funs are allowed as log fun"});
        _ -> ok
    end,
    case supervisor:start_child(?MODULE, [Options, LogFun]) of
        {ok, undefined} -> error(port_start_failed);
        {error, Error} -> error(Error);
        {ok, ChildPid} -> {oci_port, ChildPid}
    end.

del(Child) -> supervisor:terminate_child(?MODULE, Child).

bind_arg_types() ->
    [atom_to_binary(T,utf8) || {T,_} <- ?CLM_TYPES].
