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

%% Process
%-define(PORT_TIMEOUT, 			15000).
-define(PORT_TIMEOUT, 			infinity).
-define(WAIT_RESULT_TIMEOUT, 	15000).

%% Exec name
-define(EXE_NAME, "erloci").

%% Interface commands
% pool commands
% MUST use conteneous and monotonically
% increasing number codes for new commands
% MUST match with oci_marshal.h
-define(CREATE_SESSION_POOL,    0).
-define(FREE_SESSION_POOL,	    1).

-define(GET_SESSION,		    2).
-define(RELEASE_SESSION,        3).

-define(PREP_STMT,			    4).
-define(BIND_STMT,			    5).
-define(EXEC_STMT,			    6).
-define(FETCH_ROWS,			    7).

-define(R_DEBUG_MSG,		    8).
-define(QUIT,				    9).

-define(CMDSTR(__CMD), (fun
                            (?CREATE_SESSION_POOL)  -> "CREATE_SESSION_POOL";
                            (?FREE_SESSION_POOL)    -> "RELEASE_SESSION_POOL";

                            (?GET_SESSION)          -> "GET_SESSION";
                            (?RELEASE_SESSION)      -> "RELEASE_SESSION";

                            (?PREP_STMT)            -> "PREP_STMT";
                            (?BIND_STMT)            -> "BIND_STMT";
                            (?EXEC_STMT)            -> "EXEC_STMT";
                            (?FETCH_ROWS)           -> "FETCH_ROWS";

                            (?R_DEBUG_MSG)          -> "R_DEBUG_MSG";
                            (?QUIT)                 -> "QUIT";

                            (__C)                   -> "UNKNOWN "++integer_to_list(__C)
                        end)(__CMD)).

% Argument Types
-define(ARG_DIR_IN,			0).
-define(ARG_DIR_OUT,		1).
-define(ARG_DIR_INOUT,		2).

% Datatype Codes
-define(NUMBER,				0).
-define(STRING,				1).

-define(APP_START_TIME, 	1000).

% Debug flags
-define(DBG_FLAG_OFF, 		0).
-define(DBG_FLAG_ON, 		1).

%% types
-type session() :: {oci_session, pid()}.  %% parameterized Module
-type statement() :: {oci_session, any(), pid()}. %% parameterized Module
-type ora_error() :: string().            %% Oracle Error String

-include("log.hrl").
-define(LOG_TAG, "_OCI_").

-define(Debug(__M,__F,__A), ?LOG(?LOG_TAG, dbg, __M, "{~p,~4..0B} "++__F, [?MODULE,?LINE]++__A)).
-define(Debug(__F,__A),     ?LOG(?LOG_TAG, dbg,  [], "{~p,~4..0B} "++__F, [?MODULE,?LINE]++__A)).
-define(Debug(__F),         ?LOG(?LOG_TAG, dbg,  [], "{~p,~4..0B} "++__F, [?MODULE,?LINE])).

-define(Info(__M,__F,__A),  ?LOG(?LOG_TAG, nfo, __M, "{~p,~4..0B} "++__F, [?MODULE,?LINE]++__A)).
-define(Info(__F,__A),      ?LOG(?LOG_TAG, nfo,  [], "{~p,~4..0B} "++__F, [?MODULE,?LINE]++__A)).
-define(Info(__F),          ?LOG(?LOG_TAG, nfo,  [], "{~p,~4..0B} "++__F, [?MODULE,?LINE])).

-define(Error(__M,__F,__A), ?LOG(?LOG_TAG, err, __M, "{~p,~4..0B} "++__F, [?MODULE,?LINE]++__A)).
-define(Error(__F,__A),     ?LOG(?LOG_TAG, err,  [], "{~p,~4..0B} "++__F, [?MODULE,?LINE]++__A)).
-define(Error(__F),         ?LOG(?LOG_TAG, err,  [], "{~p,~4..0B} "++__F, [?MODULE,?LINE])).
