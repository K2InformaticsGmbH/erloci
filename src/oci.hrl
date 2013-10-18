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
-define(CMD_UNKWN,  0).
-define(GET_SESSN,  1).
-define(PUT_SESSN,  2).
-define(PREP_STMT,  3).
-define(BIND_ARGS,  4).
-define(EXEC_STMT,  5).
-define(FTCH_ROWS,  6).
-define(CLSE_STMT,  7).
-define(RMOTE_MSG,  8).
-define(OCIP_QUIT,  9).

-define(CMDSTR(__CMD), (fun
                            (?CMD_UNKWN)    -> "CMD_UNKWN";
                            (?GET_SESSN)    -> "GET_SESSN";
                            (?PUT_SESSN)    -> "PUT_SESSN";
                            (?PREP_STMT)    -> "PREP_STMT";
                            (?BIND_ARGS)    -> "BIND_ARGS";
                            (?EXEC_STMT)    -> "EXEC_STMT";
                            (?FTCH_ROWS)    -> "FTCH_ROWS";
                            (?CLSE_STMT)    -> "CLSE_STMT";
                            (?RMOTE_MSG)    -> "RMOTE_MSG";
                            (?OCIP_QUIT)    -> "OCIP_QUIT";
                            (__C)           -> "UNKNOWN "++integer_to_list(__C)
                        end)(__CMD)).

%% Bind arg types
% MUST match with enum VAR_TYPE ocistmt.h
-define(SQLT_CHR,  1).
-define(SQLT_NUM,  2).
-define(SQLT_INT,  3).
-define(SQLT_FLT,  4).
-define(SQLT_STR,  5).
-define(SQLT_VNU,  6).
-define(SQLT_PDN,  7).
-define(SQLT_LNG,  8).
-define(SQLT_VCS,  9).
-define(SQLT_NON,  10).
-define(SQLT_RID,  11).
-define(SQLT_DAT,  12).
-define(SQLT_VBI,  15).
-define(SQLT_BFLOAT, 21).
-define(SQLT_BDOUBLE, 22).
-define(SQLT_BIN,  23).
-define(SQLT_LBI,  24).
-define(SQLT_UIN,  68).
-define(SQLT_SLS,  91).
-define(SQLT_LVC,  94).
-define(SQLT_LVB,  95).
-define(SQLT_AFC,  96).
-define(SQLT_AVC,  97).
-define(SQLT_IBFLOAT,  100).
-define(SQLT_IBDOUBLE, 101).
-define(SQLT_CUR,  102).
-define(SQLT_RDD,  104).
-define(SQLT_LAB,  105).
-define(SQLT_OSL,  106).

-define(SQLT_NTY,  108).
-define(SQLT_REF,  110).
-define(SQLT_CLOB, 112).
-define(SQLT_BLOB, 113).
-define(SQLT_BFILEE, 114).
-define(SQLT_CFILEE, 115).
-define(SQLT_RSET, 116).
-define(SQLT_NCO,  122).
-define(SQLT_VST,  155).
-define(SQLT_ODT,  156).

-define(SQLT_DATE,                      184).
-define(SQLT_TIME,                      185).
-define(SQLT_TIME_TZ,                   186).
-define(SQLT_TIMESTAMP,                 187).
-define(SQLT_TIMESTAMP_TZ,              188).
-define(SQLT_INTERVAL_YM,               189).
-define(SQLT_INTERVAL_DS,               190).
-define(SQLT_TIMESTAMP_LTZ,             232).

-define(SQLT_PNTY,   241).

-define(SQLT_FILE, ?SQLT_BFILEE).
-define(SQLT_CFILE, ?SQLT_CFILEE).
-define(SQLT_BFILE, ?SQLT_BFILEE).


% Argument Types
-define(ARG_DIR_IN,			0).
-define(ARG_DIR_OUT,		1).
-define(ARG_DIR_INOUT,		2).

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
