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
% Name and alue inporetd from ocistmt.h
-define(CLM_TYPES, [
{'SQLT_CHR',           1},
{'SQLT_NUM',           2},
{'SQLT_INT',           3},
{'SQLT_FLT',           4},
{'SQLT_STR',           5},
{'SQLT_VNU',           6},
{'SQLT_PDN',           7},
{'SQLT_LNG',           8},
{'SQLT_VCS',           9},
{'SQLT_NON',           10},
{'SQLT_RID',           11},
{'SQLT_DAT',           12},
{'SQLT_VBI',           15},
{'SQLT_BFLOAT',        21},
{'SQLT_BDOUBLE',       22},
{'SQLT_BIN',           23},
{'SQLT_LBI',           24},
{'SQLT_UIN',           68},
{'SQLT_SLS',           91},
{'SQLT_LVC',           94},
{'SQLT_LVB',           95},
{'SQLT_AFC',           96},
{'SQLT_AVC',           97},
{'SQLT_IBFLOAT',       100},
{'SQLT_IBDOUBLE',      101},
{'SQLT_CUR',           102},
{'SQLT_RDD',           104},
{'SQLT_LAB',           105},
{'SQLT_OSL',           106},

{'SQLT_NTY',           108},
{'SQLT_REF',           110},
{'SQLT_CLOB',          112},
{'SQLT_BLOB',          113},
{'SQLT_BFILEE',        114},
{'SQLT_CFILEE',        115},
{'SQLT_RSET',          116},
{'SQLT_NCO',           122},
{'SQLT_VST',           155},
{'SQLT_ODT',           156},

{'SQLT_DATE',          184},
{'SQLT_TIME',          185},
{'SQLT_TIME_TZ',       186},
{'SQLT_TIMESTAMP',     187},
{'SQLT_TIMESTAMP_TZ',  188},
{'SQLT_INTERVAL_YM',   189},
{'SQLT_INTERVAL_DS',   190},
{'SQLT_TIMESTAMP_LTZ', 232},

{'SQLT_PNTY',          241},

{'SQLT_FILE',          114}, % SQLT_BFILEE
{'SQLT_CFILE',         115}, % SQLT_CFILEE
{'SQLT_BFILE',         114}  % SQLT_BFILEE
]).

-define(CT(__C), proplists:get_value(__C, ?CLM_TYPES, 0)).
-define(CS(__C), case lists:keyfind(__C, 2, ?CLM_TYPES) of false -> undefined; __Tuple -> element(1, __Tuple) end).

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
