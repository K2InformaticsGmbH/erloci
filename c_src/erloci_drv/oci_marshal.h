/* Copyright 2012 K2Informatics GmbH, Root Laengenbold, Switzerland
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */ 
#pragma once

// Protocol spec
#define PKT_LEN_BYTES	4

// Debug Levels
#define DBG_0			0	// All Debug (Including binaries for Rx and Tx)
#define DBG_1			1	// Function Entry Exit
#define DBG_2			2	//
#define DBG_3			3	//
#define DBG_4			4	//
#define DBG_5			5	// Debug is off

#define DEBUG			DBG_5

typedef struct _erlcmdtable {
    int cmd;
	const char * cmd_str;
    int arg_count;
    const char * cmd_description;
} erlcmdtable;

typedef enum _ERL_DEBUG {
    DBG_FLAG_OFF	= 0,
    DBG_FLAG_ON		= 1,
} ERL_DEBUG;


/* MUST use conteneous and monotonically
 * increasing number codes for new commands
 * MUST match with oci.hrl
 */
typedef enum _ERL_CMD {
    GET_SESSN			= 1,
    PUT_SESSN			= 2,
    PREP_STMT			= 3,
    BIND_ARGS			= 4,
    EXEC_STMT			= 5,
    FTCH_ROWS			= 6,
    CLSE_STMT			= 7,
    RMOTE_MSG			= 8,
    OCIP_QUIT			= 9,
} ERL_CMD;

/*
TODO: document communication interface here
Must match with that or erlang side
*/
extern const erlcmdtable cmdtbl[];
#define CMD_NAME_STR(_cmd)		cmdtbl[((_cmd) > OCIP_QUIT ? 0 : (_cmd))].cmd_str
#define CMD_ARGS_COUNT(_cmd)	cmdtbl[((_cmd) > OCIP_QUIT ? 0 : (_cmd))].arg_count
#define CMD_DESCIPTION(_cmd)	cmdtbl[((_cmd) > OCIP_QUIT ? 0 : (_cmd))].cmd_description
#define CMDTABLE \
{\
    {0,			"CMD_UNKWN",	0, "Unknown command"},\
    {GET_SESSN,	"GET_SESSN",	4, "Get a OCI session"},\
    {PUT_SESSN,	"PUT_SESSN",	2, "Release a OCI session"},\
    {PREP_STMT,	"PREP_STMT",	3, "Prepare a statement from SQL string"},\
    {BIND_ARGS,	"BIND_ARGS",	4, "Bind parameters into prepared SQL statement"},\
    {EXEC_STMT,	"EXEC_STMT",	2, "Execute a prepared statement"},\
    {FTCH_ROWS,	"FTCH_ROWS",	3, "Fetch rows from statements producing rows"},\
    {CLSE_STMT,	"CLSE_STMT",	2, "Close a statement"},\
    {RMOTE_MSG,	"RMOTE_MSG",	2, "Remote debugging turning on/off"},\
    {OCIP_QUIT,	"OCIP_QUIT",	1, "Exit the port process"}\
}

extern char * print_term(void*);

#include "oci_lib_intf.h"

// Erlang Interface Macros
#define ARG_COUNT(_command)				(erl_size(_command) - 1)
#define MAP_ARGS(_cmdcnt, _command, _target) \
{\
	(_target) = new ETERM*[(_cmdcnt)];\
	(_target)[0] = erl_element(1, _command);\
	for(int _i=1;_i<ARG_COUNT(_command); ++_i)\
		(_target)[_i] = erl_element(_i+2, _command);\
}
#define UNMAP_ARGS(_cmdcnt, _target) \
{\
	for(int _i=0;_i<(_cmdcnt); ++_i)\
		erl_free_term((_target)[_i]);\
	delete (_target);\
}

#define PRINT_ERL_ALLOC(_mark) \
{\
	unsigned long allocated, freed;\
	erl_eterm_statistics(&allocated, &freed);\
	REMOTE_LOG(_mark" eterm mem %lu, %lu\n", allocated, freed);\
}

extern bool init_marshall(void);
extern void * read_cmd(void);
extern int write_resp(void * resp_term);
extern void log_args(int, void *, const char *);
extern char * connect_tcp(int);
extern void close_tcp ();

extern inp_t * map_to_bind_args(void *);
extern void * build_term_from_bind_args(inp_t *);

#ifdef REMOTE_LOG
#undef REMOTE_LOG
#endif

#ifndef __WIN32__
#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#define memcpy_s(_dest, _noelms, _src, _count)	memcpy((_dest), (_src), (_count))
#define sprintf_s(_a, _b, _c, ...)				sprintf((_a), (_c), __VA_ARGS__)
#define strncpy_s(_a, _b, _c, _d)               strncpy((_a), (_c), (_d))
#define vsprintf_s(_a, _b, _c, _d)              vsprintf((_a), (_c), (_d))
#define REMOTE_LOG(_str, ...)		if (log_flag) log_remote(("[debug] [_PRT_] {%s:%s:%d} "_str),__FILE__,__FUNCTION__,__LINE__,##__VA_ARGS__)
#else
#define REMOTE_LOG(_str, ...)		if (log_flag) log_remote(("[debug] [_PRT_] {%s:%s:%d} "_str),__FILE__,__FUNCTION__,__LINE__,__VA_ARGS__)
#endif

#if DEBUG <= DBG_3
#define LOG_ARGS(_count,_args,_str)	    log_args((_count),(_args),(_str))
#else
#define LOG_ARGS(_count,_args,_str)
#endif

extern bool lock_log();
extern void unlock_log();

// ThreadPool
extern bool InitializeThreadPool(void);
extern void CleanupThreadPool(void);
extern bool ProcessCommand(void *);

extern size_t calculate_resp_size(void * resp);
extern void append_list_to_list(const void * sub_list, void * list);
extern void append_int_to_list(const int integer, void * list);
extern void append_string_to_list(const char * string, size_t len, void * list);
extern void append_coldef_to_list(const char * col_name, const char * data_type, const unsigned int max_len, void * list);

#define MAX_FORMATTED_STR_LEN 1024

// Printing the packet
#if DEBUG <= DBG_0
#define LOG_DUMP(__len, __buf)								\
{    														\
	char *_t = new char[__len*4+1];							\
	char *_pt = _t;											\
    for(unsigned int j=0; j<__len; ++j) {					\
		sprintf(_pt, "%03d,", (unsigned char)__buf[j]);		\
		_pt += 4;											\
    }														\
	REMOTE_LOG("HEX DUMP ---->\n%s\n<----\n", _t);			\
	delete _t;												\
}
#else
#define LOG_DUMP(__len, __buf)
#endif
