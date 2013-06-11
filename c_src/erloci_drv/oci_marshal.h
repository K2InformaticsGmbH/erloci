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

typedef enum _ERL_DEBUG {
    DBG_FLAG_OFF	= 0,
    DBG_FLAG_ON		= 1,
} ERL_DEBUG;


/* MUST use conteneous and monotonically
 * increasing number codes for new commands
 * MUST match with oci.hrl
 */
typedef enum _ERL_CMD {
    CREATE_SESSION_POOL	= 0,
    FREE_SESSION_POOL	= 1,

    GET_SESSION			= 2,
    RELEASE_SESSION		= 3,

    PREP_STMT           = 4,
    BIND_STMT           = 5,
    EXEC_STMT           = 6,
    FETCH_ROWS          = 7,

    R_DEBUG_MSG			= 8,
    QUIT				= 9,
} ERL_CMD;
extern const char *cmdnames[];
#define ERLOCI_CMD_DESC \
{\
    {CREATE_SESSION_POOL,	5, "Create TNS session pool"},\
    {FREE_SESSION_POOL,		1, "Release Session Pool"},\
\
    {GET_SESSION,			1, "Get a session from the TNS session pool"},\
    {RELEASE_SESSION,		2, "Return a previously allocated connection back to the pool"},\
\
    {PREP_STMT,				3, "Prepare a SQL statement from SQL string"},\
    {BIND_STMT,				3, "Bind variables into a prepared SQL statement"},\
    {EXEC_STMT,				3, "Execute a SQL statement"},\
    {FETCH_ROWS,			3, "Fetches the rows of a previously executed SELECT query"},\
\
    {R_DEBUG_MSG,			2, "Remote debugging turning on/off"},\
    {QUIT,					1, "Exit the port process"},\
}

#include "../erloci_lib/oci_lib_intf.h"

// Erlang Interface Macros
#define ARG_COUNT(_command)				(erl_size(_command) - 1)
#define MAP_ARGS(_command, _target)		{(_target)[0] = erl_element(1, _command); for(int _i=1;_i<ARG_COUNT(_command); ++_i)(_target)[_i] = erl_element(_i+2, _command);}

extern bool init_marshall(void);
extern inp_t * map_to_bind_args(void *);
extern void free_bind_args(inp_t *);
extern void * build_term_from_bind_args(inp_t *);
extern void * read_cmd(void);
extern int write_resp(void * resp_term);
extern void log_args(int, void *, const char *);
extern char * connect_tcp(int);
extern void close_tcp ();


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

extern char			gerrbuf[2048];
extern int			gerrcode;
extern char			session_pool_name[2048];

// ThreadPool
extern bool InitializeThreadPool(void);
extern void CleanupThreadPool(void);
extern bool ProcessCommand(void *);

extern unsigned int calculate_resp_size(void * resp);
extern void append_list_to_list(const void * sub_list, void * list);
extern void append_int_to_list(const int integer, void * list);
extern void append_string_to_list(const char * string, int len, void * list);
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

//		REMOTE_LOG("%03d,", (unsigned char)__buf[j]);		
//     REMOTE_LOG("HEX DUMP ---->\n%s\n<----\n", _t);			
