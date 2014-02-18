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
#ifndef OCI_LIB_INTF_H
#define OCI_LIB_INTF_H

typedef enum _LOG_LEVEL {
    DBG	= 0,
    INF	= 1,
	NTC = 2,
    ERR	= 3,
    WRN	= 4,
    CRT	= 5,
    FAT	= 6,
} LOG_LEVEL;

//#define MAX_RESP_SIZE 0xFFFFFFF0
#define MAX_RESP_SIZE 0x00040000UL
#define MAX_COLUMNS 500

//
// Exposed linkages (export)
//
extern void log_remote(const char *, const char *, unsigned int, unsigned int, void *, const char *, ...);

#ifndef __WIN32__
#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#define memcpy_s(_dest, _noelms, _src, _count)	memcpy((_dest), (_src), (_count))
#define sprintf_s(_a, _b, _c, ...)				sprintf((_a), (_c), __VA_ARGS__)
#define strncpy_s(_a, _b, _c, _d)               strncpy((_a), (_c), (_d))
#define vsprintf_s(_a, _b, _c, _d)              vsprintf((_a), (_c), (_d))
#define REMOTE_LOG_TERM(_level,_term,_str, ...)	if (log_flag) log_remote(__FILE__,__FUNCTION__,__LINE__,_level,_term,_str,##__VA_ARGS__)
#define REMOTE_LOG(_level,_str, ...)			if (log_flag) log_remote(__FILE__,__FUNCTION__,__LINE__,_level,NULL,_str,##__VA_ARGS__)
#define REMOTE_LOG_SINGLE(_str, ...)			if (log_flag) log_remote((_str),##__VA_ARGS__)
#else
#define REMOTE_LOG_TERM(_level,_term,_str, ...)	if (log_flag) log_remote(__FILE__,__FUNCTION__,__LINE__,_level,_term,_str,__VA_ARGS__)
#define REMOTE_LOG(_level,_str, ...)			if (log_flag) log_remote(__FILE__,__FUNCTION__,__LINE__,_level,NULL,_str,__VA_ARGS__)
#define REMOTE_LOG_SINGLE(_str, ...)			if (log_flag) log_remote((_str),__VA_ARGS__)
#endif

#include <vector>
typedef struct var {
	char name[256];
	unsigned short dty;
	std::vector<void *> valuep;
	std::vector<unsigned short> alen;
	std::vector<signed short> ind;
	unsigned short value_sz;
	void *ocibind;
	void *datap;
	unsigned long datap_len;
	var(char * _name = NULL, unsigned short _dty = 0)
	{
		dty = _dty;
		if (_name != NULL)
			strcpy((char*)name, _name);
	}
} var;

typedef enum _INTF_RET {
    SUCCESS				= 0,
    CONTINUE_WITH_ERROR	= 1,
    FAILURE				= 2,
    ERROR_VAL			= 3,
    MORE				= 4,
    DONE				= 5,
} INTF_RET;

typedef struct intf_ret {
	void		*handle;
	char		gerrbuf[512]; // from oracle example
	int			gerrcode;
	INTF_RET	fn_ret;
} intf_ret;

typedef void (*fn_coldefappend)(const char *, size_t, const unsigned short, const unsigned int,
							  const unsigned int, const unsigned int, void *);

// External linkages (import)
extern bool	log_flag;
extern unsigned long max_term_byte_size;

//extern void log_remote(const char *, const char *, unsigned int, unsigned int, void *, const char *, ...);

/* Error checking functions and macros */
#define checkerr(errhp, status) checkerr0((errhp), OCI_HTYPE_ERROR, (status), __FUNCTION__, __LINE__)
#define checkenv(envhp, status) checkerr0((envhp), OCI_HTYPE_ENV, (status), __FUNCTION__, __LINE__)
extern void checkerr0(intf_ret *, unsigned int, int, const char *, int);

#endif // OCI_LIB_INTF