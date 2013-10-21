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

#ifndef _OCISTMT_H_
#define _OCISTMT_H_

#include <iostream>
#include <vector>

using namespace std;

#include "oci_lib_intf.h"

// forward decleration, defined in cpp
struct column;
typedef struct column column;

#define LCL_DTYPE_NONE 9999 /* for code upgrade use any value not defined as OCI_DTYPE_* */

class ocistmt
{
private:
	void *_svchp;
	void *_stmthp;
	void *_errhp;
	void *_ocisess;
	unsigned int _iters;
	unsigned int _stmt_typ;
	vector<column> _columns;
	vector<var> _args;
	~ocistmt(void);

public:
	ocistmt(void *ocisess, unsigned char *stmt, unsigned int stmt_len);
	inline void del() { delete this; };

	unsigned int execute(void * column_list, void (*coldef_append)(const char *, const unsigned short, const unsigned int, void *));
	inline vector<var> & get_bind_args() { return _args; };
	intf_ret rows(void * row_list,
				void (*string_append)(const char * string, size_t len, void * list),
				void (*list_append)(const void * sub_list, void * list),
				size_t (*sizeof_resp)(void * resp),
				unsigned int maxrowcount);
	void close(void);
};

#endif