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
#ifndef OCISTMT_H
#define OCISTMT_H

#include <iostream>
#include <vector>
#include <string>

using namespace std;

#include "lib_interface.h"

// forward decleration, defined in cpp
struct column;
typedef struct column column;

#define LCL_DTYPE_NONE 9999 /* for code upgrade use any value not defined as OCI_DTYPE_* */

#define INT_SQLT_TIMESTAMP		180	// 11 bytes
#define INT_SQLT_TIMESTAMP_TZ	181	// 13 bytes
#define INT_SQLT_TIMESTAMP_LTZ	231	// 11 bytes
#define INT_SQLT_INTERVAL_YM	182	//  5 bytes
#define INT_SQLT_INTERVAL_DS	183 // 11 bytes

class ocistmt
{
public:
	ocistmt(void *ocisess, void *stmt);
	ocistmt(void *ocisess, unsigned char *stmt, size_t stmt_len);
	inline void del() { delete this; };

	unsigned int execute(void * column_list, void * rowid_list, void * out_list, bool);
	inline vector<var> & get_in_bind_args() { return _argsin; };
	inline vector<var> & get_out_bind_args() { return _argsout; };
	intf_ret rows(void * row_list, unsigned int maxrowcount);
	intf_ret lob(void * data, void * lob, unsigned long long offset, unsigned long long length);
	void close(void);

	static void config(intf_funs);

private:
	static intf_funs intf;

	char *_stmtstr;
	void *_svchp;
	void *_stmthp;
	void *_errhp;
	void *_ocisess;
	size_t _iters;
	unsigned int _stmt_typ;
	vector<column *> _columns;
	vector<var> _argsin;
	vector<var> _argsout;
	~ocistmt(void);
};

#endif // OCISTMT_H