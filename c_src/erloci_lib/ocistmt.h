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

	void execute(void * column_list, void (*coldef_append)(const char *, const char *, const unsigned int, void *));
	inline vector<var> & get_bind_args() { return _args; };
	intf_ret rows(void * row_list,
				void (*string_append)(const char * string, size_t len, void * list),
				void (*list_append)(const void * sub_list, void * list),
				size_t (*sizeof_resp)(void * resp),
				unsigned int maxrowcount);
	void close(void);
};

#endif