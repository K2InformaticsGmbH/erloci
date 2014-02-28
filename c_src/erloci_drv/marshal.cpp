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
#include "marshal.h"
#include "erl_interface.h"

#include <ocidfn.h>
#include <orl.h>

#ifdef __WIN32__
#include <windows.h>
#include <Winsock2.h>
#else
#include <netinet/in.h>
#include <pthread.h>
#include <stdlib.h>
#endif

#if DEBUG <= DBG_0
#define ENTRY()	{REMOTE_LOG(DBG, "Entry");}
#define EXIT()	{REMOTE_LOG(DBG, "Exit");}
#else
#define ENTRY()
#define EXIT()
#endif

const erlcmdtable cmdtbl[] = CMDTABLE;

size_t calculate_resp_size(void * resp)
{
    //return (size_t)erl_term_len(*(ETERM**)resp);
	return 1;
}

#if DEBUG < DBG_5
void log_args(int argc, void * argv, const char * str)
{
    int sz = 0, idx = 0;
    char *arg = NULL;
    ETERM **args = (ETERM **)argv;
    REMOTE_LOG(DBG, "CMD: %s Args(\n", str);
    for(idx=0; idx<argc; ++idx) {
        if (ERL_IS_BINARY(args[idx])) {
            sz = ERL_BIN_SIZE(args[idx]);
            arg = new char[sz+1];
            memcpy_s(arg, sz+1, ERL_BIN_PTR(args[idx]), sz);
            arg[sz] = '\0';
            REMOTE_LOG(DBG, "%s,\n", arg);
            if (arg != NULL) delete arg;
        }
		else if (ERL_IS_INTEGER(args[idx]))				{REMOTE_LOG(DBG, "%d,",	ERL_INT_VALUE(args[idx])); }
		else if (ERL_IS_UNSIGNED_INTEGER(args[idx]))	{REMOTE_LOG(DBG, "%u,",	ERL_INT_UVALUE(args[idx]));}
		else if (ERL_IS_FLOAT(args[idx]))				{REMOTE_LOG(DBG, "%lf,",	ERL_FLOAT_VALUE(args[idx]));}
		else if (ERL_IS_ATOM(args[idx]))				{REMOTE_LOG(DBG, "%.*s,", ERL_ATOM_SIZE(args[idx]), ERL_ATOM_PTR(args[idx]));}
    }
    REMOTE_LOG(DBG, ")\n");
}
#else
void log_args(int argc, void * argv, const char * str) {}
#endif

void append_list_to_list(const void * sub_list, void * list)
{
    ASSERT(list != NULL || sub_list != NULL);

    term *container_list = (term *)list;
    ASSERT(container_list->is_list());

	container_list->add((const term *)sub_list);
}

void append_int_to_list(const int integer, void * list)
{
	ASSERT(list!=NULL);

    term *container_list = (term *)list;
    ASSERT(container_list->is_list());

	container_list->add(integer);
}

void append_string_to_list(const char * string, size_t len, void * list)
{
	ASSERT(list!=NULL);

    term *container_list = *(term **)list;
	if (!container_list) {
		container_list = new term();
		container_list->lst();
	}
    ASSERT(container_list->is_list());

	if (string)
		container_list->add(term().binary(string, len));

	(*(term**)list) = container_list;
}

void append_coldef_to_list(const char * col_name, size_t len, const unsigned short data_type, const unsigned int max_len,
						   const unsigned short precision, const signed char scale, void * list)
{
	ASSERT(list!=NULL);

    term *container_list = (term *)list;
    ASSERT(container_list->is_list());

	container_list->add(term().tuple()
		.add(term().binary(col_name, len))
		.add(data_type)
		.add(max_len)
		.add(precision)
		.add(scale)
	);
}

void append_desc_to_list(const char * col_name, size_t len, const unsigned short data_type, const unsigned int max_len, void * list)
{
	ASSERT(list!=NULL);

    term *container_list = (term *)list;
    ASSERT(container_list->is_list());

	container_list->add(term().tuple()
							.add(term().binary(col_name, len))
							.add((unsigned int)data_type)
							.add(max_len));
}

void map_schema_to_bind_args(term & t, vector<var> & vars)
{
	ASSERT(t.is_list() && t.length() > 0);

    var v;

	size_t len = 0;
	for (term::iterator it = t.begin() ; it != t.end(); ++it) {
		ASSERT((*it).is_tuple()
			&& (*it).length() == 2
			&& (*it)[0].is_binary()
			&& (*it)[1].is_any_int());

		if(sizeof(v.name) < (*it)[0].str_len+1) {
			REMOTE_LOG(ERR, "variable %s is too long, max %d\n", (*it)[0].str, sizeof(v.name)-1);
			throw string("variable name is larger then 255 characters");
		}
		strncpy(v.name, (*it)[0].str, (*it)[0].str_len);
		v.name[(*it)[0].str_len]='\0';

		v.dty = (*it)[1].v.ui;

		// Initialized, to be prepared later on first execute
		v.value_sz = 0;
		v.datap = NULL;
		v.datap_len = 0;

		vars.push_back(v);
	}
}

size_t map_value_to_bind_args(term & t, vector<var> & vars)
{
	ASSERT(t.is_list());
	ASSERT(t.length() >= 0);

	intf_ret r;	
	r.fn_ret = CONTINUE_WITH_ERROR;

	void * tmp_arg = NULL;
	sb2 ind = -1;
	size_t arg_len = 0;
	
	// remove any old bind from the vars
	for(unsigned int i=0; i < vars.size(); ++i) {
		vars[i].valuep.clear();
		vars[i].alen.clear();
	}
	
	// loop through the list
	size_t bind_count = 0;
	//for (size_t li = 0; li < t.length(); ++li) {
	for (term::iterator it = t.begin() ; it != t.end(); ++it) {
		++bind_count;
		term & t1 = (*it);
        if (!t1.is_tuple() || t1.length() != vars.size()) {
			REMOTE_LOG(ERR, "malformed ETERM\n");
			strcpy(r.gerrbuf, "Malformed ETERM");
            throw r;
		}

		// loop through each value of the tuple
		int i = 0;
		for (term::iterator it1 = (*it).begin(); it1 != (*it).end(); ++it1) {
			term & t2 = (*it1);
			if (t2.is_undef()) {
				REMOTE_LOG(ERR, "row %d: missing parameter for %s\n", bind_count, vars[i].name);
				strcpy(r.gerrbuf, "Missing parameter term");
				throw r;
			}
			
			ind = -1;
			tmp_arg = NULL;
			arg_len = 0;
			switch(vars[i].dty) {
				case SQLT_BFLOAT:
				case SQLT_BDOUBLE:
				case SQLT_FLT:
					if(t2.is_any_int() || t2.is_float()) {
						ind = 0;
						arg_len = sizeof(double);
						tmp_arg = new double;
						*(double*)tmp_arg = (double)(t2.is_any_int() ? t2.v.ll : t2.v.d);
					} else {
						REMOTE_LOG(ERR, "row %d: malformed float for %s (got %s expected ERL_INTEGER or ERL_FLOAT)\n", bind_count, vars[i].name, t2.type);
						//REMOTE_LOG_TERM(ERR, arg, "bad term\n");
						//REMOTE_LOG_TERM(ERR, item, "within (%u)\n", i);
						strcpy(r.gerrbuf, "Malformed float parameter value");
						throw r;
					}
					break;
				case SQLT_NUM:
				case SQLT_INT:
					if(t2.is_any_int()) {
						ind = 0;
						arg_len = sizeof(int);
						tmp_arg = new int;
						*(int*)tmp_arg = t2.v.i;
					} else {
						REMOTE_LOG(ERR, "row %d: malformed integer for %s (got %s expected ERL_INTEGER)\n", bind_count, vars[i].name, t2.type);
						//REMOTE_LOG_TERM(ERR, arg, "bad term\n");
						//REMOTE_LOG_TERM(ERR, item, "within (%u)\n", i);
						strcpy(r.gerrbuf, "Malformed integer parameter value");
						throw r;
					}
					break;
				case SQLT_VNU:
					if(t2.is_binary() && t2.str_len > 0 && t2.str_len <= OCI_NUMBER_SIZE) {
						ind = 0;
						arg_len = t2.str_len;
						tmp_arg = new char[arg_len];
						memcpy(tmp_arg, t2.str, arg_len);
					} else {
						REMOTE_LOG(ERR, "row %d: malformed number for %s (got %s expected ERL_BINARY)\n", bind_count, vars[i].name, t2.type);
						//REMOTE_LOG_TERM(ERR, arg, "bad term\n");
						//REMOTE_LOG_TERM(ERR, item, "within (%u)\n", i);
						strcpy(r.gerrbuf, "Malformed number parameter value");
						throw r;
					}
					break;
				case SQLT_RDD:
				case SQLT_DAT:
					if(t2.is_binary() && t2.str_len > 0) {
						ind = 0;
						arg_len = t2.str_len;
						tmp_arg = new char[arg_len];
						memcpy(tmp_arg, t2.str, arg_len);
					} else {
						REMOTE_LOG(ERR, "row %d: malformed binary for %s (got %s expected ERL_BINARY)\n", bind_count, vars[i].name, t2.type);
						//REMOTE_LOG_TERM(ERR, arg, "bad term\n");
						//REMOTE_LOG_TERM(ERR, item, "within (%u)\n", i);
						strcpy(r.gerrbuf, "Malformed binary parameter value");
						throw r;
					}
					break;
				case SQLT_ODT:
					if(t2.is_binary() && t2.str_len > 0) {
						ind = 0;
						arg_len = t2.str_len;
						tmp_arg = new char[arg_len];
						memcpy(tmp_arg, t2.str, arg_len);
						((OCIDate*)tmp_arg)->OCIDateYYYY = htons((ub2)((OCIDate*)tmp_arg)->OCIDateYYYY);
					} else {
						REMOTE_LOG(ERR, "row %d: malformed date for %s (got %s expected ERL_BINARY)\n", bind_count, vars[i].name, t2.type);
						//REMOTE_LOG_TERM(ERR, arg, "bad term\n");
						//REMOTE_LOG_TERM(ERR, item, "within (%u)\n", i);
						strcpy(r.gerrbuf, "Malformed date parameter value");
						throw r;
					}
					break;
				case SQLT_AFC:
				case SQLT_CHR:
				case SQLT_LNG:
					if(t2.is_binary() && t2.str_len > 0) {
						ind = 0;
						arg_len = t2.str_len;
						tmp_arg = new char[arg_len];
						memcpy(tmp_arg, t2.str, arg_len);
					} else {
						REMOTE_LOG(ERR, "row %d: malformed string for %s (got %s expected ERL_BINARY)\n", bind_count, vars[i].name, t2.type);
						//REMOTE_LOG_TERM(ERR, arg, "bad term\n");
						//REMOTE_LOG_TERM(ERR, item, "within (%u)\n", i);
						strcpy(r.gerrbuf, "Malformed string parameter value");
						throw r;
					}
					break;
				case SQLT_STR:
					if(t2.is_binary() && t2.str_len > 0) {
						ind = 0;
						arg_len = t2.str_len;
						tmp_arg = new char[arg_len+1];
						memcpy(tmp_arg, t2.str, arg_len);
						((char*)tmp_arg)[arg_len] = '\0';
						arg_len++;
					} else {
						REMOTE_LOG(ERR, "row %d: malformed string\\0 for %s (got %s expected ERL_BINARY)\n", bind_count, vars[i].name, t2.type);
						//REMOTE_LOG_TERM(ERR, arg, "bad term\n");
						//REMOTE_LOG_TERM(ERR, item, "within (%u)\n", i);
						strcpy(r.gerrbuf, "Malformed string\\0 parameter value");
						throw r;
					}
					break;
				default:
					strcpy(r.gerrbuf, "Unsupported type in bind");
					throw r;
					break;
			}
			vars[i].alen.push_back((unsigned short)arg_len);
			if (arg_len > vars[i].value_sz)
				vars[i].value_sz = (unsigned short)arg_len;
			vars[i].valuep.push_back(tmp_arg);
			vars[i].ind.push_back(ind);
			++i;
		}
    }

	return bind_count;
}
