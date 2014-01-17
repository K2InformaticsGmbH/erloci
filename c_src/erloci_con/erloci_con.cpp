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
#include "stdafx.h"
#include "ocisession.h"

#include "string.h"
#include "stdarg.h"

#include <ocidfn.h>

void append_coldef_to_list(const char * col_name, size_t len,
						   const unsigned short data_type, const unsigned int max_len, const unsigned short prec,
						   const signed char scale, void * list)
{
	printf("\t%s\n", col_name);
}

static unsigned int string_count = 0;
void string_append(const char * string, size_t len, void * list)
{
	//printf("%.*s\t", len, string);
	string_count++;
	if (string_count % 100 == 0)
		printf("string items %d\n", string_count);
}

static unsigned int list_count = 0;
void list_append(const void * sub_list, void * list)
{
	//printf("\t");
	list_count++;
	if (list_count % 100 == 0)
		printf("list items %d\n", list_count);
}

size_t sizeof_resp(void * resp)
{
	return 0;
}

int memory_leak_test(const char *tns, const char *usr, const char *pwd)
{
	ocisession * ocisess = NULL;
	ocistmt *stmt = NULL;

	const char * qry = "SELECT * FROM ALL_TABLES";

	for(int i = 100; i>0; --i) {
		try {
			ocisess = new ocisession(tns, strlen(tns),
									 usr, strlen(usr),
									 pwd, strlen(pwd));
		}
		catch (intf_ret r) {
			switch(r.fn_ret) {
				case CONTINUE_WITH_ERROR:
					printf("oci_get_session error... %s!\n", r.gerrbuf);
					return -1;
				case FAILURE:
					printf("oci_get_session failed... %s!\n", r.gerrbuf);
					return -1;
			}
		}

		printf("Columns:\n");
		try {
			stmt = ocisess->prepare_stmt((unsigned char *)qry, strlen(qry));
			stmt->execute(NULL, append_coldef_to_list, NULL, string_append, true);
		}
		catch (intf_ret r) {
			switch(r.fn_ret) {
				case CONTINUE_WITH_ERROR:
					printf("oci_exec_sql error... %s!\n", r.gerrbuf);
					return -1;
				case FAILURE:
					printf("oci_exec_sql failed... %s!\n", r.gerrbuf);
					return -1;
			}
		}
		
		try {
			stmt->rows(NULL, string_append, list_append, sizeof_resp, 100);
		}
		catch (intf_ret r) {
			switch(r.fn_ret) {
				case CONTINUE_WITH_ERROR:
					printf("oci_produce_rows error... %s!\n", r.gerrbuf);
					return -1;
				case FAILURE:
					printf("oci_produce_rows failed... %s!\n", r.gerrbuf);
					return -1;
			}
		}

		try {
			stmt->close();
		}
		catch (intf_ret r) {
			switch(r.fn_ret) {
				case SUCCESS:
					printf("oci_close_statement success...!\n");
					break;
				case CONTINUE_WITH_ERROR:
					printf("oci_close_statement error... %s!\n", r.gerrbuf);
					return -1;
				case FAILURE:
					printf("oci_close_statement failed... %s!\n", r.gerrbuf);
					return -1;
			}
		}

		try {
			delete ocisess;
		}
		catch (intf_ret r) {
			switch(r.fn_ret) {
				case SUCCESS:
					//printf("oci_free_session success...!\n");
					break;
				case CONTINUE_WITH_ERROR:
					printf("oci_free_session error... %s!\n", r.gerrbuf);
					return -1;
				case FAILURE:
					printf("oci_free_session failed... %s!\n", r.gerrbuf);
					return -1;
			}
		}
	}
}


char modqry[1024];
int drop_create_insert_select(const char *tns, const char *usr, const char *pwd, int tid)
{
	ocisession * ocisess = NULL;
	ocistmt *stmt = NULL;
	intf_ret r;

	try {
		ocisess = new ocisession(tns, strlen(tns),
							     usr, strlen(usr),
								 pwd, strlen(pwd));
	}
	catch (intf_ret r) {
		switch(r.fn_ret) {
			case CONTINUE_WITH_ERROR:
				printf("oci_get_session error... %s!\n", r.gerrbuf);
				return -1;
			case FAILURE:
				printf("oci_get_session failed... %s!\n", r.gerrbuf);
				return -1;
		}
	}

	// Drop can fail so ignoring the errors
	sprintf(modqry, "drop table oci_test_table_%d", tid);
	try {
		stmt = ocisess->prepare_stmt((unsigned char *)modqry, strlen(modqry));
		stmt->execute(NULL, append_coldef_to_list, NULL, string_append, true);
	}
	catch (intf_ret r) {
		switch(r.fn_ret) {
			case CONTINUE_WITH_ERROR:
				printf("oci_exec_sql error... %s!\n", r.gerrbuf);
				break;
			case FAILURE:
				printf("oci_exec_sql failed... %s!\n", r.gerrbuf);
				break;
		}
	}

	// Create table
	sprintf(modqry, "create table oci_test_table_%d(pkey number,\
                                       publisher varchar2(100),\
                                       rank number,\
                                       hero varchar2(100),\
                                       real varchar2(100),\
                                       votes number,\
                                       votes_first_rank number)", tid);
	try {
		stmt = ocisess->prepare_stmt((unsigned char *)modqry, strlen(modqry));
		stmt->execute(NULL, append_coldef_to_list, NULL, string_append, true);
	}
	catch (intf_ret r) {
		switch(r.fn_ret) {
			case SUCCESS:
				//printf("oci_exec_sql success...!\n");
				break;
			case CONTINUE_WITH_ERROR:
				printf("oci_exec_sql error... %s!\n", r.gerrbuf);
				return -1;
			case FAILURE:
				printf("oci_exec_sql failed... %s!\n", r.gerrbuf);
				return -1;
		}
	}

	// insert some rows
	for(int i=10; i>0; --i) {
		sprintf(modqry, "insert into oci_test_table_%d values (%d,'publisher%d',%d,'hero%d','real%d',%d,%d)", tid, i, i, i, i, i, i, i);
		try {
			stmt = ocisess->prepare_stmt((unsigned char *)modqry, strlen(modqry));
			stmt->execute(NULL, append_coldef_to_list, NULL, string_append, true);
		}
		catch (intf_ret r) {
			switch(r.fn_ret) {
				case CONTINUE_WITH_ERROR:
					printf("oci_exec_sql error... %s!\n", r.gerrbuf);
					return -1;
				case FAILURE:
					printf("oci_exec_sql failed... %s!\n", r.gerrbuf);
					return -1;
			}
		}
	}

	// read back rows
	sprintf(modqry, "select * from oci_test_table_%d", tid);
	try {
		stmt = ocisess->prepare_stmt((unsigned char *)modqry, strlen(modqry));
		stmt->execute(NULL, append_coldef_to_list, NULL, string_append, true);
	}
	catch (intf_ret r) {
		switch(r.fn_ret) {
			case CONTINUE_WITH_ERROR:
				printf("oci_exec_sql error... %s!\n", r.gerrbuf);
				return -1;
			case FAILURE:
				printf("oci_exec_sql failed... %s!\n", r.gerrbuf);
				return -1;
		}
	}

	try {
		stmt->rows(NULL,string_append, list_append, sizeof_resp, 100);
	}
	catch (intf_ret r) {
		switch(r.fn_ret) {
			case CONTINUE_WITH_ERROR:
				printf("oci_produce_rows error... %s!\n", r.gerrbuf);
				return -1;
			case FAILURE:
				printf("oci_produce_rows failed... %s!\n", r.gerrbuf);
				return -1;
		}
	}
	
	try {
		stmt->close();
	}
	catch (intf_ret r) {
		switch(r.fn_ret) {
			case SUCCESS:
				printf("oci_close_statement success...!\n");
				break;
			case CONTINUE_WITH_ERROR:
				printf("oci_close_statement error... %s!\n", r.gerrbuf);
				return -1;
			case FAILURE:
				printf("oci_close_statement failed... %s!\n", r.gerrbuf);
				return -1;
		}
	}

	try {
		delete ocisess;
	}
	catch (intf_ret r) {
		switch(r.fn_ret) {
			case SUCCESS:
				//printf("oci_free_session success...!\n");
				break;
			case CONTINUE_WITH_ERROR:
				printf("oci_free_session error... %s!\n", r.gerrbuf);
				return -1;
			case FAILURE:
				printf("oci_free_session failed... %s!\n", r.gerrbuf);
				return -1;
		}
	}
}

int insert_bind_select(const char *tns, const char *usr, const char *pwd)
{
	ocisession * ocisess = NULL;
	ocistmt *stmt = NULL;
	intf_ret r;
	char * qry = NULL;

	try {
		ocisess = new ocisession(tns, strlen(tns),
							     usr, strlen(usr),
								 pwd, strlen(pwd));
	}
	catch (intf_ret r) {
		switch(r.fn_ret) {
			case CONTINUE_WITH_ERROR:
				printf("oci_get_session error... %s!\n", r.gerrbuf);
				return -1;
			case FAILURE:
				printf("oci_get_session failed... %s!\n", r.gerrbuf);
				return -1;
		}
	}

	// Drop can fail so ignoring the errors
	qry = "drop table erloci_table";
	try {
		stmt = ocisess->prepare_stmt((unsigned char *)qry, strlen(qry));
		stmt->execute(NULL, append_coldef_to_list, NULL, string_append, true);
	}
	catch (intf_ret r) {
		switch(r.fn_ret) {
			case CONTINUE_WITH_ERROR:
				printf("oci_exec_sql error... %s!\n", r.gerrbuf);
				break;
			case FAILURE:
				printf("oci_exec_sql failed... %s!\n", r.gerrbuf);
				break;
		}
	}

	// Create table
	qry = "create table erloci_table(pkey number,\
                                     publisher varchar2(100),\
                                     rank number,\
                                     hero varchar2(100),\
                                     real varchar2(100),\
                                     votes number,\
                                     votes_first_rank number)";
	try {
		stmt = ocisess->prepare_stmt((unsigned char *)qry, strlen(qry));
		stmt->execute(NULL, append_coldef_to_list, NULL, string_append, true);
	}
	catch (intf_ret r) {
		switch(r.fn_ret) {
			case SUCCESS:
				//printf("oci_exec_sql success...!\n");
				break;
			case CONTINUE_WITH_ERROR:
				printf("oci_exec_sql error... %s!\n", r.gerrbuf);
				return -1;
			case FAILURE:
				printf("oci_exec_sql failed... %s!\n", r.gerrbuf);
				return -1;
		}
	}

	// insert some rows
	string_count = 0;
	list_count = 0;
	qry = "insert into erloci_table values (:pkey,:publisher,:rank,:hero,:real,:votes,:votes_first_rank)";
	try {
		stmt = ocisess->prepare_stmt((unsigned char *)qry, strlen(qry));
	}
	catch (intf_ret r) {
		switch(r.fn_ret) {
			case SUCCESS:
				//printf("oci_exec_sql success...!\n");
				break;
			case CONTINUE_WITH_ERROR:
				printf("oci_exec_sql error... %s!\n", r.gerrbuf);
				return -1;
			case FAILURE:
				printf("oci_exec_sql failed... %s!\n", r.gerrbuf);
				return -1;
		}
	}
	vector<var> & varsin = stmt->get_in_bind_args();
	varsin.push_back(var(":pkey", SQLT_INT));				//0
	varsin.push_back(var(":publisher", SQLT_STR));			//1
	varsin.push_back(var(":rank", SQLT_INT));				//2
	varsin.push_back(var(":hero", SQLT_STR));				//3
	varsin.push_back(var(":real", SQLT_STR));				//4
	varsin.push_back(var(":votes", SQLT_INT));				//5
	varsin.push_back(var(":votes_first_rank", SQLT_INT));	//6

	vector<var> & varsout = stmt->get_out_bind_args();
	varsout.push_back(var(":row_id", SQLT_STR));			//0
	
	for(int i=0; i < varsin.size(); ++i) {
		varsin[i].valuep.clear();
		varsin[i].alen.clear();
		varsin[i].value_sz = 0;
		varsin[i].datap = NULL;
		varsin[i].datap_len = 0;
	}

	char tmp[100];
	int tmp_len = 0;
	char * tmpp = NULL;
	int *tmpint = NULL;
	unsigned int rows = 50000;
	for(unsigned int i=rows; i>0; --i) {

		// :pkey
		tmpint = new int;
		*tmpint = i;
		varsin[0].valuep.push_back(tmpint);
		if (sizeof(int)+1 > varsin[0].value_sz) varsin[0].value_sz = sizeof(int);
		varsin[0].alen.push_back(sizeof(int));
		varsin[0].ind.push_back(0);

		// :publisher
		sprintf(tmp, "publisher%d", i);
		tmp_len = strlen(tmp)+1;
		tmpp = new char[tmp_len];
		memcpy(tmpp, tmp, tmp_len);
		varsin[1].valuep.push_back(tmpp);
		if (tmp_len > varsin[1].value_sz) varsin[1].value_sz = tmp_len;
		varsin[1].alen.push_back(tmp_len);
		varsin[1].ind.push_back(0);

		// :rank
		tmpint = new int;
		*tmpint = i;
		varsin[2].valuep.push_back(tmpint);
		if (sizeof(int)+1 > varsin[2].value_sz) varsin[2].value_sz = sizeof(int);
		varsin[2].alen.push_back(sizeof(int));
		varsin[2].ind.push_back(0);

		// :hero
		sprintf(tmp, "hero%d", i);
		tmp_len = strlen(tmp)+1;
		tmpp = new char[tmp_len];
		memcpy(tmpp, tmp, tmp_len);
		varsin[3].valuep.push_back(tmpp);
		if (tmp_len > varsin[3].value_sz) varsin[3].value_sz = tmp_len;
		varsin[3].alen.push_back(tmp_len);
		varsin[3].ind.push_back(0);

		// :real
		sprintf(tmp, "real%d", i);
		tmp_len = strlen(tmp)+1;
		tmpp = new char[tmp_len];
		memcpy(tmpp, tmp, tmp_len);
		varsin[4].valuep.push_back(tmpp);
		if (tmp_len > varsin[4].value_sz) varsin[4].value_sz = tmp_len;
		varsin[4].alen.push_back(tmp_len);
		varsin[4].ind.push_back(0);

		// :votes
		tmpint = new int;
		*tmpint = i;
		varsin[5].valuep.push_back(tmpint);
		if (sizeof(int)+1 > varsin[5].value_sz) varsin[5].value_sz = sizeof(int);
		varsin[5].alen.push_back(sizeof(int));
		varsin[5].ind.push_back(0);

		// :votes_first_rank
		tmpint = new int;
		*tmpint = i;
		varsin[6].valuep.push_back(tmpint);
		if (sizeof(int)+1 > varsin[6].value_sz) varsin[6].value_sz = sizeof(int);
		varsin[6].alen.push_back(sizeof(int));
		varsin[6].ind.push_back(0);
	}

	printf("inserting %d rows\n", rows);
	try {
		stmt->execute(NULL, append_coldef_to_list, NULL, string_append, true);
	}
	catch (intf_ret r) {
		switch(r.fn_ret) {
			case CONTINUE_WITH_ERROR:
				printf("oci_exec_sql error... %s!\n", r.gerrbuf);
				return -1;
			case FAILURE:
				printf("oci_exec_sql failed... %s!\n", r.gerrbuf);
				return -1;
		}
	}
	try {
		stmt->close();
	}
	catch (intf_ret r) {
		switch(r.fn_ret) {
			case SUCCESS:
				printf("oci_close_statement success...!\n");
				break;
			case CONTINUE_WITH_ERROR:
				printf("oci_close_statement error... %s!\n", r.gerrbuf);
				return -1;
			case FAILURE:
				printf("oci_close_statement failed... %s!\n", r.gerrbuf);
				return -1;
		}
	}

	// read back rows
	qry = "select * from erloci_table";
	try {
		stmt = ocisess->prepare_stmt((unsigned char *)qry, strlen(qry));
		stmt->execute(NULL, append_coldef_to_list, NULL, string_append, true);
	}
	catch (intf_ret r) {
		switch(r.fn_ret) {
			case CONTINUE_WITH_ERROR:
				printf("oci_exec_sql error... %s!\n", r.gerrbuf);
				return -1;
			case FAILURE:
				printf("oci_exec_sql failed... %s!\n", r.gerrbuf);
				return -1;
		}
	}

	do {
		try {
			r = stmt->rows(NULL,string_append, list_append, sizeof_resp, 100);
		}
		catch (intf_ret r) {
			switch(r.fn_ret) {
				case CONTINUE_WITH_ERROR:
					printf("oci_produce_rows error... %s!\n", r.gerrbuf);
					return -1;
				case FAILURE:
					printf("oci_produce_rows failed... %s!\n", r.gerrbuf);
					return -1;
			}
		}
	} while (r.fn_ret == MORE);

	
	try {
		stmt->close();
	}
	catch (intf_ret r) {
		switch(r.fn_ret) {
			case SUCCESS:
				printf("oci_close_statement success...!\n");
				break;
			case CONTINUE_WITH_ERROR:
				printf("oci_close_statement error... %s!\n", r.gerrbuf);
				return -1;
			case FAILURE:
				printf("oci_close_statement failed... %s!\n", r.gerrbuf);
				return -1;
		}
	}

	try {
		delete ocisess;
	}
	catch (intf_ret r) {
		switch(r.fn_ret) {
			case SUCCESS:
				//printf("oci_free_session success...!\n");
				break;
			case CONTINUE_WITH_ERROR:
				printf("oci_free_session error... %s!\n", r.gerrbuf);
				return -1;
			case FAILURE:
				printf("oci_free_session failed... %s!\n", r.gerrbuf);
				return -1;
		}
	}
}

int _tmain(int argc, _TCHAR* argv[])
{
	const char
		*tns = "(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=tcp)(HOST=80.67.144.206)(PORT=5437)))(CONNECT_DATA=(SERVICE_NAME=XE)))",
		*usr = "scott",
		*pwd = "regit";
	intf_ret r;

	// tests for memory leak detection
	int ret = 
	//memory_leak_test(tns, usr, pwd);
	//drop_create_insert_select(tns, usr, pwd, 0);
	insert_bind_select(tns, usr, pwd);

	return ret;
}

bool log_flag = true;
void log_remote(const char * filename, const char * funcname, unsigned int linenumber, unsigned int level, const char *fmt, ...)
{
    va_list arguments;
    va_start(arguments, fmt);

    vprintf(fmt, arguments);

    va_end(arguments);
}