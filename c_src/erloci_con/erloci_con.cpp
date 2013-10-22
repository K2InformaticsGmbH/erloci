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

void append_coldef_to_list(const char * col_name, const unsigned short data_type, const unsigned int max_len, void * list)
{
	printf("\t%s\n", col_name);
}

void string_append(const char * string, size_t len, void * list)
{
	printf("%.*s\t", len, string);
}

void list_append(const void * sub_list, void * list)
{
	printf("\t");
}

size_t sizeof_resp(void * resp)
{
	return 0;
}

int memory_leak_test(const char *tns, const char *usr, const char *pwd, const char *opt)
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
			stmt->execute(NULL, append_coldef_to_list, true);
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
int drop_create_insert_select(const char *tns, const char *usr, const char *pwd, const char *opt, int tid)
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
		stmt->execute(NULL, append_coldef_to_list, true);
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
		stmt->execute(NULL, append_coldef_to_list, true);
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
			stmt->execute(NULL, append_coldef_to_list, true);
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
		stmt->execute(NULL, append_coldef_to_list, true);
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

int _tmain(int argc, _TCHAR* argv[])
{
	const char
		*tns = "(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=tcp)(HOST=80.67.144.206)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=XE)))",
		*usr = "bikram",
		*pwd = "abcd123",
		*opt = "";
	intf_ret r;

	// tests for memory leak detection
	int ret;
	ret = memory_leak_test(tns, usr, pwd, opt);
	//ret = drop_create_insert_select(tns, usr, pwd, opt, 0);

	return ret;
}

bool log_flag = true;
void log_remote(const char *fmt, ...)
{
    va_list arguments;
    va_start(arguments, fmt);

    vprintf(fmt, arguments);

    va_end(arguments);
}