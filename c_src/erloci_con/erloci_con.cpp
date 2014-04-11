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
#if 0
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
			stmt->execute(NULL, NULL, true);
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
			stmt->rows(NULL, 100);
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
		stmt->execute(NULL, NULL, true);
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
		stmt->execute(NULL, NULL, true);
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
			stmt->execute(NULL, NULL, true);
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
		stmt->execute(NULL, NULL, true);
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
		stmt->rows(NULL, 100);
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
		stmt->execute(NULL, NULL, true);
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
		stmt->execute(NULL, NULL, true);
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
		tmp_len = (int)strlen(tmp)+1;
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
		tmp_len = (int)strlen(tmp)+1;
		tmpp = new char[tmp_len];
		memcpy(tmpp, tmp, tmp_len);
		varsin[3].valuep.push_back(tmpp);
		if (tmp_len > varsin[3].value_sz) varsin[3].value_sz = tmp_len;
		varsin[3].alen.push_back(tmp_len);
		varsin[3].ind.push_back(0);

		// :real
		sprintf(tmp, "real%d", i);
		tmp_len = (int)strlen(tmp)+1;
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
		stmt->execute(NULL, NULL, true);
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
		stmt->execute(NULL, NULL, true);
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
			r = stmt->rows(NULL, 100);
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

bool log_flag = true;
void log_remote(const char * filename, const char * funcname, unsigned int linenumber, unsigned int level, void *term, const char *fmt, ...)
{
    va_list arguments;
    va_start(arguments, fmt);

    vprintf(fmt, arguments);

    va_end(arguments);
}
#else
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <oci.h>

void checkerr(OCIError * errhp, sword status, int line)
{
  text errbuf[512];
  sb4 errcode = 0;

  switch (status)
  {
  case OCI_SUCCESS:
    break;
  case OCI_SUCCESS_WITH_INFO:
    (void) printf("[%d] Error - OCI_SUCCESS_WITH_INFO\n", line);
    break;
  case OCI_NEED_DATA:
    (void) printf("[%d] Error - OCI_NEED_DATA\n", line);
    break;
  case OCI_NO_DATA:
    (void) printf("[%d] Error - OCI_NODATA\n", line);
    break;
  case OCI_ERROR:
    (void) OCIErrorGet((dvoid *)errhp, (ub4) 1, (text *) NULL, &errcode,
                        errbuf, (ub4) sizeof(errbuf), OCI_HTYPE_ERROR);
    (void) printf("[%d] Error - %.*ls\n", line, 512, errbuf);
    break;
  case OCI_INVALID_HANDLE:
    (void) printf("[%d] Error - OCI_INVALID_HANDLE\n", line);
    break;
  case OCI_STILL_EXECUTING:
    (void) printf("[%d] Error - OCI_STILL_EXECUTE\n", line);
    break;
  case OCI_CONTINUE:
    (void) printf("[%d] Error - OCI_CONTINUE\n", line);
    break;
  default:
    break;
  }
}
#endif

const char
	//*tns = "(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=tcp)(HOST=127.0.0.1)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=XE)))",
	*usr = "scott",
	*pwd = "tiger";

const char
	tns[] = {0,40,0,68,0,69,0,83,0,67,0,82,0,73,0,80,0,84,0,73,0,79,0,78,0,61,0,40,0,65,0,68,0,68,0,82,0,69,0,83,0,83,0,95,0,76,0,73,0,83,0,84,0,61,0,40,0,65,0,68,0,68,0,82,0,69,0,83,0,83,0,61,0,40,0,80,0,82,0,79,0,84,0,79,0,67,0,79,0,76,0,61,0,116,0,99,0,112,0,41,0,40,0,72,0,79,0,83,0,84,0,61,0,49,0,50,0,55,0,46,0,48,0,46,0,48,0,46,0,49,0,41,0,40,0,80,0,79,0,82,0,84,0,61,0,49,0,53,0,50,0,49,0,41,0,41,0,41,0,40,0,67,0,79,0,78,0,78,0,69,0,67,0,84,0,95,0,68,0,65,0,84,0,65,0,61,0,40,0,83,0,69,0,82,0,86,0,73,0,67,0,69,0,95,0,78,0,65,0,77,0,69,0,61,0,88,0,69,0,41,0,41,0,41,0,0};

OCIEnv		*envhp = NULL;
OCIError	*errhp = NULL;
OCIAuthInfo *authp = NULL;
OCISvcCtx   *svchp = NULL;
OCIStmt		*stmthp = NULL;

OCILobLocator *clobd = NULL;
OCILobLocator *blobd = NULL;
OCILobLocator *nclobd = NULL;
OCILobLocator *bfiled = NULL;
OCILobLocator *longd = NULL;

OCIDefine   *defnp = NULL;
void **lobptr = NULL;

sword err;

bool setup_env()
{
	err = OCIEnvNlsCreate(&envhp,OCI_THREADED, NULL, NULL, NULL, NULL, (size_t) 0, (void**) NULL, OCI_UTF16ID, OCI_UTF16ID);
	if(err != OCI_SUCCESS) return true;

	err = OCIHandleAlloc(envhp, (void **) &errhp, OCI_HTYPE_ERROR, (size_t) 0, (void **) NULL);
	if(err != OCI_SUCCESS) return true;

	err = OCIHandleAlloc(envhp,(void**)&authp, OCI_HTYPE_AUTHINFO,(size_t)0, (void **) NULL);
	if(err != OCI_SUCCESS) return true;
	err = OCIAttrSet(authp, OCI_HTYPE_AUTHINFO,(void*) usr, (ub4)strlen(usr),OCI_ATTR_USERNAME, (OCIError *)errhp);
	if(err != OCI_SUCCESS) return true;
	err = OCIAttrSet(authp, OCI_HTYPE_AUTHINFO,(void*) pwd, (ub4)strlen(pwd),OCI_ATTR_PASSWORD, (OCIError *)errhp);
	if(err != OCI_SUCCESS) return true;

	err = OCISessionGet(envhp, errhp, &svchp, authp, (OraText*) tns, (ub4)sizeof(tns), NULL, 0, NULL, NULL, NULL, OCI_DEFAULT);
	if(err != OCI_SUCCESS) {
		checkerr(errhp, err, __LINE__);
		return true;
	}
	return false;
}

bool statement(const char *stmt)
{
	err = OCIHandleAlloc(envhp, (void **) &stmthp, OCI_HTYPE_STMT, (size_t) 0, (void **) NULL);
	if(err != OCI_SUCCESS) return true;

	err = OCIStmtPrepare2(svchp, &stmthp, errhp, (OraText*) stmt, (ub4)strlen(stmt), NULL, 0, OCI_NTV_SYNTAX, OCI_DEFAULT);
	if(err != OCI_SUCCESS) {
		checkerr(errhp, err, __LINE__);
		return true;
	}

	err = OCIStmtExecute(svchp, stmthp, errhp, (ub4) 0, (ub4) 0, (CONST OCISnapshot*) 0, (OCISnapshot*) 0, (ub4) OCI_DEFAULT);
	if(err != OCI_SUCCESS) {
		checkerr(errhp, err, __LINE__);
		return true;
	}

	return false;
}

bool binds()
{
	lobptr = (void **)new char**;
	err = OCIDescriptorAlloc((void*)envhp, lobptr, (ub4) OCI_DTYPE_LOB, (size_t) 0, (void**) 0);
	if(err != OCI_SUCCESS) exit(0);
	err = OCIDefineByPos(stmthp, &defnp, errhp, (ub4)1, lobptr, (sb4)0, (ub2) SQLT_CLOB, (void*)0, (ub2*)0, (ub2*)0, (ub4) OCI_DEFAULT);
	if(err != OCI_SUCCESS) {
		checkerr(errhp, err, __LINE__);
		return true;
	}
	clobd = (OCILobLocator*)*lobptr;
	lobptr = (void **)new char**;
	err = OCIDescriptorAlloc((void*)envhp, lobptr, (ub4) OCI_DTYPE_LOB, (size_t) 0, (void**) 0);
	if(err != OCI_SUCCESS) exit(0);
	err = OCIDefineByPos(stmthp, &defnp, errhp, (ub4)2, lobptr, (sb4)0, (ub2) SQLT_BLOB, (void*)0, (ub2*)0, (ub2*)0, (ub4) OCI_DEFAULT);
	if(err != OCI_SUCCESS) {
		checkerr(errhp, err, __LINE__);
		return true;
	}
	blobd = (OCILobLocator*)*lobptr;
	lobptr = (void **)new char**;
	err = OCIDescriptorAlloc((void*)envhp, lobptr, (ub4) OCI_DTYPE_LOB, (size_t) 0, (void**) 0);
	if(err != OCI_SUCCESS) exit(0);
	err = OCIDefineByPos(stmthp, &defnp, errhp, (ub4)3, lobptr, (sb4)0, (ub2) SQLT_CLOB, (void *)0, (ub2*)0, (ub2*)0, (ub4) OCI_DEFAULT);
	if(err != OCI_SUCCESS) {
		checkerr(errhp, err, __LINE__);
		return true;
	}
	nclobd = (OCILobLocator*)*lobptr;
	lobptr = (void **)new char**;
	err = OCIDescriptorAlloc((void*)envhp, lobptr, (ub4) OCI_DTYPE_FILE, (size_t) 0, (void**) 0);
	if(err != OCI_SUCCESS) exit(0);
	err = OCIDefineByPos(stmthp, &defnp, errhp, (ub4)4, lobptr, (sb4)0, (ub2) SQLT_BFILE, (void *)0, (ub2*)0, (ub2*)0, (ub4) OCI_DEFAULT);
	if(err != OCI_SUCCESS) {
		checkerr(errhp, err, __LINE__);
		return true;
	}
	bfiled = (OCILobLocator*)*lobptr;
	lobptr = (void **)new char**;
	err = OCIDescriptorAlloc((void*)envhp, lobptr, (ub4) OCI_DTYPE_LOB, (size_t) 0, (void**) 0);
	if(err != OCI_SUCCESS) exit(0);
	longd = (OCILobLocator*)*lobptr;
	err = OCIDefineByPos(stmthp, &defnp, errhp, (ub4)5, lobptr, (sb4)4000, (ub2) SQLT_LNG, (void *)0, (ub2*)0, (ub2*)0, (ub4) OCI_DEFAULT);
	if(err != OCI_SUCCESS) {
		checkerr(errhp, err, __LINE__);
		return true;
	}

	return false;
}


bool unbind()
{
	err = OCIDescriptorFree((void *)clobd, (ub4) OCI_DTYPE_LOB);
	if(err != OCI_SUCCESS) return true;
	err = OCIDescriptorFree((void *)blobd, (ub4) OCI_DTYPE_LOB);
	if(err != OCI_SUCCESS) return true;
	err = OCIDescriptorFree((void *)nclobd, (ub4) OCI_DTYPE_LOB);
	if(err != OCI_SUCCESS) return true;
	err = OCIDescriptorFree((void *)bfiled, (ub4) OCI_DTYPE_FILE);
	if(err != OCI_SUCCESS) return true;
	err = OCIDescriptorFree((void *)longd, (ub4) OCI_DTYPE_FILE);
	if(err != OCI_SUCCESS) return true;

	return false;
}

bool get_lob(const char * field, OCILobLocator *lob)
{
	OCILobLocator *_tlob;
	ub1 csfrm;
	ub1 *buf = NULL;
	oraub8 loblen = 0;
	ub4 offset;

	err = OCILobGetLength2(svchp, errhp, lob, &loblen);
	if(err != OCI_SUCCESS) {
		checkerr(errhp, err, __LINE__);
		return true;
	}

	err = OCIDescriptorAlloc(envhp,(dvoid **)&_tlob, (ub4)OCI_DTYPE_LOB, (size_t)0, (dvoid **)0);
	if(err != OCI_SUCCESS) return true;
	err = OCILobAssign(envhp, errhp, lob, &_tlob);
	if(err != OCI_SUCCESS) {
		checkerr(errhp, err, __LINE__);
		 return true;
	}

	err = OCILobCharSetForm(envhp, errhp, _tlob, &csfrm);
	if(err != OCI_SUCCESS) return true;

	err = OCILobOpen(svchp, errhp, _tlob, OCI_LOB_READONLY);
	if(err != OCI_SUCCESS) {
		checkerr(errhp, err, __LINE__);
		return true;
	}

	buf = new ub1[loblen+1];
	memset ((dvoid*)buf, '\0', loblen+1);
	offset=1;
	oraub8 loblenc = loblen;
	err = OCILobRead2(svchp, errhp, _tlob, (oraub8*)&loblen, (oraub8*)&loblenc, offset, (dvoid *) buf, (ub4)loblen , OCI_ONE_PIECE, (dvoid *) 0, (OCICallbackLobRead2) 0, (ub2) 0, csfrm);
	if(err != OCI_SUCCESS) {
		checkerr(errhp, err, __LINE__);
		return true;
	}

	text dir[31], file[256];
	ub2 dlen, flen;
	err = OCILobFileGetName(envhp,errhp,_tlob,dir,&dlen,file,&flen);
	if(err != OCI_SUCCESS) {
		buf[loblen] = '\0';
		printf("%s %s(%d)\n", field, buf, loblen);
	} else {
		printf("%s (%d) - %.*s %.*s\n", field, loblen, dlen, dir, flen, file);
		for(ub4 i=0; i < loblen; ++i) {
			printf("%02x", buf[i]);
			if(i > 0 && (i+1) % 16 == 0)
				printf("\n");
			else
				printf(" ");
		}
		printf("\n%s (%d)\n", field, loblen);
	}
	delete buf;

	err = OCILobClose(svchp, errhp, _tlob);
	if(err != OCI_SUCCESS) {
		checkerr(errhp, err, __LINE__);
		return true;
	}

	err = OCIDescriptorFree(_tlob, (ub4)OCI_DTYPE_LOB);
	if(err != OCI_SUCCESS) {
		checkerr(errhp, err, __LINE__);
		return true;
	}

	return false;
}

int main(int argc, char* argv[])
{
	if(setup_env())
		goto error_return;

	if(statement("select longd from rawlong"))
		goto error_return;

	OCIParam *mypard = NULL;
    err = OCIParamGet(stmthp, OCI_HTYPE_STMT, errhp, (dvoid **)&mypard,(ub4) 1);
	if(err != OCI_SUCCESS) {
		checkerr(errhp, err, __LINE__);
		goto error_return;
	}
	ub4	dlen = 0;
	ub4	sizep = 0;
	ub2 dtype = 0;
	err = OCIAttrGet((dvoid*) mypard, (ub4) OCI_DTYPE_PARAM, (dvoid*) &(dtype), (ub4 *)0, (ub4)OCI_ATTR_DATA_TYPE, errhp);
	if(err != OCI_SUCCESS) {
		checkerr(errhp, err, __LINE__);
		goto error_return;
	}

	err = OCIAttrGet((dvoid*) mypard, (ub4) OCI_DTYPE_PARAM, (dvoid*) &dlen, (ub4 *)&sizep, (ub4)OCI_ATTR_DATA_SIZE, errhp);
	if(err != OCI_SUCCESS) {
		checkerr(errhp, err, __LINE__);
		goto error_return;
	}

	/*if(binds())
		goto error_return;
	
	err = OCIStmtFetch(stmthp, errhp, 1, OCI_FETCH_NEXT, OCI_DEFAULT);
	if(err != OCI_SUCCESS) {
		checkerr(errhp, err, __LINE__);
		goto error_return;
	}

	if(get_lob("clob", clobd))
		goto error_return;

	if(get_lob("bclob", blobd))
		goto error_return;
	
	if(get_lob("nclob", nclobd))
		goto error_return;

	if(get_lob("bfile", bfiled))
		goto error_return;

	ub1 csfrm;
	ub1 *buf = NULL;
	ub4 loblen = 0;
	ub4 offset;

	err = OCILobGetLength(svchp, errhp, longd, &loblen);
	if(err != OCI_SUCCESS) {
		checkerr(errhp, err, __LINE__);
		goto error_return;
	}

	err = OCILobCharSetForm (envhp, errhp, bfiled, &csfrm);
	if(err != OCI_SUCCESS) exit(0);
	buf = new ub1[loblen+1];
	memset ((dvoid*)buf, '\0', loblen+1);
	offset=1;
	err = OCILobRead(svchp, errhp, longd, &loblen, offset, (dvoid *) buf, (ub4)8000 , (dvoid *) 0, (OCICallbackLobRead) 0, (ub2) 0, csfrm);
	if(err != OCI_SUCCESS) {
		checkerr(errhp, err, __LINE__);
		goto error_return;
	}
	buf[loblen] = '\0';
	printf("longd %s(%d)\n", buf, loblen);
	delete buf;

	if(unbind()) {
		printf("[%d] UnBind failure\n", __LINE__);
	}*/

error_return:
	return 0;
}