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

  memset(errbuf, 0, sizeof(errbuf));
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
    (void) printf("[%d] Error - %s\n", line, errbuf);
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
	*tns = "(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=tcp)(HOST=127.0.0.1)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=XE)))",
	*usr = "scott",
	*pwd = "tiger";

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
	err = OCIEnvCreate(&envhp,OCI_THREADED | OCI_OBJECT, NULL, NULL, NULL, NULL, (size_t) 0, (void**) NULL);
	if(err != OCI_SUCCESS) return true;

	err = OCIHandleAlloc(envhp, (void **) &errhp, OCI_HTYPE_ERROR, (size_t) 0, (void **) NULL);
	if(err != OCI_SUCCESS) return true;

	err = OCIHandleAlloc(envhp,(void**)&authp, OCI_HTYPE_AUTHINFO,(size_t)0, (void **) NULL);
	if(err != OCI_SUCCESS) return true;
	err = OCIAttrSet(authp, OCI_HTYPE_AUTHINFO,(void*) usr, (ub4)strlen(usr),OCI_ATTR_USERNAME, (OCIError *)errhp);
	if(err != OCI_SUCCESS) return true;
	err = OCIAttrSet(authp, OCI_HTYPE_AUTHINFO,(void*) pwd, (ub4)strlen(pwd),OCI_ATTR_PASSWORD, (OCIError *)errhp);
	if(err != OCI_SUCCESS) return true;

	err = OCISessionGet(envhp, errhp, &svchp, authp, (OraText*) tns, (ub4)strlen(tns), NULL, 0, NULL, NULL, NULL, OCI_DEFAULT);
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

#define TYP_PROP(__attrtype) TYP_PROP_G(parmh, __attrtype, #__attrtype)
#define TYP_PROP_M(__attrtype) {\
	printf("\t");\
	TYP_PROP_G(mthdhd, __attrtype, #__attrtype);\
}

#define TYP_PROP_G(__parmh,__attrtype, __attrtypestr) {\
	ub1 has = 0;\
	err = OCIAttrGet((dvoid*)__parmh, OCI_DTYPE_PARAM, (dvoid*)&has, (ub4*)0, __attrtype, errhp);\
	if(err != OCI_SUCCESS) {\
		checkerr(errhp, err, __LINE__);\
		goto err_ret;\
	}\
	(void) printf("\t[%d] "__attrtypestr" - %s\n", __LINE__, has == 0 ? "no" : "yes");\
	has;\
}

#define COUNT_PROP(__attrtype, num) {\
	err = OCIAttrGet((dvoid*)parmh, OCI_DTYPE_PARAM, (dvoid*)&num, (ub4*)0, __attrtype, errhp);\
	if(err != OCI_SUCCESS) {\
		checkerr(errhp, err, __LINE__);\
		goto err_ret;\
	}\
	(void) printf("\t[%d] "#__attrtype" - %d\n", __LINE__, num);\
}

#define STRING_PROP(__attrtype) {\
	OraText *name = NULL;\
	ub4 name_len = 0;\
	err = OCIAttrGet((dvoid*)parmh, OCI_DTYPE_PARAM, (dvoid*)&name, (ub4*)&name_len, __attrtype, errhp);\
	if(err != OCI_SUCCESS) {\
		checkerr(errhp, err, __LINE__);\
		goto err_ret;\
	}\
	(void) printf("\t[%d] "#__attrtype" - %.*s\n", __LINE__, name_len, name);\
}

// sqlplus sbs0/sbs0sbs0_4dev@192.168.1.69/SBS0.k2informatics.ch
// SELECT queue, user_data  FROM aq$sisnot_aq_table
bool describe(const char objptr[])
{
	//char *objptr = "sys.aq$_jms_map_message";
	OCIDescribe *dschp = NULL;
	OCIParam *parmh = NULL;

	err = OCIHandleAlloc(envhp, (void**)&dschp, OCI_HTYPE_DESCRIBE, (size_t)0, (void**) NULL);
	if(err != OCI_SUCCESS)
		return true;

	err = OCIDescribeAny(svchp, errhp, (void*)objptr, (ub4)strlen(objptr), OCI_OTYPE_NAME, OCI_DEFAULT, OCI_PTYPE_TYPE, dschp);
	if(err != OCI_SUCCESS) {
		checkerr(errhp, err, __LINE__);
		goto err_ret;
	}

	err = OCIAttrGet((dvoid*)dschp, OCI_HTYPE_DESCRIBE, (dvoid*)&parmh, (ub4*)0, OCI_ATTR_PARAM, errhp);
	if(err != OCI_SUCCESS) {
		checkerr(errhp, err, __LINE__);
		goto err_ret;
	}

	ub1 ptyp = 0;
	err = OCIAttrGet((dvoid*)parmh, OCI_DTYPE_PARAM, (dvoid*)&ptyp, (ub4*)0, OCI_ATTR_PTYPE, errhp);
	if(err != OCI_SUCCESS) {
		checkerr(errhp, err, __LINE__);
		goto err_ret;
	}

	OCITypeCode otc;
	err = OCIAttrGet((dvoid*)parmh, OCI_DTYPE_PARAM, (dvoid*)&otc, (ub4*)0, OCI_ATTR_TYPECODE, errhp);
	if(err != OCI_SUCCESS) {
		checkerr(errhp, err, __LINE__);
		goto err_ret;
	}

	OraText *tnm = NULL;
	ub4 tnl = 0;
	err = OCIAttrGet((dvoid*)parmh, OCI_DTYPE_PARAM, (dvoid*)&tnm, (ub4 *)&tnl, OCI_ATTR_NAME, errhp);
	if(err != OCI_SUCCESS) {
		checkerr(errhp, err, __LINE__);
		goto err_ret;
	}

	TYP_PROP(OCI_ATTR_IS_INCOMPLETE_TYPE);
	TYP_PROP(OCI_ATTR_IS_SYSTEM_TYPE);
	TYP_PROP(OCI_ATTR_IS_PREDEFINED_TYPE);
	TYP_PROP(OCI_ATTR_IS_TRANSIENT_TYPE);
	TYP_PROP(OCI_ATTR_IS_SYSTEM_GENERATED_TYPE);
	TYP_PROP(OCI_ATTR_HAS_NESTED_TABLE);
	TYP_PROP(OCI_ATTR_HAS_LOB);
	TYP_PROP(OCI_ATTR_HAS_FILE);
	TYP_PROP(OCI_ATTR_IS_INVOKER_RIGHTS);
	TYP_PROP(OCI_ATTR_IS_FINAL_TYPE);
	TYP_PROP(OCI_ATTR_IS_INSTANTIABLE_TYPE);
	TYP_PROP(OCI_ATTR_IS_SUBTYPE);

	ub2 num_attrs;
	COUNT_PROP(OCI_ATTR_NUM_TYPE_ATTRS, num_attrs);
	ub2 num_method;
	COUNT_PROP(OCI_ATTR_NUM_TYPE_METHODS, num_method);

	STRING_PROP(OCI_ATTR_NAME);
	STRING_PROP(OCI_ATTR_PACKAGE_NAME);
	STRING_PROP(OCI_ATTR_SCHEMA_NAME);
	STRING_PROP(OCI_ATTR_SUPERTYPE_SCHEMA_NAME);
	STRING_PROP(OCI_ATTR_SUPERTYPE_NAME);

	if(num_attrs > 0) {
		OCIParam *attrlsthd = NULL;
		OCIParam *attrhd = NULL;
		err = OCIAttrGet((dvoid*)parmh, OCI_DTYPE_PARAM, (dvoid *)&attrlsthd, (ub4 *)0, OCI_ATTR_LIST_TYPE_ATTRS, errhp);
		if(err != OCI_SUCCESS) {
			checkerr(errhp, err, __LINE__);
			goto err_ret;
		}
		ub2 type = 0;
		ub2 size = 0;
		OraText *nm = NULL;
		ub4 nl = 0;
		OCITypeCode tc = 0;
		for (ub4 i = 1; i <= num_attrs; i++)
		{
			/* get parameter for attribute i */
			err = OCIParamGet((dvoid *)attrlsthd, OCI_DTYPE_PARAM, errhp, (dvoid**)&attrhd, i);
			if(err != OCI_SUCCESS) {
				checkerr(errhp, err, __LINE__);
				goto err_ret;
			}

			size = 0;
			err = OCIAttrGet((dvoid*)attrhd, OCI_DTYPE_PARAM, (dvoid*)&size,(ub4*)0, OCI_ATTR_DATA_SIZE, errhp);
			if(err != OCI_SUCCESS) {
				checkerr(errhp, err, __LINE__);
				goto err_ret;
			}

			tc = 0;
			err = OCIAttrGet((dvoid*)attrhd, OCI_DTYPE_PARAM, (dvoid*)&tc,(ub4*)0, OCI_ATTR_TYPECODE, errhp);
			if(err != OCI_SUCCESS) {
				checkerr(errhp, err, __LINE__);
				goto err_ret;
			}

			if (tc == OCI_TYPECODE_OBJECT)
			{
				OCIRef *attr_type_ref;
				OCIDescribe *nested_dschp;

				err = OCIHandleAlloc(envhp, (void**)&nested_dschp, OCI_HTYPE_DESCRIBE, (size_t)0, (void**) NULL);
				if(err != OCI_SUCCESS)
					goto err_ret;

				err = OCIAttrGet((void *)attrhd, OCI_DTYPE_PARAM, (void *)&attr_type_ref, (ub4*)0, OCI_ATTR_REF_TDO,errhp);
				if(err != OCI_SUCCESS)
					goto err_ret;

				err = OCIDescribeAny(svchp, errhp,(void*)attr_type_ref, 0, OCI_OTYPE_REF, OCI_DEFAULT, OCI_PTYPE_TYPE, nested_dschp);
				if(err != OCI_SUCCESS)
					goto err_ret;

				OCIParam *nested_parmh = NULL;
				err = OCIAttrGet((dvoid*)nested_dschp, OCI_HTYPE_DESCRIBE, (dvoid*)&nested_parmh, (ub4*)0, OCI_ATTR_PARAM, errhp);
				if(err != OCI_SUCCESS) {
					checkerr(errhp, err, __LINE__);
					goto err_ret;
				}

				OraText *nested_tnm = NULL;
				ub4 nested_tnl = 0;
				err = OCIAttrGet((dvoid*)nested_parmh, OCI_DTYPE_PARAM, (dvoid*)&nested_tnm, (ub4 *)&nested_tnl, OCI_ATTR_NAME, errhp);
				if(err != OCI_SUCCESS) {
					checkerr(errhp, err, __LINE__);
					goto err_ret;
				}
			}

			type = 0;
			err = OCIAttrGet((dvoid*)attrhd, OCI_DTYPE_PARAM, (dvoid*)&type, (ub4 *)0, OCI_ATTR_DATA_TYPE, errhp);
			if(err != OCI_SUCCESS) {
				checkerr(errhp, err, __LINE__);
				goto err_ret;
			}

			nm = NULL;
			nl = 0;
			err = OCIAttrGet((dvoid*)attrhd, OCI_DTYPE_PARAM, (dvoid*)&nm, (ub4 *)&nl, OCI_ATTR_NAME, errhp);
			if(err != OCI_SUCCESS) {
				checkerr(errhp, err, __LINE__);
				goto err_ret;
			}

			printf("ATTR [%d] %.*s: type code %d, type %d, size %d\n", i, nl, nm, tc, type, size);
		}
	}

	if(num_method > 0) {
		OCIParam *mthdlsthd = NULL;
		OCIParam *mthdhd = NULL;
		err = OCIAttrGet((dvoid*)parmh, OCI_DTYPE_PARAM, (dvoid *)&mthdlsthd, (ub4 *)0, OCI_ATTR_LIST_TYPE_METHODS, errhp);
		if(err != OCI_SUCCESS) {
			checkerr(errhp, err, __LINE__);
			goto err_ret;
		}
		ub2 type = 0;
		ub2 size = 0;
		OraText *nm = NULL;
		ub4 nl = 0;
		OCITypeEncap enc = OCITypeEncap::OCI_TYPEENCAP_PUBLIC;
		for (ub4 i = 1; i <= num_method; i++)
		{
			/* get parameter for attribute i */
			err = OCIParamGet((dvoid *)mthdlsthd, OCI_DTYPE_PARAM, errhp, (dvoid**)&mthdhd, i);
			if(err != OCI_SUCCESS) {
				checkerr(errhp, err, __LINE__);
				goto err_ret;
			}

			nm = NULL;
			nl = 0;
			err = OCIAttrGet((dvoid*)mthdhd, OCI_DTYPE_PARAM, (dvoid*)&nm, (ub4 *)&nl, OCI_ATTR_NAME, errhp);
			if(err != OCI_SUCCESS) {
				checkerr(errhp, err, __LINE__);
				goto err_ret;
			}

			enc = OCITypeEncap::OCI_TYPEENCAP_PUBLIC;
			err = OCIAttrGet((dvoid*)mthdhd, OCI_DTYPE_PARAM, (dvoid*)&enc,(ub4*)0, OCI_ATTR_ENCAPSULATION, errhp);
			if(err != OCI_SUCCESS) {
				checkerr(errhp, err, __LINE__);
				goto err_ret;
			}
			printf("MTHD [%d] %.*s: encapsulation %s\n", i, nl, nm, enc == OCITypeEncap::OCI_TYPEENCAP_PUBLIC ? "OCI_TYPEENCAP_PUBLIC" : "OCI_TYPEENCAP_PRIVATE");
			TYP_PROP_M(OCI_ATTR_IS_CONSTRUCTOR);
			TYP_PROP_M(OCI_ATTR_IS_DESTRUCTOR);
			TYP_PROP_M(OCI_ATTR_IS_OPERATOR);
			TYP_PROP_M(OCI_ATTR_IS_SELFISH);
			TYP_PROP_M(OCI_ATTR_IS_MAP);
			TYP_PROP_M(OCI_ATTR_IS_ORDER);
			TYP_PROP_M(OCI_ATTR_IS_RNDS);
			TYP_PROP_M(OCI_ATTR_IS_RNPS);
			TYP_PROP_M(OCI_ATTR_IS_WNDS);
			TYP_PROP_M(OCI_ATTR_IS_WNPS);
			TYP_PROP_M(OCI_ATTR_IS_FINAL_METHOD);
			TYP_PROP_M(OCI_ATTR_IS_INSTANTIABLE_METHOD);
			TYP_PROP_M(OCI_ATTR_IS_OVERRIDING_METHOD);

			OCIParam *mthdarglsthd = NULL;
			OCIParam *mthdarg = NULL;
			err = OCIAttrGet((dvoid*)mthdhd, OCI_DTYPE_PARAM, (dvoid*)&mthdarglsthd, (ub4 *)0, OCI_ATTR_LIST_ARGUMENTS, errhp);
			if(err != OCI_SUCCESS) {
				checkerr(errhp, err, __LINE__);
				goto err_ret;
			}
			
			ub2 nummthdargs = 0;
			err = OCIAttrGet((dvoid*)mthdarglsthd, OCI_DTYPE_PARAM, (dvoid*)&nummthdargs, (ub4*)0, OCI_ATTR_NUM_PARAMS, errhp);
			if(err != OCI_SUCCESS) {
				checkerr(errhp, err, __LINE__);
				goto err_ret;
			}

			if (nummthdargs > 0) {
				printf("\thas %d arguments\n", nummthdargs);
				OraText *anm = NULL;
				ub4 anl = 0;
				for (ub4 j = 1; j <= nummthdargs; j++)
				{
					err = OCIParamGet((dvoid *)mthdarglsthd, OCI_DTYPE_PARAM, errhp, (dvoid**)&mthdarg, j);
					if(err != OCI_SUCCESS) {
						checkerr(errhp, err, __LINE__);
						goto err_ret;
					}
					anm = NULL;
					anl = 0;
					err = OCIAttrGet((dvoid*)mthdarg, OCI_DTYPE_PARAM, (dvoid*)&anm, (ub4 *)&anl, OCI_ATTR_NAME, errhp);
					if(err != OCI_SUCCESS) {
						checkerr(errhp, err, __LINE__);
						goto err_ret;
					}
					printf("\t\targ %.*s\n", anl, anm);
				}
			}
		}
	}
err_ret:
	return false;
}

int main(int argc, char* argv[])
{
	if(setup_env())
		goto error_return;

	if(describe("sys.aq$_jms_map_message"))
		goto error_return;

#if 0 // statement tests
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

	if(binds())
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
	}
#endif // statement tests

error_return:
	return 0;
}