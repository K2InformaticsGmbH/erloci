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
#include "ocisession.h"
#include <algorithm>

#include "lib_interface.h"

#include <oci.h>

void * ocisession::envhp = NULL;
void * ocisession::stmt_lock = NULL;

ocisession::FNAD2L ocisession::append_desc_to_list = NULL;
list<ocisession*> ocisession::_sessions;

void ocisession::config(FNAD2L _append_desc_to_list)
{
	append_desc_to_list = _append_desc_to_list;

	// Initialize OCI layer (late initializer)
	intf_ret r;
 	r.fn_ret = SUCCESS;

	if(envhp == NULL) {
		sword ret = 0;
		ret = OCIEnvCreate((OCIEnv**)&envhp,		/* returned env handle */
						   OCI_THREADED,			/* initilization modes */
						   NULL, NULL, NULL, NULL,	/* callbacks, context */
						   (size_t) 0,				/* optional extra memory size: optional */
						   (void**) NULL);			/* returned extra memeory */

		r.handle = envhp;
		checkenv(&r, ret);
		if(r.fn_ret != SUCCESS)
        throw r;
	}

	if(stmt_lock == NULL) {
		void *ehp;
		sword ret = 0;
		checkenv(&r, OCIHandleAlloc((OCIEnv*)envhp,	/* environment handle */
                            (void **) &ehp,			/* returned err handle */
                            OCI_HTYPE_ERROR,		/* typ of handle to allocate */
                            (size_t) 0,				/* optional extra memory size */
                            (void **) NULL));		/* returned extra memeory */
		ret = OCIThreadMutexInit((OCIEnv*)envhp,
                           (OCIError*)ehp, 
                           (OCIThreadMutex**)&stmt_lock);
		r.handle = envhp;
		checkenv(&r, ret);
		if(r.fn_ret != SUCCESS)
        throw r;
	}

	REMOTE_LOG(INF, "OCI Initialized");
}

ocisession::ocisession(const char * connect_str, size_t connect_str_len,
					   const char * user_name, size_t user_name_len,
					   const char * password, size_t password_len)
{
	intf_ret r;
	OCIAuthInfo *authp = NULL;

	r.handle = envhp;

	// allocate error handle
	checkenv(&r, OCIHandleAlloc((OCIEnv*)envhp,	/* environment handle */
                            (void **) &_errhp,	/* returned err handle */
                            OCI_HTYPE_ERROR,	/* typ of handle to allocate */
                            (size_t) 0,			/* optional extra memory size */
                            (void **) NULL));	/* returned extra memeory */

	if(r.fn_ret != SUCCESS) {
   		REMOTE_LOG(ERR, "failed OCIHandleAlloc %s\n", r.gerrbuf);
        throw r;
	}

	// allocate auth handle
	checkenv(&r, OCIHandleAlloc((OCIEnv*)envhp,
							(void**)&authp, OCI_HTYPE_AUTHINFO,
							(size_t)0, (void **) NULL));

	r.handle = _errhp;

	// usrname and password
	checkerr(&r, OCIAttrSet(authp, OCI_HTYPE_AUTHINFO,
								(void*) user_name, (ub4)user_name_len,
								OCI_ATTR_USERNAME, (OCIError *)_errhp));
	checkerr(&r, OCIAttrSet(authp, OCI_HTYPE_AUTHINFO,
								(void*) password, (ub4)password_len,
								OCI_ATTR_PASSWORD, (OCIError *)_errhp));


    /* get the database connection */
    checkerr(&r, OCISessionGet((OCIEnv*)envhp, (OCIError *)_errhp,
                               (OCISvcCtx**)&_svchp,					/* returned database connection */
                               authp,									/* initialized authentication handle */                               
                               (OraText *) connect_str, (ub4)connect_str_len,/* connect string */
                               NULL, 0, NULL, NULL, NULL,				/* session tagging parameters: optional */
                               OCI_DEFAULT));					        /* modes */
	if(r.fn_ret != SUCCESS) {
		REMOTE_LOG(ERR, "failed OCISessionGet %s\n", r.gerrbuf);
        throw r;
	}

	(void) OCIHandleFree(authp, OCI_HTYPE_AUTHINFO);

	REMOTE_LOG(INF, "got session %p %.*s user %.*s\n", _svchp, connect_str_len, connect_str, user_name_len, user_name);

	_sessions.push_back(this);
}

void ocisession::commit()
{
	intf_ret r;

	checkerr(&r, OCITransCommit((OCISvcCtx*)_svchp, (OCIError*)_errhp, OCI_DEFAULT));
	if(r.fn_ret != SUCCESS) {
		REMOTE_LOG(ERR, "failed OCITransCommit %s\n", r.gerrbuf);
        throw r;
	}
}

void ocisession::rollback()
{
	intf_ret r;

	checkerr(&r, OCITransRollback((OCISvcCtx*)_svchp, (OCIError*)_errhp, OCI_DEFAULT));
	if(r.fn_ret != SUCCESS) {
		REMOTE_LOG(ERR, "failed OCITransRollback %s\n", r.gerrbuf);
        throw r;
	}
}

void ocisession::describe_object(void *objptr, size_t objptr_len, ub1 objtyp,
								 void * desc_list)
{
	intf_ret r;

	ub4 col_name_len;
	ub2 numcols = 0, col_width = 0, coltyp = 0;
	ub1 char_semantics = 0;
	text *col_name = NULL;

	OCIDescribe *dschp = (OCIDescribe *) 0;		/* describe handle */
	OCIParam *parmh = (OCIParam *) 0;			/* parameter handle */
	OCIParam *collsthd = (OCIParam *) 0;		/* handle to list of columns */
	OCIParam *colhd = (OCIParam *) 0;			/* column handle */

	r.handle = envhp;
	checkenv(&r, OCIHandleAlloc((OCIEnv*)envhp, (dvoid **)&dschp,
                  (ub4)OCI_HTYPE_DESCRIBE, (size_t)0, (dvoid **)0));
	r.handle = _errhp;

	checkerr(&r, OCIDescribeAny((OCISvcCtx*)_svchp, (OCIError*)_errhp,
					   objptr, (ub4)objptr_len, OCI_OTYPE_NAME,
                       OCI_DEFAULT, objtyp, dschp));
	if(r.fn_ret != SUCCESS) {
		REMOTE_LOG(ERR, "failed OCIDescribeAny(OCI_OTYPE_NAME) %s\n", r.gerrbuf);
        throw r;
	}

	/* get the parameter handle */
	checkerr(&r, OCIAttrGet((dvoid *)dschp, OCI_HTYPE_DESCRIBE, (dvoid *)&parmh, (ub4 *)0,
					OCI_ATTR_PARAM, (OCIError*)_errhp));
    if(r.fn_ret != SUCCESS) {
		REMOTE_LOG(ERR, "failed OCIAttrGet(OCI_HTYPE_DESCRIBE, OCI_ATTR_PARAM) %s\n", r.gerrbuf);
		goto error_exit;
	}

	/* get the number of columns in the table */
	checkerr(&r, OCIAttrGet((dvoid *)parmh, OCI_DTYPE_PARAM, (dvoid *)&numcols, (ub4 *)0,
					OCI_ATTR_NUM_COLS, (OCIError*)_errhp));
    if(r.fn_ret != SUCCESS) {
		REMOTE_LOG(ERR, "failed OCIAttrGet(OCI_DTYPE_PARAM, OCI_ATTR_NUM_COLS) %s\n", r.gerrbuf);
		goto error_exit;
	}

	/* get the handle to the column list of the table */
	checkerr(&r, OCIAttrGet((dvoid *)parmh, OCI_DTYPE_PARAM, (dvoid *)&collsthd, (ub4 *)0,
					OCI_ATTR_LIST_COLUMNS, (OCIError*)_errhp));
	if(r.fn_ret == OCI_NO_DATA) {
		REMOTE_LOG(ERR, "failed OCIAttrGet(OCI_DTYPE_PARAM, OCI_ATTR_LIST_COLUMNS) -> OCI_NO_DATA %s\n", r.gerrbuf);
		goto error_exit;
	} else if(r.fn_ret != SUCCESS) {
		REMOTE_LOG(ERR, "failed OCIAttrGet(OCI_DTYPE_PARAM, OCI_ATTR_LIST_COLUMNS) %s\n", r.gerrbuf);
		goto error_exit;
	} 

	for (int i = 1; i <= numcols; i++)
	{
		/* get parameter for column i */
		checkerr(&r, OCIParamGet((dvoid *)collsthd, OCI_DTYPE_PARAM, (OCIError*)_errhp, (dvoid **)&colhd, (ub4)i));
		if(r.fn_ret != SUCCESS) {
			REMOTE_LOG(ERR, "failed OCIParamGet(OCI_DTYPE_PARAM) %s\n", r.gerrbuf);
			goto error_exit;
		}

		/* for example, get datatype for ith column */
		coltyp = 0;
		checkerr(&r, OCIAttrGet((dvoid *)colhd, OCI_DTYPE_PARAM, (dvoid *)&coltyp, (ub4 *)0, OCI_ATTR_DATA_TYPE, (OCIError*)_errhp));
		if(r.fn_ret != SUCCESS) {
			REMOTE_LOG(ERR, "failed OCIAttrGet(OCI_DTYPE_PARAM, OCI_ATTR_DATA_TYPE) %s\n", r.gerrbuf);
			goto error_exit;
		}

		/* Retrieve the length semantics for the column */
		char_semantics = 0;
		checkerr(&r, OCIAttrGet((dvoid*) colhd, (ub4) OCI_DTYPE_PARAM, (dvoid*) &char_semantics, (ub4 *) 0, (ub4) OCI_ATTR_CHAR_USED, (OCIError*)_errhp));
		if(r.fn_ret != SUCCESS) {
			REMOTE_LOG(ERR, "failed OCIAttrGet(OCI_DTYPE_PARAM, OCI_ATTR_CHAR_USED) %s\n", r.gerrbuf);
			goto error_exit;
		}

		col_width = 0;
		if (char_semantics) {
			/* Retrieve the column width in characters */
			checkerr(&r, OCIAttrGet((dvoid*) colhd, (ub4) OCI_DTYPE_PARAM, (dvoid*) &col_width, (ub4 *) 0, (ub4) OCI_ATTR_CHAR_SIZE, (OCIError*)_errhp));
			if(r.fn_ret != SUCCESS) {
				REMOTE_LOG(ERR, "failed OCIAttrGet(OCI_DTYPE_PARAM, OCI_ATTR_CHAR_SIZE) %s\n", r.gerrbuf);
				goto error_exit;
			}
		} else {
			/* Retrieve the column width in bytes */
			checkerr(&r, OCIAttrGet((dvoid*) colhd, (ub4) OCI_DTYPE_PARAM, (dvoid*) &col_width,(ub4 *) 0, (ub4) OCI_ATTR_DATA_SIZE,
					 (OCIError*)_errhp));
			if(r.fn_ret != SUCCESS) {
				REMOTE_LOG(ERR, "failed OCIAttrGet(OCI_DTYPE_PARAM, OCI_ATTR_DATA_SIZE) %s\n", r.gerrbuf);
				goto error_exit;
			}
		}

		/* Retrieve the column name */
        col_name_len = 0;
        checkerr(&r, OCIAttrGet((dvoid*) colhd, (ub4) OCI_DTYPE_PARAM,
                                (dvoid**) &col_name, (ub4 *) &col_name_len, (ub4) OCI_ATTR_NAME,
                                (OCIError*)_errhp));
		if(r.fn_ret != SUCCESS) {
			REMOTE_LOG(ERR, "failed OCIAttrGet(OCI_DTYPE_PARAM, OCI_ATTR_NAME) error %s\n", r.gerrbuf);
			goto error_exit;
		}

		(*append_desc_to_list)((char*)col_name, col_name_len, coltyp, col_width, desc_list);
	}
	return;

error_exit:
	if (dschp)
		OCIHandleFree((dvoid *) dschp, OCI_HTYPE_DESCRIBE);
	throw r;
}

ocistmt* ocisession::prepare_stmt(OraText *stmt, size_t stmt_len)
{
	intf_ret r;

	r.handle = envhp;
	checkenv(&r, OCIThreadMutexAcquire((OCIEnv*)envhp, (OCIError *)_errhp, (OCIThreadMutex*)stmt_lock));
	if(r.fn_ret != SUCCESS) {
   		REMOTE_LOG(ERR, "failed OCIThreadMutexAcquire %s\n", r.gerrbuf);
        throw r;
	}

	ocistmt * statement = new ocistmt(this, stmt, stmt_len);
	_statements.push_back(statement);

	checkenv(&r, OCIThreadMutexRelease((OCIEnv*)envhp, (OCIError *)_errhp, (OCIThreadMutex*)stmt_lock));
	if(r.fn_ret != SUCCESS) {
   		REMOTE_LOG(ERR, "failed OCIThreadMutexRelease %s\n", r.gerrbuf);
        throw r;
	}

	return statement;
}

void ocisession::release_stmt(ocistmt *stmt)
{
	intf_ret r;

	r.handle = envhp;
	checkenv(&r, OCIThreadMutexAcquire((OCIEnv*)envhp, (OCIError *)_errhp, (OCIThreadMutex*)stmt_lock));
	if(r.fn_ret != SUCCESS) {
   		REMOTE_LOG(ERR, "failed OCIThreadMutexAcquire %s\n", r.gerrbuf);
        throw r;
	}

	list<ocistmt*>::iterator it = std::find(_statements.begin(), _statements.end(), stmt);
	if (it != _statements.end()) {
		_statements.remove(*it);
	}

	checkenv(&r, OCIThreadMutexRelease((OCIEnv*)envhp, (OCIError *)_errhp, (OCIThreadMutex*)stmt_lock));
	if(r.fn_ret != SUCCESS) {
   		REMOTE_LOG(ERR, "failed OCIThreadMutexRelease %s\n", r.gerrbuf);
        throw r;
	}
}

bool ocisession::has_statement(ocistmt *stmt)
{
	bool found = false;
	intf_ret r;

	r.handle = envhp;
	checkenv(&r, OCIThreadMutexAcquire((OCIEnv*)envhp, (OCIError *)_errhp, (OCIThreadMutex*)stmt_lock));
	if(r.fn_ret != SUCCESS) {
   		REMOTE_LOG(ERR, "failed OCIThreadMutexAcquire %s\n", r.gerrbuf);
        throw r;
	}

	for (list<ocistmt*>::iterator it = _statements.begin(); it != _statements.end(); ++it) {
		if(*it == stmt) {
			found = true;
			break;
		}
	}

	checkenv(&r, OCIThreadMutexRelease((OCIEnv*)envhp, (OCIError *)_errhp, (OCIThreadMutex*)stmt_lock));
	if(r.fn_ret != SUCCESS) {
   		REMOTE_LOG(ERR, "failed OCIThreadMutexRelease %s\n", r.gerrbuf);
        throw r;
	}

	return found;
}

ocisession::~ocisession(void)
{
	intf_ret r;

	// delete all the statements
	for (list<ocistmt*>::iterator it = _statements.begin(); it != _statements.end(); ++it)
		(*it)->del();
	_statements.clear();

	checkerr(&r, OCISessionRelease((OCISvcCtx*)_svchp, (OCIError*)_errhp, NULL, 0, OCI_DEFAULT));
	if(r.fn_ret != SUCCESS) {
		REMOTE_LOG(ERR, "failed OCISessionRelease %s\n", r.gerrbuf);
        throw r;
	}

	(void) OCIHandleFree(_errhp, OCI_HTYPE_ERROR);

	_sessions.remove(this);
}