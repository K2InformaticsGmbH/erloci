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

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include <oci.h>
#include <occi.h>
#include "oci_lib_intf.h"

using namespace std;
using namespace oracle::occi;

#ifdef __WIN32__
#define	SPRINT sprintf_s
#else
#define	SPRINT snprintf
#endif

/* constants */
#define POOL_MIN	0
#define POOL_MAX	10
#define POOL_INCR	0
#define MAX_COLUMS	1000

/* opaque types */
typedef struct column_info {
    ub2  dtype;
    ub4	 dlen;
	sb2  indp;
} column_info;

typedef struct sess_ctx {
	OCISvcCtx *svchp;
} sess_ctx;

typedef struct stmt_ctx {
	column_info	*columns;
	ub4			num_cols;
	OCIStmt		*stmthp;
} stmt_ctx;

/*
 * Global variables
 */
static OCIEnv		*envhp		= NULL;
static OCIError		*errhp		= NULL;
static OCISPool		*spoolhp	= NULL;

static char			*poolName;
static ub4			poolNameLen	= 0;

/* Error checking functions and macros */
void checkerr0(intf_ret *, ub4, sword, const char * function_name, int line_no);
#define checkerr(errhp, status) checkerr0((errhp), OCI_HTYPE_ERROR, (status), __FUNCTION__, __LINE__)
#define checkenv(envhp, status) checkerr0((envhp), OCI_HTYPE_ENV, (status), __FUNCTION__, __LINE__)

static INTF_RET function_success = SUCCESS;

void oci_init(void)
{
	intf_ret r;
	r.fn_ret = SUCCESS;

	sword ret = 0;
	ret = OCIEnvCreate(&envhp,					/* returned env handle */
                       OCI_THREADED,			/* initilization modes */
                       NULL, NULL, NULL, NULL,	/* callbacks, context */
                       (size_t) 0,				/* optional extra memory size: optional */
                       (void**) NULL);			/* returned extra memeory */

	r.handle = envhp;
    checkenv(&r, ret);
    if(r.fn_ret != SUCCESS)
        exit(1);

    checkenv(&r, OCIHandleAlloc(envhp,				/* environment handle */
                                (void **) &errhp,	/* returned err handle */
                                OCI_HTYPE_ERROR,	/* typ of handle to allocate */
                                (size_t) 0,			/* optional extra memory size */
                                (void **) NULL));	/* returned extra memeory */

    if(r.fn_ret != SUCCESS)
        exit(1);
}

void oci_cleanup(void)
{
    oci_free_session_pool();
    if (errhp != NULL) {
        OCIHandleFree(errhp, OCI_HTYPE_ERROR);
        errhp = NULL;
    }
}

intf_ret oci_create_tns_seesion_pool(const char * connect_str, const int connect_str_len,
                                 const char * user_name, const int user_name_len,
                                 const char * password, const int password_len,
                                 const char * options, const int options_len)
{
	intf_ret r;
	r.fn_ret = SUCCESS;

    poolNameLen = 0;
	ub4	stmt_cachesize = 0;

/////////////////////////////////////////////////////////////////////////////////////////////
	unsigned char *constr = new unsigned char[connect_str_len+1];
	unsigned char *uname = new unsigned char[user_name_len+1];
	unsigned char *pswd = new unsigned char[password_len+1];

	try {
		Environment *env = Environment::createEnvironment(Environment::THREADED_MUTEXED);
		REMOTE_LOG("Environment::createEnvironment\n");

		sprintf((char*)constr, "%.*s", connect_str_len, connect_str);
		sprintf((char*)uname, "%.*s", user_name_len, user_name);
		sprintf((char*)pswd, "%.*s", password_len, password);
		
		REMOTE_LOG("env->createConnection -- %s %s %s\n", uname, pswd, constr);
		Connection *conn = env->createConnection((char*)uname, (char*)pswd, (char*)constr);

		REMOTE_LOG("env->createConnection\n");
	} catch (std::exception const & ex) {
		REMOTE_LOG("failed Environment::createEnvironment for %s\n", ex.what());
	} catch (std::string const & ex) {
		REMOTE_LOG("failed Environment::createEnvironment for %s\n", ex);
	} catch (...) {
		// ...
	}

	delete constr;
	delete uname;
	delete pswd;
/////////////////////////////////////////////////////////////////////////////////////////////

	if((r = oci_free_session_pool()).fn_ret != SUCCESS) {
		REMOTE_LOG("failed oci_free_session_pool for %s\n", r.gerrbuf);
		return r;
	}

    /* allocate session pool handle
     * note: for OCIHandleAlloc() we check error on environment handle
     */
	r.handle = envhp;
    checkenv(&r, OCIHandleAlloc(envhp, (void **)&spoolhp,
                                   OCI_HTYPE_SPOOL, (size_t) 0, (void **) NULL));

    /* set the statement cache size for all sessions in the pool
     * note: this can also be set per session after obtaining the session from the pool
     */
	r.handle = errhp;
    checkerr(&r, OCIAttrSet(spoolhp, OCI_HTYPE_SPOOL,
                               &stmt_cachesize, 0, OCI_ATTR_SPOOL_STMTCACHESIZE, errhp));

    checkerr(&r, OCISessionPoolCreate(envhp, errhp,
                                         spoolhp,
                                         (OraText **) &poolName, &poolNameLen,
                                         (OraText *) connect_str, connect_str_len,
                                         POOL_MIN, POOL_MAX, POOL_INCR,
                                         (OraText*)user_name, user_name_len,		/* homo pool user specified */
                                         (OraText*)password, password_len,			/* homo pool password specified */
                                         OCI_SPC_STMTCACHE));	/* modes */
	if(r.fn_ret != SUCCESS) {
		REMOTE_LOG("failed OCISessionPoolCreate for %s\n", r.gerrbuf);
		return r;
	}

	REMOTE_LOG("created session pool %.*s\n", poolNameLen, (char *)poolName);

    ub1 spoolMode = OCI_SPOOL_ATTRVAL_NOWAIT;
    checkerr(&r, OCIAttrSet(spoolhp, OCI_HTYPE_SPOOL,
                               (void*)&spoolMode, sizeof(ub1),
                               OCI_ATTR_SPOOL_GETMODE, errhp));

    return r;
}

intf_ret oci_free_session_pool(void)
{
	intf_ret r;
	r.fn_ret = SUCCESS;
	r.handle = errhp;

    if (spoolhp != NULL) {
        checkerr(&r, OCISessionPoolDestroy(spoolhp, errhp, OCI_SPD_FORCE));
		if(r.fn_ret != SUCCESS) {
			REMOTE_LOG("failed OCISessionPoolDestroy %.*s for %s\n", poolNameLen, (char*)poolName, r.gerrbuf);
			return r;
		}

		if(OCI_SUCCESS != OCIHandleFree(spoolhp, OCI_HTYPE_SPOOL)) {
			REMOTE_LOG("failed OCIHandleFree for pool %.*s\n", poolNameLen, (char*)poolName);
			r.fn_ret = FAILURE;
            return r;
		}
        spoolhp = NULL;

		REMOTE_LOG("destroyed session pool %.*s\n", poolNameLen, (char *)poolName);
    }

	return r;
}

intf_ret oci_get_session_from_pool(void **conn_handle)
{
	OCISvcCtx *svchp = NULL;
	intf_ret r;

	r.fn_ret = SUCCESS;

	// allocate the session handle
	sess_ctx *sessctx = NULL;
	checkenv(&r, OCIHandleAlloc(envhp,					/* environment handle */
                                (void **) &svchp,		/* returned statement handle */
                                OCI_HTYPE_SVCCTX,			/* typ of handle to allocate */
                                sizeof(sess_ctx),		/* optional extra memory size */
                                (void **) &sessctx));	/* returned extra memeory */
	if(r.fn_ret != SUCCESS) {
		REMOTE_LOG("session handle alloc failed %s\n", r.gerrbuf);
        return r;
	}

    /* get the database connection */
	r.handle = errhp;
    checkerr(&r, OCISessionGet(envhp, errhp,
                               &svchp,								/* returned database connection */
                               NULL,								/* initialized authentication handle */                               
                               (OraText *) poolName, poolNameLen,	/* connect string */
                               NULL, 0, NULL, NULL, NULL,			/* session tagging parameters: optional */
                               OCI_SESSGET_SPOOL));					/* modes */
	if(r.fn_ret != SUCCESS) {
		REMOTE_LOG("failed OCISessionGet from pool %.*s for %s\n", poolNameLen, (char*)poolName, r.gerrbuf);
        return r;
	}

	sessctx->svchp = svchp;
	*conn_handle = sessctx;

	REMOTE_LOG("got session %p from pool %.*s\n", svchp, poolNameLen, (char*)poolName);
    return r;
}

intf_ret oci_return_connection_to_pool(void * conn_handle)
{
	intf_ret r;

	r.fn_ret = SUCCESS;
	r.handle = errhp;
	sess_ctx *sessctx = (sess_ctx *)conn_handle;

	if (sessctx->svchp != NULL) {
        checkerr(&r, OCISessionRelease(sessctx->svchp, errhp, NULL, 0, OCI_SESSRLS_DROPSESS));
		if(r.fn_ret != SUCCESS) {
			REMOTE_LOG("return session %p to pool %.*s failed %s\n", sessctx->svchp, poolNameLen, (char*)poolName, r.gerrbuf);
		    return r;
		}

		REMOTE_LOG("return session %p to pool %.*s\n", sessctx->svchp, poolNameLen, (char*)poolName);
	}

    return r;
}

intf_ret oci_exec_sql(const void *conn_handle, void ** stmt_handle, const unsigned char * query_str, int query_str_len
					  , inp_t *params_head, void * column_list
					  , void (*coldef_append)(const char *, const char *, const unsigned int, void *))
{
    intf_ret r;

	r.fn_ret = SUCCESS;

    OCIStmt *stmthp	= NULL;
	stmt_ctx *smtctx = NULL;
    ub4 itrs = 0;
    ub4 stmt_typ = OCI_STMT_SELECT;
    ub2 type = SQLT_INT;
    int idx = 1;
	sess_ctx *sessctx = (sess_ctx *)conn_handle;

	// allocate the statement handle
	r.handle = envhp;
	checkenv(&r, OCIHandleAlloc(envhp,					/* environment handle */
                                (void **) &stmthp,		/* returned statement handle */
                                OCI_HTYPE_STMT,			/* typ of handle to allocate */
                                sizeof(stmt_ctx),		/* optional extra memory size */
                                (void **) &smtctx));	/* returned extra memeory */
	if(r.fn_ret != SUCCESS) {
        REMOTE_LOG("statement handle from session %p alloc failed %s\n", sessctx->svchp, r.gerrbuf);
		goto error_exit;
	}

	// from this point on regular errhp will do
	r.handle = errhp;

    /* Get a prepared statement handle */
    checkerr(&r, OCIStmtPrepare2(sessctx->svchp,
                                 &stmthp,					/* returned statement handle */
                                 errhp,						/* error handle */
                                 (OraText *) query_str,		/* the statement text */
                                 query_str_len,				/* length of the text */
                                 NULL, 0,					/* tagging parameters: optional */
                                 OCI_NTV_SYNTAX, OCI_DEFAULT));

	smtctx->stmthp = stmthp;
	*stmt_handle = smtctx;

	if(r.fn_ret != SUCCESS) {
		REMOTE_LOG("failed OCIStmtPrepare2(%.*s) from session %p for %s\n", query_str_len, (char*)query_str, sessctx->svchp, r.gerrbuf);
		goto error_exit;
	}

    /* Bind variables */
    for(inp_t *param = params_head; param != NULL; param = param->next) {
        switch (param->dty) {
        case NUMBER:
            type = SQLT_INT;
            break;
        case STRING:
            type = SQLT_STR;
            break;
        }
        param->bndp = NULL;
        checkerr(&r, OCIBindByPos(stmthp, (OCIBind **)&(param->bndp), errhp, idx,
                                  (dvoid *) param->valuep, (sword) param->value_sz, type,
                                  (dvoid *) 0, (ub2 *) 0, (ub2 *) 0, (ub4) 0, (ub4 *) 0, OCI_DEFAULT));
		if(r.fn_ret != SUCCESS) {
			REMOTE_LOG("failed OCIBindByPos(%.*s) from session %p for %s\n", query_str_len, (char*)query_str, sessctx->svchp, r.gerrbuf);
			goto error_exit;
		}
        idx++;
    }

    checkerr(&r, OCIAttrGet((dvoid*) stmthp, (ub4) OCI_HTYPE_STMT,
                            (dvoid*) &stmt_typ, (ub4 *)NULL, (ub4)OCI_ATTR_STMT_TYPE, errhp));
	if(r.fn_ret != SUCCESS) {
		REMOTE_LOG("failed OCIAttrGet(%.*s) from session %p for %s\n", query_str_len, (char*)query_str, sessctx->svchp, r.gerrbuf);
		goto error_exit;
	}

    if(stmt_typ != OCI_STMT_SELECT)
        itrs = 1;

    /* execute the statement and commit */
    checkerr(&r, OCIStmtExecute(sessctx->svchp, stmthp, errhp, itrs, 0,
                                (OCISnapshot *)NULL, (OCISnapshot *)NULL,
                                OCI_COMMIT_ON_SUCCESS));
	if(r.fn_ret != SUCCESS) {
		REMOTE_LOG("failed OCIStmtExecute(%.*s) from session %p for %s\n", query_str_len, (char*)query_str, sessctx->svchp, r.gerrbuf);
		goto error_exit;
	}

    if(stmt_typ == OCI_STMT_SELECT) {
        OCIParam	*mypard;
        smtctx->num_cols	= 1;
        sb4         parm_status;

        /* Request a parameter descriptor for position 1 in the select-list */
        parm_status = OCIParamGet(stmthp, OCI_HTYPE_STMT, errhp, (dvoid **)&mypard,
                                  (ub4) smtctx->num_cols);
        checkerr(&r, parm_status);
		if(r.fn_ret != SUCCESS) {
			REMOTE_LOG("failed OCIParamGet(%.*s) from session %p for %s\n", query_str_len, (char*)query_str, sessctx->svchp, r.gerrbuf);
			goto error_exit;
		}

        /* Loop only if a descriptor was successfully retrieved for
         * current position, starting at 1
         */
        text *col_name;
        ub4 len = 0;
        char * data_type = NULL;
        smtctx->columns = NULL;
        while (parm_status == OCI_SUCCESS) {
            smtctx->columns = (column_info *)realloc(smtctx->columns, smtctx->num_cols * sizeof(column_info));

			column_info * cur_clm = &(smtctx->columns[smtctx->num_cols-1]);

			/* Retrieve the data size attribute */
            len = 0;
            checkerr(&r, OCIAttrGet((dvoid*) mypard, (ub4) OCI_DTYPE_PARAM,
                                    (dvoid*) &len, (ub4 *)0, (ub4)OCI_ATTR_DATA_SIZE,
                                    errhp));
			if(r.fn_ret != SUCCESS) {
				REMOTE_LOG("failed OCIAttrGet(%.*s) from session %p for %s\n", query_str_len, (char*)query_str, sessctx->svchp, r.gerrbuf);
				goto error_exit;
			}
            cur_clm->dlen = len;

            /* Retrieve the data type attribute */
            len = 0;
            checkerr(&r, OCIAttrGet((dvoid*) mypard, (ub4) OCI_DTYPE_PARAM,
                                    (dvoid*) &len, (ub4 *)0, (ub4)OCI_ATTR_DATA_TYPE,
                                    errhp));
			if(r.fn_ret != SUCCESS) {
				REMOTE_LOG("failed OCIAttrGet(%.*s) from session %p for %s\n", query_str_len, (char*)query_str, sessctx->svchp, r.gerrbuf);
				goto error_exit;
			}
            cur_clm->dtype = len;

            switch (len) {
            case SQLT_NUM:
            case SQLT_VNU:
            case SQLT_LNG:
                data_type = (char*)"number";
                break;
            case SQLT_AVC:
            case SQLT_AFC:
            case SQLT_CHR:
            case SQLT_STR:
            case SQLT_VCS:
                data_type = (char*)"string";
                break;
            case SQLT_INT:
            case SQLT_UIN:
                data_type = (char*)"integer";
                break;
            case SQLT_DAT:
                data_type = (char*)"date";
                break;
            case SQLT_FLT:
                data_type = (char*)"double";
                break;
            case SQLT_TIMESTAMP:
            case SQLT_TIMESTAMP_TZ:
            case SQLT_TIMESTAMP_LTZ:
                data_type = (char*)"timestamp";
                break;
            case SQLT_INTERVAL_YM:
            case SQLT_INTERVAL_DS:
                data_type = (char*)"interval";
                break;
            default:
                data_type = (char*)"undefined";
                break;
            }

            /* Retrieve the column name attribute */
            len = 0;
            checkerr(&r, OCIAttrGet((dvoid*) mypard, (ub4) OCI_DTYPE_PARAM,
                                    (dvoid**) &col_name, (ub4 *) &len, (ub4) OCI_ATTR_NAME,
                                    errhp));
			if(r.fn_ret != SUCCESS) {
				REMOTE_LOG("failed OCIAttrGet(%.*s) from session %p for %s\n", query_str_len, (char*)query_str, sessctx->svchp, r.gerrbuf);
				goto error_exit;
			}
            char * column_name = new char[len+1];
			SPRINT(column_name, len+1, "%.*s", len, col_name);
            (*coldef_append)(column_name, data_type, cur_clm->dlen, column_list);
            delete column_name;
            col_name = NULL;

            /* Increment counter and get next descriptor, if there is one */
            if(OCI_SUCCESS != OCIDescriptorFree(mypard, OCI_DTYPE_PARAM)) {
				REMOTE_LOG("failed OCIDescriptorFree(%.*s) from session %p for %s\n", query_str_len, (char*)query_str, sessctx->svchp, r.gerrbuf);
                r.fn_ret = FAILURE;
                goto error_exit;
            }
            smtctx->num_cols++;
            parm_status = OCIParamGet(stmthp, OCI_HTYPE_STMT, errhp, (dvoid **)&mypard,
                                      (ub4) smtctx->num_cols);
        }
        --smtctx->num_cols;

		if(r.fn_ret != SUCCESS) {
			REMOTE_LOG("failed OCIDescriptorFree(%.*s) from session %p for %s\n", query_str_len, (char*)query_str, sessctx->svchp, r.gerrbuf);
            goto error_exit;
		}

        //REMOTE_LOG("Port: Returning Column(s)\n");
    } else {
        if(stmthp != NULL) {
            checkerr(&r, OCIStmtRelease(stmthp, errhp, NULL, 0, OCI_DEFAULT));
            if(r.fn_ret != SUCCESS) goto error_exit;
        }
        //REMOTE_LOG("Port: Executed non-select statement!\n");
    }

	//REMOTE_LOG("Executing \"%.*s;\"\n", query_str_len, query_str);

error_exit:
	return r;
}

intf_ret oci_close_statement(void * stmt_handle)
{
	intf_ret r;
	stmt_ctx *smtctx = (stmt_ctx *)stmt_handle;
	OCIStmt *stmthp	= smtctx->stmthp;

	free(smtctx->columns);
	r.handle = errhp;
    checkerr(&r, OCIStmtRelease(stmthp, errhp, (OraText *) NULL, 0, OCI_DEFAULT));
	if(r.fn_ret != SUCCESS) {
		REMOTE_LOG("failed OCIStmtRelease %s\n", r.gerrbuf);
		return r;
	}

	REMOTE_LOG("closed statement %p\n", stmthp);
	return r;
}

intf_ret oci_produce_rows(void * stmt_handle
						 , void * row_list
						 , void (*string_append)(const char * string, int len, void * list)
						 , void (*list_append)(const void * sub_list, void * list)
						 , unsigned int (*sizeof_resp)(void * resp)
                         , int maxrowcount)
{
	intf_ret r;
	stmt_ctx *smtctx = (stmt_ctx *)stmt_handle;
	OCIStmt *stmthp	= smtctx->stmthp;
	unsigned long int total_est_row_size = 0;
    unsigned int num_rows = 0;
    void * row = NULL;
    ub4 i = 0;
    sword res = OCI_NO_DATA;
    OCIDefine **defnhp = NULL;
    void ** data_row = NULL;

	r.handle = errhp;
	r.fn_ret = FAILURE;
	if (smtctx->columns == NULL || stmthp == NULL) {
		REMOTE_LOG("invalid statement handle(%p) or no columns %p\n", stmthp, smtctx->columns);
        goto error_exit;
	}

	r.fn_ret = SUCCESS;

    defnhp = (OCIDefine **)malloc(smtctx->num_cols * sizeof(OCIDefine *));
    data_row = (void **) calloc(smtctx->num_cols, sizeof(void *));

    // overdrive preventation
    if(maxrowcount > 100)
        maxrowcount = 100;

    /*
     * Fetch the data
     */

    /* Bind appropriate variables for data based on the column type */
    for (i = 0; i < smtctx->num_cols; ++i)
        switch (smtctx->columns[i].dtype) {
        case SQLT_TIMESTAMP:
		{
			/* Allocate the descriptor (storage) for the datatype */
			data_row[i] = NULL;
			r.handle = envhp;
			checkerr(&r, OCIDescriptorAlloc(envhp,(dvoid **)&(data_row[i]), OCI_DTYPE_TIMESTAMP, 0, (dvoid **)0));
			if(r.fn_ret != SUCCESS) {
				REMOTE_LOG("failed OCIDescriptorAlloc for %p column %d(SQLT_TIMESTAMP)\n", stmthp, i);
				goto error_return;
			}
			r.handle = errhp;

            checkerr(&r, OCIDefineByPos(stmthp, &(defnhp[i]), errhp, i+1, (dvoid *)(data_row[i]),
										(sword) smtctx->columns[i].dlen + 1, SQLT_TIMESTAMP, &(smtctx->columns[i].indp), (ub2 *)0,
                                        (ub2 *)0, OCI_DEFAULT));
			if(r.fn_ret != SUCCESS) {
				REMOTE_LOG("failed OCIDefineByPos for %p column %d(SQLT_TIMESTAMP)\n", stmthp, i);
				goto error_return;
			}
        }
		break;
        case SQLT_TIMESTAMP_TZ:
		{
			/* Allocate the descriptor (storage) for the datatype */
			data_row[i] = NULL;
			r.handle = envhp;
			checkerr(&r, OCIDescriptorAlloc(envhp,(dvoid **)&(data_row[i]), OCI_DTYPE_TIMESTAMP_TZ, 0, (dvoid **)0));
			if(r.fn_ret != SUCCESS) {
				REMOTE_LOG("failed OCIDescriptorAlloc for %p column %d(SQLT_TIMESTAMP_TZ)\n", stmthp, i);
				goto error_return;
			}
			r.handle = errhp;

            checkerr(&r, OCIDefineByPos(stmthp, &(defnhp[i]), errhp, i+1, (dvoid *)(data_row[i]),
										(sword) smtctx->columns[i].dlen + 1, SQLT_TIMESTAMP_TZ, &(smtctx->columns[i].indp), (ub2 *)0,
                                        (ub2 *)0, OCI_DEFAULT));
			if(r.fn_ret != SUCCESS) {
				REMOTE_LOG("failed OCIDefineByPos for %p column %d(SQLT_TIMESTAMP_TZ)\n", stmthp, i);
				goto error_return;
			}
        }
		break;
        case SQLT_TIMESTAMP_LTZ:
		{
			/* Allocate the descriptor (storage) for the datatype */
			data_row[i] = NULL;
			r.handle = envhp;
			checkerr(&r, OCIDescriptorAlloc(envhp,(dvoid **)&(data_row[i]), OCI_DTYPE_TIMESTAMP_LTZ, 0, (dvoid **)0));
			if(r.fn_ret != SUCCESS) {
				REMOTE_LOG("failed OCIDescriptorAlloc for %p column %d(SQLT_TIMESTAMP_LTZ)\n", stmthp, i);
				goto error_return;
			}
			r.handle = errhp;

            checkerr(&r, OCIDefineByPos(stmthp, &(defnhp[i]), errhp, i+1, (dvoid *)(data_row[i]),
										(sword) smtctx->columns[i].dlen + 1, SQLT_TIMESTAMP_LTZ, &(smtctx->columns[i].indp), (ub2 *)0,
                                        (ub2 *)0, OCI_DEFAULT));
			if(r.fn_ret != SUCCESS) {
				REMOTE_LOG("failed OCIDefineByPos for %p column %d(SQLT_TIMESTAMP_LTZ)\n", stmthp, i);
				goto error_return;
			}
        }
        break;
        case SQLT_INTERVAL_YM:
		{
			/* Allocate the descriptor (storage) for the datatype */
			data_row[i] = NULL;
			r.handle = envhp;
			checkerr(&r, OCIDescriptorAlloc(envhp,(dvoid **)&(data_row[i]), OCI_DTYPE_INTERVAL_YM, 0, (dvoid **)0));
			if(r.fn_ret != SUCCESS) {
				REMOTE_LOG("failed OCIDescriptorAlloc for %p column %d(SQLT_INTERVAL_YM)\n", stmthp, i);
				goto error_return;
			}
			r.handle = errhp;

            checkerr(&r, OCIDefineByPos(stmthp, &(defnhp[i]), errhp, i+1, (dvoid *)(data_row[i]),
										(sword) smtctx->columns[i].dlen + 1, SQLT_INTERVAL_YM, &(smtctx->columns[i].indp), (ub2 *)0,
                                        (ub2 *)0, OCI_DEFAULT));
			if(r.fn_ret != SUCCESS) {
				REMOTE_LOG("failed OCIDefineByPos for %p column %d(SQLT_INTERVAL_YM)\n", stmthp, i);
				goto error_return;
			}
        }
		break;
        case SQLT_INTERVAL_DS:
		{
			/* Allocate the descriptor (storage) for the datatype */
			data_row[i] = NULL;
			r.handle = envhp;
			checkerr(&r, OCIDescriptorAlloc(envhp,(dvoid **)&(data_row[i]), OCI_DTYPE_INTERVAL_DS, 0, (dvoid **)0));
			if(r.fn_ret != SUCCESS) {
				REMOTE_LOG("failed OCIDescriptorAlloc for %p column %d(SQLT_INTERVAL_DS)\n", stmthp, i);
				goto error_return;
			}
			r.handle = errhp;

            checkerr(&r, OCIDefineByPos(stmthp, &(defnhp[i]), errhp, i+1, (dvoid *)(data_row[i]),
										(sword) smtctx->columns[i].dlen + 1, SQLT_INTERVAL_DS, &(smtctx->columns[i].indp), (ub2 *)0,
                                        (ub2 *)0, OCI_DEFAULT));
			if(r.fn_ret != SUCCESS) {
				REMOTE_LOG("failed OCIDefineByPos for %p column %d(SQLT_INTERVAL_DS)\n", stmthp, i);
				goto error_return;
			}
        }
        break;
		case SQLT_DAT:
		{
            data_row[i] = (text *) malloc((smtctx->columns[i].dlen + 1) * sizeof(text));
            checkerr(&r, OCIDefineByPos(stmthp, &(defnhp[i]), errhp, i+1, (dvoid *) (data_row[i]),
										(sword) smtctx->columns[i].dlen + 1, SQLT_DAT, &(smtctx->columns[i].indp), (ub2 *)0,
                                        (ub2 *)0, OCI_DEFAULT));
			if(r.fn_ret != SUCCESS) {
				REMOTE_LOG("failed OCIDefineByPos for %p column %d(SQLT_DAT)\n", stmthp, i);
				goto error_return;
			}
        }
        break;
        case SQLT_NUM:
        case SQLT_CHR: {
            data_row[i] = (text *) malloc((smtctx->columns[i].dlen + 1) * sizeof(text));
            checkerr(&r, OCIDefineByPos(stmthp, &(defnhp[i]), errhp, i+1, (dvoid *) (data_row[i]),
                                        (sword) smtctx->columns[i].dlen + 1, SQLT_STR, &(smtctx->columns[i].indp), (ub2 *)0,
                                        (ub2 *)0, OCI_DEFAULT));
			if(r.fn_ret != SUCCESS) {
				REMOTE_LOG("failed OCIDefineByPos for %p column %d(SQLT_DAT)\n", stmthp, i);
				goto error_return;
			}
        }
        break;
        default:
            break;
        }

    /* Fetch data by row */

    do {
        ++num_rows;
		//if(num_rows % 100 == 0) REMOTE_LOG("OCI: Fetched %lu rows of %d bytes\n", num_rows, total_est_row_size);
        res = OCIStmtFetch(stmthp, errhp, 1, 0, 0);
		checkerr(&r, res);
		if(r.fn_ret != SUCCESS) {
			REMOTE_LOG("failed OCIStmtFetch for %p row %d reason %s\n", stmthp, num_rows, r.gerrbuf);
		    goto error_return;
		}

        row = NULL;

		if (res != OCI_NO_DATA) {
			for (i = 0; i < smtctx->num_cols; ++i)
					switch (smtctx->columns[i].dtype) {
					case SQLT_TIMESTAMP:
					case SQLT_TIMESTAMP_TZ:
					case SQLT_TIMESTAMP_LTZ: {
						r.handle = envhp;
						ub1 *dtarry = NULL;
						ub4 len;
						checkerr(&r, OCIDateTimeToArray(envhp, errhp, (CONST OCIDateTime *)(data_row[i]),
                                        (CONST OCIInterval *)NULL, dtarry, &len, (ub1)0xFF));
						r.handle = errhp; }
						break;
					/*case SQLT_INTERVAL_YM:
					case SQLT_INTERVAL_DS:
						break;*/
					case SQLT_DAT: {
						char date_buf[15];
						sprintf(date_buf, "%02d%02d%02d%02d%02d%02d%02d",
							((char*)data_row[i])[0] - 100, // Century
							((char*)data_row[i])[1] - 100, // Year
							((char*)data_row[i])[2],		 // Month
							((char*)data_row[i])[3],		 // Day
							((char*)data_row[i])[4],		 // 24HH
							((char*)data_row[i])[5],		 // MM
							((char*)data_row[i])[6]);		 // DD
//						(*string_append)(date_buf, strlen(date_buf), &row); }
						(*string_append)((char*)data_row[i], 7, &row); }
						break;
					case SQLT_NUM:
					case SQLT_CHR:
						(*string_append)((char*)data_row[i], strlen((char*)data_row[i]), &row);
						break;
					}
			total_est_row_size += (*sizeof_resp)(&row);
			(*list_append)(row, row_list);
		}
    } while (res != OCI_NO_DATA
			&& num_rows < maxrowcount
			&& total_est_row_size < MAX_RESP_SIZE);

    /* cleanup only if data fetch is finished */
	if (res == OCI_NO_DATA)
		oci_close_statement(smtctx);

    free(defnhp);

	if(r.fn_ret != SUCCESS) {
		REMOTE_LOG("this should never happen reason %s\n", r.gerrbuf);
        goto error_return;
	}

    //REMOTE_LOG("Port: Returning Rows...\n");
	if(res != OCI_NO_DATA)
		r.fn_ret = MORE;
	else
		r.fn_ret = DONE;

error_return:
    /* Release the bound variables memeory */
	for (i = 0; i < smtctx->num_cols; ++i) {
        if(data_row[i] != NULL)
            free(data_row[i]);
	}

    free(data_row);

error_exit:
    return r;
}

INTF_RET describe(const void *conn_handle, unsigned char * objptr)
{
REMOTE_LOG("..........................TRACE...\n");
	int i=0;
	ub2          numcols, col_width;
	ub1          char_semantics;
	ub2  coltyp;
	ub4 objp_len = (ub4) strlen((char *)objptr);
	OCISvcCtx *svchp = (OCISvcCtx *)conn_handle;
	OCIParam *parmh = (OCIParam *) 0;         /* parameter handle */
	OCIParam *collsthd = (OCIParam *) 0;      /* handle to list of columns */
	OCIParam *colhd = (OCIParam *) 0;         /* column handle */
	OCIDescribe *dschp = (OCIDescribe *)0;      /* describe handle */

	OCIHandleAlloc((dvoid *)envhp, (dvoid **)&dschp,
			(ub4)OCI_HTYPE_DESCRIBE, (size_t)0, (dvoid **)0);

	/* get the describe handle for the table */
	if (OCIDescribeAny(svchp, errhp, (dvoid *)objptr, objp_len, OCI_OTYPE_NAME, 0,
		 OCI_PTYPE_TABLE, dschp))
	   return FAILURE;

	/* get the parameter handle */
	if (OCIAttrGet((dvoid *)dschp, OCI_HTYPE_DESCRIBE, (dvoid *)&parmh, (ub4 *)0,
					OCI_ATTR_PARAM, errhp))
		return FAILURE;

	/* The type information of the object, in this case, OCI_PTYPE_TABLE,
	is obtained from the parameter descriptor returned by the OCIAttrGet(). */
	/* get the number of columns in the table */
	numcols = 0;
	if (OCIAttrGet((dvoid *)parmh, OCI_DTYPE_PARAM, (dvoid *)&numcols, (ub4 *)0,
		 OCI_ATTR_NUM_COLS, errhp))
		return FAILURE;

	/* get the handle to the column list of the table */
	if (OCIAttrGet((dvoid *)parmh, OCI_DTYPE_PARAM, (dvoid *)&collsthd, (ub4 *)0,
		 OCI_ATTR_LIST_COLUMNS, errhp)==OCI_NO_DATA)
	   return FAILURE;

	/* go through the column list and retrieve the data-type of each column,
	and then recursively describe column types. */

	for (i = 1; i <= numcols; i++)
	{
		/* get parameter for column i */
		if (OCIParamGet((dvoid *)collsthd, OCI_DTYPE_PARAM, errhp, (dvoid **)&colhd, (ub4)i))
			return FAILURE;

		/* for example, get datatype for ith column */
		coltyp = 0;
		if (OCIAttrGet((dvoid *)colhd, OCI_DTYPE_PARAM, (dvoid *)&coltyp, (ub4 *)0, OCI_ATTR_DATA_TYPE, errhp))
			return FAILURE;

		/* Retrieve the length semantics for the column */
		char_semantics = 0;
		OCIAttrGet((dvoid*) colhd, (ub4) OCI_DTYPE_PARAM, (dvoid*) &char_semantics,(ub4 *) 0, (ub4) OCI_ATTR_CHAR_USED, (OCIError *) errhp);

		col_width = 0;
		if (char_semantics)
			/* Retrieve the column width in characters */
			OCIAttrGet((dvoid*) colhd, (ub4) OCI_DTYPE_PARAM, (dvoid*) &col_width, (ub4 *) 0, (ub4) OCI_ATTR_CHAR_SIZE, (OCIError *) errhp);
		else
			/* Retrieve the column width in bytes */
			OCIAttrGet((dvoid*) colhd, (ub4) OCI_DTYPE_PARAM, (dvoid*) &col_width,(ub4 *) 0, (ub4) OCI_ATTR_DATA_SIZE, (OCIError *) errhp);
	}

	if (dschp)
		OCIHandleFree((dvoid *) dschp, OCI_HTYPE_DESCRIBE);

	return SUCCESS;
}

/*
 * checkerr0: This function prints a detail error report.
 *			  Used to "warp" invocation of OCI calls.
 * Parameters:
 *	handle (IN)	- can be either an environment handle or an error handle.
 *				  for OCI calls that take in an OCIError Handle:
 *				  pass in an OCIError Handle
 *
 *				  for OCI calls that don't take an OCIError Handle,
 *                pass in an OCIEnv Handle
 *
 * htype (IN)   - type of handle: OCI_HTYPE_ENV or OCI_HTYPE_ERROR
 *
 * status (IN)  - the status code returned from the OCI call
 *
 * Notes:
 *				  Note that this "exits" on the first
 *                OCI_ERROR/OCI_INVALID_HANDLE.
 *				  CUSTOMIZE ACCORDING TO YOUR ERROR HANDLING REQUIREMNTS
 */
void checkerr0(intf_ret *r, ub4 htype, sword status, const char * function_name, int line_no)
{
    /* a buffer to hold the error message */
    r->gerrcode = 0;
    r->fn_ret = FAILURE;

    switch (status) {
    case OCI_SUCCESS:
		r->fn_ret = SUCCESS;
		//SPRINT(gerrbuf, sizeof(gerrbuf), "[%s:%d] Ok - OCI_SUCCESS\n", function_name, line_no);
        break;
    case OCI_SUCCESS_WITH_INFO:
		r->fn_ret = SUCCESS;
		SPRINT(r->gerrbuf, sizeof(r->gerrbuf), "[%s:%d] Error - OCI_SUCCESS_WITH_INFO\n", function_name, line_no);
        break;
    case OCI_NEED_DATA:
		SPRINT(r->gerrbuf, sizeof(r->gerrbuf), "[%s:%d] Error - OCI_NEED_DATA\n", function_name, line_no);
        break;
    case OCI_NO_DATA:
		r->fn_ret = SUCCESS;
		SPRINT(r->gerrbuf, sizeof(r->gerrbuf), "[%s:%d] Error - OCI_NO_DATA\n", function_name, line_no);
        break;
    case OCI_ERROR:
		SPRINT(r->gerrbuf, sizeof(r->gerrbuf), "[%s:%d] Error - OCI_ERROR\n", function_name, line_no);
        if (r->handle) {
            OCIErrorGet(r->handle, 1, (text *) NULL, &(r->gerrcode),
                               (OraText*)(r->gerrbuf), (ub4)sizeof(r->gerrbuf), htype);
			r->fn_ret = CONTINUE_WITH_ERROR;
        } else {
			SPRINT(r->gerrbuf, sizeof(r->gerrbuf), "[%s:%d] NULL Handle\n", function_name, line_no);
			SPRINT(r->gerrbuf, sizeof(r->gerrbuf), "[%s:%d] Unable to extract detailed diagnostic information\n", function_name, line_no);
	        r->fn_ret = FAILURE;
        }
        break;
    case OCI_INVALID_HANDLE:
		SPRINT(r->gerrbuf, sizeof(r->gerrbuf), "[%s:%d] Error - OCI_INVALID_HANDLE\n", function_name, line_no);
        break;
    case OCI_STILL_EXECUTING:
		r->fn_ret = CONTINUE_WITH_ERROR;
		SPRINT(r->gerrbuf, sizeof(r->gerrbuf), "[%s:%d] Error - OCI_STILL_EXECUTING\n", function_name, line_no);
        break;
    case OCI_CONTINUE:
		SPRINT(r->gerrbuf, sizeof(r->gerrbuf), "[%s:%d] Error - OCI_CONTINUE\n", function_name, line_no);
        break;
    default:
		SPRINT(r->gerrbuf, sizeof(r->gerrbuf), "[%s:%d] Unknown - %d\n", function_name, line_no, status);
        break;
    }
}
