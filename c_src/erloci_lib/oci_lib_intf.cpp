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

#include "oci.h"
#include "oci_lib_intf.h"

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

    if((r = oci_free_session_pool()).fn_ret != SUCCESS)
		return r;

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
    if(r.fn_ret != SUCCESS)
		return r;

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
        if(r.fn_ret != SUCCESS)return r;

        if(OCI_SUCCESS != OCIHandleFree(spoolhp, OCI_HTYPE_SPOOL))
            return r;
        spoolhp = NULL;
    }

    return r;
}

intf_ret oci_get_session_from_pool(void **conn_handle)
{
	OCISvcCtx *svchp;
	intf_ret r;

	r.fn_ret = SUCCESS;

    /* get the database connection */
	r.handle = errhp;
    checkerr(&r, OCISessionGet(envhp, errhp,
                               &svchp,								/* returned database connection */
                               NULL,								/* initialized authentication handle */                               
                               (OraText *) poolName, poolNameLen,	/* connect string */
                               NULL, 0, NULL, NULL, NULL,			/* session tagging parameters: optional */
                               OCI_SESSGET_SPOOL));					/* modes */

    if(r.fn_ret != SUCCESS)
        return r;

	//REMOTE_LOG("oci connection handle %p\n", svchp);
	*conn_handle = svchp;
    return r;
}

intf_ret oci_return_connection_to_pool(void * svchp)
{
	intf_ret r;

	r.fn_ret = SUCCESS;
	r.handle = errhp;

    if (svchp != NULL)
        checkerr(&r, OCISessionRelease((OCISvcCtx*)svchp, errhp, NULL, 0, OCI_DEFAULT));

    if(r.fn_ret != SUCCESS)
        return r;

    return r;
}

intf_ret oci_exec_sql(const void *svchp, void ** stmt_handle, const unsigned char * query_str, int query_str_len
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

	// private error structure for each statement
	OCIError * ehp = NULL;
	r.handle = envhp;
	checkenv(&r, OCIHandleAlloc(envhp,				/* environment handle */
                                (void **) &ehp,		/* returned err handle */
                                OCI_HTYPE_ERROR,	/* typ of handle to allocate */
                                (size_t) 0,			/* optional extra memory size */
                                (void **) NULL));	/* returned extra memeory */
    if(r.fn_ret != SUCCESS)
        return r;

	// allocate the statement handle
	checkenv(&r, OCIHandleAlloc(envhp,					/* environment handle */
                                (void **) &stmthp,		/* returned statement handle */
                                OCI_HTYPE_STMT,			/* typ of handle to allocate */
                                sizeof(stmt_ctx),		/* optional extra memory size */
                                (void **) &smtctx));	/* returned extra memeory */
    if(r.fn_ret != SUCCESS)
        return r;

	// from this point on regular errhp will do
	r.handle = ehp;

    /* Get a prepared statement handle */
    checkerr(&r, OCIStmtPrepare2((OCISvcCtx *)svchp,
                                 &stmthp,					/* returned statement handle */
                                 ehp,						/* error handle */
                                 (OraText *) query_str,		/* the statement text */
                                 query_str_len,				/* length of the text */
                                 NULL, 0,					/* tagging parameters: optional */
                                 OCI_NTV_SYNTAX, OCI_DEFAULT));

	smtctx->stmthp = stmthp;
	*stmt_handle = smtctx;

    if(r.fn_ret != SUCCESS) goto error_exit;

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
        checkerr(&r, OCIBindByPos(stmthp, (OCIBind **)&(param->bndp), ehp, idx,
                                  (dvoid *) param->valuep, (sword) param->value_sz, type,
                                  (dvoid *) 0, (ub2 *) 0, (ub2 *) 0, (ub4) 0, (ub4 *) 0, OCI_DEFAULT));
        if(r.fn_ret != SUCCESS) goto error_exit;
        idx++;
    }

    checkerr(&r, OCIAttrGet((dvoid*) stmthp, (ub4) OCI_HTYPE_STMT,
                            (dvoid*) &stmt_typ, (ub4 *)NULL, (ub4)OCI_ATTR_STMT_TYPE, ehp));
    if(r.fn_ret != SUCCESS) goto error_exit;
    if(stmt_typ != OCI_STMT_SELECT)
        itrs = 1;

    /* execute the statement and commit */
    checkerr(&r, OCIStmtExecute((OCISvcCtx *)svchp, stmthp, ehp, itrs, 0,
                                (OCISnapshot *)NULL, (OCISnapshot *)NULL,
                                OCI_COMMIT_ON_SUCCESS));
    if(r.fn_ret != SUCCESS) goto error_exit;

    if(stmt_typ == OCI_STMT_SELECT) {
        OCIParam	*mypard;
        smtctx->num_cols	= 1;
        sb4         parm_status;

        /* Request a parameter descriptor for position 1 in the select-list */
        parm_status = OCIParamGet(stmthp, OCI_HTYPE_STMT, ehp, (dvoid **)&mypard,
                                  (ub4) smtctx->num_cols);
        checkerr(&r, parm_status);
        if(r.fn_ret != SUCCESS) goto error_exit;

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
                                    ehp));
            if(r.fn_ret != SUCCESS) goto error_exit;
            cur_clm->dlen = len;

            /* Retrieve the data type attribute */
            len = 0;
            checkerr(&r, OCIAttrGet((dvoid*) mypard, (ub4) OCI_DTYPE_PARAM,
                                    (dvoid*) &len, (ub4 *)0, (ub4)OCI_ATTR_DATA_TYPE,
                                    ehp));
            if(r.fn_ret != SUCCESS) goto error_exit;
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
            default:
                data_type = (char*)"undefined";
                break;
            }

            /* Retrieve the column name attribute */
            len = 0;
            checkerr(&r, OCIAttrGet((dvoid*) mypard, (ub4) OCI_DTYPE_PARAM,
                                    (dvoid**) &col_name, (ub4 *) &len, (ub4) OCI_ATTR_NAME,
                                    ehp));
            if(r.fn_ret != SUCCESS) goto error_exit;
            char * column_name = new char[len+1];
			SPRINT(column_name, len+1, "%.*s", len, col_name);
            (*coldef_append)(column_name, data_type, cur_clm->dlen, column_list);
            delete column_name;
            col_name = NULL;

            /* Increment counter and get next descriptor, if there is one */
            if(OCI_SUCCESS != OCIDescriptorFree(mypard, OCI_DTYPE_PARAM)) {
                r.fn_ret = FAILURE;
                goto error_exit;
            }
            smtctx->num_cols++;
            parm_status = OCIParamGet(stmthp, OCI_HTYPE_STMT, ehp, (dvoid **)&mypard,
                                      (ub4) smtctx->num_cols);
        }
        --smtctx->num_cols;

        if(r.fn_ret != SUCCESS)
            goto error_exit;
        REMOTE_LOG("Port: Returning Column(s)\n");
    } else {
        if(stmthp != NULL) {
            checkerr(&r, OCIStmtRelease(stmthp, ehp, NULL, 0, OCI_DEFAULT));
            if(r.fn_ret != SUCCESS) goto error_exit;
        }
        REMOTE_LOG("Port: Executed non-select statement!\n");
    }

	REMOTE_LOG("Executing \"%.*s;\"\n", query_str_len, query_str);

error_exit:
    return r;
}

intf_ret oci_produce_rows(void * stmt_handle
						   , void * row_list
						   , void (*string_append)(const char * string, void * list)
						   , void (*list_append)(const void * sub_list, void * list)
						   , unsigned int (*sizeof_resp)(void * resp)
                           , int maxrowcount)
{
	intf_ret r;

	// private error structure for each statement
	OCIError * ehp = NULL;
	r.handle = envhp;
	checkenv(&r, OCIHandleAlloc(envhp,				/* environment handle */
                                (void **) &ehp,		/* returned err handle */
                                OCI_HTYPE_ERROR,	/* typ of handle to allocate */
                                (size_t) 0,			/* optional extra memory size */
                                (void **) NULL));	/* returned extra memeory */
    if(r.fn_ret != SUCCESS)
        return r;

	// from this point on regular ehp will do
	r.handle = ehp;

	r.fn_ret = FAILURE;

	stmt_ctx *smtctx = (stmt_ctx *)stmt_handle;
	OCIStmt *stmthp	= smtctx->stmthp;

    if (smtctx->columns == NULL || stmthp == NULL)
        return r;

	r.fn_ret = SUCCESS;
    sword res = OCI_NO_DATA;

    OCIDefine **defnhp = (OCIDefine **)malloc(smtctx->num_cols * sizeof(OCIDefine *));
    void ** data_row = NULL;
    data_row = (void **) calloc(smtctx->num_cols, sizeof(void *));

    // overdrive preventation
    if(maxrowcount > 100)
        maxrowcount = 100;

    /*
     * Fetch the data
     */
    unsigned int num_rows = 0;
    ub4 i = 0;
    void * row = NULL;

    /* Bind appropriate variables for data based on the column type */
    for (i = 0; i < smtctx->num_cols; ++i)
        switch (smtctx->columns[i].dtype) {
		case SQLT_DAT: 
					   {
            data_row[i] = (text *) malloc((smtctx->columns[i].dlen + 1) * sizeof(text));
            checkerr(&r, OCIDefineByPos(stmthp, &(defnhp[i]), ehp, i+1, (dvoid *) (data_row[i]),
										(sword) smtctx->columns[i].dlen + 1, SQLT_DAT, &(smtctx->columns[i].indp), (ub2 *)0,
                                        (ub2 *)0, OCI_DEFAULT));
            if(r.fn_ret != SUCCESS) return r;
        }
        break;//*/
        case SQLT_NUM:
        case SQLT_CHR: {
            data_row[i] = (text *) malloc((smtctx->columns[i].dlen + 1) * sizeof(text));
            checkerr(&r, OCIDefineByPos(stmthp, &(defnhp[i]), ehp, i+1, (dvoid *) (data_row[i]),
                                        (sword) smtctx->columns[i].dlen + 1, SQLT_STR, &(smtctx->columns[i].indp), (ub2 *)0,
                                        (ub2 *)0, OCI_DEFAULT));
            if(r.fn_ret != SUCCESS) return r;
        }
        break;
        default:
            break;
        }

    /* Fetch data by row */
	//REMOTE_LOG("OCI: Fetching rows\n");

	unsigned long int total_est_row_size = 0;
    do {
        ++num_rows;
		//if(num_rows % 100 == 0) REMOTE_LOG("OCI: Fetched %lu rows of %d bytes\n", num_rows, total_est_row_size);
        res = OCIStmtFetch(stmthp, ehp, 1, 0, 0);
		checkerr(&r, res);
	    if(r.fn_ret != SUCCESS)
		    goto error_return;

        row = NULL;

		if (res != OCI_NO_DATA) {
			for (i = 0; i < smtctx->num_cols; ++i)
					switch (smtctx->columns[i].dtype) {
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
						(*string_append)(date_buf, &row); }
						break;
					case SQLT_NUM:
					case SQLT_CHR:
						(*string_append)((char*)data_row[i], &row);
						break;
					}
			total_est_row_size += (*sizeof_resp)(&row);
			(*list_append)(row, row_list);
		}
    } while (res != OCI_NO_DATA
			&& num_rows < maxrowcount
			&& total_est_row_size < MAX_RESP_SIZE);
	//REMOTE_LOG("OCI: Row fetch and erlang term building complete with %ul rows of %d bytes\n", num_rows, total_est_row_size); //, GetTickCount() - startTime

    /* cleanup only if data fetch is finished */
    if (res == OCI_NO_DATA)
        checkerr(&r, OCIStmtRelease(stmthp, ehp, (OraText *) NULL, 0, OCI_DEFAULT));

    free(defnhp);

    if(r.fn_ret != SUCCESS)
        return r;

    //REMOTE_LOG("Port: Returning Rows...\n");
	if(res != OCI_NO_DATA) r.fn_ret = MORE; else r.fn_ret = DONE;

error_return:
    /* Release the bound variables memeory */
    for (i = 0; i < smtctx->num_cols; ++i)
        if(data_row[i] != NULL)
            free(data_row[i]);
    free(data_row);

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
