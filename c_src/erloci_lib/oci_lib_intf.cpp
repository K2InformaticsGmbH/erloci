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

typedef struct column_info {
    ub2  dtype;
    ub4	 dlen;
} column_info;

/*
 * Global variables
 */
static OCIEnv		*envhp		= NULL;
static OCIError		*errhp		= NULL;
static OCISPool		*spoolhp	= NULL;

static char			*poolName	= NULL;
static ub4			poolNameLen	= 0;
static ub4			stmt_cachesize;
static column_info	*columns = NULL;
static ub4			num_cols = 0;

char				session_pool_name[2048];
char				gerrbuf[2048];
sb4					gerrcode = 0;

/* constants */
#define POOL_MIN	0
#define POOL_MAX	10
#define POOL_INCR	0
#define MAX_COLUMS	1000

/* Error checking functions and macros */
void checkerr0(void *, ub4, sword, const char * function_name, int line_no);
#define checkerr(errhp, status) checkerr0((errhp), OCI_HTYPE_ERROR, (status), __FUNCTION__, __LINE__)
#define checkenv(envhp, status) checkerr0((envhp), OCI_HTYPE_ENV, (status), __FUNCTION__, __LINE__)

static INTF_RET function_success = SUCCESS;

void get_last_error(char *buf, int & len)
{
    len = (int)strlen(gerrbuf);
    if(buf != NULL)
#ifdef __WIN32__
        strncpy_s(buf, len+1, gerrbuf, len);
#else
		strncpy(buf, gerrbuf, len);
#endif
}

void oci_init(void)
{
	function_success = SUCCESS;
    checkenv(envhp, OCIEnvCreate(&envhp,				/* returned env handle */
                                 OCI_DEFAULT,			/* initilization modes */
                                 NULL, NULL, NULL, NULL,/* callbacks, context */
                                 (size_t) 0,			/* optional extra memory size: optional */
                                 (void**) NULL));		/* returned extra memeory */

    checkenv(envhp, OCIHandleAlloc(envhp,				/* environment handle */
                                   (void **) &errhp,	/* returned err handle */
                                   OCI_HTYPE_ERROR,		/* typ of handle to allocate */
                                   (size_t) 0,			/* optional extra memory size */
                                   (void **) NULL));	/* returned extra memeory */

    if(function_success != SUCCESS)
        exit(1);
}

void oci_cleanup(void)
{
    if (columns != NULL)
        free(columns);
    oci_free_session_pool();
    if (errhp != NULL) {
        OCIHandleFree(errhp, OCI_HTYPE_ERROR);
        errhp = NULL;
    }
}

bool oci_free_session_pool(void)
{
    function_success = SUCCESS;

    if (spoolhp != NULL) {
        checkerr(errhp, OCISessionPoolDestroy(spoolhp, errhp, OCI_SPD_FORCE));
        if(function_success != SUCCESS)return false;

        if(OCI_SUCCESS != OCIHandleFree(spoolhp, OCI_HTYPE_SPOOL))
            return false;
        spoolhp = NULL;
    }

    return true;
}

bool oci_create_tns_seesion_pool(const unsigned char * connect_str, const int connect_str_len,
                                 const unsigned char * user_name, const int user_name_len,
                                 const unsigned char * password, const int password_len,
                                 const unsigned char * options, const int options_len)
{
    function_success = SUCCESS;
    poolName = NULL;
    poolNameLen = 0;

    oci_free_session_pool();

    /* allocate session pool handle
     * note: for OCIHandleAlloc() we check error on environment handle
     */
    checkenv(envhp, OCIHandleAlloc(envhp, (void **)&spoolhp,
                                   OCI_HTYPE_SPOOL, (size_t) 0, (void **) NULL));

    /* set the statement cache size for all sessions in the pool
     * note: this can also be set per session after obtaining the session from the pool
     */
    checkerr(errhp, OCIAttrSet(spoolhp, OCI_HTYPE_SPOOL,
                               &stmt_cachesize, 0, OCI_ATTR_SPOOL_STMTCACHESIZE, errhp));

    checkerr(errhp, OCISessionPoolCreate(envhp, errhp,
                                         spoolhp,
                                         (OraText **) &poolName, &poolNameLen,
                                         (OraText *) connect_str, connect_str_len,
                                         POOL_MIN, POOL_MAX, POOL_INCR,
                                         (OraText*)user_name, user_name_len,			/* homo pool user specified */
                                         (OraText*)password, password_len,			/* homo pool password specified */
                                         OCI_SPC_STMTCACHE));	/* modes */
    if(function_success != SUCCESS)return false;

	SPRINT(session_pool_name, sizeof(session_pool_name), "%.*s", poolNameLen, (char *)poolName);

    ub1 spoolMode = OCI_SPOOL_ATTRVAL_NOWAIT;
    checkerr(errhp, OCIAttrSet(spoolhp, OCI_HTYPE_SPOOL,
                               (void*)&spoolMode, sizeof(ub1),
                               OCI_ATTR_SPOOL_GETMODE, errhp));
    if(function_success != SUCCESS)return false;

    return true;
}

void * oci_get_session_from_pool()
{
    function_success = SUCCESS;

    /* get the database connection */
    OCISvcCtx	*svchp = NULL;
    checkerr(errhp, OCISessionGet(envhp, errhp,
                                  &svchp,		/* returned database connection */
                                  NULL,		/* initialized authentication handle */
                                  /* connect string */
                                  (OraText *) poolName, poolNameLen,
                                  /* session tagging parameters: optional */
                                  NULL, 0, NULL, NULL, NULL,
                                  OCI_SESSGET_SPOOL));/* modes */

    if(function_success != SUCCESS)
        return NULL;

	//REMOTE_LOG("oci connection handle %p\n", svchp);
    return svchp;
}

bool oci_return_connection_to_pool(void * connection_handle)
{
    function_success = SUCCESS;

    OCISvcCtx *svchp = (OCISvcCtx *)connection_handle;
    if (svchp != NULL)
        checkerr(errhp, OCISessionRelease(svchp, errhp, NULL, 0, OCI_DEFAULT));

    if(function_success != SUCCESS)
        return false;

    return true;
}

#define MAX_CONNECT_STR_LEN 1024
bool oci_create_seesion_pool(const unsigned char * host_str, const int host_len, const unsigned int port,
                             const unsigned char * srv_str, const int srv_len,
                             const unsigned char * user_name, const int user_name_len,
                             const unsigned char * password, const int password_len,
                             const unsigned char * options, const int options_len)
{
    char connect_str[MAX_CONNECT_STR_LEN];
	SPRINT(connect_str, MAX_CONNECT_STR_LEN,
              "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp) (HOST=%.*s) (PORT=%d))(CONNECT_DATA=(SERVICE_NAME=%.*s)))",
              host_len, host_str,
              port,
              srv_len, srv_str);
    return oci_create_tns_seesion_pool((unsigned char *)connect_str, (int)strlen(connect_str),
                                       user_name, user_name_len,
                                       password, password_len,
                                       options, options_len);
}

INTF_RET oci_exec_sql(const void *conn_handle, void ** stmt_handle, const unsigned char * query_str, int query_str_len
					  , inp_t *params_head, void * column_list
					  , void (*coldef_append)(const char *, const char *, const unsigned int, void *))
{
    function_success = SUCCESS;

    OCISvcCtx *svchp = (OCISvcCtx *)conn_handle;
    OCIStmt *stmthp	= NULL;
    ub4 itrs = 0;
    ub4 stmt_typ = OCI_STMT_SELECT;
    ub2 type = SQLT_INT;
    int idx = 1;

    /* Get a prepared statement handle */
    checkerr(errhp, OCIStmtPrepare2(svchp,
                                    &stmthp,					/* returned statement handle */
                                    errhp,						/* error handle */
                                    (OraText *) query_str,		/* the statement text */
                                    query_str_len,				/* length of the text */
                                    NULL, 0,					/* tagging parameters: optional */
                                    OCI_NTV_SYNTAX, OCI_DEFAULT));
	*stmt_handle = stmthp;
    if(function_success != SUCCESS) goto error_exit;

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
        checkerr(errhp, OCIBindByPos(stmthp, (OCIBind **)&(param->bndp), errhp, idx,
                                     (dvoid *) param->valuep, (sword) param->value_sz, type,
                                     (dvoid *) 0, (ub2 *) 0, (ub2 *) 0, (ub4) 0, (ub4 *) 0, OCI_DEFAULT));
        if(function_success != SUCCESS) goto error_exit;
        idx++;
    }

    checkerr(errhp, OCIAttrGet((dvoid*) stmthp, (ub4) OCI_HTYPE_STMT,
                               (dvoid*) &stmt_typ, (ub4 *)NULL, (ub4)OCI_ATTR_STMT_TYPE, errhp));
    if(function_success != SUCCESS) goto error_exit;
    if(stmt_typ != OCI_STMT_SELECT)
        itrs = 1;

    /* execute the statement and commit */
    checkerr(errhp, OCIStmtExecute(svchp, stmthp, errhp, itrs, 0,
                                   (OCISnapshot *)NULL, (OCISnapshot *)NULL,
                                   OCI_COMMIT_ON_SUCCESS));
    if(function_success != SUCCESS) goto error_exit;

    if(stmt_typ == OCI_STMT_SELECT) {
        OCIParam	*mypard;
        num_cols	= 1;
        sb4         parm_status;

        /* Request a parameter descriptor for position 1 in the select-list */
        parm_status = OCIParamGet(stmthp, OCI_HTYPE_STMT, errhp, (dvoid **)&mypard,
                                  (ub4) num_cols);
        checkerr(errhp, parm_status);
        if(function_success != SUCCESS) goto error_exit;

        /* Loop only if a descriptor was successfully retrieved for
         * current position, starting at 1
         */
        text *col_name;
        ub4 len = 0;
        char * data_type = NULL;
        if (columns != NULL)
            free(columns);
        columns = NULL;
        while (parm_status == OCI_SUCCESS) {
            columns = (column_info *)realloc(columns, num_cols * sizeof(column_info));
            column_info * cur_clm = &(columns[num_cols-1]);

			/* Retrieve the data size attribute */
            len = 0;
            checkerr(errhp, OCIAttrGet((dvoid*) mypard, (ub4) OCI_DTYPE_PARAM,
                                       (dvoid*) &len, (ub4 *)0, (ub4)OCI_ATTR_DATA_SIZE,
                                       errhp));
            if(function_success != SUCCESS) goto error_exit;
            cur_clm->dlen = len;

            /* Retrieve the data type attribute */
            len = 0;
            checkerr(errhp, OCIAttrGet((dvoid*) mypard, (ub4) OCI_DTYPE_PARAM,
                                       (dvoid*) &len, (ub4 *)0, (ub4)OCI_ATTR_DATA_TYPE,
                                       errhp));
            if(function_success != SUCCESS) goto error_exit;
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
            checkerr(errhp, OCIAttrGet((dvoid*) mypard, (ub4) OCI_DTYPE_PARAM,
                                       (dvoid**) &col_name, (ub4 *) &len, (ub4) OCI_ATTR_NAME,
                                       errhp));
            if(function_success != SUCCESS) goto error_exit;
            char * column_name = new char[len+1];
			SPRINT(column_name, len+1, "%.*s", len, col_name);
            (*coldef_append)(column_name, data_type, cur_clm->dlen, column_list);
            delete column_name;
            col_name = NULL;

            /* Increment counter and get next descriptor, if there is one */
            if(OCI_SUCCESS != OCIDescriptorFree(mypard, OCI_DTYPE_PARAM)) {
                function_success = FAILURE;
                goto error_exit;
            }
            num_cols++;
            parm_status = OCIParamGet(stmthp, OCI_HTYPE_STMT, errhp, (dvoid **)&mypard,
                                      (ub4) num_cols);
        }
        --num_cols;

        if(function_success != SUCCESS)
            goto error_exit;
        REMOTE_LOG("Port: Returning Column(s)\n");
    } else {
        if(stmthp != NULL) {
            checkerr(errhp, OCIStmtRelease(stmthp, errhp, NULL, 0, OCI_DEFAULT));
            if(function_success != SUCCESS) goto error_exit;
        }
        REMOTE_LOG("Port: Executed non-select statement!\n");
    }

	REMOTE_LOG("Executing \"%.*s;\"\n", query_str_len, query_str);

error_exit:
    return function_success;
}

ROW_FETCH oci_produce_rows(void * stmt_handle
						   , void * row_list
						   , void (*string_append)(const char * string, void * list)
						   , void (*list_append)(const void * sub_list, void * list)
						   , unsigned int (*sizeof_resp)(void * resp)
                           , int maxrowcount)
{
    function_success = SUCCESS;
    OCIStmt *stmthp	= (OCIStmt *)stmt_handle;

    if (columns == NULL || stmthp == NULL)
        return ERROR;

    sword res = OCI_NO_DATA;

    OCIDefine **defnhp = (OCIDefine **)malloc(num_cols * sizeof(OCIDefine *));
    void ** data_row = NULL;
    data_row = (void **) calloc(num_cols, sizeof(void *));

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
    for (i = 0; i < num_cols; ++i)
        switch (columns[i].dtype) {
		case SQLT_DAT: 
					   {
            data_row[i] = (text *) malloc((columns[i].dlen + 1) * sizeof(text));
            checkerr(errhp, OCIDefineByPos(stmthp, &(defnhp[i]), errhp, i+1, (dvoid *) (data_row[i]),
                                           (sword) columns[i].dlen + 1, SQLT_DAT, (dvoid *) 0, (ub2 *)0,
                                           (ub2 *)0, OCI_DEFAULT));
            if(function_success != SUCCESS) return ERROR;
        }
        break;//*/
        case SQLT_NUM:
        case SQLT_CHR: {
            data_row[i] = (text *) malloc((columns[i].dlen + 1) * sizeof(text));
            checkerr(errhp, OCIDefineByPos(stmthp, &(defnhp[i]), errhp, i+1, (dvoid *) (data_row[i]),
                                           (sword) columns[i].dlen + 1, SQLT_STR, (dvoid *) 0, (ub2 *)0,
                                           (ub2 *)0, OCI_DEFAULT));
            if(function_success != SUCCESS) return ERROR;
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
        res = OCIStmtFetch(stmthp, errhp, 1, 0, 0);
        row = NULL;

		if (res != OCI_NO_DATA) {
			for (i = 0; i < num_cols; ++i)
					switch (columns[i].dtype) {
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

    /* Release the bound variables memeory */
    for (i = 0; i < num_cols; ++i)
        if(data_row[i] != NULL)
            free(data_row[i]);
    free(data_row);

    /* cleanup only if data fetch is finished */
    if (res == OCI_NO_DATA)
        checkerr(errhp, OCIStmtRelease(stmthp, errhp, (OraText *) NULL, 0, OCI_DEFAULT));

    free(defnhp);

    if(function_success != SUCCESS)
        return ERROR;

    //REMOTE_LOG("Port: Returning Rows...\n");

    return (res != OCI_NO_DATA ? MORE : DONE);
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
void checkerr0(void *handle, ub4 htype, sword status, const char * function_name, int line_no)
{
    /* a buffer to hold the error message */
    text errbuf[2048];
    gerrcode = 0;
    function_success = SUCCESS;

    switch (status) {
    case OCI_SUCCESS:
#ifdef TRACE
		SPRINT(gerrbuf, sizeof(gerrbuf), "[%s:%d] Ok - OCI_SUCCESS\n", function_name, line_no);
#endif
        break;
    case OCI_SUCCESS_WITH_INFO:
		SPRINT(gerrbuf, sizeof(gerrbuf), "[%s:%d] Error - OCI_SUCCESS_WITH_INFO\n", function_name, line_no);
        break;
    case OCI_NEED_DATA:
		SPRINT(gerrbuf, sizeof(gerrbuf), "[%s:%d] Error - OCI_NEED_DATA\n", function_name, line_no);
        break;
    case OCI_NO_DATA:
		SPRINT(gerrbuf, sizeof(gerrbuf), "[%s:%d] Error - OCI_NO_DATA\n", function_name, line_no);
        break;
    case OCI_ERROR:
		SPRINT(gerrbuf, sizeof(gerrbuf), "[%s:%d] Error - OCI_ERROR\n", function_name, line_no);
        if (handle) {
            OCIErrorGet(handle, 1, (text *) NULL, &gerrcode,
                               errbuf, (ub4)sizeof(errbuf), htype);
			SPRINT(gerrbuf, sizeof(gerrbuf), " - %.*s\n", sizeof(errbuf), errbuf);
	        function_success = CONTINUE_WITH_ERROR;
        } else {
		SPRINT(gerrbuf, sizeof(gerrbuf), "[%s:%d] NULL Handle\n", function_name, line_no);
		SPRINT(gerrbuf, sizeof(gerrbuf), "[%s:%d] Unable to extract detailed diagnostic information\n", function_name, line_no);
	        function_success = FAILURE;
        }
        break;
    case OCI_INVALID_HANDLE:
		SPRINT(gerrbuf, sizeof(gerrbuf), "[%s:%d] Error - OCI_INVALID_HANDLE\n", function_name, line_no);
        break;
    case OCI_STILL_EXECUTING:
		SPRINT(gerrbuf, sizeof(gerrbuf), "[%s:%d] Error - OCI_STILL_EXECUTING\n", function_name, line_no);
        break;
    case OCI_CONTINUE:
		SPRINT(gerrbuf, sizeof(gerrbuf), "[%s:%d] Error - OCI_CONTINUE\n", function_name, line_no);
        break;
    default:
		SPRINT(gerrbuf, sizeof(gerrbuf), "[%s:%d] Unknown - %d\n", function_name, line_no, status);
        break;
    }
}
