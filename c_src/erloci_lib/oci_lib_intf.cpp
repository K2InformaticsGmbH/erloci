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
#include "oci_lib_intf.h"

#ifdef __WIN32__
#define	SPRINT sprintf_s
#else
#define	SPRINT snprintf
#endif

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

#if 0
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
	if (OCIDescribeAny(svchp, (OCIError*)errhp, (dvoid *)objptr, objp_len, OCI_OTYPE_NAME, 0,
		 OCI_PTYPE_TABLE, dschp))
	   return FAILURE;

	/* get the parameter handle */
	if (OCIAttrGet((dvoid *)dschp, OCI_HTYPE_DESCRIBE, (dvoid *)&parmh, (ub4 *)0,
					OCI_ATTR_PARAM, (OCIError*)errhp))
		return FAILURE;

	/* The type information of the object, in this case, OCI_PTYPE_TABLE,
	is obtained from the parameter descriptor returned by the OCIAttrGet(). */
	/* get the number of columns in the table */
	numcols = 0;
	if (OCIAttrGet((dvoid *)parmh, OCI_DTYPE_PARAM, (dvoid *)&numcols, (ub4 *)0,
		 OCI_ATTR_NUM_COLS, (OCIError*)errhp))
		return FAILURE;

	/* get the handle to the column list of the table */
	if (OCIAttrGet((dvoid *)parmh, OCI_DTYPE_PARAM, (dvoid *)&collsthd, (ub4 *)0,
		 OCI_ATTR_LIST_COLUMNS, (OCIError*)errhp)==OCI_NO_DATA)
	   return FAILURE;

	/* go through the column list and retrieve the data-type of each column,
	and then recursively describe column types. */

	for (i = 1; i <= numcols; i++)
	{
		/* get parameter for column i */
		if (OCIParamGet((dvoid *)collsthd, OCI_DTYPE_PARAM, (OCIError*)errhp, (dvoid **)&colhd, (ub4)i))
			return FAILURE;

		/* for example, get datatype for ith column */
		coltyp = 0;
		if (OCIAttrGet((dvoid *)colhd, OCI_DTYPE_PARAM, (dvoid *)&coltyp, (ub4 *)0, OCI_ATTR_DATA_TYPE, (OCIError*)errhp))
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
#endif