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
#include "lib_interface.h"

unsigned long max_term_byte_size = MAX_RESP_SIZE;

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