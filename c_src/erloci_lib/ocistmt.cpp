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
#include "ocistmt.h"
#include "ocisession.h"

#include <cstring>

#include <oci.h>

#ifndef __WIN32__
#include <stdlib.h>
#endif

struct column {
    ub2  dtype;
    ub4	 dlen;
    ub2	 dprec;
    sb1	 dscale;
	sb2  indp;
	ub4  rtype;
	void * row_valp;
};

ocistmt::ocistmt(void *ocisess, OraText *stmt, ub4 stmt_len)
{
	intf_ret r;
	
	OCIEnv *envhp = (OCIEnv *)ocisession::getenv();

	_svchp = ((ocisession *)ocisess)->getsession();
	_iters = 1;
	_ocisess = ocisess,
		
	_stmtstr = string(reinterpret_cast<char*>(stmt), stmt_len);

	// allocate error handle
	r.handle = envhp;
	checkenv(&r, OCIHandleAlloc(envhp,	/* environment handle */
                            (void **) &_errhp,	/* returned err handle */
                            OCI_HTYPE_ERROR,	/* typ of handle to allocate */
                            (size_t) 0,			/* optional extra memory size */
                            (void **) NULL));	/* returned extra memeory */
	if(r.fn_ret != SUCCESS) {
   		REMOTE_LOG("failed OCIHandleAlloc %s (%s)\n", r.gerrbuf, _stmtstr.c_str());
        throw r;
	}

	// allocate the statement handle
	checkenv(&r, OCIHandleAlloc(envhp,					/* environment handle */
                                (void **) &_stmthp,		/* returned statement handle */
                                OCI_HTYPE_STMT,			/* typ of handle to allocate */
                                (size_t) 0,				/* optional extra memory size */
								(void **) NULL));		/* returned extra memeory */
	if(r.fn_ret != SUCCESS) {
   		REMOTE_LOG("failed OCIHandleAlloc %s (%s)\n", r.gerrbuf, _stmtstr.c_str());
        throw r;
	}

	/* Get a prepared statement handle */
	r.handle = _errhp;
    checkerr(&r, OCIStmtPrepare2((OCISvcCtx*)_svchp,
                                 (OCIStmt**)&_stmthp,	/* returned statement handle */
                                 (OCIError*)_errhp,		/* error handle */
                                 (OraText *) stmt,		/* the statement text */
                                 stmt_len,				/* length of the text */
                                 NULL, 0,				/* tagging parameters: optional */
                                 OCI_NTV_SYNTAX, OCI_DEFAULT));

	if(r.fn_ret != SUCCESS) {
   		REMOTE_LOG("failed OCIStmtPrepare2 %s (%s)\n", r.gerrbuf, _stmtstr.c_str());
        throw r;
	}

	_stmt_typ = 0;
    checkerr(&r, OCIAttrGet((dvoid*) _stmthp, (ub4) OCI_HTYPE_STMT,
                            (dvoid*) &_stmt_typ, (ub4 *)NULL, (ub4)OCI_ATTR_STMT_TYPE, (OCIError*)_errhp));
	if(r.fn_ret != SUCCESS) {
		REMOTE_LOG("failed OCIAttrGet error %s (%s)\n", r.gerrbuf, _stmtstr.c_str());
        throw r;
	}

	if(_stmt_typ == OCI_STMT_SELECT)
		_iters = 0;
}

/* Allocate and define the descriptor (storage) for the datatype */
#define OCIALLOC(__desctype, __dtypestr)																	\
{	cur_clm.row_valp = NULL;																				\
	cur_clm.rtype = __desctype;																				\
	r.handle = envhp;																						\
	checkerr(&r, OCIDescriptorAlloc(envhp,(dvoid **)&(cur_clm.row_valp),									\
									__desctype, 0, (dvoid **)0));											\
	if(r.fn_ret != SUCCESS) {																				\
		REMOTE_LOG("failed OCIDescriptorAlloc for %p column %d("__dtypestr")\n", _stmthp, num_cols);		\
		throw r;																							\
	}																										\
}
#define OCIDEF(__datatype, __dtypestr)																		\
{	r.handle = _errhp;																						\
	OCIDefine *__dfnp = NULL;																				\
    checkerr(&r, OCIDefineByPos((OCIStmt*)_stmthp, &__dfnp, (OCIError*)_errhp,								\
								num_cols, (dvoid *)(cur_clm.row_valp),										\
								(sword) cur_clm.dlen + 1, __datatype, &(cur_clm.indp), (ub2 *)0,			\
                                (ub2 *)0, OCI_DEFAULT));													\
	if(r.fn_ret != SUCCESS) {																				\
		REMOTE_LOG("failed OCIDefineByPos for %p column %d("__dtypestr")\n", _stmthp, num_cols);			\
		throw r;																							\
	}																										\
}

#define FETCH_ROWIDS

#ifdef FETCH_ROWIDS
//#define PRINT_ROWIDS
#endif

unsigned int ocistmt::execute(void * column_list,
					  void (*coldef_append)(const char *, size_t, const unsigned short, const unsigned int,
											const unsigned short, const signed char, void *),
					  void * rowid_list,
					  void (*string_append)(const char *, size_t, void *),
					  bool auto_commit)
{
	ub4 row_count = 0;
	intf_ret r;

	ocisession * ocisess = (ocisession *)_ocisess;
	OCIEnv *envhp = (OCIEnv *)ocisession::getenv();

	r.handle = _errhp;

	/* bind variables if any */
	size_t dat_len = 0;
	for(int i = 0; i < _argsin.size(); ++i) {
		for(int j = 0; j < _argsin[i].alen.size(); ++j) {
			unsigned short * alenarr = &_argsin[i].alen[0];
			void ** valueparr = &_argsin[i].valuep[0];
			switch(_argsin[i].dty) {
				case SQLT_BFLOAT:
				case SQLT_BDOUBLE:
				case SQLT_FLT:
					dat_len = sizeof(double);
					break;
				case SQLT_INT:
					dat_len = sizeof(int);
					break;
				case SQLT_STR:
				case SQLT_CHR:
				case SQLT_DAT:
					dat_len = _argsin[i].value_sz;
					break;
			}
			_argsin[i].datap = realloc(_argsin[i].datap, _argsin[i].datap_len + dat_len);
			memcpy((char*)_argsin[i].datap + _argsin[i].datap_len, valueparr[j], _argsin[i].alen[j]);
			_argsin[i].datap_len += (unsigned long)(dat_len);
		}
	}

	if(_argsin.size() > 0)
		_iters = _argsin[0].valuep.size();
	for(int i = 0; i < _argsin.size(); ++i) {
		checkerr(&r, OCIBindByName((OCIStmt*)_stmthp, (OCIBind**)(&_argsin[i].ocibind), (OCIError*)_errhp,
									(text*)(_argsin[i].name), -1,
									_argsin[i].datap, _argsin[i].value_sz,
									_argsin[i].dty,
									(void*)NULL, &_argsin[i].alen[0],
									(ub2*)NULL,0,
									(ub4*)NULL, OCI_DEFAULT));
		if(r.fn_ret != SUCCESS) {
			REMOTE_LOG("failed OCIBindByName error %s (%s)\n", r.gerrbuf, _stmtstr.c_str());
			ocisess->release_stmt(this);
			throw r;
		}
	}

	ub4 mode = (auto_commit ? OCI_COMMIT_ON_SUCCESS : OCI_DEFAULT);
#ifdef FETCH_ROWIDS
#ifdef PRINT_ROWIDS
	vector<char*> rowids;
#endif
	do {
		/* execute the statement one at a time with retrive row-id */
		checkerr(&r, OCIStmtExecute((OCISvcCtx*)_svchp, (OCIStmt*)_stmthp, (OCIError*)_errhp, (_stmt_typ == OCI_STMT_SELECT ? 0 : row_count+1), row_count,
									(OCISnapshot *)NULL, (OCISnapshot *)NULL,
									OCI_DEFAULT));
		if(r.fn_ret != SUCCESS) {
			REMOTE_LOG("failed OCIStmtExecute error %s (%s)\n", r.gerrbuf, _stmtstr.c_str());
			if(auto_commit) OCITransRollback((OCISvcCtx*)_svchp, (OCIError*)_errhp, OCI_DEFAULT);
			ocisess->release_stmt(this);
			throw r;
		}
		if(_stmt_typ == OCI_STMT_INSERT || _stmt_typ == OCI_STMT_UPDATE || _stmt_typ == OCI_STMT_DELETE) {
			// Get the row ID for the row that was just inserted.
			OraText *rowID = new OraText[19]; // Extra char for null termination.
			ub2 size = 18;
			OCIRowid *pRowID;
			OCIError *pError;
			memset(rowID, 0, 19); // Set to all nulls so that string will be null terminated.
			OCIHandleAlloc(ocisession::getenv(), (void**)&pError, OCI_HTYPE_ERROR, 0, NULL);
			OCIDescriptorAlloc(ocisession::getenv(), (void**)&pRowID, OCI_DTYPE_ROWID, 0, NULL);
			OCIAttrGet((OCIStmt*)_stmthp, OCI_HTYPE_STMT, pRowID, 0, OCI_ATTR_ROWID, pError);
			OCIRowidToChar(pRowID, rowID, &size, pError);
			(*string_append)((char*)rowID, strlen((char*)rowID), rowid_list);
#ifdef PRINT_ROWIDS
			rowids.push_back((char*)rowID);
#endif
		}

		++row_count;
	} while(row_count < _iters);
#else
	/* execute the statement all rows */
	checkerr(&r, OCIStmtExecute((OCISvcCtx*)_svchp, (OCIStmt*)_stmthp, (OCIError*)_errhp, _iters, 0,
								(OCISnapshot *)NULL, (OCISnapshot *)NULL,
								OCI_DEFAULT));
	if(r.fn_ret != SUCCESS) {
		REMOTE_LOG("failed OCIStmtExecute error %s (%s)\n", r.gerrbuf, _stmtstr.c_str());
		if(auto_commit) OCITransRollback((OCISvcCtx*)_svchp, (OCIError*)_errhp, OCI_DEFAULT);
		ocisess->release_stmt(this);
		throw r;
	}
#endif

	if(auto_commit && _stmt_typ != OCI_STMT_SELECT) {
		/* commit */
		checkerr(&r, OCITransCommit((OCISvcCtx*)_svchp, (OCIError*)_errhp, OCI_DEFAULT));
		if(r.fn_ret != SUCCESS) {
			REMOTE_LOG("failed OCITransCommit error %s (%s)\n", r.gerrbuf, _stmtstr.c_str());
			ocisess->release_stmt(this);
			throw r;
		}
	}

#ifdef PRINT_ROWIDS
	if(_stmt_typ == OCI_STMT_INSERT || _stmt_typ == OCI_STMT_UPDATE)
		for(int __i = 0; __i < _iters; ++__i)
			REMOTE_LOG("Inserted row id %s\n", rowids[__i]);
#endif

	for(int i = 0; i < _argsin.size(); ++i) {
		free(_argsin[i].datap);
		_argsin[i].datap = NULL;
		_argsin[i].datap_len = 0;
		_argsin[i].value_sz = 0;
		_argsin[i].valuep.clear();
		_argsin[i].alen.clear();
	}

	if(_stmt_typ == OCI_STMT_SELECT) {
        OCIParam *mypard = NULL;
        int num_cols = 1;
        sb4 parm_status;

        /* Request a parameter descriptor for position 1 in the select-list */
        parm_status = OCIParamGet(_stmthp, OCI_HTYPE_STMT, (OCIError*)_errhp, (dvoid **)&mypard,
                                  (ub4) num_cols);
        checkerr(&r, parm_status);
		if(r.fn_ret != SUCCESS) {
			REMOTE_LOG("failed OCIParamGet error %s (%s)\n", r.gerrbuf, _stmtstr.c_str());
			if(mypard)
				OCIDescriptorFree(mypard, OCI_DTYPE_PARAM);
			ocisess->release_stmt(this);
			throw r;
		}

        /* Loop only if a descriptor was successfully retrieved for
         * current position, starting at 1
         */
        text *col_name, *schm_name;
        ub4 len = 0, schm_len = 0;
		_columns.clear();

		while (parm_status == OCI_SUCCESS) {
			column cur_clm;
            cur_clm.dlen = 0;
            cur_clm.dtype = 0;
			cur_clm.dprec = 0;
			cur_clm.dscale = 0;

			/* Retrieve the data size attribute */
            checkerr(&r, OCIAttrGet((dvoid*) mypard, (ub4) OCI_DTYPE_PARAM,
                                    (dvoid*) &(cur_clm.dlen), (ub4 *)0, (ub4)OCI_ATTR_DATA_SIZE,
                                    (OCIError*)_errhp));
			if(r.fn_ret != SUCCESS) {
				REMOTE_LOG("failed OCIAttrGet(OCI_ATTR_DATA_SIZE) error %s (%s)\n", r.gerrbuf, _stmtstr.c_str());
				if(mypard)
					OCIDescriptorFree(mypard, OCI_DTYPE_PARAM);
				ocisess->release_stmt(this);
				throw r;
			}

            /* Retrieve the data type attribute */
            checkerr(&r, OCIAttrGet((dvoid*) mypard, (ub4) OCI_DTYPE_PARAM,
                                    (dvoid*) &(cur_clm.dtype), (ub4 *)0, (ub4)OCI_ATTR_DATA_TYPE,
                                    (OCIError*)_errhp));
			if(r.fn_ret != SUCCESS) {
				REMOTE_LOG("failed OCIAttrGet(OCI_ATTR_DATA_TYPE) error %s (%s)\n", r.gerrbuf, _stmtstr.c_str());
				if(mypard)
					OCIDescriptorFree(mypard, OCI_DTYPE_PARAM);
				ocisess->release_stmt(this);
				throw r;
			}

			/* Retrieve the data pricision,scale attributes */
			checkerr(&r, OCIAttrGet((dvoid*) mypard, (ub4) OCI_DTYPE_PARAM,
                                    (dvoid*) &(cur_clm.dprec), (ub4 *)0, (ub4)OCI_ATTR_PRECISION,
                                    (OCIError*)_errhp));
			if(r.fn_ret != SUCCESS) {
				REMOTE_LOG("failed OCIAttrGet(OCI_ATTR_PRECISION) error %s (%s)\n", r.gerrbuf, _stmtstr.c_str());
				if(mypard)
					OCIDescriptorFree(mypard, OCI_DTYPE_PARAM);
				ocisess->release_stmt(this);
				throw r;
			}
			checkerr(&r, OCIAttrGet((dvoid*) mypard, (ub4) OCI_DTYPE_PARAM,
                                    (dvoid*) &(cur_clm.dscale), (ub4 *)0, (ub4)OCI_ATTR_SCALE,
                                    (OCIError*)_errhp));
			if(r.fn_ret != SUCCESS) {
				REMOTE_LOG("failed OCIAttrGet(OCI_ATTR_SCALE) error %s (%s)\n", r.gerrbuf, _stmtstr.c_str());
				if(mypard)
					OCIDescriptorFree(mypard, OCI_DTYPE_PARAM);
				ocisess->release_stmt(this);
				throw r;
			}

			switch (cur_clm.dtype) {
            case SQLT_NUM:
            case SQLT_VNU:
            case SQLT_LNG:
				cur_clm.row_valp = new text*[cur_clm.dlen + 1];
				cur_clm.rtype = LCL_DTYPE_NONE;
				OCIDEF(SQLT_STR, "SQLT_STR");
                break;
            case SQLT_AVC:
            case SQLT_AFC:
            case SQLT_CHR:
            case SQLT_STR:
            case SQLT_VCS:
				cur_clm.row_valp = new text*[cur_clm.dlen + 1];
				cur_clm.rtype = LCL_DTYPE_NONE;
				OCIDEF(SQLT_STR, "SQLT_STR");
                break;
            case SQLT_INT:
            case SQLT_UIN:
                break;
            case SQLT_DAT:
				cur_clm.row_valp = new text*[cur_clm.dlen + 1];
				cur_clm.rtype = LCL_DTYPE_NONE;
				OCIDEF(SQLT_DAT, "SQLT_DAT");
                break;
            case SQLT_FLT:
            case SQLT_BFLOAT:
            case SQLT_BDOUBLE:
                break;
            case SQLT_TIMESTAMP:
				OCIALLOC(OCI_DTYPE_TIMESTAMP, "SQLT_TIMESTAMP");
				OCIDEF(SQLT_TIMESTAMP, "SQLT_TIMESTAMP");
                break;
            case SQLT_TIMESTAMP_TZ:
				OCIALLOC(OCI_DTYPE_TIMESTAMP_TZ, "SQLT_TIMESTAMP_TZ");
				OCIDEF(SQLT_TIMESTAMP_TZ, "SQLT_TIMESTAMP_TZ");
                break;
            case SQLT_TIMESTAMP_LTZ:
				OCIALLOC(OCI_DTYPE_TIMESTAMP_LTZ, "SQLT_TIMESTAMP_LTZ");
				OCIDEF(SQLT_TIMESTAMP_LTZ, "SQLT_TIMESTAMP_LTZ");
                break;
            case SQLT_INTERVAL_YM:
				OCIALLOC(OCI_DTYPE_INTERVAL_YM, "SQLT_INTERVAL_YM");
				OCIDEF(SQLT_INTERVAL_YM, "SQLT_INTERVAL_YM");
                break;
            case SQLT_INTERVAL_DS:
				OCIALLOC(OCI_DTYPE_INTERVAL_DS, "SQLT_INTERVAL_DS");
				OCIDEF(SQLT_INTERVAL_DS, "SQLT_INTERVAL_DS");
                break;
			case SQLT_RDD:
			case SQLT_RID:
				cur_clm.dlen = 19;
				cur_clm.row_valp = new text*[cur_clm.dlen + 1];
				cur_clm.rtype = LCL_DTYPE_NONE;
				OCIDEF(SQLT_STR, "SQLT_STR");
                break;
                break;
            default:
                break;
            }

            /* Retrieve the column name */
            len = 0;
            checkerr(&r, OCIAttrGet((dvoid*) mypard, (ub4) OCI_DTYPE_PARAM,
                                    (dvoid**) &col_name, (ub4 *) &len, (ub4) OCI_ATTR_NAME,
                                    (OCIError*)_errhp));
			if(r.fn_ret != SUCCESS) {
				REMOTE_LOG("failed OCIAttrGet(OCI_ATTR_NAME) error %s (%s)\n", r.gerrbuf, _stmtstr.c_str());
				if(mypard)
					OCIDescriptorFree(mypard, OCI_DTYPE_PARAM);
				ocisess->release_stmt(this);
				throw r;
			}

			(*coldef_append)((char*)col_name, len, cur_clm.dtype, cur_clm.dlen, cur_clm.dprec, cur_clm.dscale, column_list);
            col_name = NULL;

            /* Increment counter and get next descriptor, if there is one */
            if(OCI_SUCCESS != OCIDescriptorFree(mypard, OCI_DTYPE_PARAM)) {
				REMOTE_LOG("failed OCIDescriptorFree error %s (%s)\n", r.gerrbuf, _stmtstr.c_str());
                r.fn_ret = FAILURE;
				ocisess->release_stmt(this);
				throw r;
            }
            num_cols++;
			mypard = NULL;
            parm_status = OCIParamGet(_stmthp, OCI_HTYPE_STMT, (OCIError*)_errhp, (dvoid **)&mypard,
                                      (ub4) num_cols);

			_columns.push_back(cur_clm);
        }

		if(r.fn_ret != SUCCESS) {
			REMOTE_LOG("failed OCIDescriptorFree error %s (%s)\n", r.gerrbuf, _stmtstr.c_str());
			ocisess->release_stmt(this);
            throw r;
		}

		if(mypard)
			OCIDescriptorFree(mypard, OCI_DTYPE_PARAM);

        //REMOTE_LOG("Port: Returning Column(s)\n");
    }

	if (row_count < 2) {
		checkerr(&r, OCIAttrGet(_stmthp, OCI_HTYPE_STMT, &row_count, 0, OCI_ATTR_ROW_COUNT, (OCIError*)_errhp));
		if(r.fn_ret != SUCCESS) {
			REMOTE_LOG("failed OCIAttrGet(OCI_ATTR_ROW_COUNT) error %s (%s)\n", r.gerrbuf, _stmtstr.c_str());
			ocisess->release_stmt(this);
			throw r;
		}
	}

	return row_count;
}

intf_ret ocistmt::rows(void * row_list,
					   void (*string_append)(const char * string, size_t len, void * list),
					   void (*list_append)(const void * sub_list, void * list),
					   size_t (*sizeof_resp)(void * resp),
					   unsigned int maxrowcount)
{
	intf_ret r;

	r.handle = _errhp;
    unsigned int num_rows = 0;
    sword res = OCI_NO_DATA;
	size_t total_est_row_size = 0;
	OCIEnv *envhp = (OCIEnv *)ocisession::getenv();

	r.fn_ret = FAILURE;
	if (_columns.size() <= 0) {
		REMOTE_LOG("statement %s has no rows\n", _stmtstr.c_str());
        throw r;
	}
	r.fn_ret = SUCCESS;

    // overdrive preventation
    if(maxrowcount > 100)
        maxrowcount = 100;

	void * row = NULL;
    do {
        ++num_rows;
		//if(num_rows % 100 == 0) REMOTE_LOG("OCI: Fetched %lu rows of %d bytes\n", num_rows, total_est_row_size);
        res = OCIStmtFetch((OCIStmt*)_stmthp, (OCIError*)_errhp, 1, OCI_FETCH_NEXT, OCI_DEFAULT);
		checkerr(&r, res);
		if(r.fn_ret != SUCCESS) {
			REMOTE_LOG("failed OCIStmtFetch for %p row %d reason %s (%s)\n", _stmthp, num_rows, r.gerrbuf, _stmtstr.c_str());
		    throw r;
		}

        row = NULL;

		if (res != OCI_NO_DATA) {
			for (int i = 0; i < _columns.size(); ++i)
					switch (_columns[i].dtype) {
					case SQLT_TIMESTAMP:
					case SQLT_TIMESTAMP_TZ:
					case SQLT_TIMESTAMP_LTZ: {
						r.handle = envhp;
						ub1 *dtarry = NULL;
						ub4 len;
						checkerr(&r, OCIDateTimeToArray(envhp, (OCIError*)_errhp, (CONST OCIDateTime *)(_columns[i].row_valp),
                                        (CONST OCIInterval *)NULL, dtarry, &len, (ub1)0xFF));
						r.handle = _errhp; }
						break;
					/*case SQLT_INTERVAL_YM:
					case SQLT_INTERVAL_DS:
						break;*/
					case SQLT_DAT:
						{
						/*char date_buf[15];
						sprintf(date_buf, "%02d%02d%02d%02d%02d%02d%02d",
							((char*)data_row[i])[0] - 100, // Century
							((char*)data_row[i])[1] - 100, // Year
							((char*)data_row[i])[2],		 // Month
							((char*)data_row[i])[3],		 // Day
							((char*)data_row[i])[4],		 // 24HH
							((char*)data_row[i])[5],		 // MM
							((char*)data_row[i])[6]);		 // DD
						(*string_append)(date_buf, strlen(date_buf), &row);*/
						(*string_append)((char*)_columns[i].row_valp, 7, &row);
						}
						break;
					case SQLT_RID:
					case SQLT_RDD:
					case SQLT_NUM:
					case SQLT_CHR:
						(*string_append)((char*)_columns[i].row_valp, strlen((char*)_columns[i].row_valp), &row);
						break;
					}
			total_est_row_size += (*sizeof_resp)(&row);
			(*list_append)(row, row_list);
		}
    } while (res != OCI_NO_DATA
			&& num_rows < maxrowcount
			&& total_est_row_size < MAX_RESP_SIZE);

	if(r.fn_ret != SUCCESS) {
		REMOTE_LOG("this should never happen reason %s (%s)\n", r.gerrbuf, _stmtstr.c_str());
        throw r;
	}

    //REMOTE_LOG("Port: Returning Rows...\n");
	if(res != OCI_NO_DATA)
		r.fn_ret = MORE;
	else
		r.fn_ret = DONE;

	return r;
}

void ocistmt::close()
{
	((ocisession *)_ocisess)->release_stmt(this);
	delete this;
}

ocistmt::~ocistmt(void)
{
	intf_ret r;

    /* Release the bound variables memeory */
	for (int i = 0; i < _columns.size(); ++i) {
		if(_columns[i].rtype == LCL_DTYPE_NONE)
			delete (char*)(_columns[i].row_valp);
		else
			(void) OCIDescriptorFree(_columns[i].row_valp, _columns[i].rtype);
	}
	_columns.clear();

	r.handle = _errhp;
    checkerr(&r, OCIStmtRelease((OCIStmt*)_stmthp, (OCIError*)_errhp, (OraText *) NULL, 0, OCI_DEFAULT));
	if(r.fn_ret != SUCCESS) {
		REMOTE_LOG("failed OCIStmtRelease %s (%s)\n", r.gerrbuf, _stmtstr.c_str());
        throw r;
	}

	(void) OCIHandleFree(_stmthp, OCI_HTYPE_STMT);
	(void) OCIHandleFree(_errhp, OCI_HTYPE_ERROR);
}