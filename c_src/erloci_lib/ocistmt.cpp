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

#ifndef __WIN32__
#include <stdlib.h>
#include <netinet/in.h>
#else
#include <Winsock2.h>
#include <Windows.h>
#endif

#include <cstring>
#include <oci.h>

struct column {
    ub2  dtype;
    ub4	 dlen;
    ub2	 dprec;
    sb1	 dscale;
	sb2  indp;
	ub4  rtype;
	void * row_valp;
	vector<OCILobLocator*> loblps;
};

ocistmt::FNCDEFAPP ocistmt::coldef_append		= NULL;
ocistmt::FNSTRAPP ocistmt::string_append		= NULL;
ocistmt::FNTUPAPP ocistmt::tuple_append			= NULL;
ocistmt::FNTUPEAPP ocistmt::tuple_append_ext	= NULL;
ocistmt::FNSZAPP ocistmt::sizeof_resp			= NULL;
ocistmt::FNCHLDLST ocistmt::child_list			= NULL;
ocistmt::FNLOBDATA ocistmt::lob_data			= NULL;

void ocistmt::config(ocistmt::FNCDEFAPP cda, ocistmt::FNSTRAPP sa, ocistmt::FNTUPAPP tup, ocistmt::FNTUPEAPP tupe,
	ocistmt::FNSZAPP sr, ocistmt::FNCHLDLST cl, ocistmt::FNLOBDATA lobf)
{
	coldef_append = cda;
	string_append = sa;
	sizeof_resp = sr;
	tuple_append = tup;
	tuple_append_ext = tupe;
	child_list = cl;
	lob_data = lobf;
}

ocistmt::ocistmt(void *ocisess, OraText *stmt, size_t stmt_len)
{
	intf_ret r;
	
	OCIEnv *envhp = (OCIEnv *)ocisession::getenv();

	_svchp = ((ocisession *)ocisess)->getsession();
	_iters = 1;
	_ocisess = ocisess;
		
	_stmtstr = new char[stmt_len+1];
	memcpy(_stmtstr, stmt, stmt_len);
	_stmtstr[stmt_len] = '\0';

	// allocate error handle
	r.handle = envhp;
	checkenv(&r, OCIHandleAlloc(envhp,			/* environment handle */
                            (void **) &_errhp,	/* returned err handle */
                            OCI_HTYPE_ERROR,	/* typ of handle to allocate */
                            (size_t) 0,			/* optional extra memory size */
                            (void **) NULL));	/* returned extra memeory */
	if(r.fn_ret != SUCCESS) {
		REMOTE_LOG(ERR, "failed OCIHandleAlloc %s (%s)\n", r.gerrbuf, _stmtstr);
        throw r;
	}

	// allocate the statement handle
	checkenv(&r, OCIHandleAlloc(envhp,					/* environment handle */
                                (void **) &_stmthp,		/* returned statement handle */
                                OCI_HTYPE_STMT,			/* typ of handle to allocate */
                                (size_t) 0,				/* optional extra memory size */
								(void **) NULL));		/* returned extra memeory */
	if(r.fn_ret != SUCCESS) {
   		REMOTE_LOG(ERR, "failed OCIHandleAlloc %s (%s)\n", r.gerrbuf, _stmtstr);
        throw r;
	}

	/* Get a prepared statement handle */
	r.handle = _errhp;
    checkerr(&r, OCIStmtPrepare2((OCISvcCtx*)_svchp,
                                 (OCIStmt**)&_stmthp,	/* returned statement handle */
                                 (OCIError*)_errhp,		/* error handle */
                                 (OraText *) stmt,		/* the statement text */
                                 (ub4)stmt_len,				/* length of the text */
                                 NULL, 0,				/* tagging parameters: optional */
                                 OCI_NTV_SYNTAX, OCI_DEFAULT));

	if(r.fn_ret != SUCCESS) {
   		REMOTE_LOG(ERR, "failed OCIStmtPrepare2 %s (%s)\n", r.gerrbuf, _stmtstr);
        throw r;
	}

	_stmt_typ = 0;
    checkerr(&r, OCIAttrGet((dvoid*) _stmthp, (ub4) OCI_HTYPE_STMT,
                            (dvoid*) &_stmt_typ, (ub4 *)NULL, (ub4)OCI_ATTR_STMT_TYPE, (OCIError*)_errhp));
	if(r.fn_ret != SUCCESS) {
		REMOTE_LOG(ERR, "failed OCIAttrGet error %s (%s)\n", r.gerrbuf, _stmtstr);
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
		REMOTE_LOG(ERR, "failed OCIDescriptorAlloc for %p column %d("__dtypestr")\n", _stmthp, num_cols);	\
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
		REMOTE_LOG(ERR, "failed OCIDefineByPos for %p column %d("__dtypestr")\n", _stmthp, num_cols);		\
		throw r;																							\
	}																										\
}

#define FETCH_ROWIDS

#ifdef FETCH_ROWIDS
//#define PRINT_ROWIDS
#endif

unsigned int ocistmt::execute(void * column_list, void * rowid_list, bool auto_commit)
{
	ub4 row_count = 0;
	intf_ret r;

	ocisession * ocisess = (ocisession *)_ocisess;
	OCIEnv *envhp = (OCIEnv *)ocisession::getenv();

	r.handle = _errhp;

	/* bind variables if any */
	size_t dat_len = 0;
	for(unsigned int i = 0; i < _argsin.size(); ++i) {
		for(unsigned int j = 0; j < _argsin[i].alen.size(); ++j) {
			//unsigned short * alenarr = &_argsin[i].alen[0];
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
				default:
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
	for(size_t i = 0; i < _argsin.size(); ++i) {
		checkerr(&r, OCIBindByName((OCIStmt*)_stmthp, (OCIBind**)(&_argsin[i].ocibind), (OCIError*)_errhp,
									(text*)(_argsin[i].name), -1,
									_argsin[i].datap, _argsin[i].value_sz,
									_argsin[i].dty,
									&_argsin[i].ind[0], &_argsin[i].alen[0],
									(ub2*)NULL,0,
									(ub4*)NULL, OCI_DEFAULT));
		if(r.fn_ret != SUCCESS) {
			REMOTE_LOG(ERR, "failed OCIBindByName error %s (%s)\n", r.gerrbuf, _stmtstr);
			ocisess->release_stmt(this);
			throw r;
		}
	}

	//ub4 mode = (auto_commit ? OCI_COMMIT_ON_SUCCESS : OCI_DEFAULT);
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
			REMOTE_LOG(ERR, "failed OCIStmtExecute error %s (%s)\n", r.gerrbuf, _stmtstr);
			if(auto_commit) OCITransRollback((OCISvcCtx*)_svchp, (OCIError*)_errhp, OCI_DEFAULT);
			ocisess->release_stmt(this);
			throw r;
		}
		if(_stmt_typ == OCI_STMT_INSERT || _stmt_typ == OCI_STMT_UPDATE || _stmt_typ == OCI_STMT_DELETE) {
			ub4 rc = 0;
			checkerr(&r, OCIAttrGet(_stmthp, OCI_HTYPE_STMT, &rc, 0, OCI_ATTR_ROW_COUNT, (OCIError*)_errhp));
			if(r.fn_ret != SUCCESS) {
				REMOTE_LOG(ERR, "failed OCIAttrGet(OCI_ATTR_ROW_COUNT) error %s (%s)\n", r.gerrbuf, _stmtstr);
				ocisess->release_stmt(this);
				throw r;
			}

			// returned RowID is only valid if anything was changed at all
			if(rc > 0) {
				// Get the row ID for the row that was just inserted.
				OraText *rowID = new OraText[19]; // Extra char for null termination.
				ub2 size = 18;
				OCIRowid *pRowID;
				//OCIError *pError;
				memset(rowID, 0, 19); // Set to all nulls so that string will be null terminated.
				//OCIHandleAlloc(envhp, (void**)&pError, OCI_HTYPE_ERROR, 0, NULL);
				checkerr(&r, OCIDescriptorAlloc(envhp, (void**)&pRowID, OCI_DTYPE_ROWID, 0, NULL));
				if(r.fn_ret != SUCCESS) {
					REMOTE_LOG(ERR, "failed OCIStmtExecute error %s (%s)\n", r.gerrbuf, _stmtstr);
					if(auto_commit) OCITransRollback((OCISvcCtx*)_svchp, (OCIError*)_errhp, OCI_DEFAULT);
					ocisess->release_stmt(this);
					throw r;
				}

				checkerr(&r, OCIAttrGet((OCIStmt*)_stmthp, OCI_HTYPE_STMT, pRowID, 0, OCI_ATTR_ROWID, (OCIError*)_errhp));
				if(r.fn_ret != SUCCESS) {
					REMOTE_LOG(ERR, "failed OCIStmtExecute error %s (%s)\n", r.gerrbuf, _stmtstr);
					if(auto_commit) OCITransRollback((OCISvcCtx*)_svchp, (OCIError*)_errhp, OCI_DEFAULT);
					ocisess->release_stmt(this);
					throw r;
				}

				checkerr(&r, OCIRowidToChar(pRowID, rowID, &size, (OCIError*)_errhp));
				if(r.fn_ret != SUCCESS) {
					REMOTE_LOG(ERR, "failed OCIStmtExecute error %s (%s)\n", r.gerrbuf, _stmtstr);
					if(auto_commit) OCITransRollback((OCISvcCtx*)_svchp, (OCIError*)_errhp, OCI_DEFAULT);
					ocisess->release_stmt(this);
					throw r;
				}

				(*string_append)((char*)rowID, strlen((char*)rowID), rowid_list);
			} else {
				(*string_append)(NULL, 0, rowid_list);
			}
#ifdef PRINT_ROWIDS
			rowids.push_back((char*)rowID);
#endif
		}

		++row_count;
	} while((size_t)row_count < _iters);
#else
	/* execute the statement all rows */
	checkerr(&r, OCIStmtExecute((OCISvcCtx*)_svchp, (OCIStmt*)_stmthp, (OCIError*)_errhp, _iters, 0,
								(OCISnapshot *)NULL, (OCISnapshot *)NULL,
								OCI_DEFAULT));
	if(r.fn_ret != SUCCESS) {
		REMOTE_LOG("failed OCIStmtExecute error %s (%s)\n", r.gerrbuf, _stmtstr);
		if(auto_commit) OCITransRollback((OCISvcCtx*)_svchp, (OCIError*)_errhp, OCI_DEFAULT);
		ocisess->release_stmt(this);
		throw r;
	}
#endif

	if(auto_commit && _stmt_typ != OCI_STMT_SELECT) {
		/* commit */
		checkerr(&r, OCITransCommit((OCISvcCtx*)_svchp, (OCIError*)_errhp, OCI_DEFAULT));
		if(r.fn_ret != SUCCESS) {
			REMOTE_LOG(ERR, "failed OCITransCommit error %s (%s)\n", r.gerrbuf, _stmtstr);
			ocisess->release_stmt(this);
			throw r;
		}
	}

#ifdef PRINT_ROWIDS
	if(_stmt_typ == OCI_STMT_INSERT || _stmt_typ == OCI_STMT_UPDATE)
		for(int __i = 0; __i < _iters; ++__i)
			REMOTE_LOG("Inserted row id %s\n", rowids[__i]);
#endif

	for(unsigned int i = 0; i < _argsin.size(); ++i) {
		if(_argsin[i].datap) {
			free(_argsin[i].datap);
			_argsin[i].datap = NULL;
		}
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
			REMOTE_LOG(ERR, "failed OCIParamGet error %s (%s)\n", r.gerrbuf, _stmtstr);
			if(mypard)
				OCIDescriptorFree(mypard, OCI_DTYPE_PARAM);
			ocisess->release_stmt(this);
			throw r;
		}

        /* Loop only if a descriptor was successfully retrieved for
         * current position, starting at 1
         */
        text *col_name;
        ub4 len = 0;
		for (unsigned int i = 0; i < _columns.size(); ++i) {
			if(_columns[i]->rtype == LCL_DTYPE_NONE)
				delete (char*)(_columns[i]->row_valp);
			else {
				ub4 trtype = _columns[i]->rtype;
				vector<OCILobLocator *> & tlobps = _columns[i]->loblps;
				(void) OCIDescriptorFree(_columns[i]->row_valp, trtype);
				for(size_t j = 0; j < tlobps.size(); ++j) {
					(void) OCIDescriptorFree(tlobps[j], trtype);
				}
				tlobps.clear();
			}
			delete _columns[i];
		}
		_columns.clear();

		while (parm_status == OCI_SUCCESS) {
			column *_clm = new column;
			column & cur_clm = *_clm;
            cur_clm.dlen = 0;
            cur_clm.dtype = 0;
			cur_clm.dprec = 0;
			cur_clm.dscale = 0;
			cur_clm.row_valp = NULL;

			/* Retrieve the data size attribute */
            checkerr(&r, OCIAttrGet((dvoid*) mypard, (ub4) OCI_DTYPE_PARAM,
                                    (dvoid*) &(cur_clm.dlen), (ub4 *)0, (ub4)OCI_ATTR_DATA_SIZE,
                                    (OCIError*)_errhp));
			if(r.fn_ret != SUCCESS) {
				REMOTE_LOG(ERR, "failed OCIAttrGet(OCI_ATTR_DATA_SIZE) error %s (%s)\n", r.gerrbuf, _stmtstr);
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
				REMOTE_LOG(ERR, "failed OCIAttrGet(OCI_ATTR_DATA_TYPE) error %s (%s)\n", r.gerrbuf, _stmtstr);
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
				REMOTE_LOG(ERR, "failed OCIAttrGet(OCI_ATTR_PRECISION) error %s (%s)\n", r.gerrbuf, _stmtstr);
				if(mypard)
					OCIDescriptorFree(mypard, OCI_DTYPE_PARAM);
				ocisess->release_stmt(this);
				throw r;
			}
			checkerr(&r, OCIAttrGet((dvoid*) mypard, (ub4) OCI_DTYPE_PARAM,
                                    (dvoid*) &(cur_clm.dscale), (ub4 *)0, (ub4)OCI_ATTR_SCALE,
                                    (OCIError*)_errhp));
			if(r.fn_ret != SUCCESS) {
				REMOTE_LOG(ERR, "failed OCIAttrGet(OCI_ATTR_SCALE) error %s (%s)\n", r.gerrbuf, _stmtstr);
				if(mypard)
					OCIDescriptorFree(mypard, OCI_DTYPE_PARAM);
				ocisess->release_stmt(this);
				throw r;
			}

			switch (cur_clm.dtype) {
            case SQLT_FLT:
            case SQLT_BFLOAT:
            case SQLT_BDOUBLE:
            case SQLT_INT:
            case SQLT_UIN:
            case SQLT_VNU:
            case SQLT_NUM:
				cur_clm.dlen = OCI_NUMBER_SIZE;
				cur_clm.row_valp = new OCINumber;
				memset(cur_clm.row_valp, 0, sizeof(OCINumber));
				cur_clm.rtype = LCL_DTYPE_NONE;
				OCIDEF(SQLT_VNU, "SQLT_VNU");
				break;
            case SQLT_AVC:
            case SQLT_AFC:
            case SQLT_CHR:
            case SQLT_STR:
            case SQLT_VCS:
				cur_clm.row_valp = new text[cur_clm.dlen + 1];
				cur_clm.rtype = LCL_DTYPE_NONE;
				OCIDEF(SQLT_STR, "SQLT_STR");
                break;
            case SQLT_DAT:
				cur_clm.dlen = sizeof(OCIDate);
				cur_clm.row_valp = new OCIDate;
				memset(cur_clm.row_valp, 0, sizeof(OCIDate));
				cur_clm.rtype = LCL_DTYPE_NONE;
				OCIDEF(SQLT_ODT, "SQLT_ODT");
                break;
			case SQLT_BIN: // RAW
				cur_clm.row_valp = new unsigned char[cur_clm.dlen + 1];
				memset(cur_clm.row_valp, 0, (cur_clm.dlen + 1)*sizeof(unsigned char));
				cur_clm.rtype = LCL_DTYPE_NONE;
				OCIDEF(SQLT_BIN, "SQLT_BIN");
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
				cur_clm.dlen = (cur_clm.dlen < 19 ? 19 : cur_clm.dlen);
				cur_clm.row_valp = new text[cur_clm.dlen + 1];
				cur_clm.rtype = LCL_DTYPE_NONE;
				OCIDEF(SQLT_STR, "SQLT_STR");
                break;
			case SQLT_CLOB: {
				OCIALLOC(OCI_DTYPE_LOB, "SQLT_CLOB");
				void * _t = cur_clm.row_valp;
				ub4 _dlen = cur_clm.dlen;
				cur_clm.dlen = -1;
				cur_clm.row_valp = &(cur_clm.row_valp);
				OCIDEF(SQLT_CLOB, "SQLT_CLOB");
				cur_clm.row_valp = _t;
				cur_clm.dlen = _dlen;
                break;
			}
			case SQLT_BLOB: {
				OCIALLOC(OCI_DTYPE_LOB, "SQLT_BLOB");
				void * _t = cur_clm.row_valp;
				ub4 _dlen = cur_clm.dlen;
				cur_clm.dlen = -1;
				cur_clm.row_valp = &(cur_clm.row_valp);
				OCIDEF(SQLT_BLOB, "SQLT_BLOB");
				cur_clm.row_valp = _t;
				cur_clm.dlen = _dlen;
                break;
			}
			case SQLT_BFILEE: {
				OCIALLOC(OCI_DTYPE_FILE, "SQLT_BFILEE");
				void * _t = cur_clm.row_valp;
				ub4 _dlen = cur_clm.dlen;
				cur_clm.dlen = -1;
				cur_clm.row_valp = &(cur_clm.row_valp);
				OCIDEF(SQLT_BFILEE, "SQLT_BFILEE");
				cur_clm.row_valp = _t;
				cur_clm.dlen = _dlen;
                break;
			}
            default:
				r.fn_ret = FAILURE;
				REMOTE_LOG(ERR, "Unsupported column type %d\n", cur_clm.dtype);
				throw r;
                break;
            }

            /* Retrieve the column name */
            len = 0;
            checkerr(&r, OCIAttrGet((dvoid*) mypard, (ub4) OCI_DTYPE_PARAM,
                                    (dvoid**) &col_name, (ub4 *) &len, (ub4) OCI_ATTR_NAME,
                                    (OCIError*)_errhp));
			if(r.fn_ret != SUCCESS) {
				REMOTE_LOG(ERR, "failed OCIAttrGet(OCI_ATTR_NAME) error %s (%s)\n", r.gerrbuf, _stmtstr);
				if(mypard)
					OCIDescriptorFree(mypard, OCI_DTYPE_PARAM);
				ocisess->release_stmt(this);
				throw r;
			}

			(*coldef_append)((char*)col_name, len, cur_clm.dtype, cur_clm.dlen, cur_clm.dprec, cur_clm.dscale, column_list);
            col_name = NULL;

            /* Increment counter and get next descriptor, if there is one */
            if(OCI_SUCCESS != OCIDescriptorFree(mypard, OCI_DTYPE_PARAM)) {
				REMOTE_LOG(ERR, "failed OCIDescriptorFree error %s (%s)\n", r.gerrbuf, _stmtstr);
                r.fn_ret = FAILURE;
				ocisess->release_stmt(this);
				throw r;
            }
            num_cols++;
			mypard = NULL;
            parm_status = OCIParamGet(_stmthp, OCI_HTYPE_STMT, (OCIError*)_errhp, (dvoid **)&mypard,
                                      (ub4) num_cols);

			_columns.push_back(_clm);
        }

		if(r.fn_ret != SUCCESS) {
			REMOTE_LOG(ERR, "failed OCIDescriptorFree error %s (%s)\n", r.gerrbuf, _stmtstr);
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
			REMOTE_LOG(ERR, "failed OCIAttrGet(OCI_ATTR_ROW_COUNT) error %s (%s)\n", r.gerrbuf, _stmtstr);
			ocisess->release_stmt(this);
			throw r;
		}
	}

	//Sleep(50000);
	return row_count;
}

intf_ret ocistmt::rows(void * row_list, unsigned int maxrowcount)
{
	intf_ret r;

	r.handle = _errhp;
    unsigned int num_rows = 0;
    sword res = OCI_NO_DATA;
	size_t total_est_row_size = 0;
	OCIEnv *envhp = (OCIEnv *)ocisession::getenv();

	r.fn_ret = FAILURE;
	if (_columns.size() <= 0) {
		REMOTE_LOG(INF, "statement %s has no rows\n", _stmtstr);
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
			REMOTE_LOG(ERR, "failed OCIStmtFetch for %p row %d reason %s (%s)\n", _stmthp, num_rows, r.gerrbuf, _stmtstr);
		    throw r;
		}

		if (res != OCI_NO_DATA) {
	        row = (*child_list)(row_list);
			for (unsigned int i = 0; i < _columns.size(); ++i)
					switch (_columns[i]->dtype) {
					case SQLT_NUM:
						(*string_append)((char*)(_columns[i]->row_valp), _columns[i]->dlen, row);
						memset(_columns[i]->row_valp, 0, sizeof(OCINumber));
						break;
					case SQLT_TIMESTAMP:
					case SQLT_TIMESTAMP_TZ:
					case SQLT_TIMESTAMP_LTZ: {
						r.handle = envhp;
						ub1 *dtarry = NULL;
						ub4 len;
						checkerr(&r, OCIDateTimeToArray(envhp, (OCIError*)_errhp, (CONST OCIDateTime *)(_columns[i]->row_valp),
                                        (CONST OCIInterval *)NULL, dtarry, &len, (ub1)0xFF));
						r.handle = _errhp; }
						break;
					/*case SQLT_INTERVAL_YM:
					case SQLT_INTERVAL_DS:
						break;*/
					case SQLT_DAT:
						((OCIDate*)_columns[i]->row_valp)->OCIDateYYYY = ntohs((ub2)((OCIDate*)(_columns[i]->row_valp))->OCIDateYYYY);
						(*string_append)((char*)(_columns[i]->row_valp), _columns[i]->dlen, row);
						memset(_columns[i]->row_valp, 0, sizeof(OCIDate));
						break;
					case SQLT_BFILE: {
							OCILobLocator *_tlob;
							oraub8 loblen = 0;
							checkerr(&r, OCILobGetLength2((OCISvcCtx*)_svchp, (OCIError*)_errhp, (OCILobLocator*)(_columns[i]->row_valp), &loblen));
							if(r.fn_ret != SUCCESS) {
								REMOTE_LOG(ERR, "failed OCILobGetLength for %p row %d column %d reason %s (%s)\n", _stmthp, num_rows, i, r.gerrbuf, _stmtstr);
								throw r;
							}
							r.handle = envhp;
							checkerr(&r, OCIDescriptorAlloc(envhp, (dvoid **)&_tlob, (ub4)(_columns[i]->rtype), (size_t)0, (dvoid **)0));
							if(r.fn_ret != SUCCESS) {
								REMOTE_LOG(ERR, "failed OCIDescriptorAlloc for %p column %d reason %s (%s)\n", _stmthp, i, r.gerrbuf, _stmtstr);
								throw r;
							}
							r.handle = _errhp;
							checkerr(&r, OCILobLocatorAssign((OCISvcCtx*)_svchp, (OCIError*)_errhp, (OCILobLocator*)(_columns[i]->row_valp), &_tlob));
							if(r.fn_ret != SUCCESS) {
								REMOTE_LOG(ERR, "failed OCILobLocatorAssign for %p column %d reason %s (%s)\n", _stmthp, i, r.gerrbuf, _stmtstr);
								throw r;
							}
							text dir[31], file[256];
							ub2 dlen = sizeof(dir)/sizeof(dir[0]), flen = sizeof(file)/sizeof(file[0]);
							checkerr(&r, OCILobFileGetName(envhp, (OCIError*)_errhp, _tlob, dir, &dlen, file, &flen));
							if(r.fn_ret != OCI_SUCCESS) {
								REMOTE_LOG(ERR, "failed OCILobFileGetName for %p column %d reason %s (%s)\n", _stmthp, i, r.gerrbuf, _stmtstr);
								throw r;
							}
							(*tuple_append_ext)((unsigned long long)_tlob, (unsigned long long)loblen, (const char*)dir, dlen, (const char*)file, flen, row);
							_columns[i]->loblps.push_back(_tlob);
						break;
					}
					case SQLT_CLOB:
					case SQLT_BLOB: {
							OCILobLocator *_tlob;
							unsigned long long loblen = 0;
							checkerr(&r, OCILobGetLength2((OCISvcCtx*)_svchp, (OCIError*)_errhp, (OCILobLocator*)(_columns[i]->row_valp), (oraub8*)&loblen));
							if(r.fn_ret != SUCCESS) {
								REMOTE_LOG(ERR, "failed OCILobGetLength for %p row %d column %d reason %s (%s)\n", _stmthp, num_rows, i, r.gerrbuf, _stmtstr);
								throw r;
							}
							r.handle = envhp;
							checkerr(&r, OCIDescriptorAlloc(envhp, (dvoid **)&_tlob, (ub4)(_columns[i]->rtype), (size_t)0, (dvoid **)0));
							if(r.fn_ret != SUCCESS) {
								REMOTE_LOG(ERR, "failed OCIDescriptorAlloc for %p column %d reason %s (%s)\n", _stmthp, i, r.gerrbuf, _stmtstr);
								throw r;
							}
							r.handle = _errhp;
							checkerr(&r, OCILobLocatorAssign((OCISvcCtx*)_svchp, (OCIError*)_errhp, (OCILobLocator*)(_columns[i]->row_valp), &_tlob));
							if(r.fn_ret != SUCCESS) {
								REMOTE_LOG(ERR, "failed OCILobLocatorAssign for %p column %d reason %s (%s)\n", _stmthp, i, r.gerrbuf, _stmtstr);
								throw r;
							}
							(*tuple_append)((unsigned long long)_tlob, loblen, row);
							_columns[i]->loblps.push_back(_tlob);
						break;
					}
					case SQLT_CHR: {
						size_t str_len = _columns[i]->dlen;
						if(str_len > 0) // Handling for non NULL column
							str_len = strlen((char*)(_columns[i]->row_valp));
						(*string_append)((char*)(_columns[i]->row_valp), str_len, row);
						memset(_columns[i]->row_valp, 0, _columns[i]->dlen);
						}
						break;
					case SQLT_RID:
					case SQLT_RDD:
					case SQLT_AFC:
					case SQLT_STR:
						{
							size_t str_len = strlen((char*)(_columns[i]->row_valp));
							(*string_append)((char*)(_columns[i]->row_valp), str_len, row);
							memset(_columns[i]->row_valp, 0, str_len);
						}
						break;
					default:
						r.fn_ret = FAILURE;
						SPRINT(r.gerrbuf, sizeof(r.gerrbuf), "[%s:%d] unsupporetd type %u\n", __FUNCTION__, __LINE__, _columns[i]->dtype);
						REMOTE_LOG(ERR, "%s at row %d column %d (%s)\n", r.gerrbuf, num_rows, i, _stmtstr);
						throw r;
						break;
					}
			total_est_row_size += (*sizeof_resp)(row);
		}
    } while (res != OCI_NO_DATA
			&& num_rows < maxrowcount
			&& total_est_row_size < max_term_byte_size);

	if(r.fn_ret != SUCCESS) {
		REMOTE_LOG(CRT, "this should never happen reason %s (%s)\n", r.gerrbuf, _stmtstr);
        throw r;
	}

    //REMOTE_LOG("Port: Returning Rows...\n");
	if(res != OCI_NO_DATA)
		r.fn_ret = MORE;
	else
		r.fn_ret = DONE;

	//Sleep(50000);
	return r;
}

intf_ret ocistmt::lob(void * data, void * _lob, unsigned long long offset, unsigned long long length)
{
	intf_ret r;
	ub1 csfrm;
	OCIEnv *envhp = (OCIEnv *)ocisession::getenv();

	OCILobLocator * lob = (OCILobLocator *)_lob;
	r.handle = _errhp;
	r.fn_ret = SUCCESS;

	if (offset <= 0) {
		r.fn_ret = FAILURE;
		SPRINT(r.gerrbuf, sizeof(r.gerrbuf), "[%s:%d] invalid lob (%p) offset %llu\n", __FUNCTION__, __LINE__, lob, offset);
		REMOTE_LOG(ERR, "failed invalid lob offset %s (%s)\n", lob, r.gerrbuf, _stmtstr);
		throw r;
	}

	if (length <= 0) {
		r.fn_ret = FAILURE;
		SPRINT(r.gerrbuf, sizeof(r.gerrbuf), "[%s:%d] invalid lob (%p) length %llu\n", __FUNCTION__, __LINE__, lob, length);
		REMOTE_LOG(ERR, "failed invalid lob lenght %s (%s)\n", lob, r.gerrbuf, _stmtstr);
		throw r;
	}

	oraub8 loblen = 0;
	checkerr(&r, OCILobGetLength2((OCISvcCtx*)_svchp, (OCIError*)_errhp, lob, &loblen));
	if(r.fn_ret != OCI_SUCCESS) {
		REMOTE_LOG(ERR, "failed OCILobGetLength2 for %p reason %s (%s)\n", lob, r.gerrbuf, _stmtstr);
		throw r;
	}

	if (loblen < offset+length-1) {
		r.fn_ret = FAILURE;
		SPRINT(r.gerrbuf, sizeof(r.gerrbuf), "[%s:%d] lob (%p) index %llu out of bound (max %llu)\n", __FUNCTION__, __LINE__, lob, offset+length, (unsigned long long int)loblen);
		REMOTE_LOG(ERR, "failed lob index %s (%s)\n", lob, r.gerrbuf, _stmtstr);
		throw r;
	}

	r.handle = envhp;
	checkerr(&r, OCILobCharSetForm(envhp, (OCIError*)_errhp, lob, &csfrm));
	if(r.fn_ret != OCI_SUCCESS) {
		REMOTE_LOG(ERR, "failed OCILobCharSetForm for %p reason %s (%s)\n", lob, r.gerrbuf, _stmtstr);
		throw r;
	}
	r.handle = _errhp;

	checkerr(&r, OCILobOpen((OCISvcCtx*)_svchp, (OCIError*)_errhp, lob, OCI_LOB_READONLY));
	if(r.fn_ret != OCI_SUCCESS) {
		REMOTE_LOG(ERR, "failed OCILobOpen for %p reason %s (%s)\n", lob, r.gerrbuf, _stmtstr);
		throw r;
	}

	ub1 *buf = new ub1[length+1];
	memset ((dvoid*)buf, '\0', length+1);
	oraub8 loblenc = loblen;
	checkerr(&r, OCILobRead2((OCISvcCtx*)_svchp, (OCIError*)_errhp, lob, (oraub8*)&loblen, &loblenc, (oraub8)offset, (void*)buf, (oraub8)length , OCI_ONE_PIECE, (dvoid*)0, (OCICallbackLobRead2)0, (ub2)0, csfrm));
	if(r.fn_ret != OCI_SUCCESS) {
		REMOTE_LOG(ERR, "failed OCILobRead2 for %p reason %s (%s)\n", lob, r.gerrbuf, _stmtstr);
		throw r;
	}
	
	checkerr(&r, OCILobClose((OCISvcCtx*)_svchp, (OCIError*)_errhp, lob));
	if(r.fn_ret != OCI_SUCCESS) {
		REMOTE_LOG(ERR, "failed OCILobClose for %p reason %s (%s)\n", lob, r.gerrbuf, _stmtstr);
		throw r;
	}

	(*lob_data)(buf, length, data);

	delete buf;
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
	for (unsigned int i = 0; i < _columns.size(); ++i) {
		if(_columns[i]->rtype == LCL_DTYPE_NONE)
			delete (char*)(_columns[i]->row_valp);
		else {
			ub4 trtype = _columns[i]->rtype;
			vector<OCILobLocator *> & tlobps = _columns[i]->loblps;
			(void) OCIDescriptorFree(_columns[i]->row_valp, trtype);
			for(size_t j = 0; j < tlobps.size(); ++j) {
				(void) OCIDescriptorFree(tlobps[j], trtype);
			}
			tlobps.clear();
		}
		delete _columns[i];
	}
	_columns.clear();

	r.handle = _errhp;
    checkerr(&r, OCIStmtRelease((OCIStmt*)_stmthp, (OCIError*)_errhp, (OraText *) NULL, 0, OCI_DEFAULT));
	if(r.fn_ret != SUCCESS) {
		REMOTE_LOG(ERR, "failed OCIStmtRelease %s (%s)\n", r.gerrbuf, _stmtstr);
        throw r;
	}

	(void) OCIHandleFree(_stmthp, OCI_HTYPE_STMT);
	(void) OCIHandleFree(_errhp, OCI_HTYPE_ERROR);

	delete _stmtstr;
	_stmtstr = NULL;
}
