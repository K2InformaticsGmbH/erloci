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

intf_funs ocistmt::intf;

void ocistmt::config(intf_funs _intf)
{
	intf = _intf;
}

ocistmt::ocistmt(void *ocisess, void *stmt)
{
	intf_ret r;
	
	OCIEnv *envhp = (OCIEnv *)ocisession::getenv();

	_svchp = ((ocisession *)ocisess)->getsession();
	_iters = 1;
	_ocisess = ocisess;
		
	_stmtstr = new char[1];
	_stmtstr[0] = '\0';

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

	_stmthp = stmt;
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
		REMOTE_LOG(ERR, "failed OCIDescriptorAlloc for %p column %d(" __dtypestr")\n", _stmthp, num_cols);	\
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
		REMOTE_LOG(ERR, "failed OCIDefineByPos for %p column %d(" __dtypestr")\n", _stmthp, num_cols);		\
		throw r;																							\
	}																										\
}

unsigned int ocistmt::execute(void * column_list, void * rowid_list, void * out_list, bool auto_commit)
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
				case SQLT_IBFLOAT:
				case SQLT_BFLOAT:
				case SQLT_FLT:
					dat_len = sizeof(float);
					break;
				case SQLT_IBDOUBLE:
				case SQLT_BDOUBLE:
					dat_len = sizeof(double);
					break;
				case SQLT_INT:
					dat_len = sizeof(int);
					break;
				default:
					dat_len = _argsin[i].value_sz;
					break;
			}
			_argsin[i].datap = realloc(_argsin[i].datap, _argsin[i].datap_len + dat_len);
			memcpy((char*)_argsin[i].datap + _argsin[i].datap_len, valueparr[j], _argsin[i].alen[j]);
			_argsin[i].datap_len += (unsigned long)(dat_len);
		}
		switch(_argsin[i].dty) {
			case SQLT_BFLOAT:
			case SQLT_IBFLOAT:
				_argsin[i].dty = SQLT_FLT;
				break;
			case SQLT_BDOUBLE:
			case SQLT_IBDOUBLE:
				_argsin[i].dty = SQLT_BDOUBLE;
				break;
			case SQLT_TIMESTAMP:     _argsin[i].dty = INT_SQLT_TIMESTAMP;     break;
			case SQLT_TIMESTAMP_TZ:  _argsin[i].dty = INT_SQLT_TIMESTAMP_TZ;  break;
			case SQLT_TIMESTAMP_LTZ: _argsin[i].dty = INT_SQLT_TIMESTAMP_LTZ; break;
			case SQLT_INTERVAL_YM:   _argsin[i].dty = INT_SQLT_INTERVAL_YM;   break;
			case SQLT_INTERVAL_DS:   _argsin[i].dty = INT_SQLT_INTERVAL_DS;   break;
		}		
	}

	if(_argsin.size() > 0)
		_iters = _argsin[0].valuep.size();
	for(size_t i = 0; i < _argsin.size(); ++i) {
		if(_argsin[i].dty == SQLT_RSET) {

			if(_argsin[i].datap != NULL)
				free(_argsin[i].datap);

			// allocate the ref cursor statement handle
			r.handle = envhp;
			checkenv(&r, OCIHandleAlloc(envhp,							/* environment handle */
										(void **) &(_argsin[i].datap),	/* returned statement handle */
										OCI_HTYPE_STMT,					/* typ of handle to allocate */
										(size_t) 0,						/* optional extra memory size */
										(void **) NULL));				/* returned extra memeory */
			if(r.fn_ret != SUCCESS) {
   				REMOTE_LOG(ERR, "failed OCIHandleAlloc %s (%s)\n", r.gerrbuf, _stmtstr);
				throw r;
			}
			r.handle = _errhp;
			_argsin[i].value_sz = 0;
			checkerr(&r, OCIBindByName((OCIStmt*)_stmthp, (OCIBind**)(&_argsin[i].ocibind), (OCIError*)_errhp,
										(text*)(_argsin[i].name), -1,
										&(_argsin[i].datap), _argsin[i].value_sz, _argsin[i].dty,
										(dvoid*)NULL, (ub2*)NULL, (ub2*)NULL, 0, (ub4*)NULL,
										OCI_DEFAULT));
		} else {
			checkerr(&r, OCIBindByName((OCIStmt*)_stmthp, (OCIBind**)(&_argsin[i].ocibind), (OCIError*)_errhp,
										(text*)(_argsin[i].name), -1,
										_argsin[i].datap, _argsin[i].value_sz,
										_argsin[i].dty,
										&_argsin[i].ind[0], &_argsin[i].alen[0],
										(ub2*)NULL,0,
										(ub4*)NULL, OCI_DEFAULT));
		}
		if(r.fn_ret != SUCCESS) {
			REMOTE_LOG(ERR, "failed OCIBindByName error %s (%s)\n", r.gerrbuf, _stmtstr);
			ocisess->release_stmt(this);
			throw r;
		}
	}

	if (_stmtstr[0] != '\0') { // REF Cursors do not require Stmt Release
		do {
			/* execute the statement one at a time with retrive row-id */
			checkerr(&r, OCIStmtExecute((OCISvcCtx*)_svchp, (OCIStmt*)_stmthp, (OCIError*)_errhp, (_stmt_typ == OCI_STMT_SELECT ? 0 : row_count+1), row_count,
										(OCISnapshot *)NULL, (OCISnapshot *)NULL,
										OCI_DEFAULT));
			if(r.fn_ret != SUCCESS) {
				REMOTE_LOG(ERR, "failed OCIStmtExecute error %s (%s)\n", r.gerrbuf, _stmtstr);
				if(auto_commit) OCITransRollback((OCISvcCtx*)_svchp, (OCIError*)_errhp, OCI_DEFAULT);
				if (r.fn_ret != CONTINUE_WITH_ERROR) ocisess->release_stmt(this);
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
					OCIRowid *pRowID;
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

					// Get the row ID for the row that was just inserted.
					OraText *rowID;
					ub2 size = 0;
					OCIRowidToChar(pRowID, rowID, &size, (OCIError*)_errhp);
					rowID = new OraText[size + 1]; // Extra char for null termination.
					memset(rowID, 0, size + 1); // Set to all nulls so that string will be null terminated.
					checkerr(&r, OCIRowidToChar(pRowID, rowID, &size, (OCIError*)_errhp));
					if(r.fn_ret != SUCCESS) {
						REMOTE_LOG(ERR, "failed OCIRowidToChar error %s (%s)\n", r.gerrbuf, _stmtstr);
						if(auto_commit) OCITransRollback((OCISvcCtx*)_svchp, (OCIError*)_errhp, OCI_DEFAULT);
						ocisess->release_stmt(this);
						throw r;
					}
					(*intf.append_string_to_list)((char*)rowID, strlen((char*)rowID), rowid_list);
					delete rowID;
				} else {
					(*intf.append_string_to_list)(NULL, 0, rowid_list);
				}
			}

			++row_count;
		} while((size_t)row_count < _iters);

		if(auto_commit && _stmt_typ != OCI_STMT_SELECT) {
			/* commit */
			checkerr(&r, OCITransCommit((OCISvcCtx*)_svchp, (OCIError*)_errhp, OCI_DEFAULT));
			if(r.fn_ret != SUCCESS) {
				REMOTE_LOG(ERR, "failed OCITransCommit error %s (%s)\n", r.gerrbuf, _stmtstr);
				ocisess->release_stmt(this);
				throw r;
			}
		}
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
			if(_columns[i]->dtype == SQLT_NTY) {
				checkerr(&r, OCIObjectFree((OCIEnv*)ocisession::getenv(), (OCIError*)_errhp, (dvoid*)(_columns[i]->row_valp), OCI_OBJECTFREE_FORCE | OCI_OBJECTFREE_NONULL));
				if(r.fn_ret != SUCCESS)
					REMOTE_LOG(ERR, "failed OCIObjectFree for %p column %d reason %s (%s)\n", _stmthp, i, r.gerrbuf, _stmtstr);
			} else {
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
            case SQLT_BFLOAT:
			case SQLT_IBFLOAT:
				cur_clm.row_valp = new unsigned char[4];
				memset(cur_clm.row_valp, 0, sizeof(float));
				cur_clm.rtype = LCL_DTYPE_NONE;
				OCIDEF(SQLT_IBFLOAT, "SQLT_IBFLOAT");
				break;
            case SQLT_BDOUBLE:
			case SQLT_IBDOUBLE:
				cur_clm.row_valp = new unsigned char[8];
				memset(cur_clm.row_valp, 0, sizeof(double));
				cur_clm.rtype = LCL_DTYPE_NONE;
				OCIDEF(SQLT_IBDOUBLE, "SQLT_IBDOUBLE");
				break;
            case SQLT_FLT:
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
				cur_clm.dlen = cur_clm.dlen*2;
				cur_clm.row_valp = new text[cur_clm.dlen + 1];
				cur_clm.rtype = LCL_DTYPE_NONE;
				OCIDEF(SQLT_STR, "SQLT_STR");
                break;
			case SQLT_BIN: // RAW
				cur_clm.row_valp = new unsigned char[cur_clm.dlen + 1];
				memset(cur_clm.row_valp, 0, (cur_clm.dlen + 1)*sizeof(unsigned char));
				cur_clm.rtype = LCL_DTYPE_NONE;
				OCIDEF(SQLT_BIN, "SQLT_BIN");
				break;
			// 5 bytes buffer
            case SQLT_INTERVAL_YM:
				cur_clm.dlen = (cur_clm.dlen < 5 ? 5 : cur_clm.dlen);
				cur_clm.row_valp = new unsigned char[cur_clm.dlen];
				memset(cur_clm.row_valp, 0, cur_clm.dlen);
				cur_clm.rtype = LCL_DTYPE_NONE;
				OCIDEF(INT_SQLT_INTERVAL_YM, "INT_SQLT_INTERVAL_YM");
                break;
			// 7 bytes buffer
            case SQLT_DAT:
				cur_clm.dlen = (cur_clm.dlen < 7 ? 7 : cur_clm.dlen);
				cur_clm.row_valp = new unsigned char[cur_clm.dlen];
				memset(cur_clm.row_valp, 0, cur_clm.dlen);
				cur_clm.rtype = LCL_DTYPE_NONE;
				OCIDEF(SQLT_DAT, "SQLT_DAT");
                break;
			// 11 bytes buffers
            case SQLT_DATE:
				cur_clm.dlen = (cur_clm.dlen < 11 ? 11 : cur_clm.dlen);
				cur_clm.row_valp = new unsigned char[cur_clm.dlen];
				memset(cur_clm.row_valp, 0, cur_clm.dlen);
				cur_clm.rtype = LCL_DTYPE_NONE;
				OCIDEF(SQLT_DATE, "SQLT_DATE");
                break;
            case SQLT_TIMESTAMP:
				cur_clm.dlen = (cur_clm.dlen < 11 ? 11 : cur_clm.dlen);
				cur_clm.row_valp = new unsigned char[cur_clm.dlen];
				memset(cur_clm.row_valp, 0, cur_clm.dlen);
				cur_clm.rtype = LCL_DTYPE_NONE;
				OCIDEF(INT_SQLT_TIMESTAMP, "INT_SQLT_TIMESTAMP");
                break;
            case SQLT_TIMESTAMP_LTZ:
				cur_clm.dlen = (cur_clm.dlen < 11 ? 11 : cur_clm.dlen);
				cur_clm.row_valp = new unsigned char[cur_clm.dlen];
				memset(cur_clm.row_valp, 0, cur_clm.dlen);
				cur_clm.rtype = LCL_DTYPE_NONE;
				OCIDEF(INT_SQLT_TIMESTAMP_LTZ, "INT_SQLT_TIMESTAMP_LTZ");
                break;
            case SQLT_INTERVAL_DS:
				cur_clm.dlen = (cur_clm.dlen < 11 ? 11 : cur_clm.dlen);
				cur_clm.row_valp = new unsigned char[cur_clm.dlen];
				memset(cur_clm.row_valp, 0, cur_clm.dlen);
				cur_clm.rtype = LCL_DTYPE_NONE;
				OCIDEF(INT_SQLT_INTERVAL_DS, "INT_SQLT_INTERVAL_DS");
                break;
			// 13 bytes buffer
            case SQLT_TIMESTAMP_TZ:
				cur_clm.dlen = (cur_clm.dlen < 13 ? 13 : cur_clm.dlen);
				cur_clm.row_valp = new unsigned char[cur_clm.dlen];
				memset(cur_clm.row_valp, 0, cur_clm.dlen);
				cur_clm.rtype = LCL_DTYPE_NONE;
				OCIDEF(INT_SQLT_TIMESTAMP_TZ, "INT_SQLT_TIMESTAMP_TZ");
                break;
			case SQLT_RDD: {
				OCIALLOC(OCI_DTYPE_ROWID, "SQLT_RDD");
				void * _t = cur_clm.row_valp;
				ub4 _dlen = cur_clm.dlen;
				cur_clm.dlen = -1;
				cur_clm.row_valp = &(cur_clm.row_valp);
				OCIDEF(SQLT_RDD, "SQLT_RDD");
				cur_clm.row_valp = _t;
				cur_clm.dlen = _dlen;
                break;
			}
			case SQLT_RID: // 19 bytes buffer
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
			case SQLT_NTY: {
				text * col_typ_name = NULL;
				text * col_typ_schema_name = NULL;
				ub4 col_typ_name_len = 0;
				ub4 col_typ_schema_name_len = 0;
				checkerr(&r, OCIAttrGet((dvoid*) mypard, (ub4) OCI_DTYPE_PARAM,
										(dvoid**) &col_typ_name, (ub4 *) &col_typ_name_len, (ub4) OCI_ATTR_TYPE_NAME,
										(OCIError*)_errhp));
				if(r.fn_ret != SUCCESS) {
					REMOTE_LOG(ERR, "failed OCIAttrGet(OCI_ATTR_TYPE_NAME) error %s (%s)\n", r.gerrbuf, _stmtstr);
					if(mypard)
						OCIDescriptorFree(mypard, OCI_DTYPE_PARAM);
					ocisess->release_stmt(this);
					throw r;
				}				
				checkerr(&r, OCIAttrGet((dvoid*) mypard, (ub4) OCI_DTYPE_PARAM,
										(dvoid**) &col_typ_schema_name, (ub4 *) &col_typ_schema_name_len, (ub4) OCI_ATTR_SCHEMA_NAME,
										(OCIError*)_errhp));
				if(r.fn_ret != SUCCESS) {
					REMOTE_LOG(ERR, "failed OCIAttrGet(OCI_ATTR_TYPE_NAME) error %s (%s)\n", r.gerrbuf, _stmtstr);
					if(mypard)
						OCIDescriptorFree(mypard, OCI_DTYPE_PARAM);
					ocisess->release_stmt(this);
					throw r;
				}
				OCIType *tdo = NULL;
				checkerr(&r, OCITypeByName((OCIEnv*)ocisession::getenv(), (OCIError*)_errhp,  (const OCISvcCtx *) _svchp,
										   (text*)col_typ_schema_name, (ub4)col_typ_schema_name_len,  (text*)col_typ_name, (ub4) col_typ_name_len,  (text*)0, (ub4)0,
										   OCI_DURATION_SESSION, OCI_TYPEGET_ALL, (OCIType **)&tdo));
				if(r.fn_ret != SUCCESS) {
					REMOTE_LOG(ERR, "failed OCITypeByName(column:%d) error %s (%s)\n", num_cols, r.gerrbuf, _stmtstr);
					if(mypard)
						OCIDescriptorFree(mypard, OCI_DTYPE_PARAM);
					ocisess->release_stmt(this);
					throw r;
				}
				OCIDescribe *dschp = (OCIDescribe*)0;
				r.handle = (OCIEnv*)ocisession::getenv();
				checkerr(&r, OCIHandleAlloc((OCIEnv*)ocisession::getenv(), (dvoid **) &dschp, (ub4) OCI_HTYPE_DESCRIBE, (size_t) 0, (dvoid **) 0));
				if(r.fn_ret != SUCCESS) {
					REMOTE_LOG(ERR, "failed OCIHandleAlloc(column:%d) error %s (%s)\n", num_cols, r.gerrbuf, _stmtstr);
					if(mypard)
						OCIDescriptorFree(mypard, OCI_DTYPE_PARAM);
					ocisess->release_stmt(this);
					throw r;
				}
				r.handle = _errhp;
#if 0
text *namep;
ub4 text_len;
checkerr(&r, OCIDescribeAny((OCISvcCtx*)_svchp, (OCIError*)_errhp, (dvoid *) tdo, (ub4) 0, OCI_OTYPE_PTR, (ub1)1, (ub1) OCI_PTYPE_TYPE, dschp));
dvoid *parmp = (dvoid *)0;
checkerr(&r, OCIAttrGet((dvoid *) dschp, (ub4) OCI_HTYPE_DESCRIBE, (dvoid *)&parmp, (ub4 *)0, (ub4)OCI_ATTR_PARAM, (OCIError*)_errhp));
text *typenamep;
ub4 str_len;
checkerr(&r, OCIAttrGet((dvoid*) parmp,(ub4) OCI_DTYPE_PARAM, (dvoid*) &typenamep, (ub4 *) &str_len, (ub4) OCI_ATTR_NAME,(OCIError*)_errhp));
typenamep[str_len] = '\0';

/* loop through all attributes in the type */
ub2 count;
checkerr(&r, OCIAttrGet((dvoid*) parmp, (ub4) OCI_DTYPE_PARAM,(dvoid*) &count, (ub4 *) 0, (ub4) OCI_ATTR_NUM_TYPE_ATTRS,(OCIError*)_errhp));
dvoid *list_attr;
checkerr(&r, OCIAttrGet((dvoid*) parmp, (ub4) OCI_DTYPE_PARAM,(dvoid *)&list_attr, (ub4 *)0,(ub4)OCI_ATTR_LIST_TYPE_ATTRS,(OCIError*)_errhp));

dvoid *parmdp = (dvoid *)0;
for (ub2 pos = 1; pos <= count; pos++)
{
	checkerr(&r, OCIParamGet((dvoid*)list_attr, (ub4) OCI_DTYPE_PARAM, (OCIError*)_errhp, (dvoid**)&parmdp, (ub4) pos));
	checkerr(&r, OCIAttrGet((dvoid*)parmdp, (ub4) OCI_DTYPE_PARAM, (dvoid*) &namep, (ub4 *) &str_len, (ub4) OCI_ATTR_NAME, (OCIError*)_errhp));
    namep[str_len] = '\0';
}
#endif
				cur_clm.row_valp = NULL;
				checkerr(&r, OCIObjectNew((OCIEnv*)ocisession::getenv(), (OCIError*)_errhp,  (const OCISvcCtx*) _svchp,
										  OCI_TYPECODE_OBJECT, tdo, (dvoid*)NULL, OCI_DURATION_SESSION, TRUE, (dvoid**)&(cur_clm.row_valp)));
				if(r.fn_ret != SUCCESS) {
					REMOTE_LOG(ERR, "failed OCIObjectNew(column:%d) error %s (%s)\n", num_cols, r.gerrbuf, _stmtstr);
					if(mypard)
						OCIDescriptorFree(mypard, OCI_DTYPE_PARAM);
					ocisess->release_stmt(this);
					throw r;
				}
				dvoid *null_object = NULL;
				checkerr(&r, OCIObjectGetInd((OCIEnv*)ocisession::getenv(), (OCIError*)_errhp, cur_clm.row_valp, &null_object));
				if(r.fn_ret != SUCCESS) {
					REMOTE_LOG(ERR, "failed OCIObjectNew(column:%d) error %s (%s)\n", num_cols, r.gerrbuf, _stmtstr);
					if(mypard)
						OCIDescriptorFree(mypard, OCI_DTYPE_PARAM);
					ocisess->release_stmt(this);
					throw r;
				}
				OCIDefine *defnp = NULL;
				checkerr(&r, OCIDefineByPos((OCIStmt*)_stmthp, &defnp, (OCIError*)_errhp,
											num_cols, (dvoid *)(cur_clm.row_valp),
											(sword) cur_clm.dlen + 1, SQLT_NTY, &(cur_clm.indp), (ub2*)0,
											(ub2 *)0, OCI_DEFAULT));
				if(r.fn_ret != SUCCESS) {
					REMOTE_LOG(ERR, "failed OCIDefineByPos for %p column %d(SQLT_NTY)\n", _stmthp, num_cols);
					if(mypard)
						OCIDescriptorFree(mypard, OCI_DTYPE_PARAM);
					ocisess->release_stmt(this);
					throw r;
				}
				checkerr(&r, OCIDefineObject(defnp, (OCIError*)_errhp, (const OCIType*)tdo,
                        (dvoid**)&(cur_clm.row_valp), (ub4*)&(cur_clm.dlen),
						(dvoid**)NULL, (ub4*)0));
				if(r.fn_ret != SUCCESS) {
					REMOTE_LOG(ERR, "failed OCIDefineObject(column:%d) error %s (%s)\n", num_cols, r.gerrbuf, _stmtstr);
					if(mypard)
						OCIDescriptorFree(mypard, OCI_DTYPE_PARAM);
					ocisess->release_stmt(this);
					throw r;
				}
				REMOTE_LOG(ERR, "Column type %.*s.%.*s\n", col_typ_schema_name_len, col_typ_schema_name, col_typ_name_len, col_typ_name);				
				break;
			}
            default:
				r.fn_ret = FAILURE;
				SPRINT(r.gerrbuf, sizeof(r.gerrbuf), "[%s:%d] unsupporetd type %u\n", __FUNCTION__, __LINE__, cur_clm.dtype);
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

			(*intf.append_coldef_to_list)((char*)col_name, len, cur_clm.dtype, cur_clm.dlen, cur_clm.dprec, cur_clm.dscale, column_list);
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

	else if(_stmt_typ == OCI_STMT_BEGIN || _stmt_typ == OCI_STMT_CALL || _stmt_typ == OCI_STMT_DECLARE) {
		for(unsigned int i = 0; i < _argsin.size(); ++i) {
			if(_argsin[i].dir == DIR_OUT || _argsin[i].dir == DIR_INOUT) {
				switch (_argsin[i].dty) {
				case SQLT_BFLOAT:
				case SQLT_BDOUBLE:
				case SQLT_FLT:
					(*intf.append_flt_arg_tuple_to_list)((const unsigned char*)_argsin[i].name, strlen(_argsin[i].name), *(double*)(_argsin[i].datap), out_list);
					break;
				case SQLT_NUM:
				case SQLT_VNU:{
					unsigned long long len = _argsin[i].datap_len+1;
					if (_argsin[i].alen[0] > 0 && _argsin[i].alen[0] < len) len = _argsin[i].alen[0]+1;
					(*intf.append_bin_arg_tuple_to_list)((const unsigned char*)_argsin[i].name, strlen(_argsin[i].name),
														 (const unsigned char*)(_argsin[i].datap), len, out_list);
					break;}
				case SQLT_INT:
					(*intf.append_int_arg_tuple_to_list)((const unsigned char*)_argsin[i].name, strlen(_argsin[i].name), *(int*)(_argsin[i].datap), out_list);
					break;
				case SQLT_STR:
				case SQLT_CHR: {
					unsigned long long len = _argsin[i].datap_len;
					if (_argsin[i].alen[0] > 0 && _argsin[i].alen[0] < len) len = _argsin[i].alen[0];
					(*intf.append_bin_arg_tuple_to_list)((const unsigned char*)_argsin[i].name, strlen(_argsin[i].name),
														 (const unsigned char*)(_argsin[i].datap), len, out_list);
					break;}
				case SQLT_RSET:
					(*intf.append_cur_arg_tuple_to_list)((const unsigned char*)_argsin[i].name, strlen(_argsin[i].name),
														 (unsigned long long)_ocisess,
														 (unsigned long long)((ocisession*)_ocisess)->make_stmt(_argsin[i].datap), out_list);
					break;
				default:
					r.fn_ret = FAILURE;
					SPRINT(r.gerrbuf, sizeof(r.gerrbuf), "[%s:%d] unsupporetd type %u\n", __FUNCTION__, __LINE__, _argsin[i].dty);
					REMOTE_LOG(ERR, "Unsuported out variable type %d (%s)\n", _argsin[i].dty, _stmtstr);
					throw r;
					break;
				}
			}
		}
	}

	// Clear the in/out/inout arguments (if any)
	for(unsigned int i = 0; i < _argsin.size(); ++i) {
		if(_argsin[i].datap && _argsin[i].dty != SQLT_RSET) {
			free(_argsin[i].datap);
			_argsin[i].datap = NULL;
		}
		_argsin[i].datap_len = 0;
		_argsin[i].value_sz = 0;
		for (void* p : _argsin[i].valuep) free(p);
		_argsin[i].valuep.clear();
		_argsin[i].alen.clear();
	}

	if (row_count < 2) {
		checkerr(&r, OCIAttrGet(_stmthp, OCI_HTYPE_STMT, &row_count, 0, OCI_ATTR_ROW_COUNT, (OCIError*)_errhp));
		if(r.fn_ret != SUCCESS) {
			REMOTE_LOG(ERR, "failed OCIAttrGet(OCI_ATTR_ROW_COUNT) error %s (%s)\n", r.gerrbuf, _stmtstr);
			ocisess->release_stmt(this);
			throw r;
		}
	}

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
		REMOTE_LOG(ERR, "statement %s has no rows\n", _stmtstr);
		r.gerrcode = 0;
		SPRINT(r.gerrbuf, sizeof(r.gerrbuf), "statement has no rows");
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
	        row = (*intf.child_list)(row_list);
			for (unsigned int i = 0; i < _columns.size(); ++i) {
					switch (_columns[i]->dtype) {
					case SQLT_FLT:
					case SQLT_BFLOAT:
					case SQLT_IBFLOAT: // NULL is empty binary
						if(_columns[i]->indp < 0)
							(*intf.append_string_to_list)("", 0, row);
						else
							(*intf.append_float_to_list)((const unsigned char*)(_columns[i]->row_valp), row);
						memset(_columns[i]->row_valp, 0, sizeof(float));
						break;
					case SQLT_BDOUBLE:
					case SQLT_IBDOUBLE: // NULL is empty binary
						if(_columns[i]->indp < 0)
							(*intf.append_string_to_list)("", 0, row);
						else
							(*intf.append_double_to_list)((const unsigned char*)(_columns[i]->row_valp), row);
						memset(_columns[i]->row_valp, 0, sizeof(double));
						break;
					case SQLT_INT:
					case SQLT_UIN:
					case SQLT_VNU:
					case SQLT_NUM:
						(*intf.append_string_to_list)((char*)(_columns[i]->row_valp), _columns[i]->dlen, row);
						memset(_columns[i]->row_valp, 0, sizeof(OCINumber));
						break;
					case SQLT_DAT:
					case SQLT_DATE:
					case SQLT_TIMESTAMP:
					case SQLT_TIMESTAMP_TZ:
					case SQLT_TIMESTAMP_LTZ:
					case SQLT_INTERVAL_YM:
					case SQLT_INTERVAL_DS:
						(*intf.append_string_to_list)((char*)(_columns[i]->row_valp), _columns[i]->dlen, row);
						memset(_columns[i]->row_valp, 0, _columns[i]->dlen);
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
							(*intf.append_ext_tuple_to_list)((unsigned long long)_tlob, (unsigned long long)loblen, (const char*)dir, dlen, (const char*)file, flen, row);
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
							(*intf.append_tuple_to_list)((unsigned long long)_tlob, loblen, row);
							_columns[i]->loblps.push_back(_tlob);
						break;
					}
					case SQLT_CHR: {
						size_t str_len = _columns[i]->dlen;
						if(str_len > 0) // Handling for non NULL column
							str_len = strlen((char*)(_columns[i]->row_valp));
						(*intf.append_string_to_list)((char*)(_columns[i]->row_valp), str_len, row);
						memset(_columns[i]->row_valp, 0, _columns[i]->dlen);
						break;
					}
					case SQLT_RDD: {
						OCIRowid * pRowID = (OCIRowid*)_columns[i]->row_valp;
						ub2 size = 0;
						OraText *rowID;
						OCIRowidToChar(pRowID, rowID, &size, (OCIError*)_errhp);
						rowID = new OraText[size + 1]; // Extra char for null termination.
						memset(rowID, 0, size + 1); // Set to all nulls so that string will be null terminated.
						checkerr(&r, OCIRowidToChar(pRowID, rowID, &size, (OCIError*)_errhp));
						if(r.fn_ret != SUCCESS) {
							REMOTE_LOG(ERR, "failed OCIStmtExecute error %s (%s)\n", r.gerrbuf, _stmtstr);
							throw r;
						}
						size_t str_len = strlen((char*)rowID);
						(*intf.append_string_to_list)((char*)rowID, str_len, row);
						delete rowID;
						break;
					}
					case SQLT_BIN:
					case SQLT_RID:
					case SQLT_AFC:
					case SQLT_STR: {
						size_t str_len = strlen((char*)(_columns[i]->row_valp));
						(*intf.append_string_to_list)((char*)(_columns[i]->row_valp), str_len, row);
						memset(_columns[i]->row_valp, 0, str_len);
						break;
					}
					case SQLT_NTY:
						(*intf.append_string_to_list)((char*)(_columns[i]->row_valp), _columns[i]->dlen, row);
						memset(_columns[i]->row_valp, 0, _columns[i]->dlen);
						break;
					default:
						r.fn_ret = FAILURE;
						SPRINT(r.gerrbuf, sizeof(r.gerrbuf), "[%s:%d] unsupporetd type %u\n", __FUNCTION__, __LINE__, _columns[i]->dtype);
						REMOTE_LOG(ERR, "%s at row %d column %d (%s)\n", r.gerrbuf, num_rows, i, _stmtstr);
						throw r;
						break;
					}
			}
			total_est_row_size += (*intf.calculate_resp_size)(row);
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

	r.handle = envhp;
	ub2 csid = -1;
	checkerr(&r, OCILobCharSetId(envhp, (OCIError*)_errhp, lob, &csid));
	if(r.fn_ret != OCI_SUCCESS) {
		REMOTE_LOG(ERR, "failed OCILobCharSetForm for %p reason %s (%s)\n", lob, r.gerrbuf, _stmtstr);
		throw r;
	}
	r.handle = _errhp;

	// for CLOB or NCLOB allocating extra space
	// to accomodate character set encoding
	// since for CLOB and BLOB OCILobGetLength2
	// always returns number of characters
	if (csid != 0) {
		loblen *= 32;
		length = loblen;
	}
	ub1 *buf = new ub1[length+1];
	memset ((dvoid*)buf, '\0', length+1);
	oraub8 loblenc = 0;
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

	(*intf.binary_data)(buf, loblen, data);

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
		if(_columns[i]->dtype == SQLT_NTY) {
			checkerr(&r, OCIObjectFree((OCIEnv*)ocisession::getenv(), (OCIError*)_errhp, (dvoid*)(_columns[i]->row_valp), OCI_OBJECTFREE_FORCE | OCI_OBJECTFREE_NONULL));
			if(r.fn_ret != SUCCESS)
				REMOTE_LOG(ERR, "failed OCIObjectFree for %p column %d reason %s (%s)\n", _stmthp, i, r.gerrbuf, _stmtstr);
		} else {
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
		}
		delete _columns[i];
	}
	_columns.clear();

	if (_stmtstr[0] != '\0') { // REF Cursors do not require Stmt Release
		r.handle = _errhp;
		checkerr(&r, OCIStmtRelease((OCIStmt*)_stmthp, (OCIError*)_errhp, (OraText *) NULL, 0, OCI_DEFAULT));
		if(r.fn_ret != SUCCESS) {
			REMOTE_LOG(ERR, "failed OCIStmtRelease %s (%s)\n", r.gerrbuf, _stmtstr);
			throw r;
		}
	}

	(void) OCIHandleFree(_stmthp, OCI_HTYPE_STMT);
	(void) OCIHandleFree(_errhp, OCI_HTYPE_ERROR);

	delete _stmtstr;
	_stmtstr = NULL;
}
