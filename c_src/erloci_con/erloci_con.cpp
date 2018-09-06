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
#include <stdlib.h>
#include <string.h>
#include <oci.h>
#include <orid.h>

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

const char
	*tns = "(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=tcp)(HOST=127.0.0.1)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=XE)))",
	*usr = "scott",
	*pwd = "regit";

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

static void display_attr_val(OCIEnv *envhp, OCIError *errhp, text *names, OCITypeCode typecode, dvoid *attr_value)
{
	text           str_buf[200];
	double         dnum;
	ub4            text_len, str_len;
	OCIRaw         *raw = (OCIRaw *) 0;
	OCIString      *vs = (OCIString *) 0;

	/* display the data based on the type code */
	switch (typecode) {
	case OCI_TYPECODE_DATE :                    /* fixed length string */
	  str_len = 200;
	  (void) OCIDateToText(errhp, (CONST OCIDate *) attr_value, (CONST text*) "Month dd, SYYYY, HH:MI A.M.", (ub1) 27, (CONST text*) "American", (ub4) 8, (ub4 *)&str_len, str_buf);
	  str_buf[str_len+1] = '\0';
	  (void) printf("attr %s = %s\n", names, (text *) str_buf);
	  break;
	case OCI_TYPECODE_RAW :                                /* RAW */
		raw = *(OCIRaw **) attr_value;
		(void) printf("attr %s = %s\n", names, (text *) OCIRawPtr(envhp, raw));
		break;
	case OCI_TYPECODE_CHAR :                      /* fixed length string */
	case OCI_TYPECODE_VARCHAR :                              /* varchar  */
	case OCI_TYPECODE_VARCHAR2 :                             /* varchar2 */
		vs = *(OCIString **) attr_value;
		(void) printf("attr %s = %s\n", names, (text *) OCIStringPtr(envhp, vs));
		break;
	case OCI_TYPECODE_SIGNED8:                            /* BYTE - sb1  */
		(void) printf("attr %s = %d\n", names, *(sb1 *) attr_value);
		break;
	case OCI_TYPECODE_UNSIGNED8:                 /* UNSIGNED BYTE - ub1  */
		(void) printf("attr %s = %d\n", names, *(ub1 *) attr_value);
		break;
	case OCI_TYPECODE_OCTET:                                /* OCT  */
		(void) printf("attr %s = %d\n", names, *(ub1 *) attr_value);
		break;
	case OCI_TYPECODE_UNSIGNED16:                     /* UNSIGNED SHORT  */
	case OCI_TYPECODE_UNSIGNED32:                      /* UNSIGNED LONG  */
	case OCI_TYPECODE_REAL:                                   /* REAL    */
	case OCI_TYPECODE_DOUBLE:                                 /* DOUBLE  */
	case OCI_TYPECODE_INTEGER:                                   /* INT  */
	case OCI_TYPECODE_SIGNED16:                                /* SHORT  */
	case OCI_TYPECODE_SIGNED32:                                 /* LONG  */
	case OCI_TYPECODE_DECIMAL:                               /* DECIMAL  */
	case OCI_TYPECODE_FLOAT:                                 /* FLOAT    */
	case OCI_TYPECODE_NUMBER:                                /* NUMBER   */
	case OCI_TYPECODE_SMALLINT:                              /* SMALLINT */
		(void) OCINumberToReal(errhp, (CONST OCINumber *) attr_value, (uword)sizeof(dnum), (dvoid *) &dnum);
		(void) printf("attr %s = %f\n", names, dnum);
		break;
	default:
		(void) printf("attr %s - typecode %d\n", names, typecode);
		break;
	}
}

#include <list>
using namespace std;
typedef struct _cobject
{
	unsigned char *name;
	unsigned char *type;
	void * value;
	unsigned long long value_sz;
	list<struct _cobject> fields;
} cobject;

static bool dump_object(OCIEnv * envhp, OCIError * errhp, OCISvcCtx * svchp, void * tdo/* OCIType* or char* */, dvoid * obj, dvoid *null_obj, cobject & out_obj)
{
	text           *names[50], *lengths[50], *indexes[50], str_buf[200], *namep;
	ub1             status;
	ub2             count, pos;
	sb4             index;
	ub4             text_len, str_len, i;
	double          dnum;
	boolean         exist, eoc, boc;
	dvoid          *attr_null_struct, *attr_value, *object, *null_object, *list_attr, *parmp = (dvoid *) 0,
				   *parmdp = (dvoid *) 0, *parmp1 = (dvoid *) 0, *parmp2 = (dvoid *) 0, *element = (dvoid *) 0,
				   *null_element = (dvoid *) 0;
	OCIType        *object_tdo, *attr_tdo, *element_type;
	OCIInd          attr_null_status;
	OCITypeCode     typecode;
	OCITypeElem    *ado;
	OCIDescribe    *dschp = (OCIDescribe *) 0, *dschp1 = (OCIDescribe *) 0;
	OCIIter        *itr = (OCIIter *) 0;
	OCIRef         *elem_ref = (OCIRef *) 0, *type_ref, *tdo_ref;

	if(obj && (null_obj == NULL && (status = OCIObjectGetInd(envhp, errhp, (dvoid *)obj, (dvoid **) &null_obj)) != OCI_SUCCESS))
		(void) printf("BUG -- OCIObjectGetInd, expect OCI_SUCCESS.\n");

	err = OCIHandleAlloc((dvoid *) envhp, (dvoid **) &dschp, (ub4) OCI_HTYPE_DESCRIBE, (size_t) 0, (dvoid **) 0);
	if(err != OCI_SUCCESS) { checkerr(errhp, err, __LINE__); return true; }
	err = OCIDescribeAny(svchp, errhp, (dvoid *) tdo, (ub4) 0, OCI_OTYPE_PTR, (ub1)1, (ub1) OCI_PTYPE_TYPE, dschp);
	if(err != OCI_SUCCESS) {
		checkerr(errhp, err, __LINE__);

		// Try assuming tdo as string one more time
		err = OCIDescribeAny(svchp, errhp, (void*)tdo, (ub4)strlen((const char *)tdo), OCI_OTYPE_NAME, OCI_DEFAULT, OCI_PTYPE_TYPE, dschp);
		if(err != OCI_SUCCESS) { checkerr(errhp, err, __LINE__); return true; }
		tdo = NULL;
	}
	err = OCIAttrGet((dvoid *) dschp, (ub4) OCI_HTYPE_DESCRIBE, (dvoid *)&parmp, (ub4 *)0, (ub4)OCI_ATTR_PARAM, errhp);
	if(err != OCI_SUCCESS) { checkerr(errhp, err, __LINE__); return true; }
	err = OCIAttrGet((dvoid*) parmp,(ub4) OCI_DTYPE_PARAM,(dvoid*) &(out_obj.type), (ub4 *) &str_len,(ub4) OCI_ATTR_NAME, (OCIError *) errhp);
	if(err != OCI_SUCCESS) { checkerr(errhp, err, __LINE__); return true; }
	
	if(tdo == NULL) {
		err = OCIAttrGet((dvoid*) parmp,(ub4) OCI_DTYPE_PARAM,(dvoid*) &tdo_ref, (ub4 *) 0,(ub4) OCI_ATTR_REF_TDO,(OCIError *) errhp);
		if(err != OCI_SUCCESS) { checkerr(errhp, err, __LINE__); return true; }
		err = OCITypeByRef(envhp, errhp, tdo_ref, OCI_DURATION_SESSION,OCI_TYPEGET_HEADER, (OCIType**)&tdo);
		if(err != OCI_SUCCESS) { checkerr(errhp, err, __LINE__); return true; }
	}

	out_obj.type[str_len] = '\0';
	printf("starting displaying instance of type '%s'\n", out_obj.type);

	/* loop through all attributes in the type */
	err = OCIAttrGet((dvoid*) parmp, (ub4) OCI_DTYPE_PARAM, (dvoid*) &count, (ub4 *) 0, (ub4) OCI_ATTR_NUM_TYPE_ATTRS, (OCIError *) errhp);
	if(err != OCI_SUCCESS) { checkerr(errhp, err, __LINE__); return true; }
	err = OCIAttrGet((dvoid *) parmp, (ub4) OCI_DTYPE_PARAM, (dvoid *)&list_attr, (ub4 *)0, (ub4)OCI_ATTR_LIST_TYPE_ATTRS, (OCIError *)errhp);
	if(err != OCI_SUCCESS) { checkerr(errhp, err, __LINE__); return true; }

	/* loop through all attributes in the type */
	for (pos = 1; pos <= count; pos++) {
		cobject other_obj;
		err = OCIParamGet((dvoid *) list_attr, (ub4) OCI_DTYPE_PARAM, errhp, (dvoid**)&parmdp, (ub4) pos);
		if(err != OCI_SUCCESS) { checkerr(errhp, err, __LINE__); return true; }
		err = OCIAttrGet((dvoid*) parmdp, (ub4) OCI_DTYPE_PARAM, (dvoid*) &(other_obj.name), (ub4 *) &str_len, (ub4) OCI_ATTR_NAME, (OCIError *) errhp);
		if(err != OCI_SUCCESS) { checkerr(errhp, err, __LINE__); return true; }
		other_obj.name[str_len] = '\0';

		/* get the attribute */
		if (obj && (OCIObjectGetAttr(envhp, errhp, obj, null_obj, (OCIType*)tdo, (const oratext**)&(other_obj.name), &str_len, 1, (ub4 *)0, 0, &attr_null_status, &attr_null_struct, &attr_value, &attr_tdo) != OCI_SUCCESS))
			(void) printf("BUG -- OCIObjectGetAttr, expect OCI_SUCCESS.\n");

		/* get the type code of the attribute */
		err = OCIAttrGet((dvoid*) parmdp, (ub4) OCI_DTYPE_PARAM, (dvoid*) &typecode, (ub4 *) 0, (ub4) OCI_ATTR_TYPECODE, (OCIError *) errhp);
		if(err != OCI_SUCCESS) { checkerr(errhp, err, __LINE__); return true; }

		/* support only fixed length string, ref and embedded object */
		switch (typecode) {
		case OCI_TYPECODE_OBJECT:                     /* embedded object */
			printf("attribute %s is an embedded object. Display instance ....\n", other_obj.name);

			/* recursive call to dump nested object data */
			if(obj == NULL) {
				err = OCIAttrGet((dvoid*) parmdp,(ub4) OCI_DTYPE_PARAM,(dvoid*) &tdo_ref, (ub4 *) 0,(ub4) OCI_ATTR_REF_TDO,(OCIError *) errhp);
				if(err != OCI_SUCCESS) { checkerr(errhp, err, __LINE__); return true; }
				err = OCITypeByRef(envhp, errhp, tdo_ref, OCI_DURATION_SESSION,OCI_TYPEGET_HEADER, (OCIType**)&attr_tdo);
				if(err != OCI_SUCCESS) { checkerr(errhp, err, __LINE__); return true; }
				attr_value = NULL;
				attr_null_struct = NULL;
			}
			if(dump_object(envhp, errhp, svchp, attr_tdo, attr_value, attr_null_struct, other_obj))
				return true;
			out_obj.fields.push_back(other_obj);
			break;
       case OCI_TYPECODE_REF :                        /* embedded object */
			printf("attribute %s is a ref. Pin and display instance ...\n", namep);

			/* pin the object */
			if(OCIObjectPin(envhp, errhp, *(OCIRef **)attr_value, (OCIComplexObject *)0, OCI_PIN_ANY, OCI_DURATION_SESSION, OCI_LOCK_NONE, (dvoid **)&object) != OCI_SUCCESS)
				(void) printf("BUG -- OCIObjectPin, expect OCI_SUCCESS.\n");

			/* allocate the ref */
			if((status = OCIObjectNew(envhp, errhp, svchp, OCI_TYPECODE_REF, (OCIType *)0, (dvoid *)0, OCI_DURATION_DEFAULT, TRUE, (dvoid **) &type_ref))  != OCI_SUCCESS)
				(void) printf("BUG -- OCIObjectNew, expect OCI_SUCCESS.\n");

			/* get the ref of the type from the object */
			if((status = OCIObjectGetTypeRef(envhp, errhp, object, type_ref)) != OCI_SUCCESS)
				(void) printf("BUG -- ORIOGTR, expect OCI_SUCCESS.\n");

			/* pin the type ref to get the type object */
			if(OCIObjectPin(envhp, errhp, type_ref,  (OCIComplexObject *)0, OCI_PIN_ANY, OCI_DURATION_SESSION, OCI_LOCK_NONE, (dvoid **)&object_tdo) != OCI_SUCCESS)
				(void) printf("BUG -- OCIObjectPin, expect OCI_SUCCESS.\n");

			/* get null struct of the object */
			if (( status = OCIObjectGetInd(envhp, errhp, object, &null_object)) != OCI_SUCCESS)
				(void) printf("BUG -- ORIOGNS, expect OCI_SUCCESS.\n");

			/* call the function recursively to dump the pinned object */
			if(dump_object(envhp, errhp, svchp, object_tdo, object, null_object, other_obj))
				return true;
			out_obj.fields.push_back(other_obj);
			break;
       case OCI_TYPECODE_NAMEDCOLLECTION:
			err = OCIHandleAlloc((dvoid *) envhp, (dvoid **) &dschp1, (ub4) OCI_HTYPE_DESCRIBE, (size_t) 0, (dvoid **) 0);
			err = OCIDescribeAny(svchp, errhp, (dvoid *) attr_tdo,(ub4) 0, OCI_OTYPE_PTR, (ub1)1,(ub1) OCI_PTYPE_TYPE, dschp1);
			err = OCIAttrGet((dvoid *) dschp1, (ub4) OCI_HTYPE_DESCRIBE, (dvoid *)&parmp1, (ub4 *)0, (ub4)OCI_ATTR_PARAM, errhp);

			/* get the collection type code of the attribute */
			err = OCIAttrGet((dvoid*) parmp1, (ub4) OCI_DTYPE_PARAM,(dvoid*) &typecode, (ub4 *) 0,(ub4) OCI_ATTR_COLLECTION_TYPECODE,(OCIError *) errhp);

           switch (typecode) {
		   case OCI_TYPECODE_VARRAY:                /* variable array */
				(void) printf("\n---> Dump the table from the top to the bottom.\n");
				err = OCIAttrGet((dvoid*) parmp1, (ub4)OCI_DTYPE_PARAM,(dvoid*) &parmp2, (ub4 *) 0,(ub4) OCI_ATTR_COLLECTION_ELEMENT,(OCIError *) errhp);
				err = OCIAttrGet((dvoid*) parmp2,(ub4) OCI_DTYPE_PARAM,(dvoid*) &elem_ref, (ub4 *) 0,(ub4) OCI_ATTR_REF_TDO,(OCIError *) errhp);
				err = OCITypeByRef(envhp, errhp, elem_ref, OCI_PIN_DEFAULT,(OCITypeGetOpt)OCI_TYPEGET_HEADER, &element_type);

				/* initialize the iterator */
				err = OCIIterCreate(envhp, errhp, (CONST OCIColl*)attr_value, &itr);

				/* loop through the iterator */
				for(eoc = FALSE;!OCIIterNext(envhp, errhp, itr, (dvoid **) &element,(dvoid **)&null_element, &eoc) && !eoc;) {
					/* if type is named type, call the same function recursively */
					if (typecode == OCI_TYPECODE_OBJECT)
						dump_object(envhp, errhp, svchp, element_type, element, null_element, other_obj);
					else  /* else, display the scaler type attribute */
						display_attr_val(envhp, errhp, namep, typecode, element);
				}
				out_obj.fields.push_back(other_obj);
				break;
			case OCI_TYPECODE_TABLE:                    /* nested table */
				(void) printf("\n---> Dump the table from the top to the bottom.\n");

				/* go to the first element and print out the index */
				err = OCIAttrGet((dvoid*) parmp1, (ub4) OCI_DTYPE_PARAM, (dvoid*) &parmp2, (ub4 *) 0, (ub4) OCI_ATTR_COLLECTION_ELEMENT, (OCIError *) errhp);
				err = OCIAttrGet((dvoid*) parmp2,(ub4) OCI_DTYPE_PARAM,(dvoid*) &elem_ref, (ub4 *) 0,(ub4) OCI_ATTR_REF_TDO,(OCIError *) errhp);
				err = OCITypeByRef(envhp, errhp, elem_ref, OCI_DURATION_SESSION,OCI_TYPEGET_HEADER, &element_type);

				attr_value = *(dvoid **)attr_value;

				/* move to the first element in the nested table */
				err = OCITableFirst(envhp, errhp, (CONST OCITable*) attr_value, &index);

				(void) printf("     The index of the first element is : %d.\n", index);

				/* print out the element */
				err = OCICollGetElem(envhp, errhp,(CONST OCIColl *) attr_value, index,&exist, (dvoid **) &element,(dvoid **) &null_element);

				/* if it is named type, recursively call the same function */
				err = OCIAttrGet((dvoid*) parmp2,(ub4) OCI_DTYPE_PARAM,(dvoid*) &typecode, (ub4 *) 0,(ub4) OCI_ATTR_TYPECODE,(OCIError *) errhp);

				if (typecode == OCI_TYPECODE_OBJECT)
					dump_object(envhp, errhp, svchp, element_type, (dvoid *)element, (dvoid *)null_element, other_obj);
				else
					display_attr_val(envhp, errhp, namep, typecode, element);

				for(;!OCITableNext(envhp, errhp, index, (CONST OCITable *) attr_value, &index, &exist) && exist;) {
					err = OCICollGetElem(envhp, errhp, (CONST OCIColl *)attr_value, index,&exist, (dvoid **) &element, (dvoid **) &null_element);
					if (typecode == OCI_TYPECODE_OBJECT)
						dump_object(envhp, errhp, svchp, element_type, (dvoid *)element, (dvoid *)null_element, other_obj);
					else
						display_attr_val(envhp, errhp, namep, typecode, element);
				}
				out_obj.fields.push_back(other_obj);
				break;
			default:
				break;
		   }
           err = OCIHandleFree((dvoid *) dschp1, (ub4) OCI_HTYPE_DESCRIBE);
           break;
		default:   /* scaler type, display the attribute value */
			if(obj) {
				if (attr_null_status == OCI_IND_NOTNULL) {
					display_attr_val(envhp, errhp, namep, typecode, attr_value);
				} else printf("attr %s is null\n", namep);
			}
			out_obj.fields.push_back(other_obj);
			break;
		}
	}
	err = OCIHandleFree((dvoid*) dschp, (ub4) OCI_HTYPE_DESCRIBE);
	printf("finishing displaying instance of type '%s'\n", out_obj.type);
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
	dvoid * obj = NULL;
	dvoid * null_obj = NULL;
	cobject o;

	if(setup_env())
		goto error_return;

	if(statement("select rowid from myiot"))
		goto error_return;

	// http://docs.oracle.com/cd/B28359_01/appdev.111/b28395/oci03typ.htm#i423684

	OCIRowid * pRowID;
	err = OCIDescriptorAlloc(envhp, (void**)&pRowID, OCI_DTYPE_ROWID, 0, NULL);
	if(err != OCI_SUCCESS) {
		checkerr(errhp, err, __LINE__);
		return true;
	}
	err = OCIDefineByPos(stmthp, &defnp, errhp, (ub4)1, (void*)&pRowID, (sb4)0, (ub2) SQLT_RDD,
		(void*)0, (ub2*)0, (ub2*)0, (ub4) OCI_DEFAULT);
	if(err != OCI_SUCCESS) {
		checkerr(errhp, err, __LINE__);
		return true;
	}

	err = OCIStmtFetch((OCIStmt*)stmthp, (OCIError*)errhp, 1, OCI_FETCH_NEXT, OCI_DEFAULT);
	if(err != OCI_SUCCESS) {
		checkerr(errhp, err, __LINE__);
		return true;
	}

	ub2 size = 4999;
	OraText *rowID = new OraText[size + 1];
	memset(rowID, 0, 19);

	err = OCIRowidToChar(pRowID, rowID, &size, errhp);
	if(err != OCI_SUCCESS) {
		checkerr(errhp, err, __LINE__);
		return true;
	}

	printf("ROWID %s\n", (unsigned char *)rowID);
#if 0
	if(dump_object(envhp, errhp, svchp, "sys.aq$_jms_map_message", obj, null_obj, o))
	//if(describe("sys.aq$_jms_map_message"))
		goto error_return;
#endif

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
