#include "ocisession.h"
#include <algorithm>

#include "oci_lib_intf.h"

#include <oci.h>

void * ocisession::envhp = NULL;
list<ocisession*> ocisession::_sessions;

ocisession::ocisession(const char * connect_str, const int connect_str_len,
					   const char * user_name, const int user_name_len,
					   const char * password, const int password_len)
{
	intf_ret r;
	OCIAuthInfo *authp = NULL;

	init();

	r.handle = envhp;

	// allocate error handle
	checkenv(&r, OCIHandleAlloc((OCIEnv*)envhp,	/* environment handle */
                            (void **) &_errhp,	/* returned err handle */
                            OCI_HTYPE_ERROR,	/* typ of handle to allocate */
                            (size_t) 0,			/* optional extra memory size */
                            (void **) NULL));	/* returned extra memeory */

	if(r.fn_ret != SUCCESS) {
   		REMOTE_LOG("failed OCISessionGet %s\n", r.gerrbuf);
        throw r;
	}

	// allocate auth handle
	checkenv(&r, OCIHandleAlloc((OCIEnv*)envhp,
							(void**)&authp, OCI_HTYPE_AUTHINFO,
							(size_t)0, (void **) NULL));

	r.handle = _errhp;

	// usrname and password
	checkerr(&r, OCIAttrSet(authp, OCI_HTYPE_AUTHINFO,
								(void*) user_name, user_name_len,
								OCI_ATTR_USERNAME, (OCIError *)_errhp));
	checkerr(&r, OCIAttrSet(authp, OCI_HTYPE_AUTHINFO,
								(void*) password, password_len,
								OCI_ATTR_PASSWORD, (OCIError *)_errhp));


    /* get the database connection */
    checkerr(&r, OCISessionGet((OCIEnv*)envhp, (OCIError *)_errhp,
                               (OCISvcCtx**)&_svchp,					/* returned database connection */
                               authp,									/* initialized authentication handle */                               
                               (OraText *) connect_str, connect_str_len,/* connect string */
                               NULL, 0, NULL, NULL, NULL,				/* session tagging parameters: optional */
                               OCI_DEFAULT));					/* modes */
	if(r.fn_ret != SUCCESS) {
		REMOTE_LOG("failed OCISessionGet %s\n", r.gerrbuf);
        throw r;
	}

	(void) OCIHandleFree(authp, OCI_HTYPE_AUTHINFO);

	REMOTE_LOG("got session %p %.*s user %.*s\n", _svchp, connect_str_len, connect_str, user_name_len, user_name);

	_sessions.push_back(this);
}

ocistmt* ocisession::prepare_stmt(OraText *stmt, ub4 stmt_len)
{
	ocistmt * statement = new ocistmt(this, stmt, stmt_len);
	_statements.push_back(statement);
	return statement;
}

void ocisession::release_stmt(ocistmt *stmt)
{
	list<ocistmt*>::iterator it = std::find(_statements.begin(), _statements.end(), stmt);
	if (it != _statements.end()) {
		_statements.remove(*it);
	}
}

ocisession::~ocisession(void)
{
	intf_ret r;

	// delete all the statements
	for (list<ocistmt*>::const_iterator it = _statements.begin(); it != _statements.end(); it++)
		(*it)->close();
	_statements.clear();

	checkerr(&r, OCISessionRelease((OCISvcCtx*)_svchp, (OCIError*)_errhp, NULL, 0, OCI_DEFAULT));
	if(r.fn_ret != SUCCESS) {
		REMOTE_LOG("failed OCISessionRelease %s\n", r.gerrbuf);
        throw r;
	}

	(void) OCIHandleFree(_errhp, OCI_HTYPE_ERROR);

	// cleanup the environment if this is the last oci session from this environment
	_sessions.remove(this);

	//REMOTE_LOG("release session %p\n", _svchp);
}

void ocisession::init(void)
{
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
}
