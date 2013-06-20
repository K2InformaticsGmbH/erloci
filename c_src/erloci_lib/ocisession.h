#ifndef _OCISESSION_H_
#define _OCISESSION_H_

#pragma once

#include <iostream>
#include <list>

#include "ocistmt.h"

using namespace std;

class ocisession
{
private:
	static void * envhp;
	static list<ocisession*> _sessions;

	static void init();

	void *_svchp;
	void *_errhp;
	list<ocistmt*> _statements;

public:
	static inline void * getenv() { return envhp; };

	inline void *getsession() { return _svchp; }
	ocisession(const char * connect_str, const int connect_str_len,
		const char * user_name, const int user_name_len,
		const char * password, const int password_len);

	ocistmt* prepare_stmt(unsigned char *stmt, unsigned int stmt_len);
	void release_stmt(ocistmt *stmt);

	~ocisession(void);
};

#endif