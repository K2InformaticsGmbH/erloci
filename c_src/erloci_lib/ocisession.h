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
#ifndef OCISESSION_H
#define OCISESSION_H

#include <iostream>
#include <list>

#include "ocistmt.h"

using namespace std;

class ocisession
{
public:
	static void config(intf_funs);
	static inline void * getenv() { return envhp; };

	inline void *getsession() { return _svchp; }
	ocisession(const char * connect_str, size_t connect_str_len,
		const char * user_name, size_t user_name_len,
		const char * password, size_t password_len);
	void commit(void);
	void rollback(void);
	void describe_object(void *objptr, size_t objptr_len, unsigned char objtyp, void *desc_list);
	ocistmt* prepare_stmt(unsigned char *stmt, size_t stmt_len);
	ocistmt* make_stmt(void *stmt);
	void release_stmt(ocistmt *stmt);
	bool has_statement(ocistmt *stmt);

	~ocisession(void);

private:
	static intf_funs intf;
	static void * envhp;
	static void * stmt_lock;
	static list<ocisession*> _sessions;

	void *_svchp;
	void *_errhp;
	list<ocistmt*> _statements;
};

#endif // OCISESSION_H