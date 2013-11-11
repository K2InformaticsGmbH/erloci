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
	void commit(void);
	void rollback(void);
	void describe_object(void *objptr, unsigned int objptr_len, unsigned char objtyp, void *desc_list,
						 void (*append_desc_to_list)(const char * col_name, size_t len, const unsigned short data_type,
													 const unsigned int max_len, void * list));
	ocistmt* prepare_stmt(unsigned char *stmt, unsigned int stmt_len);
	void release_stmt(ocistmt *stmt);

	~ocisession(void);
};

#endif