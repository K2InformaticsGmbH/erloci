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
#ifndef COMMAND_H
#define COMMAND_H

#include "marshal.h"

#include "erl_interface.h"
#include "ei.h"

#include "platform.h"
#include "port.h"
#include "transcoder.h"
#include "term.h"

class command
{
private:
	static port & p;
	static transcoder & tc;

	static bool change_log_flag(term &);
	static bool get_session(term &);
	static bool release_conn(term &);
	static bool commit(term &);
	static bool rollback(term &);
	static bool describe(term &);
	static bool prep_sql(term &);
	static bool fetch_rows(term &);
	static bool exec_stmt(term &);
	static bool close_stmt(term &);
	static bool echo(term &);
	static bool bind_args(term &);

public:
	static bool process(term &);
	static void config(
		void * (*)(void *),						// child_list
		size_t (*)(void *),						// calculate_resp_size
		//void (*)(const void *, void *),			// append_list_to_list
		void (*)(const int, void *),			// append_int_to_list
		void (*)(const char *, size_t, void *),	// append_string_to_list
		// append_coldef_to_list
		void (*)(const char *, size_t, const unsigned short, const unsigned int, const unsigned short, const signed char, void *),
		// append_desc_to_list
		void (*)(const char *, size_t, const unsigned short, const unsigned int, void *)
		);
};

#endif // COMMAND_H