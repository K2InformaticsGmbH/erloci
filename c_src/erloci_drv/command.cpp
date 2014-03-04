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

#include "command.h"
#include "ocisession.h"

#include "transcoder.h"
#include "term.h"

port & command::p			= port::instance();
transcoder & command::tc	= transcoder::instance();

extern void map_schema_to_bind_args(term &, vector<var> &);
extern size_t map_value_to_bind_args(term &, vector<var> &);

void command::config(
		void * (*child_list)(void *),								// child_list
		size_t (*calculate_resp_size)(void *),						// calculate_resp_size
		//void (*append_list_to_list)(const void *, void *),			// append_list_to_list
		void (*append_int_to_list)(const int, void *),				// append_int_to_list
		void (*append_string_to_list)(const char *, size_t, void *),// append_string_to_list
		// append_coldef_to_list
		void (*append_coldef_to_list)(const char *, size_t, const unsigned short, const unsigned int, const unsigned short, const signed char, void *),
		// append_desc_to_list
		void (*append_desc_to_list)(const char *, size_t, const unsigned short, const unsigned int, void *)
		)
{
	ocisession::config((ocisession::FNAD2L)append_desc_to_list);
	ocistmt::config((ocistmt::FNCDEFAPP)append_coldef_to_list,
					(ocistmt::FNSTRAPP)append_string_to_list,
					/*(ocistmt::FNLISTAPP)append_list_to_list,*/
					(ocistmt::FNSZAPP)calculate_resp_size,
					(ocistmt::FNCHLDLST)child_list);
}

bool command::change_log_flag(term & t)
{
    bool ret = false;
	term resp;

    // {undefined, RMOTE_MSG, DBG_FLAG_OFF/DBG_FLAG_ON}
    if(t[2].is_any_int()) {
		unsigned int log = t[2].v.ui;

        switch(log) {
        case DBG_FLAG_OFF:
            REMOTE_LOG(INF, "Disabling logging...");
            log_flag = false;
            REMOTE_LOG(CRT, "This line will never show up!!\n");
			resp.tuple()
				.add(t[0])
				.add(RMOTE_MSG);
			resp.insert().atom("log_disabled");
            break;
        case DBG_FLAG_ON:
            log_flag = true;
			resp.tuple()
				.add(t[0])
				.add(RMOTE_MSG);
			resp.insert().atom("log_enabled");
			REMOTE_LOG(INF, "Enabled logging...");
            break;
        default:
			resp.tuple()
				.add(t[0])
				.add(RMOTE_MSG);
			term & _t = resp.insert();
			_t.tuple();
			_t.insert().atom("error");
			_t.insert().atom("badarg");
			REMOTE_LOG(ERR, "ERROR badarg %d\n", log);
            break;
        }
    } else {
//		REMOTE_LOG_TERM(ERR, command, "argument type(s) missmatch\n");
	}

	if(resp.is_undef()) REMOTE_LOG(CRT, "driver error: no resp generated, shutting down port\n");
    vector<unsigned char> respv = tc.encode(resp);
    if(p.write_cmd(respv) <= 0)
        ret = true;

	return ret;
}

bool command::get_session(term & t)
{
    bool ret = false;
	term resp;

	// {{pid, ref}, GET_SESSN, Connection String, User name, Password}
	term & con_str = t[2];
	term & usr_str = t[3];
	term & passwrd = t[4];
    if(con_str.is_binary() && usr_str.is_binary() && passwrd.is_binary()) {

		   try {
				ocisession * conn_handle = new ocisession(
					&con_str.str[0], con_str.str_len,		// Connect String
					&usr_str.str[0], usr_str.str_len,		// User Name String
					&passwrd.str[0], passwrd.str_len);		// Password String
		        REMOTE_LOG(INF, "got connection %lu\n", (unsigned long long)conn_handle);
				resp.tuple()
					.add(t[0])
					.add(GET_SESSN)
					.add((unsigned long long)conn_handle);
		   } catch (intf_ret r) {
				resp.tuple()
					.add(t[0])
					.add(GET_SESSN)
					.add(term().tuple()
							.add(term().atom("error"))
							.add(term().tuple()
									.add(r.gerrcode)
									.add(term().strng(r.gerrbuf))));
				if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR %s\n", r.gerrbuf);
		   } catch (string str) {
				resp.tuple()
					.add(t[0])
					.add(GET_SESSN)
					.add(term().tuple()
							.add(term().atom("error"))
							.add(term().tuple()
									.add(0)
									.add(term().strng(str.c_str()))));
				if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR %s\n", str.c_str());
		   } catch (...) {
				resp.tuple()
					.add(t[0])
					.add(GET_SESSN)
					.add(term().tuple()
							.add(term().atom("error"))
							.add(term().tuple()
									.add(0)
									.add(term().atom("unknwon"))));
				if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR unknown\n");
		   }
	} else {
		//REMOTE_LOG_TERM(ERR, command, "argument type(s) missmatch\n");
	}

	if(resp.is_undef()) REMOTE_LOG(CRT, "driver error: no resp generated, shutting down port\n");
    vector<unsigned char> respv = tc.encode(resp);
    if(p.write_cmd(respv) <= 0)
        ret = true;

	return ret;
}

bool command::release_conn(term & t)
{
    bool ret = false;
    term resp;
	
	// {{pid, ref}, PUT_SESSN, Connection Handle}
	if(t[2].is_any_int()) {
		ocisession * conn_handle = (ocisession *)(t[2].v.ll);
		try {
			delete conn_handle;
			resp.tuple()
				.add(t[0])
				.add(PUT_SESSN)
				.add(term().atom("ok"));
		} catch (intf_ret r) {
			resp.tuple()
				.add(t[0])
				.add(PUT_SESSN)
				.add(term().tuple()
						.add(term().atom("error"))
						.add(term().tuple()
								.add(r.gerrcode)
								.add(term().strng(r.gerrbuf))));
			ret = true;
			if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR %s\n", r.gerrbuf);
		} catch (string str) {
			resp.tuple()
				.add(t[0])
				.add(PUT_SESSN)
				.add(term().tuple()
						.add(term().atom("error"))
						.add(term().tuple()
								.add(0)
								.add(term().strng(str.c_str()))));
			ret = true;
			if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR %s\n", str.c_str());
		} catch (...) {
			resp.tuple()
				.add(t[0])
				.add(PUT_SESSN)
				.add(term().tuple()
						.add(term().atom("error"))
						.add(term().tuple()
								.add(0)
								.add(term().atom("unknwon"))));
			ret = true;
			if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR unknown\n");
		}
    } else {
		//REMOTE_LOG_TERM(ERR, command, "argument type(s) missmatch\n");
	}

	if(resp.is_undef()) REMOTE_LOG(CRT, "driver error: no resp generated, shutting down port\n");
    vector<unsigned char> respv = tc.encode(resp);
    if(p.write_cmd(respv) <= 0)
        ret = true;

	return ret;
}

bool command::commit(term & t)
{
    bool ret = false;
    term resp;

	// {{pid, ref}, CMT_SESSN, Connection Handle}
	if(t[2].is_any_int()) {

		ocisession * conn_handle = (ocisession *)(t[2].v.ll);
		try {
			conn_handle->commit();
            resp.tuple()
				.add(t[0])
				.add(CMT_SESSN)
				.add(term().atom("ok"));
		} catch (intf_ret r) {
            resp.tuple()
				.add(t[0])
				.add(CMT_SESSN)
				.add(term().tuple()
						.add(term().atom("error"))
						.add(term().tuple()
								.add(r.gerrcode)
								.add(term().strng(r.gerrbuf))));
			ret = true;
			if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR %s\n", r.gerrbuf);
		} catch (string str) {
			resp.tuple()
				.add(t[0])
				.add(CMT_SESSN)
				.add(term().tuple()
						.add(term().atom("error"))
						.add(term().tuple()
								.add(0)
								.add(term().strng(str.c_str()))));
			ret = true;
			if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR %s\n", str.c_str());
		} catch (...) {
			resp.tuple()
				.add(t[0])
				.add(PUT_SESSN)
				.add(term().tuple()
						.add(term().atom("error"))
						.add(term().tuple()
								.add(0)
								.add(term().atom("unknwon"))));
			ret = true;
			if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR unknown\n");
		}
    } else {
		// REMOTE_LOG_TERM(ERR, command, "argument type(s) missmatch\n");
	}

	if(resp.is_undef()) REMOTE_LOG(CRT, "driver error: no resp generated, shutting down port\n");
    vector<unsigned char> respv = tc.encode(resp);
    if(p.write_cmd(respv) <= 0)
        ret = true;

	return ret;
}

bool command::rollback(term & t)
{
    bool ret = false;
    term resp;

	// {{pid, ref}, RBK_SESSN, Connection Handle}
	if(t[2].is_any_int()) {

		ocisession * conn_handle = (ocisession *)(t[2].v.ll);
		try {
			conn_handle->rollback();
            resp.tuple()
				.add(t[0])
				.add(RBK_SESSN)
				.add(term().atom("ok"));
		} catch (intf_ret r) {
            resp.tuple()
				.add(t[0])
				.add(RBK_SESSN)
				.add(term().tuple()
						.add(term().atom("error"))
						.add(term().tuple()
								.add(r.gerrcode)
								.add(term().strng(r.gerrbuf))));
			ret = true;
			if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR %s\n", r.gerrbuf);
		} catch (string str) {
			resp.tuple()
				.add(t[0])
				.add(RBK_SESSN)
				.add(term().tuple()
						.add(term().atom("error"))
						.add(term().tuple()
								.add(0)
								.add(term().strng(str.c_str()))));
			ret = true;
			if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR %s\n", str.c_str());
		} catch (...) {
			resp.tuple()
				.add(t[0])
				.add(RBK_SESSN)
				.add(term().tuple()
						.add(term().atom("error"))
						.add(term().tuple()
								.add(0)
								.add(term().atom("unknwon"))));
			ret = true;
			if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR unknown\n");
		}
    } else {
		//REMOTE_LOG_TERM(ERR, command, "argument type(s) missmatch\n");
	}

	if(resp.is_undef()) REMOTE_LOG(CRT, "driver error: no resp generated, shutting down port\n");
    vector<unsigned char> respv = tc.encode(resp);
    if(p.write_cmd(respv) <= 0)
        ret = true;

	return ret;
}

bool command::describe(term & t)
{
	bool ret = false;
    term resp;

	// {{pid, ref}, CMD_DSCRB, Connection Handle, Describe Object BinString, Describe Object Type int}
	term & connection = t[2];
	term & obj_string = t[3];
	term & objct_type = t[4];
    if(connection.is_any_int() && t[3].is_binary() && t[4].is_any_int()) {

		ocisession * conn_handle = (ocisession *)(connection.v.ll);
		unsigned char desc_typ = (unsigned char)(objct_type.v.ll);
		term describes;
		describes.lst();

		try {
	        conn_handle->describe_object(&obj_string.str[0], obj_string.str_len, desc_typ, &describes);
			resp.tuple()
				.add(t[0])
				.add(CMD_DSCRB)
				.add(term().tuple()
							.add(term().atom("desc"))
							.add(describes));
		} catch (intf_ret r) {
			resp.tuple()
				.add(t[0])
				.add(CMD_DSCRB)
				.add(term().tuple()
						.add(term().atom("error"))
						.add(term().tuple()
								.add(r.gerrcode)
								.add(term().strng(r.gerrbuf))));
			if (r.fn_ret == CONTINUE_WITH_ERROR) {
				if(resp.is_undef())
					REMOTE_LOG(INF, "Continue with ERROR Execute DESCRIBE \"%.*s;\" -> %s\n",
						t[3].str_len, t[3].str, r.gerrbuf);
			} else {
				if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR %s\n", r.gerrbuf);
				ret = true;
			}
		} catch (string str) {
			resp.tuple()
				.add(t[0])
				.add(CMD_DSCRB)
				.add(term().tuple()
						.add(term().atom("error"))
						.add(term().tuple()
								.add(0)
								.add(term().strng(str.c_str()))));
			ret = true;
			if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR %s\n", str.c_str());
		} catch (...) {
			resp.tuple()
				.add(t[0])
				.add(CMD_DSCRB)
				.add(term().tuple()
						.add(term().atom("error"))
						.add(term().tuple()
								.add(0)
								.add(term().atom("unknwon"))));
			ret = true;
			if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR unknown\n");
		}
    } else {
//		REMOTE_LOG_TERM(ERR, command, "argument type(s) missmatch\n");
	}

	if(resp.is_undef()) REMOTE_LOG(CRT, "driver error: no resp generated, shutting down port\n");
    vector<unsigned char> respv = tc.encode(resp);
    if(p.write_cmd(respv) <= 0)
        ret = true;

	return ret;
}

bool command::prep_sql(term & t)
{
	bool ret = false;
    term resp;

	// {{pid, ref}, PREP_STMT, Connection Handle, SQL String}
	term & connection = t[2];
	term & sql_string = t[3];
    if(connection.is_any_int() && sql_string.is_binary()) {

        //LOG_ARGS(ARG_COUNT(command), args, "Execute SQL statement");

		ocisession * conn_handle = (ocisession *)(connection.v.ll);
		try {
	        ocistmt * statement_handle = conn_handle->prepare_stmt((unsigned char *)&sql_string.str[0], sql_string.str_len);
			resp.tuple()
				.add(t[0])
				.add(PREP_STMT)
				.add(term().tuple()
						.add(term().atom("stmt"))
						.add((unsigned long long)statement_handle));
		} catch (intf_ret r) {
			resp.tuple()
				.add(t[0])
				.add(PREP_STMT)
				.add(term().tuple()
						.add(term().atom("error"))
						.add(term().tuple()
								.add(r.gerrcode)
								.add(term().strng(r.gerrbuf))));
			if (r.fn_ret == CONTINUE_WITH_ERROR) {
				if(resp.is_undef())
					REMOTE_LOG(INF, "Continue with ERROR Execute SQL \"%.*s;\" -> %s\n",
						t[3].str_len, t[3].str, r.gerrbuf);
			} else {
				if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR %s\n", r.gerrbuf);
				ret = true;
			}
		} catch (string str) {
			resp.tuple()
				.add(t[0])
				.add(PREP_STMT)
				.add(term().tuple()
						.add(term().atom("error"))
						.add(term().tuple()
								.add(0)
								.add(term().strng(str.c_str()))));
			ret = true;
			if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR %s\n", str.c_str());
		} catch (...) {
			resp.tuple()
				.add(t[0])
				.add(PREP_STMT)
				.add(term().tuple()
						.add(term().atom("error"))
						.add(term().tuple()
								.add(0)
								.add(term().atom("unknwon"))));
			ret = true;
			if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR unknown\n");
		}
    } else {
//		REMOTE_LOG_TERM(ERR, command, "argument type(s) missmatch\n");
	}

	if(resp.is_undef()) REMOTE_LOG(CRT, "driver error: no resp generated, shutting down port\n");
    vector<unsigned char> respv = tc.encode(resp);
    if(p.write_cmd(respv) <= 0)
        ret = true;

	return ret;
}

bool command::bind_args(term & t)
{
	bool ret = false;
    term resp;

	// {{pid, ref}, BIND_ARGS, Connection Handle, Statement Handle, BindList}
	term & conection = t[2];
	term & statement = t[3];
	term & bind_list = t[4];
    if(conection.is_any_int() && statement.is_any_int() && bind_list.is_list()) {
		ocisession * conn_handle = (ocisession *)(conection.v.ll);
		ocistmt * statement_handle = (ocistmt*)(statement.v.ll);

		try {
			if (!conn_handle->has_statement(statement_handle)) {
				resp.tuple()
					.add(t[0])
					.add(BIND_ARGS)
					.add(term().tuple()
							.add(term().atom("error"))
							.add(0)
							.add(term().strng("invalid statement handle")));
				if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR invalid statement handle\n");
			} else {
				map_schema_to_bind_args(bind_list, statement_handle->get_in_bind_args());
				resp.tuple()
					.add(t[0])
					.add(BIND_ARGS)
					.add(term().atom("ok"));
			}
		} catch (intf_ret r) {
			resp.tuple()
				.add(t[0])
				.add(BIND_ARGS)
				.add(term().tuple()
						.add(term().atom("error"))
						.add(term().tuple()
								.add(r.gerrcode)
								.add(term().strng(r.gerrbuf))));
			ret = true;
			if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR %s\n", r.gerrbuf);
		} catch (string & str) {
			resp.tuple()
				.add(t[0])
				.add(BIND_ARGS)
				.add(term().tuple()
						.add(term().atom("error"))
						.add(term().tuple()
								.add(0)
								.add(term().strng(str.c_str()))));
			ret = true;
			if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR %s\n", str.c_str());
		} catch (...) {
			resp.tuple()
				.add(t[0])
				.add(BIND_ARGS)
				.add(term().tuple()
						.add(term().atom("error"))
						.add(term().tuple()
								.add(0)
								.add(term().atom("unknwon"))));
			ret = true;
			if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR unknown\n");
		}
    } else {
		//REMOTE_LOG_TERM(ERR, command, "argument type(s) missmatch\n");
	}

	if(resp.is_undef()) REMOTE_LOG(CRT, "driver error: no resp generated, shutting down port\n");
    vector<unsigned char> respv = tc.encode(resp);
    if(p.write_cmd(respv) <= 0)
        ret = true;

    return ret;
}

bool command::exec_stmt(term & t)
{
	bool ret = false;
    term resp;

	// {{pid, ref}, EXEC_STMT, Connection Handle, Statement Handle, BindList, auto_commit}
	term & conection = t[2];
	term & statement = t[3];
	term & bind_list = t[4];
	term & auto_cmit = t[5];
    if(conection.is_any_int() && statement.is_any_int() && bind_list.is_list() && auto_cmit.is_any_int()) {
		ocisession * conn_handle = (ocisession *)(conection.v.ll);
		ocistmt * statement_handle = (ocistmt *)(statement.v.ll);
		bool auto_commit = (auto_cmit.v.i) > 0 ? true : false;
	    term columns, rowids;
		columns.lst();
		rowids.lst();
		try {
			if (!conn_handle->has_statement(statement_handle)) {
				resp.tuple()
					.add(t[0])
					.add(EXEC_STMT)
					.add(term().tuple()
							.add(term().atom("error"))
							.add(0)
							.add(term().strng("invalid statement handle")));
				if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR invalid statement handle\n");
			} else {
				size_t bound_count = map_value_to_bind_args(bind_list, statement_handle->get_in_bind_args());
				unsigned int exec_ret = statement_handle->execute(&columns, &rowids, auto_commit);
				if (bound_count) REMOTE_LOG(DBG, "Bounds %u", bound_count);
				// TODO : Also return bound values from here
				if (columns.length() == 0 && rowids.length() == 0)
					resp.tuple()
						.add(t[0])
						.add(EXEC_STMT)
						.add(term().tuple()
								.add(term().atom("executed"))
								.add(exec_ret));
				else if (columns.length() > 0 && rowids.length() == 0)
					resp.tuple()
						.add(t[0])
						.add(EXEC_STMT)
						.add(term().tuple()
								.add(term().atom("cols"))
								.add(columns));
				else if (columns.length() == 0 && rowids.length() > 0)
					resp.tuple()
						.add(t[0])
						.add(EXEC_STMT)
						.add(term().tuple()
								.add(term().atom("rowids"))
								.add(rowids));
				else {
					resp.tuple()
						.add(t[0])
						.add(EXEC_STMT)
						.add(term().tuple()
								.add(term().atom("cols"))
								.add(columns))
						.add(term().tuple()
								.add(term().atom("rowids"))
								.add(rowids));
				}
			}
		} catch (intf_ret r) {
			resp.tuple()
				.add(t[0])
				.add(EXEC_STMT)
				.add(term().tuple()
						.add(term().atom("error"))
						.add(term().tuple()
								.add(r.gerrcode)
								.add(term().strng(r.gerrbuf))));
			if (r.fn_ret == CONTINUE_WITH_ERROR) {
				if(resp.is_undef())
					REMOTE_LOG(INF, "Continue with ERROR Execute SQL \"%.*s;\" -> %s\n",
						t[3].str_len, t[3].str, r.gerrbuf);
			} else {
				if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR %s\n", r.gerrbuf);
				ret = true;
			}
		} catch (string str) {
			resp.tuple()
				.add(t[0])
				.add(EXEC_STMT)
				.add(term().tuple()
						.add(term().atom("error"))
						.add(term().tuple()
								.add(0)
								.add(term().strng(str.c_str()))));
			ret = true;
			if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR %s\n", str.c_str());
		} catch (...) {
			resp.tuple()
				.add(t[0])
				.add(EXEC_STMT)
				.add(term().tuple()
						.add(term().atom("error"))
						.add(term().tuple()
								.add(0)
								.add(term().atom("unknwon"))));
			ret = true;
			if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR unknown\n");
		}
	} else {
//		REMOTE_LOG_TERM(ERR, command, "argument type(s) missmatch\n");
	}

	if(resp.is_undef()) REMOTE_LOG(CRT, "driver error: no resp generated, shutting down port\n");
    vector<unsigned char> respv = tc.encode(resp);
    if(p.write_cmd(respv) <= 0)
        ret = true;

	return ret;
}

bool command::fetch_rows(term & t)
{
	bool ret = false;
    term resp;

	// {{pid, ref}, FTCH_ROWS, Connection Handle, Statement Handle, Rowcount}
	term & conection = t[2];
	term & statement = t[3];
	term & row_count = t[4];
    if(conection.is_any_int() && statement.is_any_int() && row_count.is_any_int()) {

		ocisession * conn_handle = (ocisession *)(conection.v.ll);
		ocistmt * statement_handle = (ocistmt*)(statement.v.ll);
        int rowcount = (row_count.v.i);

		term rows;
		rows.lst();
		try {
			if (!conn_handle->has_statement(statement_handle)) {
				resp.tuple()
					.add(t[0])
					.add(FTCH_ROWS)
					.add(term().tuple()
							.add(term().atom("error"))
							.add(0)
							.add(term().strng("invalid statement handle")));
				if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR invalid statement handle\n");
			} else {
				intf_ret r = statement_handle->rows(&rows, rowcount);
				if (r.fn_ret == MORE || r.fn_ret == DONE) {
					resp.tuple()
						.add(t[0])
						.add(FTCH_ROWS)
						.add(term().tuple()
									.add(term().tuple()
											.add(term().atom("rows"))
											.add(rows))
									.add(term().atom((r.fn_ret == MORE && rows.length() > 0) ? "false" : "true")));
				}
			}
		} catch (intf_ret r) {
			resp.tuple()
				.add(t[0])
				.add(FTCH_ROWS)
				.add(term().tuple()
						.add(term().atom("error"))
						.add(term().tuple()
								.add(r.gerrcode)
								.add(term().strng(r.gerrbuf))));
			if (r.fn_ret == CONTINUE_WITH_ERROR) {
				if(resp.is_undef())
					REMOTE_LOG(INF, "Continue with ERROR fetch STMT %s\n", r.gerrbuf);
			} else {
				if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR %s\n", r.gerrbuf);
				ret = true;
			}
		} catch (string str) {
			resp.tuple()
				.add(t[0])
				.add(FTCH_ROWS)
				.add(term().tuple()
						.add(term().atom("error"))
						.add(term().tuple()
								.add(0)
								.add(term().strng(str.c_str()))));
			ret = true;
			if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR %s\n", str.c_str());
		} catch (...) {
			resp.tuple()
				.add(t[0])
				.add(FTCH_ROWS)
				.add(term().tuple()
						.add(term().atom("error"))
						.add(term().tuple()
								.add(0)
								.add(term().atom("unknwon"))));
			ret = true;
			if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR unknown\n");
		}
    } else {
//		REMOTE_LOG_TERM(ERR, command, "argument type(s) missmatch\n");
	}

	if(resp.is_undef()) REMOTE_LOG(CRT, "driver error: no resp generated, shutting down port\n");
    vector<unsigned char> respv = tc.encode(resp);
    if(p.write_cmd(respv) <= 0)
        ret = true;

    return ret;
}

bool command::close_stmt(term & t)
{
	bool ret = false;
    term resp;

	// {{pid, ref}, CLSE_STMT, Connection Handle, Statement Handle}
    term & conection = t[2];
	term & statement = t[3];
	if(conection.is_any_int() && statement.is_any_int()) {

		ocisession * conn_handle = (ocisession *)(conection.v.ll);
		ocistmt * statement_handle = (ocistmt*)(statement.v.ll);
		try {
			if (!conn_handle->has_statement(statement_handle)) {
				resp.tuple()
					.add(t[0])
					.add(CLSE_STMT)
					.add(term().tuple()
							.add(term().atom("error"))
							.add(0)
							.add(term().strng("invalid statement handle")));
				if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR invalid statement handle\n");
			} else {
				statement_handle->close();
				resp.tuple()
					.add(t[0])
					.add(CLSE_STMT)
					.add(term().atom("ok"));
			}
		} catch (intf_ret r) {
			resp.tuple()
				.add(t[0])
				.add(CLSE_STMT)
				.add(term().tuple()
						.add(term().atom("error"))
						.add(term().tuple()
								.add(r.gerrcode)
								.add(term().strng(r.gerrbuf))));
			ret = true;
			if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR %s\n", r.gerrbuf);
		} catch (string str) {
			resp.tuple()
				.add(t[0])
				.add(CLSE_STMT)
				.add(term().tuple()
						.add(term().atom("error"))
						.add(term().tuple()
								.add(0)
								.add(term().strng(str.c_str()))));
			ret = true;
			if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR %s\n", str.c_str());
		} catch (...) {
			resp.tuple()
				.add(t[0])
				.add(CLSE_STMT)
				.add(term().tuple()
						.add(term().atom("error"))
						.add(term().tuple()
								.add(0)
								.add(term().atom("unknwon"))));
			ret = true;
			if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR unknown\n");
		}
    } else {
//		REMOTE_LOG_TERM(ERR, command, "argument type(s) missmatch\n");
	}

	if(resp.is_undef()) REMOTE_LOG(CRT, "driver error: no resp generated, shutting down port\n");
    vector<unsigned char> respv = tc.encode(resp);
    if(p.write_cmd(respv) <= 0)
        ret = true;

    return ret;
}

bool command::echo(term & t)
{
	bool ret = false;
    term resp;

	// {{pid, ref}, CMD_ECHOT, Term}
	try {
		resp.tuple()
			.add(t[0])
			.add(CMD_ECHOT)
			.add(t[2]);
	} catch (intf_ret r) {
		resp.tuple()
			.add(t[0])
			.add(CMD_ECHOT)
			.add(term().tuple()
					.add(term().atom("error"))
					.add(term().tuple()
							.add(r.gerrcode)
							.add(term().strng(r.gerrbuf))));
		ret = true;
			if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR %s\n", r.gerrbuf);
	} catch (string str) {
		resp.tuple()
			.add(t[0])
			.add(CMD_ECHOT)
			.add(term().tuple()
					.add(term().atom("error"))
					.add(term().tuple()
							.add(0)
							.add(term().strng(str.c_str()))));
		ret = true;
		if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR %s\n", str.c_str());
	} catch (...) {
		resp.tuple()
			.add(t[0])
			.add(CMD_ECHOT)
			.add(term().tuple()
					.add(term().atom("error"))
					.add(term().tuple()
							.add(0)
							.add(term().atom("unknwon"))));
		ret = true;
		if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR unknown\n");
	}    

	if(resp.is_undef()) REMOTE_LOG(CRT, "driver error: no resp generated, shutting down port\n");
    vector<unsigned char> respv = tc.encode(resp);
    if(p.write_cmd(respv) <= 0)
        ret = true;

    return ret;
}

//#define PRINTCMD

bool command::process(term & t)
{
	bool ret = false;
#ifdef PRINTCMD
	char * tmpbuf = print_term(command);
	REMOTE_LOG(DBG, "========================================\nCOMMAND : %s %s\n========================================\n",
		CMD_NAME_STR(ERL_INT_VALUE(cmd)),
		tmpbuf);
	delete tmpbuf;
#endif

	if(t.is_tuple() && t[1].is_integer()) {
        int cmd = t[1].v.i;
        if((t.length() - 1) != (size_t)CMD_ARGS_COUNT(cmd)) {
	    	term resp;
            resp.tuple();
			resp.add(t[0]);
			resp.insert().integer(cmd);
			term & _t = resp.insert().tuple();
	    	_t.insert().atom("error");
	    	_t.insert().atom("badarg");
	    	if(resp.is_undef())
                REMOTE_LOG(ERR, "ERROR badarg %s expected %d, got %d\n", CMD_NAME_STR(cmd)
                    , CMD_ARGS_COUNT(cmd), (t.length() - 1));
	        if(resp.is_undef()) REMOTE_LOG(CRT, "driver error: no resp generated, shutting down port\n");
            vector<unsigned char> respv = tc.encode(resp);
            p.write_cmd(respv);
	    } else {
		    switch(cmd) {
            case RMOTE_MSG:	ret = change_log_flag(t);	break;
            case GET_SESSN:	ret = get_session(t);		break;
            case PUT_SESSN:	ret = release_conn(t);		break;
		    case CMT_SESSN:	ret = commit(t);			break;
            case RBK_SESSN:	ret = rollback(t);			break;
            case CMD_DSCRB:	ret = describe(t);			break;
            case PREP_STMT:	ret = prep_sql(t);			break;
            case BIND_ARGS:	ret = bind_args(t);			break;
            case EXEC_STMT:	ret = exec_stmt(t);			break;
            case FTCH_ROWS:	ret = fetch_rows(t);		break;
            case CLSE_STMT:	ret = close_stmt(t);		break;
            case CMD_ECHOT:	ret = echo(t);				break;
            default:
		    	ret = true;
                break;
            }
        }
    }

//	PRINT_ERL_ALLOC("end");

	return ret;
}
