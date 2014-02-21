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

bool command::change_log_flag(term & t)
{
    bool ret = false;
	term resp;

	if((t.lt.size() - 1) != CMD_ARGS_COUNT(RMOTE_MSG)) {
		resp.tuple()
			.add(t.lt[0])
			.add(RMOTE_MSG)
			.add(term().tuple()
						.add(term().atom("error"))
						.add(term().atom("badarg")));
		if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR badarg %s expected %d, got %d\n", CMD_NAME_STR(RMOTE_MSG), CMD_ARGS_COUNT(RMOTE_MSG), (t.lt.size() - 1));
		ret = true;
	    goto error_exit;
	}

    // {undefined, RMOTE_MSG, DBG_FLAG_OFF/DBG_FLAG_ON}
    if(t.lt[2].is_any_int()) {
		unsigned int log = t.lt[2].v.ui;

        switch(log) {
        case DBG_FLAG_OFF:
            REMOTE_LOG(INF, "Disabling logging...");
            log_flag = false;
            REMOTE_LOG(CRT, "This line will never show up!!\n");
			resp.tuple()
				.add(t.lt[0])
				.add(RMOTE_MSG)
				.add(term().atom("log_disabled"));
            break;
        case DBG_FLAG_ON:
            log_flag = true;
			resp.tuple()
				.add(t.lt[0])
				.add(RMOTE_MSG)
				.add(term().atom("log_enabled"));
			REMOTE_LOG(INF, "Enabled logging...");
            break;
        default:
			resp.tuple()
				.add(t.lt[0])
				.add(RMOTE_MSG)
				.add(term().tuple()
						.add(term().atom("error"))
						.add(term().atom("badarg")));
			REMOTE_LOG(ERR, "ERROR badarg %d\n", log);
            break;
        }
    } else {
//		REMOTE_LOG_TERM(ERR, command, "argument type(s) missmatch\n");
	}

error_exit:
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

	if((t.lt.size() - 1) != CMD_ARGS_COUNT(GET_SESSN)) {
		resp.tuple()
			.add(t.lt[0])
			.add(GET_SESSN)
			.add(term().tuple()
						.add(term().atom("error"))
						.add(term().atom("badarg")));
		if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR badarg %s expected %d, got %d\n", CMD_NAME_STR(GET_SESSN), CMD_ARGS_COUNT(GET_SESSN), (t.lt.size() - 1));
		ret = true;
	    goto error_exit;
	}

	// {{pid, ref}, GET_SESSN, Connection String, User name, Password}
	term & con_str = t.lt[2];
	term & usr_str = t.lt[3];
	term & passwrd = t.lt[4];
    if(con_str.is_binary() && usr_str.is_binary() && passwrd.is_binary()) {

		   try {
				ocisession * conn_handle = new ocisession(
					con_str.str, con_str.str_len,		// Connect String
					usr_str.str, usr_str.str_len,		// User Name String
					passwrd.str, passwrd.str_len);		// Password String
		        REMOTE_LOG(INF, "got connection %lu\n", (unsigned long long)conn_handle);
				resp.tuple()
					.add(t.lt[0])
					.add(GET_SESSN)
					.add((unsigned long long)conn_handle);
		   } catch (intf_ret r) {
				resp.tuple()
					.add(t.lt[0])
					.add(GET_SESSN)
					.add(term().tuple()
							.add(term().atom("error"))
							.add(term().tuple()
									.add(r.gerrcode)
									.add(term().strng(r.gerrbuf))));
				if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR %s\n", r.gerrbuf);
		   } catch (string str) {
				resp.tuple()
					.add(t.lt[0])
					.add(GET_SESSN)
					.add(term().tuple()
							.add(term().atom("error"))
							.add(term().tuple()
									.add(0)
									.add(term().strng(str.c_str()))));
				if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR %s\n", str.c_str());
		   } catch (...) {
				resp.tuple()
					.add(t.lt[0])
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

error_exit:
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

    if((t.lt.size() - 1) != CMD_ARGS_COUNT(PUT_SESSN)) {
		resp.tuple()
			.add(t.lt[0])
			.add(PUT_SESSN)
			.add(term().tuple()
						.add(term().atom("error"))
						.add(term().atom("badarg")));
		if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR badarg %s expected %d, got %d\n", CMD_NAME_STR(PUT_SESSN), CMD_ARGS_COUNT(PUT_SESSN), (t.lt.size() - 1));
		ret = true;
	    goto error_exit;
	}
	
	// {{pid, ref}, PUT_SESSN, Connection Handle}
	if(t.lt[2].is_any_int()) {
		ocisession * conn_handle = (ocisession *)(t.lt[2].v.ll);
		try {
			delete conn_handle;
			resp.tuple()
				.add(t.lt[0])
				.add(PUT_SESSN)
				.add(term().atom("ok"));
		} catch (intf_ret r) {
			resp.tuple()
				.add(t.lt[0])
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
				.add(t.lt[0])
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
				.add(t.lt[0])
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

error_exit:
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

    if((t.lt.size() - 1) != CMD_ARGS_COUNT(CMT_SESSN)) {
		resp.tuple()
			.add(t.lt[0])
			.add(CMT_SESSN)
			.add(term().tuple()
						.add(term().atom("error"))
						.add(term().atom("badarg")));
		if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR badarg %s expected %d, got %d\n", CMD_NAME_STR(CMT_SESSN), CMD_ARGS_COUNT(CMT_SESSN), (t.lt.size() - 1));
		ret = true;
	    goto error_exit;
	}

	// {{pid, ref}, CMT_SESSN, Connection Handle}
	if(t.lt[2].is_any_int()) {

		ocisession * conn_handle = (ocisession *)(t.lt[2].v.ll);
		try {
			conn_handle->commit();
            resp.tuple()
				.add(t.lt[0])
				.add(CMT_SESSN)
				.add(term().atom("ok"));
		} catch (intf_ret r) {
            resp.tuple()
				.add(t.lt[0])
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
				.add(t.lt[0])
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
				.add(t.lt[0])
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

error_exit:
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

    if((t.lt.size() - 1) != CMD_ARGS_COUNT(RBK_SESSN)) {
		resp.tuple()
			.add(t.lt[0])
			.add(RBK_SESSN)
			.add(term().tuple()
						.add(term().atom("error"))
						.add(term().atom("badarg")));
		if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR badarg %s expected %d, got %d\n", CMD_NAME_STR(RBK_SESSN), CMD_ARGS_COUNT(RBK_SESSN), (t.lt.size() - 1));
		ret = true;
	    goto error_exit;
	}

	// {{pid, ref}, RBK_SESSN, Connection Handle}
	if(t.lt[2].is_any_int()) {

		ocisession * conn_handle = (ocisession *)(t.lt[2].v.ll);
		try {
			conn_handle->rollback();
            resp.tuple()
				.add(t.lt[0])
				.add(RBK_SESSN)
				.add(term().atom("ok"));
		} catch (intf_ret r) {
            resp.tuple()
				.add(t.lt[0])
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
				.add(t.lt[0])
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
				.add(t.lt[0])
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

error_exit:
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

    if((t.lt.size() - 1) != CMD_ARGS_COUNT(CMD_DSCRB)) {
		resp.tuple()
			.add(t.lt[0])
			.add(CMD_DSCRB)
			.add(term().tuple()
						.add(term().atom("error"))
						.add(term().atom("badarg")));
		if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR badarg %s expected %d, got %d\n", CMD_NAME_STR(CMD_DSCRB), CMD_ARGS_COUNT(CMD_DSCRB), (t.lt.size() - 1));
		ret = true;
	    goto error_exit;
	}

	// {{pid, ref}, CMD_DSCRB, Connection Handle, Describe Object BinString, Describe Object Type int}
	term & connection = t.lt[2];
	term & obj_string = t.lt[3];
	term & objct_type = t.lt[4];
    if(connection.is_any_int() && t.lt[3].is_binary() && t.lt[4].is_any_int()) {

		ocisession * conn_handle = (ocisession *)(connection.v.ll);
		unsigned char desc_typ = (unsigned char)(objct_type.v.ll);
		term describes;
		describes.list();

		try {
	        conn_handle->describe_object(obj_string.str, obj_string.str_len, desc_typ, &describes, append_desc_to_list);
			resp.tuple()
				.add(t.lt[0])
				.add(CMD_DSCRB)
				.add(term().tuple()
							.add(term().atom("desc"))
							.add(describes));
		} catch (intf_ret r) {
			resp.tuple()
				.add(t.lt[0])
				.add(CMD_DSCRB)
				.add(term().tuple()
						.add(term().atom("error"))
						.add(term().tuple()
								.add(r.gerrcode)
								.add(term().strng(r.gerrbuf))));
			if (r.fn_ret == CONTINUE_WITH_ERROR) {
				if(resp.is_undef())
					REMOTE_LOG(INF, "Continue with ERROR Execute DESCRIBE \"%.*s;\" -> %s\n",
						t.lt[3].str_len, t.lt[3].str, r.gerrbuf);
			} else {
				if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR %s\n", r.gerrbuf);
				ret = true;
			}
		} catch (string str) {
			resp.tuple()
				.add(t.lt[0])
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
				.add(t.lt[0])
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

error_exit:
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

    if((t.lt.size() - 1) != CMD_ARGS_COUNT(PREP_STMT)) {
		resp.tuple()
			.add(t.lt[0])
			.add(PREP_STMT)
			.add(term().tuple()
						.add(term().atom("error"))
						.add(term().atom("badarg")));
		if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR badarg %s expected %d, got %d\n", CMD_NAME_STR(PREP_STMT), CMD_ARGS_COUNT(PREP_STMT), (t.lt.size() - 1));
		ret = true;
	    goto error_exit;
	}

	// {{pid, ref}, PREP_STMT, Connection Handle, SQL String}
	term & connection = t.lt[2];
	term & sql_string = t.lt[3];
    if(connection.is_any_int() && sql_string.is_binary()) {

        //LOG_ARGS(ARG_COUNT(command), args, "Execute SQL statement");

		ocisession * conn_handle = (ocisession *)(connection.v.ll);
		try {
	        ocistmt * statement_handle = conn_handle->prepare_stmt((unsigned char *)sql_string.str, sql_string.str_len);
			resp.tuple()
				.add(t.lt[0])
				.add(PREP_STMT)
				.add(term().tuple()
						.add(term().atom("stmt"))
						.add((unsigned long long)statement_handle));
		} catch (intf_ret r) {
			resp.tuple()
				.add(t.lt[0])
				.add(PREP_STMT)
				.add(term().tuple()
						.add(term().atom("error"))
						.add(term().tuple()
								.add(r.gerrcode)
								.add(term().strng(r.gerrbuf))));
			if (r.fn_ret == CONTINUE_WITH_ERROR) {
				if(resp.is_undef())
					REMOTE_LOG(INF, "Continue with ERROR Execute SQL \"%.*s;\" -> %s\n",
						t.lt[3].str_len, t.lt[3].str, r.gerrbuf);
			} else {
				if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR %s\n", r.gerrbuf);
				ret = true;
			}
		} catch (string str) {
			resp.tuple()
				.add(t.lt[0])
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
				.add(t.lt[0])
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

error_exit:
	if(resp.is_undef()) REMOTE_LOG(CRT, "driver error: no resp generated, shutting down port\n");
    vector<unsigned char> respv = tc.encode(resp);
    if(p.write_cmd(respv) <= 0)
        ret = true;

	return ret;
}

extern void map_schema_to_bind_args(term &, vector<var> &);
bool command::bind_args(term & t)
{
	bool ret = false;
    term resp;

    if((t.lt.size() - 1) != CMD_ARGS_COUNT(BIND_ARGS)) {
		resp.tuple()
			.add(t.lt[0])
			.add(BIND_ARGS)
			.add(term().tuple()
						.add(term().atom("error"))
						.add(term().atom("badarg")));
		if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR badarg %s expected %d, got %d\n", CMD_NAME_STR(BIND_ARGS), CMD_ARGS_COUNT(BIND_ARGS), (t.lt.size() - 1));
		ret = true;
	    goto error_exit;
	}

	// {{pid, ref}, BIND_ARGS, Connection Handle, Statement Handle, BindList}
	term & conection = t.lt[2];
	term & statement = t.lt[3];
	term & bind_list = t.lt[4];
    if(conection.is_any_int() && statement.is_any_int() && bind_list.is_list()) {
		ocisession * conn_handle = (ocisession *)(conection.v.ll);
		ocistmt * statement_handle = (ocistmt*)(statement.v.ll);

		try {
			if (!conn_handle->has_statement(statement_handle)) {
				resp.tuple()
					.add(t.lt[0])
					.add(BIND_ARGS)
					.add(term().tuple()
							.add(term().atom("error"))
							.add(0)
							.add(term().strng("invalid statement handle")));
				if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR invalid statement handle\n");
			} else {
				map_schema_to_bind_args(bind_list, statement_handle->get_in_bind_args());
				resp.tuple()
					.add(t.lt[0])
					.add(BIND_ARGS)
					.add(term().atom("ok"));
			}
		} catch (intf_ret r) {
			resp.tuple()
				.add(t.lt[0])
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
				.add(t.lt[0])
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
				.add(t.lt[0])
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

error_exit:
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

    if((t.lt.size() - 1) != CMD_ARGS_COUNT(EXEC_STMT)){
		resp.tuple()
			.add(t.lt[0])
			.add(EXEC_STMT)
			.add(term().tuple()
						.add(term().atom("error"))
						.add(term().atom("badarg")));
		if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR badarg %s expected %d, got %d\n", CMD_NAME_STR(EXEC_STMT), CMD_ARGS_COUNT(EXEC_STMT), (t.lt.size() - 1));
		ret = true;
	    goto error_exit;
	}

	// {{pid, ref}, EXEC_STMT, Connection Handle, Statement Handle, BindList, auto_commit}
	term & conection = t.lt[2];
	term & statement = t.lt[3];
	term & bind_list = t.lt[4];
	term & auto_cmit = t.lt[5];
    if(conection.is_any_int() && statement.is_any_int() && bind_list.is_list() && auto_cmit.is_any_int()) {
		ocisession * conn_handle = (ocisession *)(conection.v.ll);
		ocistmt * statement_handle = (ocistmt *)(statement.v.ll);
		bool auto_commit = (auto_cmit.v.i) > 0 ? true : false;
	    term columns, rowids;
		columns.list();
		rowids.list();
		try {
			if (!conn_handle->has_statement(statement_handle)) {
				resp.tuple()
					.add(t.lt[0])
					.add(EXEC_STMT)
					.add(term().tuple()
							.add(term().atom("error"))
							.add(0)
							.add(term().strng("invalid statement handle")));
				if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR invalid statement handle\n");
			} else {
				size_t bound_count = map_value_to_bind_args(bind_list, statement_handle->get_in_bind_args());
				unsigned int exec_ret = statement_handle->execute(&columns, append_coldef_to_list, &rowids, append_string_to_list, auto_commit);
				if (bound_count) REMOTE_LOG(DBG, "Bounds %u", bound_count);
				// TODO : Also return bound values from here
				if (columns.length() == 0 && rowids.length() == 0)
					resp.tuple()
						.add(t.lt[0])
						.add(EXEC_STMT)
						.add(term().tuple()
								.add(term().atom("executed"))
								.add(exec_ret));
				else if (columns.length() > 0 && rowids.length() == 0)
					resp.tuple()
						.add(t.lt[0])
						.add(EXEC_STMT)
						.add(term().tuple()
								.add(term().atom("cols"))
								.add(columns));
				else if (columns.length() == 0 && rowids.length() > 0)
					resp.tuple()
						.add(t.lt[0])
						.add(EXEC_STMT)
						.add(term().tuple()
								.add(term().atom("rowids"))
								.add(rowids));
				else {
					resp.tuple()
						.add(t.lt[0])
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
				.add(t.lt[0])
				.add(EXEC_STMT)
				.add(term().tuple()
						.add(term().atom("error"))
						.add(term().tuple()
								.add(r.gerrcode)
								.add(term().strng(r.gerrbuf))));
			if (r.fn_ret == CONTINUE_WITH_ERROR) {
				if(resp.is_undef())
					REMOTE_LOG(INF, "Continue with ERROR Execute SQL \"%.*s;\" -> %s\n",
						t.lt[3].str_len, t.lt[3].str, r.gerrbuf);
			} else {
				if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR %s\n", r.gerrbuf);
				ret = true;
			}
		} catch (string str) {
			resp.tuple()
				.add(t.lt[0])
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
				.add(t.lt[0])
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

error_exit:
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

    if((t.lt.size() - 1) != CMD_ARGS_COUNT(FTCH_ROWS)) {
		resp.tuple()
			.add(t.lt[0])
			.add(FTCH_ROWS)
			.add(term().tuple()
						.add(term().atom("error"))
						.add(term().atom("badarg")));
		if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR badarg %s expected %d, got %d\n", CMD_NAME_STR(PREP_STMT), CMD_ARGS_COUNT(PREP_STMT), (t.lt.size() - 1));
		ret = true;
	    goto error_exit;
	}

	// {{pid, ref}, FTCH_ROWS, Connection Handle, Statement Handle, Rowcount}
	term & conection = t.lt[2];
	term & statement = t.lt[3];
	term & row_count = t.lt[4];
    if(conection.is_any_int() && statement.is_any_int() && row_count.is_any_int()) {

		ocisession * conn_handle = (ocisession *)(conection.v.ll);
		ocistmt * statement_handle = (ocistmt*)(statement.v.ll);
        int rowcount = (row_count.v.i);

		term rows;
		rows.list();
		try {
			if (!conn_handle->has_statement(statement_handle)) {
				resp.tuple()
					.add(t.lt[0])
					.add(FTCH_ROWS)
					.add(term().tuple()
							.add(term().atom("error"))
							.add(0)
							.add(term().strng("invalid statement handle")));
				if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR invalid statement handle\n");
			} else {
				intf_ret r = statement_handle->rows(&rows,
												   append_string_to_list,
												   append_list_to_list,
												   calculate_resp_size,
												   rowcount);
				if (r.fn_ret == MORE || r.fn_ret == DONE) {
					resp.tuple()
						.add(t.lt[0])
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
				.add(t.lt[0])
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
				.add(t.lt[0])
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
				.add(t.lt[0])
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

error_exit:
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

    if((t.lt.size() - 1) != CMD_ARGS_COUNT(CLSE_STMT)) {
		resp.tuple()
			.add(t.lt[0])
			.add(CLSE_STMT)
			.add(term().tuple()
						.add(term().atom("error"))
						.add(term().atom("badarg")));
		if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR badarg %s expected %d, got %d\n", CMD_NAME_STR(CLSE_STMT), CMD_ARGS_COUNT(CLSE_STMT), (t.lt.size() - 1));
		ret = true;
	    goto error_exit;
	}

	// {{pid, ref}, CLSE_STMT, Connection Handle, Statement Handle}
    term & conection = t.lt[2];
	term & statement = t.lt[3];
	if(conection.is_any_int() && statement.is_any_int()) {

		ocisession * conn_handle = (ocisession *)(conection.v.ll);
		ocistmt * statement_handle = (ocistmt*)(statement.v.ll);
		try {
			if (!conn_handle->has_statement(statement_handle)) {
				resp.tuple()
					.add(t.lt[0])
					.add(CLSE_STMT)
					.add(term().tuple()
							.add(term().atom("error"))
							.add(0)
							.add(term().strng("invalid statement handle")));
				if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR invalid statement handle\n");
			} else {
				statement_handle->close();
				resp.tuple()
					.add(t.lt[0])
					.add(CLSE_STMT)
					.add(term().atom("ok"));
			}
		} catch (intf_ret r) {
			resp.tuple()
				.add(t.lt[0])
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
				.add(t.lt[0])
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
				.add(t.lt[0])
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

error_exit:
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

    if((t.lt.size() - 1) != CMD_ARGS_COUNT(CMD_ECHOT)) {
		resp.tuple()
			.add(t.lt[0])
			.add(CMD_ECHOT)
			.add(term().tuple()
						.add(term().atom("error"))
						.add(term().atom("badarg")));
		if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR badarg %s expected %d, got %d\n", CMD_NAME_STR(CMD_ECHOT), CMD_ARGS_COUNT(CMD_ECHOT), (t.lt.size() - 1));
		ret = true;
	    goto error_exit;
	}

	// {{pid, ref}, CMD_ECHOT, Term}
	try {
		resp.tuple()
			.add(t.lt[0])
			.add(CMD_ECHOT)
			.add(t.lt[2]);
	} catch (intf_ret r) {
		resp.tuple()
			.add(t.lt[0])
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
			.add(t.lt[0])
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
			.add(t.lt[0])
			.add(CMD_ECHOT)
			.add(term().tuple()
					.add(term().atom("error"))
					.add(term().tuple()
							.add(0)
							.add(term().atom("unknwon"))));
		ret = true;
		if(resp.is_undef()) REMOTE_LOG(ERR, "ERROR unknown\n");
	}    

error_exit:
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

	if(t.is_tuple() && t.lt[1].is_integer()) {
		switch(t.lt[1].v.i) {
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

//	PRINT_ERL_ALLOC("end");

	return ret;
}