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
#include "stdafx.h"

#include <stdio.h>
#include <stdlib.h>

#include "cmd_processors.h"
#include "ocisession.h"

bool change_log_flag(ETERM * command)
{
    bool ret = false;
    ETERM *resp;

    ETERM **args;
    MAP_ARGS(CMD_ARGS_COUNT(RMOTE_MSG), command, args);

	if(ARG_COUNT(command) != CMD_ARGS_COUNT(RMOTE_MSG)) {
	    resp = erl_format((char*)"{~w,~i,{error,badarg}}", args[0], RMOTE_MSG);
		REMOTE_LOG("ERROR badarg\n");
		ret = true;
	    goto error_exit;
	}

    // Args : {log, DBG_FLAG_OFF/DBG_FLAG_ON}
    if(ERL_IS_INTEGER(args[1])) {
		unsigned int log = ERL_INT_UVALUE(args[1]);

        switch(log) {
        case DBG_FLAG_OFF:
            REMOTE_LOG("Disabling logging...");
            log_flag = false;
            REMOTE_LOG("This line will never show up!!\n");
            resp = erl_format((char*)"{~w,~i,log_disabled}", args[0], RMOTE_MSG);
            break;
        case DBG_FLAG_ON:
            log_flag = true;
            resp = erl_format((char*)"{~w,~i,log_enabled}", args[0], RMOTE_MSG);
            REMOTE_LOG("Enabled logging...\n");
            break;
        default:
            resp = erl_format((char*)"{~w,~i,{error,badarg}}", args[0], RMOTE_MSG);
			REMOTE_LOG("ERROR badarg %d\n", log);
            break;
        }
    }

error_exit:
    if(write_resp(resp) < 0)
        ret = true;

    erl_free_compound(command);
	UNMAP_ARGS(CMD_ARGS_COUNT(RMOTE_MSG), args);

	return ret;
}

bool cmd_get_session(ETERM * command)
{
    bool ret = false;
	ETERM *resp;

    ETERM **args;
    MAP_ARGS(CMD_ARGS_COUNT(GET_SESSN), command, args);

	if(ARG_COUNT(command) != CMD_ARGS_COUNT(GET_SESSN)) {
	    resp = erl_format((char*)"{~w,~i,{error,badarg}}", args[0], GET_SESSN);
		REMOTE_LOG("ERROR badarg\n");
		ret = true;
	    goto error_exit;
	}

	// Args : Connection String, User name, Password
    if(ERL_IS_BINARY(args[1]) &&
       ERL_IS_BINARY(args[2]) &&
       ERL_IS_BINARY(args[3])) {

		   try {
				ocisession * conn_handle = new ocisession(
					(char*)ERL_BIN_PTR(args[1]), ERL_BIN_SIZE(args[1]),		// Connect String
					(char*)ERL_BIN_PTR(args[2]), ERL_BIN_SIZE(args[2]),		// User Name String
					(char*)ERL_BIN_PTR(args[3]), ERL_BIN_SIZE(args[3]));	// Password String
		        REMOTE_LOG("got connection %lu\n", (unsigned long long)conn_handle);
				ETERM *conh = erl_mk_ulonglong((unsigned long long)conn_handle);
				resp = erl_format((char*)"{~w,~i,~w}", args[0], GET_SESSN, conh);
				erl_free_term(conh);
		   } catch (intf_ret r) {
				REMOTE_LOG("ERROR %s\n", r.gerrbuf);
				resp = erl_format((char*)"{~w,~i,{error,{~i,~s}}", args[0], GET_SESSN, r.gerrcode, r.gerrbuf);
				ret = true;
		   } catch (string str) {
				REMOTE_LOG("ERROR %s\n", str.c_str());
				resp = erl_format((char*)"{~w,~i,{error,{0,~s}}", args[0], GET_SESSN, str.c_str());
				ret = true;
		   } catch (...) {
				REMOTE_LOG("ERROR unknown\n");
				resp = erl_format((char*)"{~w,~i,{error,{0,unknown}}", args[0], GET_SESSN);
				ret = true;
		   }
	}

error_exit:
    if(write_resp(resp) < 0)
        ret = true;

    erl_free_compound(command);
	UNMAP_ARGS(CMD_ARGS_COUNT(GET_SESSN), args);

	return ret;
}

bool cmd_release_conn(ETERM * command)
{
    bool ret = false;
    ETERM * resp;

    ETERM **args;
    MAP_ARGS(CMD_ARGS_COUNT(PUT_SESSN), command, args);

    if(ARG_COUNT(command) != CMD_ARGS_COUNT(PUT_SESSN)) {
	    resp = erl_format((char*)"{~w,~i,{error,badarg}}", args[0], PUT_SESSN);
		REMOTE_LOG("ERROR badarg\n");
		ret = true;
	    goto error_exit;
	}

	if(ERL_IS_INTEGER(args[1]) || ERL_IS_UNSIGNED_LONGLONG(args[1]) || ERL_IS_LONGLONG(args[1])) {

		ocisession * conn_handle = (ocisession *)(ERL_IS_INTEGER(args[1])
													? ERL_INT_VALUE(args[1])
													: ERL_LL_UVALUE(args[1]));
		try {
			delete conn_handle;
            resp = erl_format((char*)"{~w,~i,ok}", args[0], PUT_SESSN);
		} catch (intf_ret r) {
			REMOTE_LOG("ERROR %s\n", r.gerrbuf);
            resp = erl_format((char*)"{~w,~i,{error,{~i,~s}}}", args[0], PUT_SESSN, r.gerrcode, r.gerrbuf);
			ret = true;
		} catch (string str) {
			REMOTE_LOG("ERROR %s\n", str.c_str());
			resp = erl_format((char*)"{~w,~i,{error,{0,~s}}", args[0], PUT_SESSN, str.c_str());
			ret = true;
		} catch (...) {
			REMOTE_LOG("ERROR unknown\n");
			resp = erl_format((char*)"{~w,~i,{error,{0,unknown}}", args[0], PUT_SESSN);
			ret = true;
		}
    }

error_exit:
    if(write_resp(resp) < 0)
        ret = true;

    erl_free_compound(command);
	UNMAP_ARGS(CMD_ARGS_COUNT(PUT_SESSN), args);

	return ret;
}

//inp_t * bind_args = map_to_bind_args(args[3]);
bool cmd_prep_sql(ETERM * command)
{
	bool ret = false;
    ETERM * resp;

    ETERM **args;
    MAP_ARGS(CMD_ARGS_COUNT(PREP_STMT), command, args);

    if(ARG_COUNT(command) != CMD_ARGS_COUNT(PREP_STMT)) {
	    resp = erl_format((char*)"{~w,~i,{error,badarg}}", args[0], PREP_STMT);
		REMOTE_LOG("ERROR badarg\n");
		ret = true;
	    goto error_exit;
	}

    // Args: Conn Handle, Sql String
    if((ERL_IS_INTEGER(args[1]) || ERL_IS_UNSIGNED_LONGLONG(args[1]) || ERL_IS_LONGLONG(args[1])) &&
       ERL_IS_BINARY(args[2])) {

        LOG_ARGS(ARG_COUNT(command), args, "Execute SQL statement");

		ocisession * conn_handle = (ocisession *)(ERL_IS_INTEGER(args[1])
													? ERL_INT_VALUE(args[1])
													: ERL_LL_UVALUE(args[1]));
		try {
	        ocistmt * statement_handle = conn_handle->prepare_stmt(ERL_BIN_PTR(args[2]), ERL_BIN_SIZE(args[2]));
            //REMOTE_LOG("statement handle %lu\n", (unsigned long long)statement_handle);
			ETERM *stmth = erl_mk_ulonglong((unsigned long long)statement_handle);
			resp = erl_format((char*)"{~w,~i,{stmt,~w}}", args[0], PREP_STMT, stmth);
			erl_free_term(stmth);
		} catch (intf_ret r) {
			resp = erl_format((char*)"{~w,~i,{error,{~i,~s}}}", args[0], PREP_STMT, r.gerrcode, r.gerrbuf);
			if (r.fn_ret == CONTINUE_WITH_ERROR)
				REMOTE_LOG("Continue with ERROR Execute SQL \"%.*s;\" -> %s\n", ERL_BIN_SIZE(args[2]), ERL_BIN_PTR(args[2]), r.gerrbuf);
			else {
				REMOTE_LOG("ERROR %s\n", r.gerrbuf);
				ret = true;
			}
		} catch (string str) {
			REMOTE_LOG("ERROR %s\n", str.c_str());
			resp = erl_format((char*)"{~w,~i,{error,{0,~s}}", args[0], PREP_STMT, str.c_str());
			ret = true;
		} catch (...) {
			REMOTE_LOG("ERROR unknown\n");
			resp = erl_format((char*)"{~w,~i,{error,{0,unknown}}", args[0], PREP_STMT);
			ret = true;
		}
    }

error_exit:
    if(write_resp(resp) < 0)
        ret = true;

    erl_free_compound(command);
	UNMAP_ARGS(CMD_ARGS_COUNT(PREP_STMT), args);

	return ret;
}

bool cmd_bind_args(ETERM * command)
{
	bool ret = false;
    ETERM * resp;

    ETERM **args;
    MAP_ARGS(CMD_ARGS_COUNT(BIND_ARGS), command, args);

    if(ARG_COUNT(command) != CMD_ARGS_COUNT(BIND_ARGS)) {
	    resp = erl_format((char*)"{~w,~i,{error,badarg}}", args[0], BIND_ARGS);
		REMOTE_LOG("ERROR badarg\n");
		ret = true;
	    goto error_exit;
	}

    // Args: Statement Handle, BindList
    if((ERL_IS_INTEGER(args[1]) || ERL_IS_UNSIGNED_LONGLONG(args[1]) || ERL_IS_LONGLONG(args[1]))
		&& ERL_IS_LIST(args[2])) {

		ocistmt * statement_handle = (ocistmt*)(ERL_IS_INTEGER(args[1])
												? ERL_INT_VALUE(args[1])
												: ERL_LL_UVALUE(args[1]));

		try {
			map_schema_to_bind_args(args[2], statement_handle->get_bind_args());
			resp = erl_format((char*)"{~w,~i,ok}", args[0], BIND_ARGS);
		} catch (intf_ret r) {
			resp = erl_format((char*)"{~w,~i,{error,{~i,~s}}}", args[0], BIND_ARGS, r.gerrcode, r.gerrbuf);
			REMOTE_LOG("ERROR %s\n", r.gerrbuf);
			ret = true;
		} catch (string & str) {
			REMOTE_LOG("ERROR %s\n", str.c_str());
			resp = erl_format((char*)"{~w,~i,{error,{0,~s}}", args[0], BIND_ARGS, str.c_str());
			ret = true;
		} catch (...) {
			REMOTE_LOG("ERROR unknown\n");
			resp = erl_format((char*)"{~w,~i,{error,{0,unknown}}", args[0], BIND_ARGS);
			ret = true;
		}
    }

error_exit:
	if(write_resp(resp) < 0)
        ret = true;

	erl_free_compound(command);
	UNMAP_ARGS(CMD_ARGS_COUNT(BIND_ARGS), args);

    return ret;
}

bool cmd_exec_stmt(ETERM * command)
{
	bool ret = false;
    ETERM * resp;

    ETERM **args;
    MAP_ARGS(CMD_ARGS_COUNT(EXEC_STMT), command, args);

    if(ARG_COUNT(command) != CMD_ARGS_COUNT(EXEC_STMT)){
	    resp = erl_format((char*)"{~w,~i,{error,badargcount,~i,~i}}", args[0], EXEC_STMT, ARG_COUNT(command), CMD_ARGS_COUNT(EXEC_STMT));
		REMOTE_LOG("ERROR bad arguments count\n");
		ret = true;
	    goto error_exit;
	}

    // Args: Conn Handle, Bind List
    if((ERL_IS_INTEGER(args[1]) || ERL_IS_UNSIGNED_LONGLONG(args[1]) || ERL_IS_LONGLONG(args[1]))
	   && ERL_IS_LIST(args[2])) {
		ocistmt * statement_handle = (ocistmt *)(ERL_IS_INTEGER(args[1])
													? ERL_INT_VALUE(args[1])
													: ERL_LL_UVALUE(args[1]));
		try {
		    ETERM *columns = NULL;
			map_value_to_bind_args(args[2], statement_handle->get_bind_args());
			unsigned int exec_ret = statement_handle->execute(&columns, append_coldef_to_list);
			// TODO : Also return bound return values from here
			if (columns == NULL)
				resp = erl_format((char*)"{~w,~i,{executed,~i}}", args[0], EXEC_STMT, exec_ret);
			else {
				resp = erl_format((char*)"{~w,~i,{cols,~w}}", args[0], EXEC_STMT, columns);
				erl_free_term(columns);
			}
		} catch (intf_ret r) {
			resp = erl_format((char*)"{~w,~i,{error,{~i,~s}}}", args[0], EXEC_STMT, r.gerrcode, r.gerrbuf);
			if (r.fn_ret == CONTINUE_WITH_ERROR)
				REMOTE_LOG("Continue with ERROR Execute STMT %s\n", r.gerrbuf);
			else {
				REMOTE_LOG("ERROR %s\n", r.gerrbuf);
				ret = true;
			}
		} catch (string str) {
			REMOTE_LOG("ERROR %s\n", str.c_str());
			resp = erl_format((char*)"{~w,~i,{error,{0,~s}}", args[0], EXEC_STMT, str.c_str());
			ret = true;
		} catch (...) {
			REMOTE_LOG("ERROR unknown\n");
			resp = erl_format((char*)"{~w,~i,{error,{0,unknown}}", args[0], EXEC_STMT);
			ret = true;
		}
	}

error_exit:
    if(write_resp(resp) < 0)
        ret = true;

    erl_free_compound(command);
	UNMAP_ARGS(CMD_ARGS_COUNT(EXEC_STMT), args);

	return ret;
}

// TODO inp_t * bind_args = map_to_bind_args(args[4]);
bool cmd_fetch_rows(ETERM * command)
{
	bool ret = false;
    ETERM * resp;

    ETERM **args;
    MAP_ARGS(CMD_ARGS_COUNT(FTCH_ROWS), command, args);

    if(ARG_COUNT(command) != CMD_ARGS_COUNT(FTCH_ROWS)) {
	    resp = erl_format((char*)"{~w,~i,{error,badarg}}", args[0], FTCH_ROWS);
		REMOTE_LOG("ERROR badarg\n");
		ret = true;
	    goto error_exit;
	}

    // Args: Statement Handle, Rowcount
    if((ERL_IS_INTEGER(args[1]) || ERL_IS_UNSIGNED_LONGLONG(args[1]) || ERL_IS_LONGLONG(args[1]))
		&& ERL_IS_INTEGER(args[2])) {

		ocistmt * statement_handle = (ocistmt*)(ERL_IS_INTEGER(args[1])
												? ERL_INT_VALUE(args[1])
												: ERL_LL_UVALUE(args[1]));
        int rowcount = ERL_INT_VALUE(args[2]);

		ETERM *rows = NULL;
		try {
			intf_ret r = statement_handle->rows(&rows,
											   append_string_to_list,
											   append_list_to_list,
											   calculate_resp_size,
											   rowcount);
			if (r.fn_ret == MORE || r.fn_ret == DONE) {
				if (rows != NULL) {
                    resp = erl_format((char*)"{~w,~i,{{rows,~w},~a}}", args[0], FTCH_ROWS, rows, (r.fn_ret == MORE ? "false" : "true"));
					erl_free_term(rows);
				}
                else
                    resp = erl_format((char*)"{~w,~i,{{rows,[]},true}}", args[0], FTCH_ROWS);
            }
		} catch (intf_ret r) {
			resp = erl_format((char*)"{~w,~i,{error,{~i,~s}}}", args[0], FTCH_ROWS, r.gerrcode, r.gerrbuf);
			if (r.fn_ret == CONTINUE_WITH_ERROR)
				REMOTE_LOG("Continue with ERROR fetch STMT %s\n", r.gerrbuf);
			else {
				REMOTE_LOG("ERROR %s\n", r.gerrbuf);
				ret = true;
			}
		} catch (string str) {
			REMOTE_LOG("ERROR %s\n", str.c_str());
			resp = erl_format((char*)"{~w,~i,{error,{0,~s}}", args[0], FTCH_ROWS, str.c_str());
			ret = true;
		} catch (...) {
			REMOTE_LOG("ERROR unknown\n");
			resp = erl_format((char*)"{~w,~i,{error,{0,unknown}}", args[0], FTCH_ROWS);
			ret = true;
		}
    }

error_exit:
	if(write_resp(resp) < 0)
        ret = true;

	erl_free_compound(command);
	UNMAP_ARGS(CMD_ARGS_COUNT(FTCH_ROWS), args);

    return ret;
}

bool cmd_close_stmt(ETERM * command)
{
	bool ret = false;
    ETERM * resp;

    ETERM **args;
    MAP_ARGS(CMD_ARGS_COUNT(CLSE_STMT), command, args);

    if(ARG_COUNT(command) != CMD_ARGS_COUNT(CLSE_STMT)) {
	    resp = erl_format((char*)"{~w,~i,{error,badarg}}", args[0], CLSE_STMT);
		REMOTE_LOG("ERROR badarg\n");
		ret = true;
	    goto error_exit;
	}

    // Args: Statement Handle
    if(ERL_IS_INTEGER(args[1]) || ERL_IS_UNSIGNED_LONGLONG(args[1]) || ERL_IS_LONGLONG(args[1])) {

		ocistmt * statement_handle = (ocistmt*)(ERL_IS_INTEGER(args[1])
												? ERL_INT_VALUE(args[1])
												: ERL_LL_UVALUE(args[1]));
		try {
			statement_handle->close();
			resp = erl_format((char*)"{~w,~i,ok}", args[0], CLSE_STMT);
		} catch (intf_ret r) {
			REMOTE_LOG("ERROR %s\n", r.gerrbuf);
			resp = erl_format((char*)"{~w,~i,{error,{~i,~s}}}", args[0], CLSE_STMT, r.gerrcode, r.gerrbuf);
			ret = true;
		} catch (string str) {
			REMOTE_LOG("ERROR %s\n", str.c_str());
			resp = erl_format((char*)"{~w,~i,{error,{0,~s}}", args[0], CLSE_STMT, str.c_str());
			ret = true;
		} catch (...) {
			REMOTE_LOG("ERROR unknown\n");
			resp = erl_format((char*)"{~w,~i,{error,{0,unknown}}", args[0], CLSE_STMT);
			ret = true;
		}
    }

error_exit:
    if(write_resp(resp) < 0)
        ret = true;

	erl_free_compound(command);
	UNMAP_ARGS(CMD_ARGS_COUNT(CLSE_STMT), args);

    return ret;
}

//#define PRINTCMD

bool cmd_processor(void * param)
{
	bool ret = false;
	ETERM *command = (ETERM *)param;

	//PRINT_ERL_ALLOC("start");

	ETERM *cmd = erl_element(2, (ETERM *)command);
#ifdef PRINTCMD
	char * tmpbuf = print_term(command);
	REMOTE_LOG("========================================\nCOMMAND : %s %s\n========================================\n",
		CMD_NAME_STR(ERL_INT_VALUE(cmd)),
		tmpbuf);
	delete tmpbuf;
#endif

	if(ERL_IS_INTEGER(cmd)) {
        switch(ERL_INT_VALUE(cmd)) {
        case GET_SESSN:	ret = cmd_get_session(command);		break;
        case PUT_SESSN:	ret = cmd_release_conn(command);	break;
        case PREP_STMT:	ret = cmd_prep_sql(command);		break;
        case BIND_ARGS:	ret = cmd_bind_args(command);		break;
        case EXEC_STMT:	ret = cmd_exec_stmt(command);		break;
        case FTCH_ROWS:	ret = cmd_fetch_rows(command);		break;
        case CLSE_STMT:	ret = cmd_close_stmt(command);		break;
        case RMOTE_MSG:	ret = change_log_flag(command);		break;
        case OCIP_QUIT:
        default:
			ret = true;
            break;
        }
    }
	erl_free_term(cmd);

//	PRINT_ERL_ALLOC("end");

	erl_eterm_release();
	return ret;
}
