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
    ETERM **args = new ETERM*[CMD_ARGS_COUNT(RMOTE_MSG)];
    ETERM *resp;

    MAP_ARGS(command, args);

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

    delete args;

	return ret;
}

/*bool cmd_create_tns_ssn_pool(ETERM * command)
{
    bool ret = false;
    ETERM **args = new ETERM*[CMD_ARGS_COUNT(CREATE_SESSION_POOL)];
    ETERM *resp;

    MAP_ARGS(command, args);

	if(ARG_COUNT(command) != CMD_ARGS_COUNT(CREATE_SESSION_POOL))
        goto error_exit;

    // Args : Connection String, User name, Password, Options
    if(ERL_IS_BINARY(args[1]) &&
       ERL_IS_BINARY(args[2]) &&
       ERL_IS_BINARY(args[3]) &&
       ERL_IS_BINARY(args[4])) {

		LOG_ARGS(ARG_COUNT(command), args, "Create Pool");

		intf_ret r = oci_create_tns_seesion_pool(
                (char*)ERL_BIN_PTR(args[1]), ERL_BIN_SIZE(args[1]),		// Connect String
                (char*)ERL_BIN_PTR(args[2]), ERL_BIN_SIZE(args[2]),		// User Name String
                (char*)ERL_BIN_PTR(args[3]), ERL_BIN_SIZE(args[3]),		// Password String
                (char*)ERL_BIN_PTR(args[4]), ERL_BIN_SIZE(args[4]));	// Options String
		if (r.fn_ret == SUCCESS)
            resp = erl_format((char*)"{~w,~i,ok}", args[0], CREATE_SESSION_POOL);
        else {
			REMOTE_LOG("CMD: Connect ERROR - %s", r.gerrbuf);
			resp = erl_format((char*)"{~w,~i,{error,{~i,~s}}}", args[0], CREATE_SESSION_POOL, r.gerrcode, r.gerrbuf);
        }
    } else {
error_exit:
		REMOTE_LOG("ERROR badarg\n");
        resp = erl_format((char*)"{~w,~i,{error,badarg}}", args[0], CREATE_SESSION_POOL);
    }

    if(write_resp(resp) < 0)
        ret = true;

    erl_free_compound(command);

    delete args;

	return ret;
}
bool cmd_free_ssn_pool(ETERM * command)
{
    bool ret = false;
    ETERM **args = new ETERM*[CMD_ARGS_COUNT(FREE_SESSION_POOL)];
    ETERM *resp = NULL;
    intf_ret r;

    MAP_ARGS(command, args);

	REMOTE_LOG("----- FREE POOL -----\n");
	if(ARG_COUNT(command) != CMD_ARGS_COUNT(FREE_SESSION_POOL)) {
        resp = erl_format((char*)"{~w,~i,error,badarg}", args[0], FREE_SESSION_POOL);
		REMOTE_LOG("ERROR badarg\n");
        goto error_exit;
	}

	r = oci_free_session_pool();
	if (r.fn_ret == SUCCESS)
        resp = erl_format((char*)"{~w,~i}", args[0], FREE_SESSION_POOL);
    else {
		REMOTE_LOG("CMD: free session pool ERROR - %s", r.gerrbuf);
        resp = erl_format((char*)"{~w,~i,{error,{~i,~s}}}", args[0], FREE_SESSION_POOL, r.gerrcode, r.gerrbuf);
    }

error_exit:
    if(write_resp(resp) < 0)
        ret = true;

    erl_free_compound(command);

	delete args;
	return ret;
}*/

bool cmd_get_session(ETERM * command)
{
    bool ret = false;
    ETERM **args = new ETERM*[CMD_ARGS_COUNT(GET_SESSN)];
	ETERM *resp;

    MAP_ARGS(command, args);

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
				resp = erl_format((char*)"{~w,~i,~w}", args[0], GET_SESSN, erl_mk_ulonglong((unsigned long long)conn_handle));
		   } catch (intf_ret r) {
				REMOTE_LOG("ERROR %s\n", r.gerrbuf);
				resp = erl_format((char*)"{~w,~i,{error,{~i,~s}}", args[0], GET_SESSN, r.gerrcode, r.gerrbuf);
				ret = true;
		   } catch (string str) {
				REMOTE_LOG("ERROR %s\n", str);
				resp = erl_format((char*)"{~w,~i,{error,{0,~s}}", args[0], GET_SESSN, str);
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

	delete args;
	return ret;
}

bool cmd_release_conn(ETERM * command)
{
    bool ret = false;
    ETERM **args = new ETERM*[CMD_ARGS_COUNT(PUT_SESSN)];
    ETERM * resp;

    MAP_ARGS(command, args);

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
			REMOTE_LOG("ERROR %s\n", str);
			resp = erl_format((char*)"{~w,~i,{error,{0,~s}}", args[0], PUT_SESSN, str);
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

    delete args;
	return ret;
}

//inp_t * bind_args = map_to_bind_args(args[3]);
bool cmd_prep_sql(ETERM * command)
{
	bool ret = false;
    ETERM **args = new ETERM*[CMD_ARGS_COUNT(PREP_STMT)];
    ETERM * resp;

    MAP_ARGS(command, args);

    if(ARG_COUNT(command) != CMD_ARGS_COUNT(PREP_STMT)) {
	    resp = erl_format((char*)"{~w,~i,{error,badarg}}", args[0], PREP_STMT);
		REMOTE_LOG("ERROR badarg\n");
		ret = true;
	    goto error_exit;
	}

    // Args: Conn Handle, Sql Statement
    if((ERL_IS_INTEGER(args[1]) || ERL_IS_UNSIGNED_LONGLONG(args[1]) || ERL_IS_LONGLONG(args[1])) &&
       ERL_IS_BINARY(args[2])) {

        LOG_ARGS(ARG_COUNT(command), args, "Execute SQL");

		ocisession * conn_handle = (ocisession *)(ERL_IS_INTEGER(args[1])
													? ERL_INT_VALUE(args[1])
													: ERL_LL_UVALUE(args[1]));
		try {
	        ocistmt * statement_handle = conn_handle->prepare_stmt(ERL_BIN_PTR(args[2]), ERL_BIN_SIZE(args[2]));
            //REMOTE_LOG("statement handle %lu\n", (unsigned long long)statement_handle);
			resp = erl_format((char*)"{~w,~i,{stmt,~w}}", args[0], PREP_STMT, erl_mk_ulonglong((unsigned long long)statement_handle));
		} catch (intf_ret r) {
			resp = erl_format((char*)"{~w,~i,{error,{~i,~s}}}", args[0], PREP_STMT, r.gerrcode, r.gerrbuf);
			if (r.fn_ret == CONTINUE_WITH_ERROR)
				REMOTE_LOG("Continue with ERROR Execute SQL \"%.*s;\" -> %s\n", ERL_BIN_SIZE(args[2]), ERL_BIN_PTR(args[2]), r.gerrbuf);
			else {
				REMOTE_LOG("ERROR %s\n", r.gerrbuf);
				ret = true;
			}
		} catch (string str) {
			REMOTE_LOG("ERROR %s\n", str);
			resp = erl_format((char*)"{~w,~i,{error,{0,~s}}", args[0], PREP_STMT, str);
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
    delete args;
    return ret;
}

bool cmd_exec_stmt(ETERM * command)
{
	bool ret = false;
    ETERM **args = new ETERM*[CMD_ARGS_COUNT(EXEC_STMT)];
    ETERM * resp;

    MAP_ARGS(command, args);

    if(ARG_COUNT(command) != CMD_ARGS_COUNT(EXEC_STMT)){
	    resp = erl_format((char*)"{~w,~i,{error,badarg}}", args[0], EXEC_STMT);
		REMOTE_LOG("ERROR badarg\n");
		ret = true;
	    goto error_exit;
	}

    // Args: Conn Handle
    if(ERL_IS_INTEGER(args[1]) || ERL_IS_UNSIGNED_LONGLONG(args[1]) || ERL_IS_LONGLONG(args[1])) {
		ocistmt * statement_handle = (ocistmt *)(ERL_IS_INTEGER(args[1])
													? ERL_INT_VALUE(args[1])
													: ERL_LL_UVALUE(args[1]));
		try {
		    ETERM *columns = NULL;
			statement_handle->execute(&columns, append_coldef_to_list);
			// TODO : Also return bound return values from here
			if (columns == NULL)
				resp = erl_format((char*)"{~w,~i,{executed,no_ret}}", args[0], EXEC_STMT);
			else
				resp = erl_format((char*)"{~w,~i,{cols,~w}}", args[0], EXEC_STMT, columns);
		} catch (intf_ret r) {
			resp = erl_format((char*)"{~w,~i,{error,{~i,~s}}}", args[0], EXEC_STMT, r.gerrcode, r.gerrbuf);
			if (r.fn_ret == CONTINUE_WITH_ERROR)
				REMOTE_LOG("Continue with ERROR Execute STMT %s\n", r.gerrbuf);
			else {
				REMOTE_LOG("ERROR %s\n", r.gerrbuf);
				ret = true;
			}
		} catch (string str) {
			REMOTE_LOG("ERROR %s\n", str);
			resp = erl_format((char*)"{~w,~i,{error,{0,~s}}", args[0], EXEC_STMT, str);
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

    delete args;
	return ret;
}

// TODO inp_t * bind_args = map_to_bind_args(args[4]);
bool cmd_fetch_rows(ETERM * command)
{
	bool ret = false;
    ETERM **args = new ETERM*[CMD_ARGS_COUNT(FTCH_ROWS)];
    ETERM * resp;

    MAP_ARGS(command, args);

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
                if (rows != NULL)
                    resp = erl_format((char*)"{~w,~i,{{rows,~w},~a}}", args[0], FTCH_ROWS, rows, (r.fn_ret == MORE ? "false" : "true"));
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
			REMOTE_LOG("ERROR %s\n", str);
			resp = erl_format((char*)"{~w,~i,{error,{0,~s}}", args[0], FTCH_ROWS, str);
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

    delete args;
    return ret;
}

bool cmd_close_stmt(ETERM * command)
{
	bool ret = false;
    ETERM **args = new ETERM*[CMD_ARGS_COUNT(CLSE_STMT)];
    ETERM * resp;

    MAP_ARGS(command, args);

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
			REMOTE_LOG("ERROR %s\n", str);
			resp = erl_format((char*)"{~w,~i,{error,{0,~s}}", args[0], CLSE_STMT, str);
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

    delete args;
    return ret;
}

//#define PRINTCMD

bool cmd_processor(void * param)
{
	ETERM *command = (ETERM *)param;
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
        case GET_SESSN:		return cmd_get_session(command);
        case PUT_SESSN:		return cmd_release_conn(command);
        case PREP_STMT:		return cmd_prep_sql(command);
        case EXEC_STMT:		return cmd_exec_stmt(command);
        case FTCH_ROWS:		return cmd_fetch_rows(command);
        case CLSE_STMT:		return cmd_close_stmt(command);
        case RMOTE_MSG:		return change_log_flag(command);
        case OCIP_QUIT:
        default:
            break;
        }
    }
    return true;
}
