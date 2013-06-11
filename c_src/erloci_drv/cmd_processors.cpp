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

#include "cmd_processors.h"

#include <stdio.h>
#include <stdlib.h>

static const struct cmdtable g_cmdtable[] = ERLOCI_CMD_DESC;

#define CMD_ARGS_COUNT(_cmd) g_cmdtable[_cmd].arg_count

bool change_log_flag(ETERM * command)
{
    bool ret = false;
    ETERM **args = new ETERM*[CMD_ARGS_COUNT(R_DEBUG_MSG)];
    ETERM *resp;

    MAP_ARGS(command, args);

	if(ARG_COUNT(command) != CMD_ARGS_COUNT(R_DEBUG_MSG))
        goto error_exit;

    // Args : {log, DBG_FLAG_OFF/DBG_FLAG_ON}
    if(ERL_IS_INTEGER(args[1])) {
		unsigned int log = ERL_INT_UVALUE(args[1]);

        switch(log) {
        case DBG_FLAG_OFF:
            REMOTE_LOG("Disabling logging...");
            log_flag = false;
            REMOTE_LOG("This line will never show up!!\n");
            resp = erl_format((char*)"{~w,~i,log_disabled}", args[0], R_DEBUG_MSG);
            break;
        case DBG_FLAG_ON:
            log_flag = true;
            resp = erl_format((char*)"{~w,~i,log_enabled}", args[0], R_DEBUG_MSG);
            REMOTE_LOG("Enabled logging...\n");
            break;
        default:
            resp = erl_format((char*)"{~w,~i,{error,badarg}}", args[0], R_DEBUG_MSG);
			REMOTE_LOG("ERROR badarg %d\n", log);
            break;
        }
    } else {
error_exit:
        resp = erl_format((char*)"{~w,~i,{error,badarg}}", args[0], R_DEBUG_MSG);
		REMOTE_LOG("ERROR badarg\n");
    }

    if(write_resp(resp) < 0)
        ret = true;

    erl_free_compound(command);

    delete args;

	return ret;
}

bool cmd_create_tns_ssn_pool(ETERM * command)
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

bool cmd_get_session(ETERM * command)
{
    bool ret = false;
    ETERM **args = new ETERM*[CMD_ARGS_COUNT(GET_SESSION)];
	ETERM *resp;
    void * conn_handle = NULL;
    intf_ret r;

    MAP_ARGS(command, args);

	if(ARG_COUNT(command) != CMD_ARGS_COUNT(GET_SESSION)) {
	    resp = erl_format((char*)"{~w,~i,{error,badarg}}", args[0], GET_SESSION);
		REMOTE_LOG("ERROR badarg\n");
	    goto error_exit;
	}

	r = oci_get_session_from_pool(&conn_handle);
	if (r.fn_ret == SUCCESS) {
        REMOTE_LOG("connection from session pool %lu\n", (unsigned long long)conn_handle);
		resp = erl_format((char*)"{~w,~i,~w}", args[0], GET_SESSION, erl_mk_ulonglong((unsigned long long)conn_handle));
	}
    else {
		REMOTE_LOG("ERROR %s\n", r.gerrbuf);
        resp = erl_format((char*)"{~w,~i,{error,{~i,~s}}", args[0], GET_SESSION, r.gerrcode, r.gerrbuf);
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
    intf_ret r;
    bool ret = false;
    ETERM **args = new ETERM*[CMD_ARGS_COUNT(RELEASE_SESSION)];
    ETERM * resp;

    MAP_ARGS(command, args);

    if(ARG_COUNT(command) != CMD_ARGS_COUNT(RELEASE_SESSION))
        goto error_exit;

	if(ERL_IS_INTEGER(args[1]) || ERL_IS_UNSIGNED_LONGLONG(args[1]) || ERL_IS_LONGLONG(args[1])) {

		void * conn_handle = (void *) (ERL_IS_INTEGER(args[1]) ? ERL_INT_VALUE(args[1]) : ERL_LL_UVALUE(args[1]));

		r = oci_return_connection_to_pool(conn_handle);
        if (r.fn_ret == SUCCESS)
            resp = erl_format((char*)"{~w,~i,ok}", args[0], RELEASE_SESSION);
        else {
			REMOTE_LOG("ERROR %s\n", r.gerrbuf);
            resp = erl_format((char*)"{~w,~i,{error,{~i,~s}}}", args[0], RELEASE_SESSION, r.gerrcode, r.gerrbuf);
        }
    } else {
error_exit:
        resp = erl_format((char*)"{~w,~i,error,badarg}", args[0], RELEASE_SESSION);
		REMOTE_LOG("ERROR badarg\n");
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
}

bool cmd_prepare_statement(ETERM * command)
{
    ETERM **args = new ETERM*[CMD_ARGS_COUNT(PREP_STMT)];
    ETERM * resp;

    MAP_ARGS(command, args);

    if(ARG_COUNT(command) != CMD_ARGS_COUNT(PREP_STMT))
        goto error_exit_pre;

    // Args: Conn Handle, Sql String
    if((ERL_IS_INTEGER(args[1]) || ERL_IS_UNSIGNED_LONGLONG(args[1]) || ERL_IS_LONGLONG(args[1])) &&
       ERL_IS_BINARY(args[2])
       ) {

        LOG_ARGS(ARG_COUNT(command), args, "Execute SQL statement");

		void * conn_handle = (void *)(ERL_IS_INTEGER(args[1]) ? ERL_INT_VALUE(args[1]) : ERL_LL_UVALUE(args[1]));
        void * statement_handle = NULL;

		intf_ret r = oci_prepare_stmt(conn_handle, &statement_handle, ERL_BIN_PTR(args[2]), ERL_BIN_SIZE(args[2]));
		switch(r.fn_ret) {
			case SUCCESS:
		        REMOTE_LOG("statement handle %lu\n", (unsigned long long)statement_handle);
				resp = erl_format((char*)"{~w,~i,{stmt,~w}}", args[0], PREP_STMT, erl_mk_ulonglong((unsigned long long)statement_handle));
				if(write_resp(resp) < 0) goto error_exit;
	            //REMOTE_LOG("SUCCESS \"%.*s;\"\n", ERL_BIN_SIZE(args[2]), ERL_BIN_PTR(args[2]));
				break;
			case CONTINUE_WITH_ERROR:
                {
					REMOTE_LOG("Continue with ERROR Execute SQL \"%.*s;\" -> %s\n", ERL_BIN_SIZE(args[2]), ERL_BIN_PTR(args[2]), r.gerrbuf);
					resp = erl_format((char*)"{~w,~i,{error,{~i,~s}}}", args[0], EXEC_STMT, r.gerrcode, r.gerrbuf);
					write_resp(resp);
				}
	            REMOTE_LOG("CONTINUE_WITH_ERROR \"%.*s;\"\n", ERL_BIN_SIZE(args[2]), ERL_BIN_PTR(args[2]));
				break;
			case FAILURE:
            default:
                {
					REMOTE_LOG("ERROR Execute SQL \"%.*s;\" -> %s\n", ERL_BIN_SIZE(args[2]), ERL_BIN_PTR(args[2]), r.gerrbuf);
					resp = erl_format((char*)"{~w,~i,{error,{~i,~s}}}", args[0], EXEC_STMT, r.gerrcode, r.gerrbuf);
					write_resp(resp);
					goto error_exit;
				}
	            REMOTE_LOG("FAILURE \"%.*s;\"\n", ERL_BIN_SIZE(args[2]), ERL_BIN_PTR(args[2]));
				break;
        }
    } else {
error_exit_pre:
        resp = erl_format((char*)"{~w,~i,error,badarg}", args[0], EXEC_STMT);
		REMOTE_LOG("ERROR badarg %d\n", ERL_TYPE(args[1]));

#if 0
		     if(ERL_IS_INTEGER(args[1]))			REMOTE_LOG("conn %d\n", ERL_INT_VALUE(args[1])));
		else if(ERL_IS_UNSIGNED_INTEGER(args[1]))	REMOTE_LOG("conn %d\n", ERL_INT_UVALUE(args[1])));
		else if(ERL_IS_LONGLONG(args[1]))			REMOTE_LOG("conn %d\n", ERL_LL_VALUE(args[1])));
		else if(ERL_IS_UNSIGNED_INTEGER(args[1]))	REMOTE_LOG("conn %d\n", ERL_INT_UVALUE(args[1])));
		else if(ERL_IS_UNSIGNED_INTEGER(args[1]))	REMOTE_LOG("conn %d\n", ERL_INT_UVALUE(args[1])));
		else if(ERL_IS_UNSIGNED_INTEGER(args[1]))	REMOTE_LOG("conn %d\n", ERL_INT_UVALUE(args[1])));
		else if(ERL_IS_UNSIGNED_INTEGER(args[1]))	REMOTE_LOG("conn %d\n", ERL_INT_UVALUE(args[1])));
		else if(ERL_IS_UNSIGNED_INTEGER(args[1]))	REMOTE_LOG("conn %d\n", ERL_INT_UVALUE(args[1])));
		else if(ERL_IS_UNSIGNED_INTEGER(args[1]))	REMOTE_LOG("conn %d\n", ERL_INT_UVALUE(args[1])));
#endif

		if(write_resp(resp) < 0) goto error_exit;
    }
    erl_free_compound(command);
    delete args;
    return false;

error_exit:
    erl_free_compound(command);
    delete args;
    return true;
}

bool cmd_bind_statement(ETERM * command)
{
    ETERM **args = new ETERM*[CMD_ARGS_COUNT(BIND_STMT)];
    ETERM * resp;
    inp_t * bind_args = NULL;

    MAP_ARGS(command, args);

    if(ARG_COUNT(command) != CMD_ARGS_COUNT(BIND_STMT))
        goto error_exit;

    if((ERL_IS_INTEGER(args[1]) || ERL_IS_UNSIGNED_LONGLONG(args[1]) || ERL_IS_LONGLONG(args[1]))
    && ERL_IS_LIST(args[2])) {
        bind_args = map_to_bind_args(args[2]);
        void * statement_handle = (void *)(ERL_IS_INTEGER(args[1]) ? ERL_INT_VALUE(args[1]) : ERL_LL_UVALUE(args[1]));
		intf_ret r = oci_bind_stmt(statement_handle, bind_args);
		switch(r.fn_ret) {
			case SUCCESS:
		        REMOTE_LOG("bind agrs %lu\n", (unsigned long long)bind_args);
				resp = erl_format((char*)"{~w,~i,~w}", args[0], BIND_STMT, erl_mk_ulonglong((unsigned long long)bind_args ));
				if(write_resp(resp) < 0) goto error_exit;
				break;
			case CONTINUE_WITH_ERROR:
				REMOTE_LOG("continue error, statement bind failed (%lu -> %s)\n", (unsigned long long)statement_handle, r.gerrbuf);
				resp = erl_format((char*)"{~w,~i,{error,{~i,~s}}}", args[0], BIND_STMT, r.gerrcode, r.gerrbuf);
				write_resp(resp);
                delete bind_args;
				break;
			case FAILURE:
            default:
				REMOTE_LOG("ERROR bind statement %lu -> %s\n", (unsigned long long)statement_handle, r.gerrbuf);
				resp = erl_format((char*)"{~w,~i,{error,{~i,~s}}}", args[0], BIND_STMT, r.gerrcode, r.gerrbuf);
				write_resp(resp);
                delete bind_args;
				goto error_exit;
				break;
        }
    }
    erl_free_compound(command);
    delete args;
    return false;

error_exit:
    erl_free_compound(command);
    delete args;
    return true;
}

bool cmd_exec_stmt(ETERM * command)
{
    ETERM **args = new ETERM*[CMD_ARGS_COUNT(EXEC_STMT)];
    ETERM * resp;
    inp_t * bind_args = NULL;

    MAP_ARGS(command, args);

    if(ARG_COUNT(command) != CMD_ARGS_COUNT(EXEC_STMT))
        goto error_exit_pre;

    // Args: Statement Handle, bind variable linked list head pointer
    if((ERL_IS_INTEGER(args[1]) || ERL_IS_UNSIGNED_LONGLONG(args[1]) || ERL_IS_LONGLONG(args[1]))
      || (ERL_IS_INTEGER(args[2]) || ERL_IS_UNSIGNED_LONGLONG(args[2]) || ERL_IS_LONGLONG(args[2]))) {

        LOG_ARGS(ARG_COUNT(command), args, "Execute SQL statement");

        void * statement_handle = (void *)(ERL_IS_INTEGER(args[1]) ? ERL_INT_VALUE(args[1]) : ERL_LL_UVALUE(args[1]));
        bind_args = (inp_t *)(ERL_IS_INTEGER(args[2]) ? ERL_INT_VALUE(args[2]) : ERL_LL_UVALUE(args[2]));

        /* Transfer the columns */
        ETERM *columns = NULL;
		intf_ret r = oci_exec_stmt(statement_handle, bind_args, &columns, append_coldef_to_list);
		switch(r.fn_ret) {
			case SUCCESS:
				if (columns == NULL && bind_args != NULL)
					resp = erl_format((char*)"{~w,~i,{executed,~w}}", args[0], EXEC_STMT, build_term_from_bind_args(bind_args));
				else if (columns == NULL && bind_args == NULL)
					resp = erl_format((char*)"{~w,~i,{executed,no_ret}}", args[0], EXEC_STMT);
				else {
		            REMOTE_LOG("statement handle %lu\n", (unsigned long long)statement_handle);
					resp = erl_format((char*)"{~w,~i,{cols,~w}}", args[0], EXEC_STMT, columns);
				}
				if(write_resp(resp) < 0) goto error_exit;
	            //REMOTE_LOG("SUCCESS \"%.*s;\"\n", ERL_BIN_SIZE(args[2]), ERL_BIN_PTR(args[2]));
				break;
			case CONTINUE_WITH_ERROR:
                {
					REMOTE_LOG("Continue with ERROR Execute SQL \"%.*s;\" -> %s\n", ERL_BIN_SIZE(args[2]), ERL_BIN_PTR(args[2]), r.gerrbuf);
					resp = erl_format((char*)"{~w,~i,{error,{~i,~s}}}", args[0], EXEC_STMT, r.gerrcode, r.gerrbuf);
					write_resp(resp);
				}
	            REMOTE_LOG("CONTINUE_WITH_ERROR \"%.*s;\"\n", ERL_BIN_SIZE(args[2]), ERL_BIN_PTR(args[2]));
				break;
			case FAILURE:
            default:
                {
					REMOTE_LOG("ERROR Execute SQL \"%.*s;\" -> %s\n", ERL_BIN_SIZE(args[2]), ERL_BIN_PTR(args[2]), r.gerrbuf);
					resp = erl_format((char*)"{~w,~i,{error,{~i,~s}}}", args[0], EXEC_STMT, r.gerrcode, r.gerrbuf);
					write_resp(resp);
					goto error_exit;
				}
	            REMOTE_LOG("FAILURE \"%.*s;\"\n", ERL_BIN_SIZE(args[2]), ERL_BIN_PTR(args[2]));
				break;
        }
    } else {
error_exit_pre:
        resp = erl_format((char*)"{~w,~i,error,badarg}", args[0], EXEC_STMT);
		REMOTE_LOG("ERROR badarg %d\n", ERL_TYPE(args[1]));

		if(write_resp(resp) < 0) goto error_exit;
    }

    erl_free_compound(command);

    delete args;
    return false;

error_exit:
    erl_free_compound(command);
    delete args;
    if(bind_args)
        free_bind_args(bind_args);
    return true;
}

bool cmd_fetch_rows(ETERM * command)
{
    ETERM **args = new ETERM*[CMD_ARGS_COUNT(FETCH_ROWS)];
    ETERM * resp;

    MAP_ARGS(command, args);

    if(ARG_COUNT(command) != CMD_ARGS_COUNT(FETCH_ROWS))
        goto error_exit_pre;

    // Args: Connection Handle, Statement Handle, Rowcount
    if((ERL_IS_INTEGER(args[1]) || ERL_IS_UNSIGNED_LONGLONG(args[1]) || ERL_IS_LONGLONG(args[1])) && ERL_IS_INTEGER(args[2])) {

		void * statement_handle = (void *)(ERL_IS_INTEGER(args[1]) ? ERL_INT_VALUE(args[1]) : ERL_LL_UVALUE(args[1]));
        int rowcount = ERL_INT_VALUE(args[2]);

        //inp_t * bind_args = map_to_bind_args(args[4]);

        /* Transfer the rows */
        if (statement_handle != NULL) {
            ETERM *rows = NULL;
			intf_ret r = oci_produce_rows(statement_handle, &rows, append_string_to_list, append_list_to_list, calculate_resp_size, rowcount);
			if (r.fn_ret == MORE || r.fn_ret == DONE) {
                if (rows != NULL) {
                    resp = erl_format((char*)"{~w,~i,{{rows,~w},~a}}", args[0], FETCH_ROWS, rows, (r.fn_ret == MORE ? "false" : "true"));
                    if(write_resp(resp) < 0) goto error_exit;
					erl_free_compound(rows);
                } else {
                    resp = erl_format((char*)"{~w,~i,{{rows,[]},true}}", args[0], FETCH_ROWS);
                    if(write_resp(resp) < 0) goto error_exit;
                }
            }
        }
    } else {
error_exit_pre:
        resp = erl_format((char*)"{~w,~i,error,badarg}", args[0], FETCH_ROWS);
		REMOTE_LOG("ERROR badarg args[1] %d, args[2] %d\n", ERL_TYPE(args[1]), ERL_TYPE(args[2]));
        if(write_resp(resp) < 0) goto error_exit;
    }
    erl_free_compound(command);

    delete args;
    return false;

error_exit:
    erl_free_compound(command);

    delete args;
    return true;
}

#define PRINTCMD

#ifdef PRINTCMD
static FILE *tfp = NULL;
static char buffer[1024*1024];
#endif

bool cmd_processor(void * param)
{
	ETERM *command = (ETERM *)param;
    ETERM *cmd = erl_element(2, (ETERM *)command);

#ifdef PRINTCMD
	if(tfp != NULL) fclose(tfp);
	tfp = tmpfile();
	erl_print_term(tfp, command);
	rewind(tfp);
	fread(buffer, 1, sizeof(buffer), tfp);
	REMOTE_LOG("========================================\nCOMMAND : %s %s\n========================================\n", cmdnames[ERL_INT_VALUE(cmd)], buffer);
#endif

	if(ERL_IS_INTEGER(cmd)) {
        switch(ERL_INT_VALUE(cmd)) {
        case CREATE_SESSION_POOL:	return cmd_create_tns_ssn_pool(command);
        case FREE_SESSION_POOL:		return cmd_free_ssn_pool(command);

        case GET_SESSION:			return cmd_get_session(command);
        case RELEASE_SESSION:		return cmd_release_conn(command);

        case PREP_STMT:             return cmd_prepare_statement(command);
        case BIND_STMT:             return cmd_bind_statement(command);
        case EXEC_STMT:				return cmd_exec_stmt(command);
        case FETCH_ROWS:			return cmd_fetch_rows(command);

        case R_DEBUG_MSG:			return change_log_flag(command);
        case QUIT:
        default:
            break;
        }
    }
    return true;
}
