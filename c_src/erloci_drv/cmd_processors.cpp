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

static const struct cmdtable g_cmdtable[] = {
    {CREATE_SESSION_POOL,	5, "Create TNS session pool"},
    {GET_SESSION,			1, "Get a session from the TNS session pool"},
    {RELEASE_SESSION,		2, "Return a previously allocated connection back to the pool"},
    {EXEC_SQL,				4, "Execute a SQL query"},
    {FETCH_ROWS,			3, "Fetches the rows of a previously executed SELECT query"},
    {R_DEBUG_MSG,			2, "Remote debugging turning on/off"},
    {FREE_SESSION_POOL,		1, "Release Session Pool"},
    {QUIT,					1, "Exit the port process"},
};

#define CMD_ARGS_COUNT(_cmd) g_cmdtable[_cmd].arg_count

bool change_log_flag(ETERM * command)
{
    bool ret = false;
    ETERM **args = new ETERM*[CMD_ARGS_COUNT(R_DEBUG_MSG)];
    ETERM *resp;

    MAP_ARGS(command, args);

	REMOTE_LOG("----- CHANGE LOG FLAG -----\n");
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
            resp = erl_format((char*)"{~w,~i,error,badarg}", args[0], R_DEBUG_MSG);
			REMOTE_LOG("ERROR badarg %d\n", log);
            break;
        }
    } else {
error_exit:
        resp = erl_format((char*)"{~w,~i,error,badarg}", args[0], R_DEBUG_MSG);
		REMOTE_LOG("ERROR badarg\n");
    }

    if(write_resp(resp) < 0)
        ret = true;

    erl_free_compound(command);

    delete args;
    REMOTE_LOG("---------------------------\n");
    return ret;
}

bool cmd_create_tns_ssn_pool(ETERM * command)
{
    bool ret = false;
    ETERM **args = new ETERM*[CMD_ARGS_COUNT(CREATE_SESSION_POOL)];
    ETERM *resp;

    MAP_ARGS(command, args);

	REMOTE_LOG("----- CREATE POOL -----\n");
    if(ARG_COUNT(command) != CMD_ARGS_COUNT(CREATE_SESSION_POOL))
        goto error_exit;

    // Args : Connection String, User name, Password, Options
    if(ERL_IS_BINARY(args[1]) &&
       ERL_IS_BINARY(args[2]) &&
       ERL_IS_BINARY(args[3]) &&
       ERL_IS_BINARY(args[4])) {

		LOG_ARGS(ARG_COUNT(command), args, "Create Pool");

        if (oci_create_tns_seesion_pool(
                ERL_BIN_PTR(args[1]), ERL_BIN_SIZE(args[1]),	// Connect String
                ERL_BIN_PTR(args[2]), ERL_BIN_SIZE(args[2]),	// User Name String
                ERL_BIN_PTR(args[3]), ERL_BIN_SIZE(args[3]),	// Password String
                ERL_BIN_PTR(args[4]), ERL_BIN_SIZE(args[4])))	// Options String
            resp = erl_format((char*)"{~w,~i,~s}", args[0], CREATE_SESSION_POOL, session_pool_name);
        else {
            int err_str_len = 0;
            char * err_str;
            get_last_error(NULL, err_str_len);
            err_str = new char[err_str_len+1];
            get_last_error(err_str, err_str_len);
			REMOTE_LOG("CMD: Connect ERROR - %s", err_str);
            resp = erl_format((char*)"{~w,~i,error,~s}", args[0], CREATE_SESSION_POOL, err_str);
        }
    } else {
error_exit:
		REMOTE_LOG("ERROR badarg\n");
        resp = erl_format((char*)"{~w,~i,error,badarg}", args[0], CREATE_SESSION_POOL);
    }

    if(write_resp(resp) < 0)
        ret = true;

    erl_free_compound(command);

    delete args;
    REMOTE_LOG("-----------------------\n");
    return ret;
}

bool cmd_get_session(ETERM * command)
{
    bool ret = false;
    ETERM **args = new ETERM*[CMD_ARGS_COUNT(GET_SESSION)];
	ETERM *resp;
    void * conn_handle = NULL;

    MAP_ARGS(command, args);

	REMOTE_LOG("----- GET SESSION -----\n");
	if(ARG_COUNT(command) != CMD_ARGS_COUNT(GET_SESSION)) {
	    resp = erl_format((char*)"{~w,~i,error,badarg}", args[0], GET_SESSION);
		REMOTE_LOG("ERROR badarg\n");
	    goto error_exit;
	}

	if ((conn_handle = oci_get_session_from_pool()) != NULL) {
        REMOTE_LOG("connection from session pool %lu\n", (unsigned long long)conn_handle);
		resp = erl_format((char*)"{~w,~i,~w}", args[0], GET_SESSION, erl_mk_ulonglong((unsigned long long)conn_handle));
	}
    else {
        int err_str_len = 0;
        char * err_str;
        get_last_error(NULL, err_str_len);
        err_str = new char[err_str_len+1];
        get_last_error(err_str, err_str_len);
		REMOTE_LOG("ERROR %s\n", err_str);
        resp = erl_format((char*)"{~w,~i,error,~s}", args[0], GET_SESSION, err_str);
    }

error_exit:
    if(write_resp(resp) < 0)
        ret = true;

    erl_free_compound(command);

	REMOTE_LOG("--------------------------\n");
    return ret;
}

bool cmd_release_conn(ETERM * command)
{
    bool ret = false;
    ETERM **args = new ETERM*[CMD_ARGS_COUNT(RELEASE_SESSION)];
    ETERM * resp;

    MAP_ARGS(command, args);

	REMOTE_LOG("----- RELEASE CONNECTION -----\n");
    if(ARG_COUNT(command) != CMD_ARGS_COUNT(RELEASE_SESSION))
        goto error_exit;

    if(ERL_IS_INTEGER/*ERL_IS_UNSIGNED_LONGLONG*/(args[1])) {
		//void * conn_handle = (void *)ERL_LL_UVALUE(args[1]);
		void * conn_handle = (void *)ERL_INT_VALUE(args[1]);

        if (oci_return_connection_to_pool(conn_handle))
            resp = erl_format((char*)"{~w,~i,ok}", args[0], RELEASE_SESSION);
        else {
            int err_str_len = 0;
            char * err_str;
            get_last_error(NULL, err_str_len);
            err_str = new char[err_str_len+1];
            get_last_error(err_str, err_str_len);
            REMOTE_LOG("Connect Return %s\n", err_str);
            resp = erl_format((char*)"{~w,~i,{error,~s}}", args[0], RELEASE_SESSION, err_str);
            delete err_str;
			REMOTE_LOG("ERROR %s\n", err_str);
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
	REMOTE_LOG("------------------------------\n");
    return ret;
}

bool cmd_free_ssn_pool(ETERM * command)
{
    bool ret = false;
    ETERM **args = new ETERM*[CMD_ARGS_COUNT(FREE_SESSION_POOL)];
    ETERM *resp = NULL;

    MAP_ARGS(command, args);

	REMOTE_LOG("----- FREE POOL -----\n");
	if(ARG_COUNT(command) != CMD_ARGS_COUNT(FREE_SESSION_POOL)) {
        resp = erl_format((char*)"{~w,~i,error,badarg}", args[0], FREE_SESSION_POOL);
		REMOTE_LOG("ERROR badarg\n");
        goto error_exit;
	}

    if (oci_free_session_pool())
        resp = erl_format((char*)"{~w,~i}", args[0], FREE_SESSION_POOL);
    else {
        int err_str_len = 0;
        char * err_str;
        get_last_error(NULL, err_str_len);
        err_str = new char[err_str_len+1];
        get_last_error(err_str, err_str_len);
		REMOTE_LOG("CMD: free session pool ERROR - %s", err_str);
        resp = erl_format((char*)"{~w,~i,error,~s}", args[0], FREE_SESSION_POOL, err_str);
    }

error_exit:
    if(write_resp(resp) < 0)
        ret = true;

    erl_free_compound(command);

	REMOTE_LOG("---------------------\n");
    return ret;
}

bool cmd_exec_sql(ETERM * command)
{
    ETERM **args = new ETERM*[CMD_ARGS_COUNT(EXEC_SQL)];
    ETERM * resp;

    MAP_ARGS(command, args);

	REMOTE_LOG("----- EXECUTE SQL -----\n");
    if(ARG_COUNT(command) != CMD_ARGS_COUNT(EXEC_SQL))
        goto error_exit_pre;

    // Args: Conn Handle, Sql Statement
    if(
#ifdef __WIN32__
    ERL_IS_INTEGER(args[1]) &&
#else
    (ERL_IS_UNSIGNED_LONGLONG(args[1]) || ERL_IS_LONGLONG(args[1])) &&
#endif
       ERL_IS_BINARY(args[2]) &&
       ERL_IS_LIST(args[3])) {

        LOG_ARGS(ARG_COUNT(command), args, "Execute SQL");

#ifdef __WIN32__
		void * conn_handle = (void *)ERL_INT_VALUE(args[1]);
#else
		void * conn_handle = (void *)ERL_LL_UVALUE(args[1]);
#endif
        inp_t * bind_args = map_to_bind_args(args[3]);
        void * statement_handle = NULL;

        /* Transfer the columns */
        ETERM *columns = NULL;
		//sprintf(conn_handle_str, "%p", conn_handle);
		switch(oci_exec_sql(conn_handle, &statement_handle, ERL_BIN_PTR(args[2]), ERL_BIN_SIZE(args[2]), bind_args, &columns, append_coldef_to_list)) {
			case SUCCESS:
				if (columns == NULL && bind_args != NULL)
					resp = erl_format((char*)"{~w,~i,{executed,~w}}", args[0], EXEC_SQL, build_term_from_bind_args(bind_args));
				else if (columns == NULL && bind_args == NULL)
					resp = erl_format((char*)"{~w,~i,{executed,no_ret}}", args[0], EXEC_SQL);
				else {
					resp = erl_format((char*)"{~w,~i,{{stmt,~w},{cols,~w}}}", args[0], EXEC_SQL, erl_mk_ulonglong((unsigned long long)statement_handle), columns);
				}
				if(write_resp(resp) < 0) goto error_exit;
				REMOTE_LOG("SUCCESS\n");
				break;
			case CONTINUE_WITH_ERROR:
                {
					int err_str_len = 0;
					char * err_str;
					get_last_error(NULL, err_str_len);
					err_str = new char[err_str_len+1];
					get_last_error(err_str, err_str_len);
					REMOTE_LOG("ERROR Execute SQL %s\n", err_str);
					resp = erl_format((char*)"{~w,~i,{error,~s}}", args[0], EXEC_SQL, err_str);
					delete err_str;
					write_resp(resp);
				}
				REMOTE_LOG("CONTINUE_WITH_ERROR\n");
				break;
			case FAILURE:
                {
					int err_str_len = 0;
					char * err_str;
					get_last_error(NULL, err_str_len);
					err_str = new char[err_str_len+1];
					get_last_error(err_str, err_str_len);
					REMOTE_LOG("ERROR Execute SQL %s\n", err_str);
					resp = erl_format((char*)"{~w,~i,{error,~s}}", args[0], EXEC_SQL, err_str);
					delete err_str;
					write_resp(resp);
					goto error_exit;
				}
				REMOTE_LOG("FAILURE\n");
				break;
        }
    } else {
error_exit_pre:
        resp = erl_format((char*)"{~w,~i,error,badarg}", args[0], EXEC_SQL);
		REMOTE_LOG("ERROR badarg %d\n", ERL_TYPE(args[1]));

		/*     if(ERL_IS_INTEGER(args[1]))			REMOTE_LOG("conn %d\n", ERL_INT_VALUE(args[1])));
		else if(ERL_IS_UNSIGNED_INTEGER(args[1]))	REMOTE_LOG("conn %d\n", ERL_INT_UVALUE(args[1])));
		else if(ERL_IS_LONGLONG(args[1]))	REMOTE_LOG("conn %d\n", ERL_LL_VALUE(args[1])));
		else if(ERL_IS_UNSIGNED_INTEGER(args[1]))	REMOTE_LOG("conn %d\n", ERL_INT_UVALUE(args[1])));
		else if(ERL_IS_UNSIGNED_INTEGER(args[1]))	REMOTE_LOG("conn %d\n", ERL_INT_UVALUE(args[1])));
		else if(ERL_IS_UNSIGNED_INTEGER(args[1]))	REMOTE_LOG("conn %d\n", ERL_INT_UVALUE(args[1])));
		else if(ERL_IS_UNSIGNED_INTEGER(args[1]))	REMOTE_LOG("conn %d\n", ERL_INT_UVALUE(args[1])));
		else if(ERL_IS_UNSIGNED_INTEGER(args[1]))	REMOTE_LOG("conn %d\n", ERL_INT_UVALUE(args[1])));
		else if(ERL_IS_UNSIGNED_INTEGER(args[1]))	REMOTE_LOG("conn %d\n", ERL_INT_UVALUE(args[1])));*/

		if(write_resp(resp) < 0) goto error_exit;
    }
    erl_free_compound(command);
    delete args;
	REMOTE_LOG("-----------------------\n");
    return false;

error_exit:
    erl_free_compound(command);
    delete args;
	REMOTE_LOG("-----------------------\n");
    return true;
}

bool cmd_fetch_rows(ETERM * command)
{
    ETERM **args = new ETERM*[CMD_ARGS_COUNT(FETCH_ROWS)];
    ETERM * resp;

    MAP_ARGS(command, args);

	REMOTE_LOG("----- FETCH ROWS -----\n");
    if(ARG_COUNT(command) != CMD_ARGS_COUNT(FETCH_ROWS))
        goto error_exit_pre;

    // Args: Connection Handle, Statement Handle	
    if(
#ifdef __WIN32__
    ERL_IS_INTEGER(args[1]) &&
#else
    (ERL_IS_UNSIGNED_LONGLONG(args[1]) || ERL_IS_LONGLONG(args[1])) &&
#endif
#ifdef __WIN32__
    ERL_IS_INTEGER(args[2])
#else
    (ERL_IS_UNSIGNED_LONGLONG(args[2]) || ERL_IS_LONGLONG(args[2]))
#endif
    ) {

#ifdef __WIN32__
		void * conn_handle = (void *)ERL_INT_VALUE(args[1]);
#else
		void * conn_handle = (void *)ERL_LL_UVALUE(args[1]);
#endif
#ifdef __WIN32__
		void * statement_handle = (void *)ERL_INT_VALUE(args[2]);
#else
        void * statement_handle = (void *)ERL_LL_UVALUE(args[2]);
#endif

        inp_t * bind_args = map_to_bind_args(args[3]);

        /* Transfer the rows */
        if (statement_handle != NULL) {
            ETERM *rows = NULL;
			ROW_FETCH row_fetch = oci_produce_rows(statement_handle, &rows, append_string_to_list, append_list_to_list, calculate_resp_size);
            if (row_fetch != ERROR) {
                if (rows == NULL) {
                    resp = erl_format((char*)"{~w,~i,{{rows,[]},done}}", args[0], FETCH_ROWS);
                    if(write_resp(resp) < 0) goto error_exit;
                } else {
                    resp = erl_format((char*)"{~w,~i,{{rows,~w},~a}}", args[0], FETCH_ROWS, rows, (row_fetch == MORE ? "more" : "done"));
					//erl_free_compound(rows);
                    //resp = erl_format((char*)"{~i,~i,{{rows,[]},more}}", FETCH_ROWS, (unsigned long)connection_handle);
                    if(write_resp(resp) < 0) goto error_exit;
                }
            }
        }
    } else {
error_exit_pre:
        resp = erl_format((char*)"{~w,~i,error,badarg}", args[0], FETCH_ROWS);
		REMOTE_LOG("ERROR badarg\n");
        if(write_resp(resp) < 0) goto error_exit;
    }
    erl_free_compound(command);
    delete args;
	REMOTE_LOG("----------------------\n");
    return false;

error_exit:
    erl_free_compound(command);
    delete args;
	REMOTE_LOG("----------------------\n");
    return true;
}

static FILE *tfp = NULL;
static char buffer[1024*1024];

bool cmd_processor(void * param)
{
	ETERM *command = (ETERM *)param;
    ETERM *cmd = erl_element(2, (ETERM *)command);

    REMOTE_LOG("========================================\n");

	if(tfp != NULL) fclose(tfp);
	tfp = tmpfile();
	erl_print_term(tfp, command);
	rewind(tfp);
	fread(buffer, 1, sizeof(buffer), tfp);
	REMOTE_LOG("COMMAND : %s %s\n", cmdnames[ERL_INT_VALUE(cmd)], buffer);

	if(ERL_IS_INTEGER(cmd)) {
        switch(ERL_INT_VALUE(cmd)) {
        case CREATE_SESSION_POOL:	return cmd_create_tns_ssn_pool(command);
        case GET_SESSION:			return cmd_get_session(command);
        case RELEASE_SESSION:		return cmd_release_conn(command);
        case EXEC_SQL:				return cmd_exec_sql(command);
        case FETCH_ROWS:			return cmd_fetch_rows(command);
        case R_DEBUG_MSG:			return change_log_flag(command);
        case FREE_SESSION_POOL:		return cmd_free_ssn_pool(command);
        case QUIT:
        default:
            break;
        }
    }
    return true;
}
