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
 *
 * @file occi_lib_intf.cpp
 * @brief Oracle access interface through OCCI
 * @author bikram.chatterjee@k2informatics.ch
 */ 

#ifdef OCCI

#include <iostream>
#include <cstring>
#include <string>
#include <stdlib.h>

#include "oci_lib_intf.h"

#include <occi.h>

#ifdef __WIN32__
#define	SPRINT sprintf_s
#else
#define	SPRINT snprintf
#endif

using namespace std;
using namespace oracle::occi;

/*
 * Globals
 */
Environment             *env = NULL;
StatelessConnectionPool *pool = NULL;

void oci_init(void)
{
	try {
        env = Environment::createEnvironment(Environment::THREADED_MUTEXED);
	} catch (SQLException const & ex) {
    	REMOTE_LOG("oci_init error: %s\n", ex.getMessage().c_str());
        goto error_exit;
	} catch (exception const & ex) {
    	REMOTE_LOG("oci_init error: %s\n", ex.what());
        goto error_exit;
	} catch (string const & ex) {
    	REMOTE_LOG("oci_init error: %s\n", ex.c_str());
        goto error_exit;
	} catch (...) {
    	REMOTE_LOG("oci_init exit with unknown error\n");
        goto error_exit;
	}

    return;

error_exit:
    exit(0);
}

void oci_cleanup(void)
{
	try {
		Environment::terminateEnvironment(env);
	} catch (SQLException const & ex) {
    	REMOTE_LOG("failed createEnvironment error: %s\n", ex.getMessage().c_str());
        goto error_exit;
	} catch (exception const & ex) {
    	REMOTE_LOG("failed createEnvironment error: %s\n", ex.what());
        goto error_exit;
	} catch (string const & ex) {
    	REMOTE_LOG("failed createEnvironment error: %s\n", ex.c_str());
        goto error_exit;
	} catch (...) {
    	REMOTE_LOG("failed createEnvironment with unknown error\n");
        goto error_exit;
	}
    return;

error_exit:
    exit(0);
}

intf_ret oci_create_tns_seesion_pool(const char * connect_str, const int connect_str_len,
                                     const char * user_name, const int user_name_len,
                                     const char * password, const int password_len,
                                     const char * options, const int options_len)
{
	intf_ret r;
	r.fn_ret = SUCCESS;

    char * uname = new char[user_name_len+1];
    char * pswd = new char[password_len+1];
    char * connstr = new char[connect_str_len+1];

    sprintf(uname, "%.*s", user_name_len, user_name);
    sprintf(pswd, "%.*s", password_len, password);
    sprintf(connstr, "%.*s", connect_str_len, connect_str);

	try {
        pool = env->createStatelessConnectionPool(uname, pswd, connstr,
                                                  1, 0, 1,
                                                  StatelessConnectionPool::HOMOGENEOUS);
	} catch (SQLException const & ex) {
	    r.fn_ret = FAILURE;
        r.gerrcode = ex.getErrorCode();
        std::strcpy(r.gerrbuf,ex.getMessage().c_str());
    	REMOTE_LOG("create pool error: %s\n", r.gerrbuf);
        return r;
	} catch (exception const & ex) {
	    r.fn_ret = FAILURE;
        std::strcpy(r.gerrbuf,ex.what());
    	REMOTE_LOG("create pool error: %s\n", r.gerrbuf);
        return r;
	} catch (string const & ex) {
	    r.fn_ret = FAILURE;
        std::strcpy(r.gerrbuf,ex.c_str());
    	REMOTE_LOG("create pool error: %s\n", r.gerrbuf);
        return r;
	} catch (...) {
	    r.fn_ret = FAILURE;
        std::strcpy(r.gerrbuf,"unknown");
    	REMOTE_LOG("create pool exit with unknown error\n");
        return r;
	}

    REMOTE_LOG("created pool %s with %s, %s, %s\n", pool->getPoolName().c_str(), uname, pswd, connstr);

    delete uname;
    delete pswd;
    delete connstr;

    return r;
}

intf_ret oci_free_session_pool(void)
{
	intf_ret r;
	r.fn_ret = SUCCESS;

	try {
        env->terminateStatelessConnectionPool(pool);
	} catch (SQLException const & ex) {
	    r.fn_ret = FAILURE;
        r.gerrcode = ex.getErrorCode();
        std::strcpy(r.gerrbuf,ex.getMessage().c_str());
    	REMOTE_LOG("terminate pool error: %s\n", r.gerrbuf);
        return r;
	} catch (exception const & ex) {
	    r.fn_ret = FAILURE;
        std::strcpy(r.gerrbuf,ex.what());
    	REMOTE_LOG("terminate pool error: %s\n", r.gerrbuf);
        return r;
	} catch (string const & ex) {
	    r.fn_ret = FAILURE;
        std::strcpy(r.gerrbuf,ex.c_str());
    	REMOTE_LOG("terminate pool error: %s\n", r.gerrbuf);
        return r;
	} catch (...) {
	    r.fn_ret = FAILURE;
        std::strcpy(r.gerrbuf,"unknown");
    	REMOTE_LOG("terminate pool exit with unknown error\n");
        return r;
	}

    return r;
}

intf_ret oci_get_session_from_pool(void **conn_handle)
{
	intf_ret r;
	r.fn_ret = SUCCESS;
    Connection * conn = NULL;
    
	try {
        conn = pool->getAnyTaggedConnection("");
	} catch (SQLException const & ex) {
	    r.fn_ret = FAILURE;
        r.gerrcode = ex.getErrorCode();
        std::strcpy(r.gerrbuf,ex.getMessage().c_str());
    	REMOTE_LOG("get connection error: %s\n", r.gerrbuf);
        return r;
	} catch (exception const & ex) {
	    r.fn_ret = FAILURE;
        std::strcpy(r.gerrbuf,ex.what());
    	REMOTE_LOG("get connection error: %s\n", r.gerrbuf);
        return r;
	} catch (string const & ex) {
	    r.fn_ret = FAILURE;
        std::strcpy(r.gerrbuf,ex.c_str());
    	REMOTE_LOG("get connection error: %s\n", r.gerrbuf);
        return r;
	} catch (...) {
	    r.fn_ret = FAILURE;
        std::strcpy(r.gerrbuf,"unknown");
    	REMOTE_LOG("get connection with unknown error\n");
        return r;
	}

    *conn_handle = conn;

    return r;
}

intf_ret oci_return_connection_to_pool(void * conn_handle)
{
	intf_ret r;
	r.fn_ret = SUCCESS;
    Connection * conn = (Connection *)conn_handle;
    
	try {
        pool->releaseConnection(conn, "");
	} catch (SQLException const & ex) {
	    r.fn_ret = FAILURE;
        r.gerrcode = ex.getErrorCode();
        std::strcpy(r.gerrbuf,ex.getMessage().c_str());
    	REMOTE_LOG("release connection error: %s\n", r.gerrbuf);
        return r;
	} catch (exception const & ex) {
	    r.fn_ret = FAILURE;
        std::strcpy(r.gerrbuf,ex.what());
    	REMOTE_LOG("release connection error: %s\n", r.gerrbuf);
        return r;
	} catch (string const & ex) {
	    r.fn_ret = FAILURE;
        std::strcpy(r.gerrbuf,ex.c_str());
    	REMOTE_LOG("release connection error: %s\n", r.gerrbuf);
        return r;
	} catch (...) {
	    r.fn_ret = FAILURE;
        std::strcpy(r.gerrbuf,"unknown");
    	REMOTE_LOG("release connection with unknown error\n");
        return r;
	}

    return r;
}

intf_ret oci_exec_sql(const void *conn_handle, void ** stmt_handle,
                      const unsigned char * query_str, int query_str_len,
                      inp_t *params_head, void * column_list,
                      void (*coldef_append)(const char *, const char *, const unsigned int, void *))
{
    unsigned int i;
	intf_ret r;
	r.fn_ret = SUCCESS;
    Connection * conn = (Connection *)conn_handle;
    Statement * stmt = NULL;

    char * qstr = new char[query_str_len+1];

    sprintf(qstr, "%.*s", query_str_len, query_str);

	try {
        stmt = conn->createStatement();
        stmt->setSQL(qstr);
#if 1
        switch(stmt->status()) {
            case Statement::UNPREPARED:
    	        REMOTE_LOG("Statement UNPREPARED\n");
                break;
            case Statement::PREPARED:
    	        REMOTE_LOG("Statement PREPARED\n");
                break;
            case Statement::RESULT_SET_AVAILABLE:
    	        REMOTE_LOG("Statement RESULT_SET_AVAILABLE\n");
                break;
            case Statement::UPDATE_COUNT_AVAILABLE:
    	        REMOTE_LOG("Statement UPDATE_COUNT_AVAILABLE\n");
                break;
            case Statement::NEEDS_STREAM_DATA:
    	        REMOTE_LOG("Statement NEEDS_STREAM_DATA\n");
                break;
            default:
    	        REMOTE_LOG("Statement %d\n", stmt->status());
                break;
        }
#endif
        stmt->execute();
        delete qstr;
        qstr = NULL;
        if(stmt->status() == Statement::RESULT_SET_AVAILABLE) {
		    ResultSet *rs = stmt->getResultSet();
            vector<MetaData> listOfColumns = rs->getColumnListMetaData();
            for (i=0; i < listOfColumns.size(); i++) {
                MetaData columnObj=listOfColumns[i];
                (*coldef_append)(columnObj.getString(MetaData::ATTR_NAME).c_str(),
                                 columnObj.getString(MetaData::ATTR_TYPE_NAME).c_str(),
                                 columnObj.getInt(MetaData::ATTR_DATA_SIZE),
                                 column_list);
            }
        }
        else {
            r = oci_close_statement(stmt);
            return r;
        }
	} catch (SQLException const & ex) {
        if (stmt)
            oci_close_statement(stmt);
	    r.fn_ret = CONTINUE_WITH_ERROR;
        r.gerrcode = ex.getErrorCode();
        std::strcpy(r.gerrbuf,ex.getMessage().c_str());
    	REMOTE_LOG("exec sql error: %s\n", r.gerrbuf);
        return r;
	} catch (exception const & ex) {
	    r.fn_ret = FAILURE;
        std::strcpy(r.gerrbuf,ex.what());
    	REMOTE_LOG("exec sql error: %s\n", r.gerrbuf);
        return r;
	} catch (string const & ex) {
	    r.fn_ret = FAILURE;
        std::strcpy(r.gerrbuf,ex.c_str());
    	REMOTE_LOG("exec sql error: %s\n", r.gerrbuf);
        return r;
	} catch (...) {
	    r.fn_ret = FAILURE;
        std::strcpy(r.gerrbuf,"unknown");
    	REMOTE_LOG("exec sql with unknown error\n");
        return r;
	}

    *stmt_handle = stmt;

    if(qstr)
        delete qstr;

    return r;
}

intf_ret oci_produce_rows(void * stmt_handle
						 , void * row_list
						 , void (*string_append)(const char * string, int len, void * list)
						 , void (*list_append)(const void * sub_list, void * list)
						 , unsigned int (*sizeof_resp)(void * resp)
                         , int maxrowcount)
{
	unsigned long int total_est_row_size = 0;
    unsigned int num_rows = 0;
	intf_ret r;
    unsigned int i;
    Statement * stmt = (Statement *)stmt_handle;
    void * row = NULL;

	r.fn_ret = SUCCESS;
	try {
        if(stmt->status() == Statement::RESULT_SET_AVAILABLE) {
		    ResultSet *rs = stmt->getResultSet();
            vector<MetaData> listOfColumns = rs->getColumnListMetaData();

            while (num_rows < maxrowcount
			       && total_est_row_size < MAX_RESP_SIZE
                   && rs->next() // IMP: must be the last condition to ensure no extra fetch
            ) {
                ++num_rows;
                row = NULL;
		        //if(num_rows % 100 == 0) REMOTE_LOG("OCI: Fetched %lu rows of %d bytes\n", num_rows, total_est_row_size);

			    for (i = 0; i < listOfColumns.size(); ++i) {
                    MetaData columnObj=listOfColumns[i];
                    string str = rs->getString(i+1);
				    (*string_append)(str.c_str(), strlen(str.c_str()), &row);
                }
			    total_est_row_size += (*sizeof_resp)(&row);
			    (*list_append)(row, row_list);
            }
            r.fn_ret = MORE;
            if(rs->status() == ResultSet::END_OF_FETCH) {
                r = oci_close_statement(stmt_handle);
                if(r.fn_ret == SUCCESS)
                    r.fn_ret = DONE;
                return r;
            }
		}
	} catch (SQLException const & ex) {
        if (stmt)
            oci_close_statement(stmt);
	    r.fn_ret = CONTINUE_WITH_ERROR;
        r.gerrcode = ex.getErrorCode();
        std::strcpy(r.gerrbuf,ex.getMessage().c_str());
    	REMOTE_LOG("rows error: %s\n", r.gerrbuf);
        return r;
	} catch (exception const & ex) {
	    r.fn_ret = FAILURE;
        std::strcpy(r.gerrbuf,ex.what());
    	REMOTE_LOG("rows error: %s\n", r.gerrbuf);
        return r;
	} catch (string const & ex) {
	    r.fn_ret = FAILURE;
        std::strcpy(r.gerrbuf,ex.c_str());
    	REMOTE_LOG("rows error: %s\n", r.gerrbuf);
        return r;
	} catch (...) {
	    r.fn_ret = FAILURE;
        std::strcpy(r.gerrbuf,"unknown");
    	REMOTE_LOG("rows with unknown error\n");
        return r;
	}

    return r;
}

intf_ret oci_close_statement(void * stmt_handle)
{
	intf_ret r;
	r.fn_ret = SUCCESS;

    Statement * stmt = (Statement *)stmt_handle;
	try {
        Connection * conn = stmt->getConnection();
		conn->terminateStatement(stmt);
	} catch (SQLException const & ex) {
	    r.fn_ret = FAILURE;
        r.gerrcode = ex.getErrorCode();
        std::strcpy(r.gerrbuf,ex.getMessage().c_str());
    	REMOTE_LOG("close statement error: %s\n", r.gerrbuf);
        return r;
	} catch (exception const & ex) {
	    r.fn_ret = FAILURE;
        std::strcpy(r.gerrbuf,ex.what());
    	REMOTE_LOG("close statement error: %s\n", r.gerrbuf);
        return r;
	} catch (string const & ex) {
	    r.fn_ret = FAILURE;
        std::strcpy(r.gerrbuf,ex.c_str());
    	REMOTE_LOG("close statement error: %s\n", r.gerrbuf);
        return r;
	} catch (...) {
	    r.fn_ret = FAILURE;
        std::strcpy(r.gerrbuf,"unknown");
    	REMOTE_LOG("close statement with unknown error\n");
        return r;
	}

    return r;
}

#endif //OCCI

#if 0
int main(void)
{
	const char* userName = "bikram";
	const char* password = "abcd123";
	const char* connectString = "(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=tcp)(HOST=80.67.144.206)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=XE)))";
	const char* sql = "SELECT * FROM ALL_TABLES";
    int i = 0;

	try {
		Environment *env = Environment::createEnvironment(Environment::THREADED_MUTEXED);
		Connection *conn = env->createConnection((char*)userName, (char*)password, (char*)connectString);

        for (i=1000; i>0; --i) {
            int rows = 0;
		    Statement *stmt = conn->createStatement(sql);
		    ResultSet *rs = stmt->executeQuery();
            vector<MetaData> listOfColumns = rs->getColumnListMetaData();
            cout << i << " columns " << listOfColumns.size() << endl;
            for (i=0; i < listOfColumns.size(); i++) {
                MetaData columnObj=listOfColumns[i];
                //cout << (columnObj.getString(MetaData::ATTR_NAME))
                //     << "(" << (columnObj.getInt(MetaData::ATTR_DATA_TYPE)) << "), ";
            }
            while(rs->next()) {
                ++rows;
                for (i=0; i < listOfColumns.size(); i++)
                    string str = rs->getString(i+1);
                    //cout << rs->getString(j+1) << ", ";
                //cout << endl;
            }
            cout << i << " rows " << rows << endl;
            cout << "----------------------" << endl;
		    rs->next();
		    stmt->closeResultSet(rs);
		    conn->terminateStatement(stmt);
        }

		env->terminateConnection(conn);
		Environment::terminateEnvironment(env);
		cout << "success!!!!" << endl;
	} catch (std::exception const & ex) {
		cout << ex.what() << endl;
	} catch (std::string const & ex) {
		cout << ex << endl;
	} catch (...) {
		// ...
	}

    return 0;
}
#endif
