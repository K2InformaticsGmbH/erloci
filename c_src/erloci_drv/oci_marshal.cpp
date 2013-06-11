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

#include <iostream>

#include "oci_marshal.h"
#include "erl_interface.h"

#ifdef __WIN32__
#include <windows.h>
#else
#include <pthread.h>
#endif

using namespace std;

#if DEBUG <= DBG_1
#define ENTRY()	{REMOTE_LOG("Entry\n");}
#define EXIT()	{REMOTE_LOG("Exit\n");}
#else
#define ENTRY()
#define EXIT()
#endif

// 32 bit packet header
typedef union _pack_hdr {
    char len_buf[4];
    u_long len;
} pkt_hdr;

const char *cmdnames[] = {
                        	"CREATE_SESSION_POOL",
							"FREE_SESSION_POOL",

							"GET_SESSION",
							"RELEASE_SESSION",

                            "PREP_STMT",
                            "BIND_STMT",
							"EXEC_STMT",
							"FETCH_ROWS",

							"R_DEBUG_MSG",
							"QUIT",
						 };

void * build_term_from_bind_args(inp_t * bind_var_list_head)
{
    if (bind_var_list_head == NULL)
        return NULL;

    inp_t * param_t = NULL;
    ETERM * resp_args = erl_mk_empty_list();
    for(inp_t *param = bind_var_list_head; param != NULL;) {
        if(param->dir == DIR_OUT || param->dir == DIR_INOUT) {
            switch(param->dty) {
            case NUMBER:
                resp_args = erl_cons(erl_mk_int(*(int*)param->vp), resp_args);
                break;
            case STRING:
                resp_args = erl_cons(erl_mk_string((char*)param->vp), resp_args);
                break;
            }
        }
        param_t = param;
        param = param->next;
        delete param_t;
    }

    return resp_args;
}

inp_t * map_to_bind_args(void * _args)
{
    ETERM * args = (ETERM *)_args;
    if(!ERL_IS_LIST(args) || ERL_IS_EMPTY_LIST(args))
        return NULL;

    int num_vars = erl_length(args),
        idx = 0;
    inp_t * bind_var_list_head	= NULL,
             * bind_var_cur		= NULL,
                   * bind_var_t			= NULL;

    ETERM * item = NULL;
    ETERM * arg = NULL;
    DATA_TYPES dty;
    DATA_DIR dir;
    do {
        if ((item = erl_hd(args)) == NULL	||
            !ERL_IS_TUPLE(item)				||
            erl_size(item) != 3)
            break;

        if ((arg = erl_element(1, item)) == NULL || !ERL_IS_INTEGER(arg))
            break;
        dty = (DATA_TYPES)ERL_INT_VALUE(arg);

        if ((arg = erl_element(2, item)) == NULL || !ERL_IS_INTEGER(arg))
            break;
        dir = (DATA_DIR)ERL_INT_VALUE(arg);

        if ((arg = erl_element(3, item)) == NULL)
            break;
        else {
            bool error = false;
            switch(dty) {
            case NUMBER:
                if (!ERL_IS_INTEGER(arg)) error = true;
                bind_var_t = (inp_t*) new unsigned char[sizeof(inp_t)+sizeof(int)];
                bind_var_t->vlen = sizeof(int);
                bind_var_t->vp = (((char *)bind_var_t) + sizeof(inp_t));
                *(int*)(bind_var_t->vp) = ERL_INT_VALUE(arg);
                break;
            case STRING:
                if (!ERL_IS_BINARY(arg)) error = true;
                bind_var_t = (inp_t*) new unsigned char[sizeof(inp_t)+ERL_BIN_SIZE(arg)+1];
                bind_var_t->vlen = ERL_BIN_SIZE(arg) + 1;
                bind_var_t->vp = (((char *)bind_var_t) + sizeof(inp_t));
                strncpy_s((char*)(bind_var_t->vp), bind_var_t->vlen, (const char *) ERL_BIN_PTR(arg), ERL_BIN_SIZE(arg));
                ((char*)(bind_var_t->vp))[ERL_BIN_SIZE(arg)] = '\0';
                break;
            default:
                break;
            };

            if (!error && bind_var_t != NULL) {
                bind_var_t->dty = dty;
                bind_var_t->dir = dir;
                bind_var_t->next = NULL;
                if (idx == 0)
                    bind_var_list_head = bind_var_cur = bind_var_t;
                else {
                    bind_var_cur->next = bind_var_t;
                    bind_var_cur = bind_var_t;
                }
                bind_var_t = NULL;
            } else
                break;
        }

        args = erl_tl(args);
        ++idx;
    } while (args != NULL && !ERL_IS_EMPTY_LIST(args));

    if (idx != num_vars) {
        while(bind_var_list_head != NULL) {
            bind_var_t = bind_var_list_head;
            bind_var_list_head = bind_var_list_head->next;
            delete bind_var_t;
        }
        bind_var_list_head = NULL;
    }
    return bind_var_list_head;
}


void free_bind_args(inp_t * bind_args)
{
    inp_t * _t = NULL;
    for(inp_t * h = bind_args; h != NULL;) {
        _t = h->next;
        delete h;
        h = _t;
    }
}

unsigned int calculate_resp_size(void * resp)
{
    return (unsigned int)erl_term_len(*(ETERM**)resp);
}

#if DEBUG < DBG_5
void log_args(int argc, void * argv, const char * str)
{
    int sz = 0, idx = 0;
    char *arg = NULL;
    ETERM **args = (ETERM **)argv;
    REMOTE_LOG("CMD: %s Args(\n", str);
    for(idx=0; idx<argc; ++idx) {
        if (ERL_IS_BINARY(args[idx])) {
            sz = ERL_BIN_SIZE(args[idx]);
            arg = new char[sz+1];
            memcpy_s(arg, sz+1, ERL_BIN_PTR(args[idx]), sz);
            arg[sz] = '\0';
            REMOTE_LOG("%s,\n", arg);
            if (arg != NULL) delete arg;
        }
		else if (ERL_IS_INTEGER(args[idx]))				{REMOTE_LOG("%d,",	ERL_INT_VALUE(args[idx])); }
		else if (ERL_IS_UNSIGNED_INTEGER(args[idx]))	{REMOTE_LOG("%u,",	ERL_INT_UVALUE(args[idx]));}
		else if (ERL_IS_FLOAT(args[idx]))				{REMOTE_LOG("%lf,",	ERL_FLOAT_VALUE(args[idx]));}
		else if (ERL_IS_ATOM(args[idx]))				{REMOTE_LOG("%.*s,", ERL_ATOM_SIZE(args[idx]), ERL_ATOM_PTR(args[idx]));}
    }
    REMOTE_LOG(")\n");
}
#else
void log_args(int argc, void * argv, const char * str) {}
#endif

void append_list_to_list(const void * sub_list, void * list)
{
    if (list == NULL || sub_list == NULL)
        return;

    ETERM *container_list = (ETERM *)(*(ETERM**)list);
    if (container_list == NULL)
        container_list = erl_mk_empty_list();

    container_list = erl_cons(erl_format((char*)"~w", (ETERM*)sub_list), container_list);
    erl_free_compound((ETERM*)sub_list);

    (*(ETERM**)list) = container_list;
}

void append_int_to_list(const int integer, void * list)
{
    if (list==NULL)
        return;

    ETERM *container_list = (ETERM *)(*(ETERM**)list);
    if (container_list == NULL)
        container_list = erl_mk_empty_list();

    container_list = erl_cons(erl_format((char*)"~i", integer), container_list);
    (*(ETERM**)list) = container_list;
}

void append_string_to_list(const char * string, int len, void * list)
{
    if (list==NULL)
        return;

    ETERM *container_list = (ETERM *)(*(ETERM**)list);
    if (container_list == NULL)
        container_list = erl_mk_empty_list();

	ETERM *binstr = erl_mk_binary(string, len);
    container_list = erl_cons(erl_format((char*)"~w", binstr), container_list);
    (*(ETERM**)list) = container_list;
}

void append_coldef_to_list(const char * col_name, const char * data_type, const unsigned int max_len, void * list)
{
    if (list==NULL)
        return;

    ETERM *container_list = (ETERM *)(*(ETERM**)list);
    if (container_list == NULL)
        container_list = erl_mk_empty_list();

	ETERM *cname = erl_mk_binary(col_name, strlen(col_name));
    container_list = erl_cons(erl_format((char*)"{~w,~a,~i}", cname, data_type, max_len), container_list);

    (*(ETERM**)list) = container_list;
}

void * read_cmd(void)
{
    pkt_hdr hdr;
    unsigned int rx_len;
    ETERM *t;
    char * rx_buf;

    ENTRY();

    // Read and convert the length to host Byle order
    cin.read(hdr.len_buf, sizeof(hdr.len_buf));
    if((unsigned)cin.gcount() < sizeof(hdr.len_buf)) {
        EXIT();
        return NULL;
    }
    rx_len = ntohl(hdr.len);

    //REMOTE_LOG("RX Packet length %d\n", rx_len);

    // Read the Term binary
    rx_buf = new char[rx_len];
    if (rx_buf == NULL) { // Memory allocation error
        EXIT();
        return NULL;
    }

    cin.read(rx_buf, rx_len);
    if((unsigned int) cin.gcount() < rx_len) {
        // Unable to get Term binary
        delete rx_buf;
        EXIT();
        return NULL;
    }

	LOG_DUMP(rx_len, rx_buf);

	int indx = 0;
    //t = erl_decode((unsigned char*)rx_buf);	
    if (ei_decode_term(rx_buf, &indx, &t) < 0) {
        // Term de-marshaling failed
        delete rx_buf;
        EXIT();
        return NULL;
    }

    if(NULL != rx_buf)
        delete rx_buf;

    EXIT();
    return t;
}

#ifdef __WIN32__
static HANDLE write_mutex;
static HANDLE log_mutex;
#else
static pthread_mutex_t write_mutex;
static pthread_mutex_t log_mutex;
#endif
bool init_marshall(void)
{
#ifdef __WIN32__
    write_mutex = CreateMutex(NULL, FALSE, NULL);
    if (NULL == write_mutex) {
        REMOTE_LOG("Write Mutex creation failed\n");
        return false;
    }
    log_mutex = CreateMutex(NULL, FALSE, NULL);
    if (NULL == log_mutex) {
        REMOTE_LOG("Log Mutex creation failed\n");
        return false;
    }
#else
    if(pthread_mutex_init(&write_mutex, NULL) != 0) {
        REMOTE_LOG("Write Mutex creation failed");
        return false;
    }
    if(pthread_mutex_init(&log_mutex, NULL) != 0) {
        REMOTE_LOG("Log Mutex creation failed");
        return false;
    }
#endif
    return true;
}

int write_resp(void * resp_term)
{
    int tx_len;
    int pkt_len = -1;
    pkt_hdr *hdr;
    unsigned char * tx_buf;
    ETERM * resp = (ETERM *)resp_term;

    if (resp == NULL) {
        pkt_len = -1;
        goto error_exit;
    }

    tx_len = erl_term_len(resp);				// Length of the required binary buffer
    pkt_len = tx_len+PKT_LEN_BYTES;

    //REMOTE_LOG("TX Packet length %d\n", tx_len);

    // Allocate temporary buffer for transmission of the Term
    tx_buf = new unsigned char[pkt_len];
    hdr = (pkt_hdr *)tx_buf;
    hdr->len = htonl(tx_len);		// Length adjusted to network byte order

    erl_encode(resp, tx_buf+PKT_LEN_BYTES);			// Encode the Term into the buffer after the length field

	LOG_DUMP(pkt_len, tx_buf);

    if(
#ifdef __WIN32__
        WAIT_OBJECT_0 == WaitForSingleObject(write_mutex,INFINITE)
#else
        0 == pthread_mutex_lock(&write_mutex)
#endif
    ) {
        cout.write((char *) tx_buf, pkt_len);
        cout.flush();
#ifdef __WIN32__
        ReleaseMutex(write_mutex);
#else
        pthread_mutex_unlock(&write_mutex);
#endif
    }

    // Free the temporary allocated buffer
    delete tx_buf;

    if (cout.fail()) {
        pkt_len = -1;
        goto error_exit;
    }

    EXIT();

error_exit:
    erl_free_compound(resp);
    return pkt_len;
}

bool lock_log()
{
	if(
#ifdef __WIN32__
    WAIT_OBJECT_0 == WaitForSingleObject(log_mutex,INFINITE)
#else
    0 == pthread_mutex_lock(&log_mutex)
#endif
	)
		return true;
	else
		return false;
}

void unlock_log()
{
#ifdef __WIN32__
        ReleaseMutex(log_mutex);
#else
        pthread_mutex_unlock(&log_mutex);
#endif
}
