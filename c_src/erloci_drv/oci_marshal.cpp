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

#include "oci_marshal.h"
#include "erl_interface.h"
#include <ocidfn.h>

#ifdef __WIN32__
#include <windows.h>
#else
#include <pthread.h>
#endif

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

const erlcmdtable cmdtbl[] = CMDTABLE;

char * print_term(void *term)
{
	FILE *tfp = NULL;
	long sz = 0;
	char *termbuffer = NULL;

	tfp = tmpfile();
	erl_print_term(tfp, (ETERM*)term);
	fseek(tfp, 0L, SEEK_END);
	sz = ftell(tfp);
	rewind(tfp);
	termbuffer = new char[sz+1];
	fread(termbuffer, 1, sz, tfp);
	termbuffer[sz] = '\0';
	fclose(tfp);
	return termbuffer;
}

size_t calculate_resp_size(void * resp)
{
    return (size_t)erl_term_len(*(ETERM**)resp);
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

    ETERM *new_container_list = erl_cons((ETERM*)sub_list, container_list);
    erl_free_term((ETERM*)sub_list);
	erl_free_term(container_list);

    (*(ETERM**)list) = new_container_list;
}

void append_int_to_list(const int integer, void * list)
{
    if (list==NULL)
        return;

    ETERM *container_list = (ETERM *)(*(ETERM**)list);
    if (container_list == NULL)
        container_list = erl_mk_empty_list();

    ETERM *new_container_list = erl_cons(erl_format((char*)"~i", integer), container_list);
	erl_free_term(container_list);
    (*(ETERM**)list) = new_container_list;
}

void append_string_to_list(const char * string, size_t len, void * list)
{
    if (list==NULL)
        return;

    ETERM *container_list = (ETERM *)(*(ETERM**)list);
    if (container_list == NULL)
        container_list = erl_mk_empty_list();

	ETERM *binstr = erl_mk_binary(string, len);
	ETERM *new_container_list = erl_cons(binstr, container_list);
    erl_free_term(binstr);
    erl_free_term(container_list);
	(*(ETERM**)list) = new_container_list;
}

void append_coldef_to_list(const char * col_name, const unsigned short data_type, const unsigned int max_len, void * list)
{
    if (list==NULL)
        return;

    ETERM *container_list = (ETERM *)(*(ETERM**)list);
    if (container_list == NULL)
        container_list = erl_mk_empty_list();

	ETERM *cname = erl_mk_binary(col_name, strlen(col_name));
    ETERM *new_container_list = erl_cons(erl_format((char*)"{~w,~i,~i}", cname, data_type, max_len), container_list);
    erl_free_term(cname);
    erl_free_term(container_list);

    (*(ETERM**)list) = new_container_list;
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

void * read_cmd(void)
{
    pkt_hdr hdr;
    unsigned int rx_len;
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

	// allocate for the entire term
	// to be freeded in the caller's scope
    rx_buf = new char[rx_len];
    if (rx_buf == NULL) {
        EXIT();
        return NULL;
    }

	// Read the Term binary
    cin.read(rx_buf, rx_len);
    if((unsigned int) cin.gcount() < rx_len) {
        // Unable to get Term binary
        delete rx_buf;
        EXIT();
        return NULL;
    }

	LOG_DUMP(rx_len, rx_buf);

    EXIT();
    return rx_buf;
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

void map_schema_to_bind_args(void * _args, vector<var> & vars)
{
    ETERM * args = (ETERM *)_args;
    if(!ERL_IS_LIST(args) || ERL_IS_EMPTY_LIST(args))
        return;

    int num_vars = erl_length(args);
    var v;

    ETERM * item = NULL;
    ETERM * arg = NULL;
	size_t len = 0;
    do {
        if ((item = erl_hd(args)) == NULL	||
            !ERL_IS_TUPLE(item)				||
            erl_size(item) != 2)
            break;

        if ((arg = erl_element(1, item)) == NULL || !ERL_IS_BINARY(arg))
            break;
		len = ERL_BIN_SIZE(arg);
		if(sizeof(v.name) < len+1) {
			REMOTE_LOG("variable %.*s is too long, max %d\n", len, (char*)ERL_BIN_PTR(arg), sizeof(v.name)-1);
			throw string("variable name is larger then 255 characters");
		}
        strncpy(v.name, (char*)ERL_BIN_PTR(arg), len);
		v.name[len]='\0';

        if ((arg = erl_element(2, item)) == NULL || !ERL_IS_INTEGER(arg))
            break;
        v.dty = (unsigned short)ERL_INT_VALUE(arg);

		// Initialized, to be prepared later on first execute
		v.value_sz = 0;
		v.datap = NULL;
		v.datap_len = 0;

		vars.push_back(v);

        args = erl_tl(args);
    } while (args != NULL && !ERL_IS_EMPTY_LIST(args));
}

void map_value_to_bind_args(void * _args, vector<var> & vars)
{
    ETERM * args = (ETERM *)_args;
    if(!ERL_IS_LIST(args) || ERL_IS_EMPTY_LIST(args))
        return;

    ETERM * item = NULL;
    ETERM * arg = NULL;
	void * tmp_arg = NULL;
	size_t len = 0;
	unsigned short arg_len = 0;
	
	// remove any old bind from the vars
	for(int i=0; i < vars.size(); ++i) {
		vars[i].valuep.clear();
		vars[i].alen.clear();
	}
	
	// loop through the list
    do {
        if ((item = erl_hd(args)) == NULL	||
            !ERL_IS_TUPLE(item)				||
			erl_size(item) != vars.size())
            break;

		len = erl_size(item);

		// loop through each value of the list
		for(int i=0; i<len; ++i) {
			if ((arg = erl_element(i+1, item)) == NULL)
		        break;
			
			switch(vars[i].dty) {
				case SQLT_BFLOAT:
				case SQLT_BDOUBLE:
				case SQLT_FLT:
					if(ERL_IS_INTEGER(arg) || ERL_IS_FLOAT(arg)) {
						arg_len = sizeof(double);
						tmp_arg = new double;
						*(double*)tmp_arg = (double)(ERL_IS_INTEGER(arg) ? ERL_INT_VALUE(arg) : ERL_FLOAT_VALUE(arg));
					}
					break;
				case SQLT_NUM:
				case SQLT_INT:
					if(ERL_IS_INTEGER(arg)) {
						arg_len = sizeof(int);
						tmp_arg = new int;
						*(int*)tmp_arg = (int)ERL_INT_VALUE(arg);
					}
					break;
				case SQLT_RDD:
				case SQLT_DAT:
					if(ERL_IS_BINARY(arg)) {
						arg_len = ERL_BIN_SIZE(arg);
						tmp_arg = new char[arg_len];
						memcpy(tmp_arg, ERL_BIN_PTR(arg), arg_len);
					}
					break;
				case SQLT_STR:
				case SQLT_CHR:
					if(ERL_IS_BINARY(arg)) {
						arg_len = ERL_BIN_SIZE(arg);
						tmp_arg = new char[arg_len+1];
						memcpy(tmp_arg, ERL_BIN_PTR(arg), arg_len);
						((char*)tmp_arg)[arg_len]='\0';
						arg_len++;
					}
					break;
			}
			vars[i].alen.push_back(arg_len);
			if (arg_len > vars[i].value_sz)
				vars[i].value_sz = arg_len;
			vars[i].valuep.push_back(tmp_arg);
		}

        args = erl_tl(args);
    } while (args != NULL && !ERL_IS_EMPTY_LIST(args));
}