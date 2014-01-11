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
#include <orl.h>

#ifdef __WIN32__
#include <windows.h>
#include <Winsock2.h>
#else
#include <netinet/in.h>
#include <pthread.h>
#endif

#if DEBUG <= DBG_1
#define ENTRY()	{REMOTE_LOG(DBG, "Entry\n");}
#define EXIT()	{REMOTE_LOG(DBG, "Exit\n");}
#else
#define ENTRY()
#define EXIT()
#endif

const erlcmdtable cmdtbl[] = CMDTABLE;

#ifdef __WIN32__
static HANDLE write_mutex;
#else
static pthread_mutex_t write_mutex;
#endif

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
    REMOTE_LOG(DBG, "CMD: %s Args(\n", str);
    for(idx=0; idx<argc; ++idx) {
        if (ERL_IS_BINARY(args[idx])) {
            sz = ERL_BIN_SIZE(args[idx]);
            arg = new char[sz+1];
            memcpy_s(arg, sz+1, ERL_BIN_PTR(args[idx]), sz);
            arg[sz] = '\0';
            REMOTE_LOG(DBG, "%s,\n", arg);
            if (arg != NULL) delete arg;
        }
		else if (ERL_IS_INTEGER(args[idx]))				{REMOTE_LOG(DBG, "%d,",	ERL_INT_VALUE(args[idx])); }
		else if (ERL_IS_UNSIGNED_INTEGER(args[idx]))	{REMOTE_LOG(DBG, "%u,",	ERL_INT_UVALUE(args[idx]));}
		else if (ERL_IS_FLOAT(args[idx]))				{REMOTE_LOG(DBG, "%lf,",	ERL_FLOAT_VALUE(args[idx]));}
		else if (ERL_IS_ATOM(args[idx]))				{REMOTE_LOG(DBG, "%.*s,", ERL_ATOM_SIZE(args[idx]), ERL_ATOM_PTR(args[idx]));}
    }
    REMOTE_LOG(DBG, ")\n");
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

	if (string) {
		ETERM *binstr = erl_mk_binary(string, len);
		ETERM *new_container_list = erl_cons(binstr, container_list);
		erl_free_term(binstr);
		erl_free_term(container_list);
		(*(ETERM**)list) = new_container_list;
	} else {
		(*(ETERM**)list) = container_list;
	}
}

void append_coldef_to_list(const char * col_name, size_t len, const unsigned short data_type, const unsigned int max_len,
						   const unsigned short precision, const signed char scale, void * list)
{
    if (list==NULL)
        return;

    ETERM *container_list = (ETERM *)(*(ETERM**)list);
    if (container_list == NULL)
        container_list = erl_mk_empty_list();

	ETERM *cname = erl_mk_binary(col_name, len);
    ETERM *new_container_list = erl_cons(erl_format((char*)"{~w,~i,~i,~i,~i}", cname, data_type, max_len, precision, scale), container_list);
    erl_free_term(cname);
    erl_free_term(container_list);

    (*(ETERM**)list) = new_container_list;
}

void append_desc_to_list(const char * col_name, size_t len, const unsigned short data_type, const unsigned int max_len, void * list)
{
    if (list==NULL)
        return;

    ETERM *container_list = (ETERM *)(*(ETERM**)list);
    if (container_list == NULL)
        container_list = erl_mk_empty_list();

	ETERM *cname = erl_mk_binary(col_name, len);
    ETERM *new_container_list = erl_cons(erl_format((char*)"{~w,~i,~i}", cname, data_type, max_len), container_list);
    erl_free_term(cname);
    erl_free_term(container_list);

    (*(ETERM**)list) = new_container_list;
}

bool init_marshall(void)
{
#ifdef __WIN32__
    write_mutex = CreateMutex(NULL, FALSE, NULL);
    if (NULL == write_mutex) {
        REMOTE_LOG(CRT, "Write Mutex creation failed\n");
        return false;
    }
#else
    if(pthread_mutex_init(&write_mutex, NULL) != 0) {
        REMOTE_LOG(CRT, "Write Mutex creation failed");
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

    //REMOTE_LOG(DBG, "RX Packet length %d\n", rx_len);

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

    if (!resp) {
        pkt_len = -1;
        goto error_exit;
    }

    tx_len = erl_term_len(resp);				// Length of the required binary buffer
    pkt_len = tx_len+PKT_LEN_BYTES;

    //REMOTE_LOG(DBG, "TX Packet length %d\n", tx_len);

    // Allocate temporary buffer for transmission of the Term
    tx_buf = new unsigned char[pkt_len];
    hdr = (pkt_hdr *)tx_buf;
    hdr->len = htonl(tx_len);		// Length adjusted to network byte order

    erl_encode(resp, tx_buf+PKT_LEN_BYTES);			// Encode the Term into the buffer after the length field

	LOG_DUMP(pkt_len, tx_buf);

    if(lock(write_mutex)) {
        cout.write((char *) tx_buf, pkt_len);
        cout.flush();
		unlock(write_mutex);
    }

    // Free the temporary allocated buffer
    delete tx_buf;

    if (cout.fail()) {
        pkt_len = -1;
        goto error_exit;
    }

    EXIT();

error_exit:
    if (resp)
		erl_free_compound(resp);
    return pkt_len;
}

void map_schema_to_bind_args(void * _args, vector<var> & vars)
{
    ETERM * args = (ETERM *)_args;
    if(!ERL_IS_LIST(args) || ERL_IS_EMPTY_LIST(args))
        return;

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
			REMOTE_LOG(ERR, "variable %.*s is too long, max %d\n", len, (char*)ERL_BIN_PTR(arg), sizeof(v.name)-1);
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
	intf_ret r;
	
	r.fn_ret = CONTINUE_WITH_ERROR;
    if(!ERL_IS_LIST(args) || ERL_IS_EMPTY_LIST(args))
        return;

    ETERM * item = NULL;
    ETERM * arg = NULL;
	void * tmp_arg = NULL;
	size_t len = 0;
	sb2 ind = -1;
	unsigned short arg_len = 0;
	
	// remove any old bind from the vars
	for(unsigned int i=0; i < vars.size(); ++i) {
		vars[i].valuep.clear();
		vars[i].alen.clear();
	}
	
	// loop through the list
    do {
        if ((item = erl_hd(args)) == NULL	||
            !ERL_IS_TUPLE(item)				||
			(unsigned int)erl_size(item) != vars.size()) {
			REMOTE_LOG(ERR, "failed map_value_to_bind_args malformed ETERM\n");
			strcpy(r.gerrbuf, "Malformed ETERM");
            throw r;
		}

		len = erl_size(item);

		// loop through each value of the tuple
		for(size_t i=0; i<len; ++i) {
			if ((arg = erl_element(i+1, item)) == NULL) {
				REMOTE_LOG(ERR, "failed map_value_to_bind_args missing parameter for %s\n", vars[i].name);
				strcpy(r.gerrbuf, "Missing parameter term");
				throw r;
			}
			
			ind = -1;
			tmp_arg = NULL;
			arg_len = 0;
			switch(vars[i].dty) {
				case SQLT_BFLOAT:
				case SQLT_BDOUBLE:
				case SQLT_FLT:
					if(ERL_IS_INTEGER(arg) || ERL_IS_FLOAT(arg)) {
						ind = 0;
						arg_len = sizeof(double);
						tmp_arg = new double;
						*(double*)tmp_arg = (double)(ERL_IS_INTEGER(arg) ? ERL_INT_VALUE(arg) : ERL_FLOAT_VALUE(arg));
					} else {
						REMOTE_LOG(ERR, "failed map_value_to_bind_args malformed float for %s\n", vars[i].name);
						strcpy(r.gerrbuf, "Malformed float parameter value");
						throw r;
					}
					break;
				case SQLT_NUM:
				case SQLT_INT:
					if(ERL_IS_INTEGER(arg)) {
						ind = 0;
						arg_len = sizeof(int);
						tmp_arg = new int;
						*(int*)tmp_arg = (int)ERL_INT_VALUE(arg);
					} else {
						REMOTE_LOG(ERR, "failed map_value_to_bind_args malformed integer for %s\n", vars[i].name);
						strcpy(r.gerrbuf, "Malformed integer parameter value");
						throw r;
					}
					break;
				case SQLT_VNU:
					if(ERL_IS_BINARY(arg) && (arg_len = ERL_BIN_SIZE(arg)) && arg_len <= OCI_NUMBER_SIZE) {
						ind = 0;
						tmp_arg = new char[arg_len];
						memcpy(tmp_arg, ERL_BIN_PTR(arg), arg_len);
					} else {
						REMOTE_LOG(ERR, "failed map_value_to_bind_args malformed number for %s\n", vars[i].name);
						strcpy(r.gerrbuf, "Malformed number parameter value");
						throw r;
					}
					break;
				case SQLT_RDD:
				case SQLT_DAT:
					if(ERL_IS_BINARY(arg) && (arg_len = ERL_BIN_SIZE(arg))) {
						ind = 0;
						tmp_arg = new char[arg_len];
						memcpy(tmp_arg, ERL_BIN_PTR(arg), arg_len);
					} else {
						REMOTE_LOG(ERR, "failed map_value_to_bind_args malformed binary for %s\n", vars[i].name);
						strcpy(r.gerrbuf, "Malformed binary parameter value");
						throw r;
					}
					break;
				case SQLT_ODT:
					if(ERL_IS_BINARY(arg) && (arg_len = ERL_BIN_SIZE(arg))) {
						ind = 0;
						tmp_arg = new char[arg_len];
						memcpy(tmp_arg, ERL_BIN_PTR(arg), arg_len);
						((OCIDate*)tmp_arg)->OCIDateYYYY = htons((ub2)((OCIDate*)tmp_arg)->OCIDateYYYY);
					} else {
						REMOTE_LOG(ERR, "failed map_value_to_bind_args malformed date for %s\n", vars[i].name);
						strcpy(r.gerrbuf, "Malformed date parameter value");
						throw r;
					}
					break;
				case SQLT_AFC:
				case SQLT_CHR:
				case SQLT_LNG:
					if(ERL_IS_BINARY(arg) && (arg_len = ERL_BIN_SIZE(arg))) {
						ind = 0;
						tmp_arg = new char[arg_len];
						memcpy(tmp_arg, ERL_BIN_PTR(arg), arg_len);
					} else {
						REMOTE_LOG(ERR, "failed map_value_to_bind_args malformed string for %s\n", vars[i].name);
						strcpy(r.gerrbuf, "Malformed string parameter value");
						throw r;
					}
					break;
				case SQLT_STR:
					if(ERL_IS_BINARY(arg) && (arg_len = ERL_BIN_SIZE(arg))) {
						ind = 0;
						tmp_arg = new char[arg_len+1];
						memcpy(tmp_arg, ERL_BIN_PTR(arg), arg_len);
						((char*)tmp_arg)[arg_len] = '\0';
						arg_len++;
					} else {
						REMOTE_LOG(ERR, "failed map_value_to_bind_args malformed string\\0 for %s\n", vars[i].name);
						strcpy(r.gerrbuf, "Malformed string\\0 parameter value");
						throw r;
					}
					break;
				default:
					strcpy(r.gerrbuf, "Unsupported type in bind");
					throw r;
					break;
			}
			vars[i].alen.push_back(arg_len);
			if (arg_len > vars[i].value_sz)
				vars[i].value_sz = arg_len;
			vars[i].valuep.push_back(tmp_arg);
			vars[i].ind.push_back(ind);
		}

        args = erl_tl(args);
    } while (args != NULL && !ERL_IS_EMPTY_LIST(args));
}

void * walk_term(void * _term)
{
	ETERM * term = (ETERM *)_term;
	ETERM * list_item = NULL;
	ETERM * tuple_arg = NULL;
	size_t len = 0;

	if(!term)
		return NULL;

	if(ERL_IS_TUPLE(term)) {
		len = erl_size(term);
		for(size_t i=0; i<len; ++i) {
			if ((tuple_arg = erl_element(i+1, term)) == NULL)
				break;
			walk_term(tuple_arg);
		}
	}
	else if(ERL_IS_LIST(term)) {
		do {
			list_item = erl_hd(term);
			walk_term(list_item);
		    term = erl_tl(term);
	    } while (term != NULL && !ERL_IS_EMPTY_LIST(term));
	}
	else if(ERL_IS_ATOM(term)) {
	}
	else if(ERL_IS_UNSIGNED_LONGLONG(term)) {
	}
	else if(ERL_IS_LONGLONG(term)) {
	}
	else if(ERL_IS_UNSIGNED_INTEGER(term)) {
	}
	else if(ERL_IS_INTEGER(term)) {
	}
	else if(ERL_IS_FLOAT(term)) {
	}
	else if(ERL_IS_PID(term)) {
	}
	else if(ERL_IS_REF(term)) {
	}
	else if(ERL_IS_PORT(term)) {
	}
	else if(ERL_IS_CONS(term)) {
	}
	else if(ERL_IS_EMPTY_LIST(term)) {
	}
	else if(ERL_IS_NIL(term)) {
	}
	else if(ERL_IS_BINARY(term)) {
	}

	return _term;
}
