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
#include "oci_lib_intf.h"

#include "erl_interface.h"
#include "ei.h"

#include "cmd_processors.h"

#ifdef __WIN32__
#include <io.h>
#include <fcntl.h>
#include <share.h>
#include <WinBase.h>
#else
#include <stdlib.h>
#include <unistd.h>
#endif

#include <time.h>

typedef unsigned char byte;

bool log_flag;
bool exit_loop = false;
unsigned long port_idle_timeout = PORT_IDLE_TIMEOUT;
bool is_idle = false;

#ifdef __WIN32__
DWORD WINAPI check_idle_thread( LPVOID lpParam )
#else
void *check_idle_thread(void * argument)
#endif
{
	REMOTE_LOG("Idle timeout checker thread started with %d ms timeout\n", port_idle_timeout);
	do {
		is_idle = true;
#ifdef __WIN32__
		Sleep(port_idle_timeout);
#else
		usleep(port_idle_timeout*1000);
		//sleep(port_idle_timeout / 1000);
#endif
		// is set in cmd_ping
		if (is_idle) {
			REMOTE_LOG("No ping from port master erlang process after %d ms. dying...\n", port_idle_timeout);
			break;
		}
	} while(true);
#ifdef __WIN32__
	ExitProcess(3);
	return 0;
#else
    exit(0);
	return NULL;
#endif
}

#ifdef __WIN32__
HANDLE check_idle_thread_handle;
DWORD  check_idle_thread_id;
#else
pthread_t check_idle_thread_handle;
int  check_idle_thread_id;
#endif
void create_start_idle_check_thread()
{
	check_idle_thread_handle =
#ifdef __WIN32__
		CreateThread( 
            NULL,                   // default security attributes
            0,                      // use default stack size  
            check_idle_thread,		// thread function name
            NULL,					// argument to thread function 
            0,                      // use default creation flags 
            &check_idle_thread_id);
#else
	check_idle_thread_id = pthread_create(&check_idle_thread_handle, NULL, check_idle_thread, NULL);
#endif
}

#ifdef __WIN32__
int _tmain(int argc, _TCHAR* argv[])
#else
int main(int argc, char * argv[])
#endif
{
    bool threaded = false;
    ETERM *cmd_tuple;

#ifdef __WIN32__
    _setmode( _fileno( stdout ), _O_BINARY );
    _setmode( _fileno( stdin  ), _O_BINARY );
#endif

    erl_init(NULL, 0);
    log_flag = false;

	// Max term byte size
	if (argc >= 2) {
		max_term_byte_size =
#ifdef __WIN32__
			_wtol(argv[1]);
#else
            atoi(argv[1]);
#endif
	}

	// Enable Logging
    if (argc >= 3) {
        if (
#ifdef __WIN32__
            wcscmp(argv[2], L"true") == 0
#else
            strcmp(argv[2], "true") == 0
#endif
        ) log_flag = true;
    }

	// Log listner port
	int log_tcp_port = 0;
	if (argc >= 4) {
		log_tcp_port = 
#ifdef __WIN32__
            _wtoi(argv[3]);
#else
            atoi(argv[3]);
#endif
		char * ret = connect_tcp(log_tcp_port);
		if(ret != NULL) {
			return -1;
		}
	}

	// Ping Timeout
	if (argc >= 5) {
		port_idle_timeout =
#ifdef __WIN32__
			_wtol(argv[4]);
#else
            atoi(argv[4]);
#endif
	}

	REMOTE_LOG("Port process configs : erlang term max size 0x%08X bytes, logging %s, TCP port for logs %d, idle timeout %ul ms\n"
		, max_term_byte_size, (log_flag ? "enabled" : "disabled"), log_tcp_port, port_idle_timeout);

    init_marshall();

    REMOTE_LOG("Port: OCI Process started...\n");

    threaded = InitializeThreadPool();
    if(threaded)
        REMOTE_LOG("Port: Thread pool created...\n");

    REMOTE_LOG("Port: Initialized Oracle OCI\n");

	create_start_idle_check_thread();
    REMOTE_LOG("Idle check thread started\n");

    while(!exit_loop && (cmd_tuple = (ETERM *)read_cmd()) != NULL) {
        if(threaded && ProcessCommand(cmd_tuple)) {
            //REMOTE_LOG("Port: Command sumitted to thread-pool for processing...");
        }
    }

    CleanupThreadPool();
    REMOTE_LOG("Port: Thread pool destroyed\n");

	close_tcp();
    REMOTE_LOG("Port: tcp log socket closed\n");

    REMOTE_LOG("Port: Process oci terminating...\n");
    return 0;
}
