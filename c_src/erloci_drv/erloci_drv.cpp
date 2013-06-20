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
#include "ei.h"

#include "cmd_processors.h"

#ifdef __WIN32__
#include <io.h>
#include <fcntl.h>
#include <share.h>
#else
#include <stdlib.h>
#endif

#include <time.h>

typedef unsigned char byte;

bool log_flag;
bool exit_loop = false;

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

    if (argc >= 2) {
        if (
#ifdef __WIN32__
            wcscmp(argv[1], L"true") == 0
#else
            strcmp(argv[1], "true") == 0
#endif
        ) log_flag = true;
    }

	if (argc >= 3) {
		int ListenPortNo = 
#ifdef __WIN32__
            _wtoi(argv[2]);
#else
            atoi(argv[2]);
#endif
		char * ret = connect_tcp(ListenPortNo);
		if(ret != NULL) {
			return -1;
		} else
			REMOTE_LOG("Logging over TCP, Voila!\n");
	}

    init_marshall();

    REMOTE_LOG("Port: OCI Process started...\n");

    threaded = InitializeThreadPool();
    if(threaded)
        REMOTE_LOG("Port: Thread pool created...\n");

    REMOTE_LOG("Port: Initialized Oracle OCI\n");

    while(!exit_loop && (cmd_tuple = (ETERM *)read_cmd()) != NULL) {
        if(threaded && ProcessCommand(cmd_tuple)) {
            //REMOTE_LOG("Port: Command sumitted to thread-pool for processing...");
        }
    }

    REMOTE_LOG("Port: Process oci terminating...\n");
	close_tcp();

    REMOTE_LOG("Port: Thread pool destroyed...\n");
    CleanupThreadPool();

    return 0;
}
