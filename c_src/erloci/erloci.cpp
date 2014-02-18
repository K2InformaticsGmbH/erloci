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
#include "marshal.h"
#include "logger.h"

#include "erl_interface.h"
#include "ei.h"

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
#include "port.h"

typedef unsigned char byte;

bool log_flag;
//bool exit_loop = false;

#ifdef __WIN32__
extern HANDLE cmd_queue_mutex;
#else
extern pthread_mutex_t cmd_queue_mutex;
#endif
extern queue<vector<unsigned char> > cmd_queue;

int main(int argc, char * argv[])
{
#ifdef __WIN32__
    _setmode( _fileno( stdout ), _O_BINARY );
    _setmode( _fileno( stdin  ), _O_BINARY );
#endif

    erl_init(NULL, 0);
    log_flag = false;

	// Max term byte size
	if (argc >= 2) {
		max_term_byte_size = atol(argv[1]);
	}

	// Enable Logging
    if (argc >= 3) {
        if (strcmp(argv[2], "true") == 0)
			log_flag = true;
    }

	// Log listner port
	int log_tcp_port = 0;
	if (argc >= 4) {
		log_tcp_port = atol(argv[3]);
		char * ret = logger::init(log_tcp_port);
		if(ret != NULL) {
			return -1;
		}
	}

	REMOTE_LOG(INF, "Port process configs : erlang term max size 0x%08X bytes, logging %s, TCP port for logs %d"
		, max_term_byte_size, (log_flag ? "enabled" : "disabled"), log_tcp_port);

    init_marshall();

    REMOTE_LOG(DBG, "Port: OCI Process started...");

    InitializeThreadPool();
	ProcessCommand();
	REMOTE_LOG(DBG, "Port: Thread pool created...");

    REMOTE_LOG(DBG, "Port: Initialized Oracle OCI");

	port& prt = port::getInstance();
	vector<unsigned char> read_buf;
    while(prt.read_cmd(read_buf) > 0) {
		if(lock(cmd_queue_mutex)) {
			cmd_queue.push(read_buf);
 			unlock(cmd_queue_mutex);
		}
    }

    CleanupThreadPool();
    REMOTE_LOG(DBG, "Port: Thread pool destroyed");

	REMOTE_LOG(DBG, "Port: Process oci terminating...");
    return 0;
}