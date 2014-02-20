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
#include "cmd_queue.h"
#include "transcoder.h"

typedef unsigned char byte;

bool log_flag;
extern bool run_threads;

//bool exit_loop = false;

int main(int argc, char * argv[])
{
#ifdef __WIN32__
    _setmode( _fileno( stdout ), _O_BINARY );
    _setmode( _fileno( stdin  ), _O_BINARY );
#endif

    //erl_init(NULL, 0);
	transcoder::instance();
	/*unsigned char b[] = {131,104,5,104,2,103,100,0,13,110,111,110,111,100,101,64,110,111,104,111,115,116,0,0,0,54,0,0,0,0,0,114,0,3,100,0,13,110,111,110,111,100,101,64,110,111,104,111,115,116,0,0,0,0,254,0,0,0,0,0,0,0,0,97,2,109,0,0,0,115,40,68,69,83,67,82,73,80,84,73,79,78,61,40,65,68,68,82,69,83,83,95,76,73,83,84,61,40,65,68,68,82,69,83,83,61,40,80,82,79,84,79,67,79,76,61,116,99,112,41,40,72,79,83,84,61,49,57,50,46,49,54,56,46,49,46,52,51,41,40,80,79,82,84,61,49,53,50,49,41,41,41,40,67,79,78,78,69,67,84,95,68,65,84,65,61,40,83,69,82,86,73,67,69,95,78,65,77,69,61,88,69,41,41,41,109,0,0,0,5,115,99,111,116,116,109,0,0,0,5,114,101,103,105,116};
	vector<unsigned char> buf(b, b + sizeof(b) / sizeof(b[0]));
	term t = transcoder::instance().decode(buf);*/

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

    REMOTE_LOG(DBG, "OCI Process started...");

    InitializeThreadPool();
	ProcessCommand();
	REMOTE_LOG(DBG, "Thread pool created...");

    REMOTE_LOG(DBG, "Initialized Oracle OCI");

	port& prt = port::instance();
	vector<unsigned char> read_buf;
    while(prt.read_cmd(read_buf) > 0) {
		cmd_queue::push(read_buf);
    }
	run_threads = false;

    CleanupThreadPool();
    REMOTE_LOG(DBG, "Thread pool destroyed");

	REMOTE_LOG(DBG, "Process oci terminating...");
    return 0;
}