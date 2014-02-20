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
#if 0
	unsigned char b[] = {
		131,112,0,0,2,139,0,32,134,155,11,165,194,128,94,159,150,25,37,107,162,125,
		47,0,0,0,20,0,0,0,4,100,0,8,101,114,108,95,101,118,97,108,97,20,98,1,4,52,
		216,103,100,0,13,110,111,110,111,100,101,64,110,111,104,111,115,116,0,0,0,32,
		0,0,0,0,0,106,104,2,100,0,5,118,97,108,117,101,112,0,0,0,96,2,196,253,215,
		225,209,128,71,181,240,134,182,254,135,48,200,150,0,0,0,5,0,0,0,1,100,0,5,
		115,104,101,108,108,97,5,98,6,39,238,191,103,100,0,13,110,111,110,111,100,
		101,64,110,111,104,111,115,116,0,0,0,32,0,0,0,0,0,103,100,0,13,110,111,110,
		111,100,101,64,110,111,104,111,115,116,0,0,0,26,0,0,0,0,0,104,2,100,0,4,101,
		118,97,108,112,0,0,1,161,3,196,253,215,225,209,128,71,181,240,134,182,254,
		135,48,200,150,0,0,0,21,0,0,0,4,100,0,5,115,104,101,108,108,97,21,98,6,39,
		238,191,103,100,0,13,110,111,110,111,100,101,64,110,111,104,111,115,116,0,0,
		0,32,0,0,0,0,0,104,2,100,0,5,118,97,108,117,101,112,0,0,0,96,2,196,253,215,
		225,209,128,71,181,240,134,182,254,135,48,200,150,0,0,0,5,0,0,0,1,100,0,5,
		115,104,101,108,108,97,5,98,6,39,238,191,103,100,0,13,110,111,110,111,100,
		101,64,110,111,104,111,115,116,0,0,0,32,0,0,0,0,0,103,100,0,13,110,111,110,
		111,100,101,64,110,111,104,111,115,116,0,0,0,26,0,0,0,0,0,98,0,0,32,15,112,0,
		0,0,208,1,196,253,215,225,209,128,71,181,240,134,182,254,135,48,200,150,0,0,
		0,12,0,0,0,3,100,0,5,115,104,101,108,108,97,12,98,6,39,238,191,103,100,0,13,
		110,111,110,111,100,101,64,110,111,104,111,115,116,0,0,0,32,0,0,0,0,0,104,2,
		100,0,5,118,97,108,117,101,112,0,0,0,96,2,196,253,215,225,209,128,71,181,240,
		134,182,254,135,48,200,150,0,0,0,5,0,0,0,1,100,0,5,115,104,101,108,108,97,5,
		98,6,39,238,191,103,100,0,13,110,111,110,111,100,101,64,110,111,104,111,115,
		116,0,0,0,32,0,0,0,0,0,103,100,0,13,110,111,110,111,100,101,64,110,111,104,
		111,115,116,0,0,0,26,0,0,0,0,0,98,0,0,32,15,103,100,0,13,110,111,110,111,100,
		101,64,110,111,104,111,115,116,0,0,0,26,0,0,0,0,0,103,100,0,13,110,111,110,
		111,100,101,64,110,111,104,111,115,116,0,0,0,26,0,0,0,0,0,108,0,0,0,1,104,5,
		100,0,6,99,108,97,117,115,101,97,1,106,106,108,0,0,0,1,104,3,100,0,4,97,116,
		111,109,97,1,100,0,2,111,107,106,106};
	vector<unsigned char> buf(b, b + sizeof(b) / sizeof(b[0]));
	term t = transcoder::instance().decode(buf);
#endif

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