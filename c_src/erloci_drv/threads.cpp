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
#include "platform.h"
#include "threads.h"

#include "marshal.h"
#include "command.h"

bool threads::run_threads = true;
transcoder & threads::tc = transcoder::instance();


#ifdef __WIN32__
	PTP_POOL threads::pool							= NULL;
	TP_CALLBACK_ENVIRON threads::CallBackEnviron;
	PTP_CLEANUP_GROUP threads::cleanupgroup			= NULL;
	UINT threads::rollback							= 0;
#else
	threadpool_t * threads::pTp						= NULL;
#endif

#define THREAD          10
#define QUEUE           256

threads::threads(void)
{
    REMOTE_LOG(DBG, "Initializing Thread pool...");

	command::config(marshall_intf_funs);

#ifdef USING_THREAD_POOL
#ifdef __WIN32__
    InitializeThreadpoolEnvironment(&CallBackEnviron);

    if (NULL == (pool = CreateThreadpool(NULL))) {
	    REMOTE_LOG(CRT, "CreateThreadpool failed. LastError: %u", GetLastError());
        goto main_cleanup;
    }
    rollback = 1; // pool creation succeeded

    SetThreadpoolThreadMaximum(pool, THREAD);

    if (FALSE == SetThreadpoolThreadMinimum(pool, 1)) {
	    REMOTE_LOG(CRT, "SetThreadpoolThreadMinimum failed. LastError: %u", GetLastError());
        goto main_cleanup;
    }

    if (NULL == (cleanupgroup = CreateThreadpoolCleanupGroup())) {
	    REMOTE_LOG(CRT, "CreateThreadpoolCleanupGroup failed. LastError: %u", GetLastError());
        goto main_cleanup;
    }
    rollback = 2;  // Cleanup group creation succeeded

	SetThreadpoolCallbackPool(&CallBackEnviron, pool);
    SetThreadpoolCallbackCleanupGroup(&CallBackEnviron, cleanupgroup, NULL);
	return;

main_cleanup:
    CloseThreadpool(pool);
    pool = NULL;
#else
    pTp = threadpool_create(THREAD, QUEUE, 0);
    if(NULL != pTp)
        return;
#endif
    exit(0);
#else
#endif
}

threads::~threads(void)
{
    REMOTE_LOG(DBG, "Cleanup Thread pool...");

#ifdef USING_THREAD_POOL
#ifdef __WIN32__
    BOOL bRet = FALSE;

    // Wait for all callbacks to finish.
    CloseThreadpoolCleanupGroupMembers(cleanupgroup, TRUE, NULL);
    rollback = 2;
    goto main_cleanup;

main_cleanup:
    switch (rollback) {
    case 3:
        // Clean up the cleanup group members.
        CloseThreadpoolCleanupGroupMembers(cleanupgroup,FALSE,NULL);
    case 2:
        // Clean up the cleanup group.
        CloseThreadpoolCleanupGroup(cleanupgroup);
    case 1:
        // Clean up the pool.
        CloseThreadpool(pool);

    default:
        break;
    }
#else
    if(pTp)
        threadpool_destroy(pTp, 0);
#endif
#else
#endif
}

#include "cmd_queue.h"
#include "term.h"

//
// This is the thread pool work callback function.
//
static
#ifdef USING_THREAD_POOL
#ifdef __WIN32__
	VOID CALLBACK
#else
	void
#endif
#else
	DWORD WINAPI
#endif
ProcessCommandCb (
#ifdef USING_THREAD_POOL
#ifdef __WIN32__
    PTP_CALLBACK_INSTANCE Instance,
    PVOID                 arg,
    PTP_WORK              Work
#else
    void *arg
#endif
#else
	LPVOID arg
#endif
)
{
    //REMOTE_LOG(DBG, "Port: WorkThread processing command...\n");
#ifdef USING_THREAD_POOL
#ifdef __WIN32__
    // Instance, Parameter, and Work not used in this example.
    UNREFERENCED_PARAMETER(Instance);
    UNREFERENCED_PARAMETER(arg);
    UNREFERENCED_PARAMETER(Work);
#endif
#else
    UNREFERENCED_PARAMETER(arg);
#endif

	vector<unsigned char> rxpkt;
#ifdef __WIN32__
	DWORD start, end;	
#endif
	while (threads::run_threads) {
		rxpkt = cmd_queue::pop();
		if (rxpkt.size() > 0)
			break;
#ifdef __WIN32__
		start = GetTickCount();
		if(!SwitchToThread())
			Sleep(50);
		end = GetTickCount();
		if (end - start < 50) {
			rxpkt = cmd_queue::pop();
			if (rxpkt.size() > 0)
				break;
			Sleep(end - start);
		}
#ifdef __APPLE__
        pthread_yield_np();
#else
		pthread_yield();
#endif
#else
		usleep(50000);
#endif
	}
	if(!threads::run_threads)
#ifdef USING_THREAD_POOL
		return;
#else
		return 0;
#endif
#ifdef USING_THREAD_POOL
	threads::start();
#endif

	term t;
	threads::tc.decode(rxpkt, t);
	if(command::process(t))
		exit(1);

#ifdef USING_THREAD_POOL
	return;
#else
    return 0;
#endif
}

void threads::start(void)
{
    //REMOTE_LOG(DBG, "Port: Delegating command processing to WorkThread...");
#ifdef USING_THREAD_POOL
	#ifdef __WIN32__
		PTP_WORK work = NULL;
		work = CreateThreadpoolWork(ProcessCommandCb, NULL, &CallBackEnviron);

		if (NULL == work) {
			REMOTE_LOG(CRT, "CreateThreadpoolWork failed. LastError: %u\n", GetLastError());
			exit(0);
		}

		rollback = 3;  // Creation of work succeeded

		//
		// Submit the work to the pool. Because this was a pre-allocated
		// work item (using CreateThreadpoolWork), it is guaranteed to execute.
		//
		SubmitThreadpoolWork(work);
	#else
		threadpool_add(pTp, &ProcessCommandCb, NULL, 0);
	#endif
#else
	#ifdef __WIN32__
	if (NULL == CreateThread( 
            NULL,                   // default security attributes
            0,                      // use default stack size  
            ProcessCommandCb,       // thread function name
            NULL,					// argument to thread function 
            0,                      // use default creation flags 
            NULL)) {
		REMOTE_LOG(CRT, "CreateThreadpoolWork failed. LastError: %u\n", GetLastError());
		exit(0);
	}
	#else
	#endif
#endif
}
