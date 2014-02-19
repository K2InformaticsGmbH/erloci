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
#include "erl_interface.h"
#include "ei.h"

#include "marshal.h"
#include "command.h"
#include "lib_interface.h"

#define THREAD          10
#define QUEUE           256

#ifdef __WIN32__
#include <windows.h>
#include <tchar.h>
#else
#include <sys/time.h>
#include <event.h>
#include <stdlib.h>
#include <unistd.h>
#include "threadpool.h"
#endif

#include <stdio.h>

#ifdef __WIN32__
	static PTP_POOL pool = NULL;
	static TP_CALLBACK_ENVIRON CallBackEnviron;
	static PTP_CLEANUP_GROUP cleanupgroup = NULL;
	static UINT rollback = 0;
#else
	static threadpool_t *pTp            = NULL;
#endif

void InitializeThreadPool(void)
{
    REMOTE_LOG(DBG, "Port: Initializing Thread pool...\n");

#ifdef __WIN32__
    InitializeThreadpoolEnvironment(&CallBackEnviron);

    if (NULL == (pool = CreateThreadpool(NULL))) {
        _tprintf(_T("CreateThreadpool failed. LastError: %u\n"), GetLastError());
        goto main_cleanup;
    }
    rollback = 1; // pool creation succeeded

    SetThreadpoolThreadMaximum(pool, THREAD);

    if (FALSE == SetThreadpoolThreadMinimum(pool, 1)) {
        _tprintf(_T("SetThreadpoolThreadMinimum failed. LastError: %u\n"),
                 GetLastError());
        goto main_cleanup;
    }

    if (NULL == (cleanupgroup = CreateThreadpoolCleanupGroup())) {
        _tprintf(_T("CreateThreadpoolCleanupGroup failed. LastError: %u\n"), GetLastError());
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
}

void CleanupThreadPool(void)
{
    REMOTE_LOG(DBG, "Port: Cleanup Thread pool...");

#ifdef __WIN32__
    BOOL bRet = FALSE;

    //
    // Wait for all callbacks to finish.
    // CloseThreadpoolCleanupGroupMembers also releases objects
    // that are members of the cleanup group, so it is not necessary
    // to call close functions on individual objects
    // after calling CloseThreadpoolCleanupGroupMembers.
    //
    CloseThreadpoolCleanupGroupMembers(cleanupgroup, TRUE, NULL);

    //
    // Already cleaned up the work item with the
    // CloseThreadpoolCleanupGroupMembers, so set rollback to 2.
    //
    rollback = 2;
    goto main_cleanup;

main_cleanup:
    //
    // Clean up any individual pieces manually
    // Notice the fall-through structure of the switch.
    // Clean up in reverse order.
    //

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
}

bool run_threads = true;
#include "cmd_queue.h"
#include "eterm.h"
#include "term.h"

//
// This is the thread pool work callback function.
//
static
#ifdef __WIN32__
VOID CALLBACK
#else
void
#endif
ProcessCommandCb(
#ifdef __WIN32__
    PTP_CALLBACK_INSTANCE Instance,
    PVOID                 arg,
    PTP_WORK              Work
#else
    void *arg
#endif
)
{
    //REMOTE_LOG(DBG, "Port: WorkThread processing command...\n");
#ifdef __WIN32__
    // Instance, Parameter, and Work not used in this example.
    UNREFERENCED_PARAMETER(Instance);
    UNREFERENCED_PARAMETER(arg);
    UNREFERENCED_PARAMETER(Work);
#endif

	vector<unsigned char> rxpkt;
	while (run_threads) {
		rxpkt = cmd_queue::pop();
		if (rxpkt.size() > 0)
			break;
#ifdef __WIN32__
		if(!SwitchToThread())
			Sleep(50);
#else
		pthread_yield();
		usleep(50000);
#endif
	}
	if(!run_threads)
		return;
	ProcessCommand();

	eterm &et = eterm::getInstance();
	//term t = et.decode(rxpkt);

	void * cmd_tuple = erl_decode(&rxpkt[0]);
	if (!cmd_tuple) {
        REMOTE_LOG(CRT, "Term (%d) decoding failed...", rxpkt.size());
		DUMP("rxpkt.buf", rxpkt.size(), ((unsigned char*)&rxpkt[0]));
		exit(1);
    }

	if(command::process(cmd_tuple))
		exit(1);

	erl_free_compound((ETERM*)cmd_tuple);
	
	// ETERM memory house keeping
	unsigned long allocated, freed;
	erl_eterm_statistics(&allocated,&freed);
	if(freed * 2 < allocated) {
		REMOTE_LOG(WRN, "ETERM alloc limit alloc %lu freed %lu!\n", allocated, freed);
		erl_eterm_release();
	}

	return;
}

void ProcessCommand()
{
    //REMOTE_LOG(DBG, "Port: Delegating command processing to WorkThread...");
#ifdef __WIN32__
    PTP_WORK work = NULL;
    //
    // Create work with the callback environment.
    //
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
}
