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

#include "erl_interface.h"
#include "ei.h"

#include "oci_marshal.h"
#include "cmd_processors.h"
#include "oci_lib_intf.h"

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
    static struct event ev;
    static struct timeval tv;
#endif

#ifndef __WIN32__
unsigned long timeout_remaining()
{
    struct timeval t;
    struct timeval now;
    memset(&t, 0, sizeof(struct timeval));
    event_pending(&ev, EV_TIMEOUT, &t);
    gettimeofday(&now, NULL);
    return (t.tv_sec - now.tv_sec);
}

void timer_thread_start_function(void *ptr)
{
    event_init();
    event_set(&ev, 0, EV_TIMEOUT, IdleTimerCb, NULL);
    event_add(&ev, &tv);
    REMOTE_LOG(DBG, "IdleTimerCb firing if idle for %d sec\n", timeout_remaining());
    event_dispatch();
}
#endif

bool InitializeThreadPool(void)
{
    REMOTE_LOG(DBG, "Port: Initializing Thread pool...\n");

#ifdef __WIN32__
    BOOL bRet = FALSE;

    InitializeThreadpoolEnvironment(&CallBackEnviron);

    //
    // Create a custom, dedicated thread pool.
    //
    pool = CreateThreadpool(NULL);

    if (NULL == pool) {
        _tprintf(_T("CreateThreadpool failed. LastError: %u\n"), GetLastError());
        goto main_cleanup;
    }

    rollback = 1; // pool creation succeeded

    //
    // The thread pool is made persistent simply by setting
    // both the minimum and maximum threads to 1.
    //
    SetThreadpoolThreadMaximum(pool, THREAD);

    bRet = SetThreadpoolThreadMinimum(pool, 1);

    if (FALSE == bRet) {
        _tprintf(_T("SetThreadpoolThreadMinimum failed. LastError: %u\n"),
                 GetLastError());
        goto main_cleanup;
    }

    //
    // Create a cleanup group for this thread pool.
    //
    cleanupgroup = CreateThreadpoolCleanupGroup();

    if (NULL == cleanupgroup) {
        _tprintf(_T("CreateThreadpoolCleanupGroup failed. LastError: %u\n"), GetLastError());
        goto main_cleanup;
    }

    rollback = 2;  // Cleanup group creation succeeded

    //
    // Associate the callback environment with our thread pool.
    //
    SetThreadpoolCallbackPool(&CallBackEnviron, pool);

    //
    // Associate the cleanup group with our thread pool.
    // Objects created with the same callback environment
    // as the cleanup group become members of the cleanup group.
    //
    SetThreadpoolCallbackCleanupGroup(&CallBackEnviron, cleanupgroup, NULL);

    return true;

main_cleanup:
    CloseThreadpool(pool);
    pool = NULL;
    return false;
#else
    pTp = threadpool_create(THREAD, QUEUE, 0);
    if(NULL != pTp)
        return true;
    return false;
#endif
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
    CloseThreadpoolCleanupGroupMembers(cleanupgroup, FALSE, NULL);

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
    //UNREFERENCED_PARAMETER(Parameter);
    UNREFERENCED_PARAMETER(Work);
#endif

	void * cmd_tuple = NULL;

	int indx = 0;
    //cmd_tuple = erl_decode((unsigned char*)arg);	
    if (ei_decode_term((char*)arg, &indx, &cmd_tuple) < 0) {
        // Term decoding failed
        delete arg;
        return;
    }

    if(NULL != arg) delete arg;
    exit_loop = cmd_processor(cmd_tuple);

    return;
}

bool ProcessCommand(void * param)
{
    //REMOTE_LOG(DBG, "Port: Delegating command processing to WorkThread...");
#ifdef __WIN32__
    PTP_WORK work = NULL;
    //
    // Create work with the callback environment.
    //
    work = CreateThreadpoolWork(ProcessCommandCb, param, &CallBackEnviron);

    if (NULL == work) {
        _tprintf(_T("CreateThreadpoolWork failed. LastError: %u\n"),
                 GetLastError());
        return false;
    }

    rollback = 3;  // Creation of work succeeded

    //
    // Submit the work to the pool. Because this was a pre-allocated
    // work item (using CreateThreadpoolWork), it is guaranteed to execute.
    //
    SubmitThreadpoolWork(work);
#else
    threadpool_add(pTp, &ProcessCommandCb, param, 0);
#endif

    return true;
}
