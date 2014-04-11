#ifndef _PLATFORM_H_
#define _PLATFORM_H_

#define USING_THREAD_POOL

#ifdef __WIN32__
	#include <io.h>
	#include <fcntl.h>
	#include <stdio.h>
	#define WIN32_LEAN_AND_MEAN
	#include <Windows.h>
	#include <WinSock2.h>
	#include <crtdbg.h>

	#pragma comment(lib, "Ws2_32.lib")

	typedef u_long ul4;
	typedef HANDLE mutex_type;
	typedef SOCKET sock;

	#define INIT_LOCK(_Lock)	(((_Lock) = CreateMutex(NULL, FALSE, NULL)) == NULL)
	#define LOCK(_Lock)			(WAIT_OBJECT_0 == WaitForSingleObject(_Lock,INFINITE))
	#define UNLOCK(_Lock)		ReleaseMutex(_Lock);
	#define SLEEP(_S)			Sleep(_S)
	#define ASSERT				_ASSERTE
#else
	#include <stdlib.h>
	#include <stdarg.h>
	#include <unistd.h>
	#include <string.h>
	#include <unistd.h>
	#include <arpa/inet.h>
    #include <pthread.h>
	#include <assert.h>
	#include <sys/time.h>
	#include <event.h>
	#include <stdlib.h>
	#include <unistd.h>
	#include "threadpool.h"

	typedef pthread_mutex_t mutex_type;
	typedef uint32_t ul4;
	typedef int sock;

	#define INIT_LOCK(_Lock)	(pthread_mutex_init(&(_Lock), NULL) != 0)
	#define LOCK(_Lock)			(0 == pthread_mutex_lock(&(_Lock)))
    #define UNLOCK(_Lock)		pthread_mutex_unlock(&(_Lock))
	#define SLEEP(_S)			usleep(1000 * (_S))
	#define ASSERT				assert
#endif

#endif //_PLATFORM_H_
