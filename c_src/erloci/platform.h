#pragma once
#ifndef _PLATFORM_H_
#define _PLATFORM_H_

#ifdef __WIN32__
	#include <io.h>
	#include <fcntl.h>
	#include <stdio.h>
	#include <WinSock2.h>
	#include <Windows.h>
	typedef u_long ul4;
	typedef HANDLE mutex_type;

	#define INIT_LOCK(_Lock)	(((_Lock) = CreateMutex(NULL, FALSE, NULL)) == NULL)
	#define LOCK(_Lock)			(WAIT_OBJECT_0 == WaitForSingleObject(_Lock,INFINITE))
	#define UNLOCK(_Lock)		ReleaseMutex(_Lock);
#else
	#include <stdlib.h>
	#include <unistd.h>
	#include <string.h>
	#include <unistd.h>
	#include <arpa/inet.h>
	typedef  pthread_mutex_t mutex_type;
	typedef uint32_t ul4;

	#define INIT_LOCK(_Lock)	(pthread_mutex_init(&(_Lock), NULL) != 0)
	#define LOCK(_Lock)			(0 == pthread_mutex_lock(&(_Lock)))
    #define UNLOCK(_Lock)		pthread_mutex_unlock(&(_Lock));
#endif

#endif //_PLATFORM_H_