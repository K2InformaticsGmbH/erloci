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
#ifndef LOGGER_H
#define LOGGER_H

#ifdef __WIN32__
#include <Winsock2.h>
#else
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <unistd.h>
#endif

class logger
{
private:
	static logger self;

	#ifdef __WIN32__
	SOCKET log_sock; //Socket handle
	#else
	int log_sock;
	#endif

	logger(void) {};
	logger(logger const&);         // Not implemented
    void operator=(logger const&); // Not implemented

#ifdef __WIN32__
	HANDLE mutex;
	inline bool lck()	{ return (WAIT_OBJECT_0 == WaitForSingleObject((mutex),INFINITE)); }
	inline void ulck()	{ ReleaseMutex(mutex); }
#else
	pthread_mutex_t mutex;
	inline bool lck()	{ return (0 == pthread_mutex_lock(&(mutex))); }
	inline void ulck()	{ pthread_mutex_unlock(&(mutex)); }
#endif

public:
	static char * init(int);
	static void log(const char *, const char *, unsigned int, unsigned int, void *, const char *);

	~logger(void);
};

#endif // LOGGER_H
