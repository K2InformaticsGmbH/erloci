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

#include "platform.h"

class logger
{
private:
	static logger self;

	mutex_type log_lock;
	sock log_sock; //Socket handle

	logger(void) {};
	logger(logger const&);         // Not implemented
    void operator=(logger const&); // Not implemented

	inline bool lock()		{ return LOCK(log_lock);	}
	inline void unlock()	{ UNLOCK(log_lock);			}

public:
	static char * init(int);
	static void log(const char *, const char *, unsigned int, unsigned int, void *, const char *);

	~logger(void);
};

#endif // LOGGER_H
