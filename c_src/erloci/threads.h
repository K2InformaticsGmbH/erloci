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
#ifndef THREADS_H
#define THREADS_H

#include "transcoder.h"

class threads {
public:
	static bool run_threads;
	static transcoder & tc;

	static threads & init(void)
	{
		static threads t;
		start();
		return t;
	}
	static void start(void);
	~threads(void);

private:
#ifdef __WIN32__
	static PTP_POOL pool;
	static TP_CALLBACK_ENVIRON CallBackEnviron;
	static PTP_CLEANUP_GROUP cleanupgroup;
	static UINT rollback;
#else
	static threadpool_t *pTp            = NULL;
#endif

	threads(void);
	threads(threads const&);		// Not implemented
    void operator=(threads const&);	// Not implemented
};

#endif // THREADS_H