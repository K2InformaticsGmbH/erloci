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


#include "lib_interface.h"

#ifndef __WIN32__
#else
#include <Windows.h>
static double PCFreq = -1;
__int64 CounterStart = 0;
#endif

void tick_init(void)
{
	LARGE_INTEGER li;
	if(PCFreq < 0) {
		if(QueryPerformanceFrequency(&li))
			PCFreq = double(li.QuadPart);
	}

	if(QueryPerformanceCounter(&li))
    	CounterStart = li.QuadPart;
}

__int64 tick_diff(void)
{
	LARGE_INTEGER EndingTime;

	if(QueryPerformanceCounter(&EndingTime)) {
		return __int64(double(EndingTime.QuadPart - CounterStart) / PCFreq);
	}

	return 0;
}