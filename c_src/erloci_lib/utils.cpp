/* Copyright 2018 K2Informatics GmbH, Root Laengenbold, Switzerland
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


#include "timing.h"

erloci_tick tick_init(void)
{
#ifndef __WIN32__
    return clock();
#else
    LARGE_INTEGER start;
    if(PCFreq < 0) {
        if(QueryPerformanceFrequency(&start))
            PCFreq = double(li.QuadPart);
    }

    QueryPerformanceCounter(&start);

    return start;
#endif
}

double tick_diff(erloci_tick start)
{
#ifndef __WIN32__
    clock_t end = clock();

    return ((double)(end - start)) / CLOCKS_PER_SEC;
#else
    LARGE_INTEGER EndingTime;

    if(QueryPerformanceCounter(&EndingTime)) {
        return double(EndingTime.QuadPart - start) / PCFreq;
    }

    return 0;
#endif
}
