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
    erloci_tick start;
#ifndef __WIN32__
    struct timezone tz;
    gettimeofday(&start, &tz);
#else
    if(PCFreq < 0) {
        if(QueryPerformanceFrequency(&start))
            PCFreq = double(start.QuadPart);
    }

    QueryPerformanceCounter(&start);

#endif
    return start;
}

double tick_diff(erloci_tick start)
{
    erloci_tick end;
    double diff = 0.0;
#ifndef __WIN32__
    struct timezone tz;
    gettimeofday(&end, &tz);

    diff = (double)(end.tv_sec - start.tv_sec);
#else
    LARGE_INTEGER EndingTime;

    if(QueryPerformanceCounter(&EndingTime)) {
        diff = double(EndingTime.QuadPart - start.QuadPart) / PCFreq;
    }
#endif

    return diff;
}
