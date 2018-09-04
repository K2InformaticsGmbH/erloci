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
#ifndef OCI_LIB_TIMING_H
#define OCI_LIB_TIMING_H

#ifndef __WIN32__

#include <sys/time.h>
typedef struct timeval erloci_tick;

#else

#include <Windows.h>
typedef LARGE_INTEGER erloci_tick;
static double PCFreq = -1;

#endif

extern erloci_tick tick_init (void);
extern double      tick_diff (erloci_tick);

#endif // OCI_LIB_TIMING_H
