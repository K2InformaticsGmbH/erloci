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
#ifndef OCILOCK_H
#define OCILOCK_H

#include "lib_interface.h"
#include <oci.h>

class ocilock
{
	OCIEnv * envhp_;
	OCIError * errhp_;
	OCIThreadMutex * lock_;

public:
    ocilock(void *envhp, void *errhp, void *lock)
      : envhp_((OCIEnv*)envhp), errhp_((OCIError*)errhp), lock_((OCIThreadMutex*)lock)
    {
		intf_ret r;
		r.handle = envhp;

		checkenv(&r, OCIThreadMutexAcquire((OCIEnv*)envhp, (OCIError*)errhp, (OCIThreadMutex*)lock));
		if(r.fn_ret != SUCCESS) {
	   		REMOTE_LOG(ERR, "failed OCIThreadMutexAcquire %s\n", r.gerrbuf);
		    throw r;
		}
	}
    ~ocilock()
    {
		intf_ret r;
		r.handle = envhp_;

		checkenv(&r, OCIThreadMutexRelease(envhp_, errhp_, lock_));
		if(r.fn_ret != SUCCESS) {
   			REMOTE_LOG(ERR, "failed OCIThreadMutexRelease %s\n", r.gerrbuf);
			throw r;
		}
    }
};

#endif // OCILOCK_H