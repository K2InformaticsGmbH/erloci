/* Copyright 2014 K2Informatics GmbH, Root Laengenbold, Switzerland
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
#ifndef CMD_QUEUE_H
#define CMD_QUEUE_H

#include "platform.h"

#include <vector>
#include <queue>

using namespace std;

class cmd_queue
{
private:
	static cmd_queue self;
	mutex_type q_lock;
	queue<vector<unsigned char> > cmdsq;

	cmd_queue(void);
	cmd_queue(cmd_queue const&);        // Not implemented
    void operator=(cmd_queue const&);	// Not implemented

	inline bool lock()		{ return LOCK(q_lock);	}
	inline void unlock()	{ UNLOCK(q_lock);		}

public:
	static vector<unsigned char> pop(void);
	static void push(vector<unsigned char> &);
	static inline size_t size() { return self.cmdsq.size(); };
};

#endif // CMD_QUEUE_H
