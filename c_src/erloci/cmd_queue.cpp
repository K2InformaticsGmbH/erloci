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

#include "cmd_queue.h"

cmd_queue cmd_queue::self;

cmd_queue::cmd_queue(void)
{
	
	if (INIT_LOCK(self.q_lock)) {
        return;
    }
}

vector<unsigned char> cmd_queue::pop()
{
	vector<unsigned char> p;
	if(self.lck()) {
		if (!self.cmdsq.empty()) {
			p = self.cmdsq.front();
			self.cmdsq.pop();
		}
 		self.ulck();
    }
	return p;
}

void cmd_queue::push(vector<unsigned char> & buf)
{
	if(self.lck()) {
		self.cmdsq.push(buf);
		self.ulck();
	}
}
