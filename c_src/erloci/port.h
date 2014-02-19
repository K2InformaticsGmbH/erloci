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
#ifndef PORT_H
#define PORT_H

#include "platform.h"

#include <iostream>
#include <vector>

using namespace std;

class port
{
private:
	int stdi;
	int stdo;
	mutex_type port_r_lock;
	mutex_type port_w_lock;
	inline bool lockr();
	inline void unlockr();
	inline bool lockw();
	inline void unlockw();
	int read_exact(vector<unsigned char> &, unsigned long);
	int write_exact(vector<unsigned char> &);

	port(void);
	port(port const&);          // Not implemented
    void operator=(port const&); // Not implemented

public:
	static port & instance()
	{
		static port p;
		return p;
	}
	int read_cmd(vector<unsigned char>&);
	int write_cmd(vector<unsigned char>&);

	inline ~port(void) {};
};

#endif // PORT_H