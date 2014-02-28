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
#ifndef ETERM_H
#define ETERM_H

#include <iostream>
#include <vector>

#include "platform.h"

#include "erl_interface.h"
#include "ei.h"
#include "term.h"

class transcoder
{
private:
	mutex_type transcoder_lock;
	inline bool lock();
	inline void unlock();
	void erlterm_to_stl(ETERM *, term &);
	ETERM * stl_to_erlterm(term &);

	transcoder(void);
	transcoder(transcoder const&);      // Not implemented
    void operator=(transcoder const&);	// Not implemented

public:
	static transcoder & instance()
	{
		static transcoder t;
		return t;
	}
	static inline void stats(unsigned long & allocated, unsigned long & freed) { erl_eterm_statistics(&allocated,&freed); };
	void decode(vector<unsigned char> &, term &);
	vector<unsigned char> encode(term &);
	vector<unsigned char> encode_with_header(term &);
	inline ~transcoder(void) {};
};

#endif //ETERM_H