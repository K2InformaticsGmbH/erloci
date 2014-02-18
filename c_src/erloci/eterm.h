#pragma once
#ifndef _ETERM_H_
#define _ETERM_H_
#include <iostream>
#include <string>
#include <vector>

#include "platform.h"

#include "erl_interface.h"
#include "ei.h"
#include "term.h"

class eterm
{
private:
	mutex_type transcoder_lock;
	inline bool lock();
	inline void unlock();
	void erlterm_to_stl(ETERM *, term &);
	ETERM * stl_to_erlterm(term &);

	eterm(void);
	eterm(eterm const&);          // Not implemented
    void operator=(eterm const&); // Not implemented

public:
	static eterm & getInstance()
	{
		static eterm instance;
		return instance;
	}
	static inline void get_stats(unsigned long & allocated, unsigned long & freed) { erl_eterm_statistics(&allocated,&freed); };
	term decode(vector<unsigned char> &);
	vector<unsigned char> encode(term &);
	inline ~eterm(void) {};
};

#endif //_ETERM_H_