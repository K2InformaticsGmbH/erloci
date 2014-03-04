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
#ifndef TERM_H
#define TERM_H

#include <iostream>
#include <string>
#include <list>
#include <vector>
#include <sstream>

using namespace std;

#include "erl_interface.h"
#include "ei.h"

class term {
private:
	list<term> lt; // list or tuple

public:
	typedef list<term>::iterator iterator;
	enum Type {
		UNDEF		= ERL_UNDEF,
		ATOM		= ERL_ATOM,
		FLOAT		= ERL_FLOAT,
		PID			= ERL_PID,
		PORT		= ERL_PORT,
		REF			= ERL_REF,
		BINARY		= ERL_BINARY,
		INTEGER		= ERL_INTEGER,
		U_INTEGER	= ERL_U_INTEGER,
		U_LONGLONG	= ERL_U_LONGLONG,
		LONGLONG	= ERL_LONGLONG,
		LIST		= ERL_LIST,
		TUPLE		= ERL_TUPLE
	};
	Type type;

	vector<char> str;
	size_t str_len;
	union {
		int	i;
		unsigned int ui;
		double d;
		long l;
		long long ll;
		unsigned long ul;
		unsigned long long ull;
		struct {
			int n;
			int s;
			int c;
		} ppr; // pid, port, ref, string or binary
	} v;

	inline term()	{ str_len = 0; type = UNDEF; };

	inline bool is_undef()		{ return type == UNDEF;			}
	inline bool is_atom()		{ return type == ATOM;			}
	inline bool is_float()		{ return type == FLOAT;			}
	inline bool is_pid()		{ return type == PID;			}
	inline bool is_port()		{ return type == PORT;			}
	inline bool is_ref()		{ return type == REF;			}
	inline bool is_binary()		{ return type == BINARY;		}
	inline bool is_integer()	{ return type == INTEGER;		}
	inline bool is_u_integer()	{ return type == U_INTEGER;		}
	inline bool is_u_longlong()	{ return type == U_LONGLONG;	}
	inline bool is_longlong()	{ return type == LONGLONG;		}
	inline bool is_list()		{ return type == LIST;			}
	inline bool is_tuple()		{ return type == TUPLE;			}

	inline bool is_any_int()
	{
		return (type == INTEGER
			|| type == U_INTEGER
			|| type == U_LONGLONG
			|| type == LONGLONG);
	}

	void set(Type, char *);
	void set(Type, char *, int, int);
	void set(Type, char *, int, int, int);
	void set(Type, unsigned char *, int);
	void set(Type, double);
	void set(Type, int);
	void set(Type, unsigned int);
	void set(Type, long long);
	void set(Type, unsigned long long);
	void set(Type, term &, unsigned long long);

	unsigned long long length();

	inline term & tuple(void)						{ lt.clear();	type = TUPLE;		return *this;	};
	inline term & lst(void)							{ lt.clear();	type = LIST;		return *this;	};
	inline term & integer(int i)					{ v.i = i;		type = INTEGER;		return *this;	};
	inline term & integer(long l)					{ v.l = l;		type = INTEGER;		return *this;	};
	inline term & integer(long long ll)				{ v.ll = ll;	type = LONGLONG;	return *this;	};
	inline term & integer(unsigned int ui)			{ v.ui = ui;	type = INTEGER;		return *this;	};
	inline term & integer(unsigned long ul)			{ v.ul = ul;	type = INTEGER;		return *this;	};
	inline term & integer(unsigned long long ull)	{ v.ull = ull;	type = LONGLONG;	return *this;	};
	inline term & dbl(float f)						{ v.d = f;		type = FLOAT;		return *this;	};
	inline term & dbl(double d)						{ v.d = d;		type = FLOAT;		return *this;	};

	inline term & add(const term & t)				{ lt.push_back(t);					return *this;	};
	inline term & add(int i)						{ lt.push_back(term().integer(i));	return *this;	};
	inline term & add(long i)						{ lt.push_back(term().integer(i));	return *this;	};
	inline term & add(long long i)					{ lt.push_back(term().integer(i));	return *this;	};
	inline term & add(unsigned int i)				{ lt.push_back(term().integer(i));	return *this;	};
	inline term & add(unsigned long i)				{ lt.push_back(term().integer(i));	return *this;	};
	inline term & add(unsigned long long i)			{ lt.push_back(term().integer(i));	return *this;	};
	inline term & add(float i)						{ lt.push_back(term().dbl(i));		return *this;	};
	inline term & add(double i)						{ lt.push_back(term().dbl(i));		return *this;	};

	inline term & insert()
	{
		lt.resize(lt.size()+1);
		return lt.back();
	}

	inline term & atom(const char *_str)
	{
		type = ATOM;
		str_len = strlen(_str)+1;
		str.resize(str_len);
		str.assign(_str, _str + str_len);
		return *this;
	};

	inline term & binary(const char *_str)
	{
		type = BINARY;
		str_len = strlen(_str);
		str.resize(str_len+1);
		str.assign(_str, _str+str_len+1);
		str.push_back('\0');
		return *this;
	};
	inline term & binary(const char *_str, size_t len)
	{
		type = BINARY;
		str.resize(len+1);
		str.assign(_str, _str + len + 1);
		str.push_back('\0');
		str_len = len;
		return *this;
	};
	inline term & strng(const char *_str)
	{
		type = LIST;
		for(size_t idx = 0; idx < strlen(_str); ++idx) {
			term & t = this->insert();
			t.integer((int)_str[idx]);
		}
		str_len = 0;
		return *this;
	};
	inline term & pid(char *_str, int n, int s, int c)
	{
		type = PID;
		v.ppr.n = n;
		v.ppr.s = s;
		v.ppr.c = c;
		str_len = strlen(_str)+1;
		str.assign(_str, _str + str_len);
		return *this;
	};
	inline term & ref(char *_str, int n, int c)
	{
		type = REF;
		v.ppr.n = n;
		v.ppr.c = c;
		str_len = strlen(_str)+1;
		str.assign(_str, _str + str_len);
		return *this;
	};
	inline term & port(char *_str, int n, int c)
	{
		type = PORT;
		v.ppr.n = n;
		v.ppr.c = c;
		str_len = strlen(_str)+1;
		str.assign(_str, _str + str_len);
		return *this;
	};

	term & operator[] (size_t x)
	{
		int _x = (int)x;
		if(_x<0)
			throw("index out of bounds");
		for (iterator it = lt.begin(); it != lt.end(); ++it)
			if(--_x < 0)
				return (*it);
		throw("index out of bounds");
    }
	inline iterator begin()		{ return lt.begin();	}
	inline iterator end()		{ return lt.end();		}

	string print();
};

#endif // TERM_H