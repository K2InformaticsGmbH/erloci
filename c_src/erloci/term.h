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
#include <vector>
#include <sstream>

using namespace std;

#include "erl_interface.h"
#include "ei.h"

class term {
public:
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

	vector<term> lt; // list or tuple
	char * str;
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

	inline term()	{ str = NULL; str_len = 0; type = UNDEF; };
	inline ~term()	{ if(str) delete str;		};
	inline term(const term& t)
		: type(t.type), v(t.v), lt(t.lt), str_len(t.str_len)
	{
		str = NULL;
		if (t.str_len > 0 && t.str) {
			str = new char[t.str_len];
			copy(t.str, t.str + t.str_len, str);
		}
	};

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

	inline term & tuple(void)						{				type = TUPLE;		return *this;	};
	inline term & list(void)						{				type = LIST;		return *this;	};
	inline term & integer(int i)					{ v.i = i;		type = INTEGER;		return *this;	};
	inline term & integer(long l)					{ v.l = l;		type = INTEGER;		return *this;	};
	inline term & integer(long long ll)				{ v.ll = ll;	type = LONGLONG;	return *this;	};
	inline term & integer(unsigned int ui)			{ v.ui = ui;	type = INTEGER;		return *this;	};
	inline term & integer(unsigned long ul)			{ v.ul = ul;	type = INTEGER;		return *this;	};
	inline term & integer(unsigned long long ull)	{ v.ull = ull;	type = LONGLONG;	return *this;	};
	inline term & dbl(float f)						{ v.d = f;		type = FLOAT;		return *this;	};
	inline term & dbl(double d)						{ v.d = d;		type = FLOAT;		return *this;	};

	inline term & add(const term & t)				{ lt.push_back(t);						return *this;	};
	inline term & add(int i)						{ lt.push_back(term().integer(i));		return *this;	};
	inline term & add(long i)						{ lt.push_back(term().integer(i));		return *this;	};
	inline term & add(long long i)					{ lt.push_back(term().integer(i));		return *this;	};
	inline term & add(unsigned int i)				{ lt.push_back(term().integer(i));		return *this;	};
	inline term & add(unsigned long i)				{ lt.push_back(term().integer(i));		return *this;	};
	inline term & add(unsigned long long i)			{ lt.push_back(term().integer(i));		return *this;	};
	inline term & add(float i)						{ lt.push_back(term().dbl(i));			return *this;	};
	inline term & add(double i)						{ lt.push_back(term().dbl(i));			return *this;	};

	inline term & atom(const char *_str)
	{
		type = ATOM;
		if(str)	delete str;
		str_len = strlen(_str)+1;
		str = new char[str_len];
		strcpy(str, _str);
		return *this;
	};
	inline term & binary(const char *_str)
	{
		type = BINARY;
		if(str)	delete str;
		str_len = strlen(_str);
		str = new char[str_len+1];
		copy(_str, _str + str_len, str);
		str[str_len] = '\0';
		return *this;
	};
	inline term & binary(const char *_str, size_t len)
	{
		type = BINARY;
		if(str)	delete str;
		str_len = len+1;
		str = new char[str_len];
		copy(_str, _str + len, str);
		return *this;
	};
	inline term & strng(const char *_str)
	{
		type = LIST;
		for(size_t idx = 0; idx < strlen(_str); ++idx) {
			term t;
			t.integer((int)_str[idx]);
			lt.push_back(t);
		}
		str = NULL; str_len = 0;
		return *this;
	};
	inline term & pid(char *_str, int n, int s, int c)
	{
		type = PID;
		v.ppr.n = n;
		v.ppr.s = s;
		v.ppr.c = c;
		if(str)	delete str;
		str_len = strlen(_str)+1;
		str = new char[str_len];
		strcpy(str, _str);
		return *this;
	};
	inline term & ref(char *_str, int n, int c)
	{
		type = REF;
		v.ppr.n = n;
		v.ppr.c = c;
		if(str)	delete str;
		str_len = strlen(_str)+1;
		str = new char[str_len];
		strcpy(str, _str);
		return *this;
	};
	inline term & port(char *_str, int n, int c)
	{
		type = PORT;
		v.ppr.n = n;
		v.ppr.c = c;
		if(str)	delete str;
		str_len = strlen(_str)+1;
		str = new char[str_len];
		strcpy(str, _str);
		return *this;
	};

	string print();

private:
};

#endif // TERM_H