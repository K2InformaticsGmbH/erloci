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
	vector<term> lt; // list or tuple
	string str;
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
	string type;
	unsigned char type_code;


	inline bool is_atom()		{ return type_code == ERL_ATOM;			}
	inline bool is_float()		{ return type_code == ERL_FLOAT;		}
	inline bool is_pid()		{ return type_code == ERL_PID;			}
	inline bool is_port()		{ return type_code == ERL_PORT;			}
	inline bool is_ref()		{ return type_code == ERL_REF;			}
	inline bool is_binary()		{ return type_code == ERL_BINARY;		}
	inline bool is_integer()	{ return type_code == ERL_INTEGER;		}
	inline bool is_u_integer()	{ return type_code == ERL_U_INTEGER;	}
	inline bool is_u_longlong()	{ return type_code == ERL_U_LONGLONG;	}
	inline bool is_longlong()	{ return type_code == ERL_LONGLONG;		}
	inline bool is_list()		{ return type_code == ERL_LIST;			}
	inline bool is_tuple()		{ return type_code == ERL_TUPLE;		}

	void set(unsigned char, string, char *);
	void set(unsigned char, string, char *, int, int);
	void set(unsigned char, string, char *, int, int, int);
	void set(unsigned char, string, unsigned char *, int);
	void set(unsigned char, string, double);
	void set(unsigned char, string, int);
	void set(unsigned char, string, unsigned int);
	void set(unsigned char, string, long long);
	void set(unsigned char, string, unsigned long long);
	void set(unsigned char, string, term, unsigned long long);

	unsigned long long length();

	static inline term atom(const char *s)						{ return term((unsigned char)ERL_ATOM, string(s));	};
	static inline term atom(string & s)							{ return term((unsigned char)ERL_ATOM, s);		};
	static inline term binary(string & s)						{ return term((unsigned char)ERL_BINARY, s);	};
	static inline term strng(string & s)						{ return term((unsigned char)ERL_LIST, s);		};
	static inline term integer(int i)							{ return term(i);				};
	static inline term integer(long l)							{ return term(l);				};
	static inline term integer(long long ll)					{ return term(ll);				};
	static inline term integer(unsigned int ui)					{ return term(ui);				};
	static inline term integer(unsigned long ul)				{ return term(ul);				};
	static inline term integer(unsigned long long ull)			{ return term(ull);				};
	static inline term dbl(double d)							{ return term(d);				};
	static inline term pid(string & v, int n, int s, int c)		{ return term(v, n, s, c);		};
	static inline term ref(string & v, int n, int c)			{ return term((unsigned char)ERL_REF, v, n, c);};
	static inline term port(string & v, int n, int c)			{ return term((unsigned char)ERL_PORT, v, n, c);};

	static inline term tuple(void)								{ return term((unsigned char)ERL_TUPLE);		};
	static inline term list(void)								{ return term((unsigned char)ERL_LIST);		};

	inline void add(term t)										{ lt.push_back(t);				}
	string print();

	inline term() { };
	inline ~term() { };

private:
	inline term(int i)					{ v.i = i;		type = "ERL_INTEGER",		type_code = ERL_INTEGER;	};
	inline term(long l)					{ v.l = l;		type = "ERL_INTEGER",		type_code = ERL_INTEGER;	};
	inline term(long long ll)			{ v.ll = ll;	type = "ERL_LONGLONG",		type_code = ERL_LONGLONG;	};
	inline term(unsigned int ui)		{ v.ui = ui;	type = "ERL_U_INTEGER",		type_code = ERL_U_INTEGER;	};
	inline term(unsigned long ul)		{ v.ul = ul;	type = "ERL_U_INTEGER",		type_code = ERL_U_INTEGER;	};
	inline term(unsigned long long ull)	{ v.ull = ull;	type = "ERL_U_LONGLONG",	type_code = ERL_U_LONGLONG;	};
	inline term(float f)				{ v.d = f;		type = "ERL_FLOAT",			type_code = ERL_FLOAT;		};
	inline term(double d)				{ v.d = d;		type = "ERL_FLOAT",			type_code = ERL_FLOAT;		};

	inline term(unsigned char t)
	{
		switch (t) {
			case ERL_TUPLE:
				type = "ERL_TUPLE";
				type_code = ERL_TUPLE;
				break;
			case ERL_LIST:
				type = "ERL_LIST";
				type_code = ERL_LIST;
				break;
			default:
				break;
		};
	}

	inline term(unsigned char t, string s)
	{
		switch (t) {
			case ERL_ATOM:
				type = "ERL_ATOM";
				type_code = ERL_ATOM;
				str = s;
				break;
			case ERL_BINARY:
				type = "ERL_BINARY";
				type_code = ERL_BINARY;
				str = s;
				break;
			case ERL_LIST:
				type = "ERL_LIST";
				type_code = ERL_LIST;
				for(size_t idx = 0; idx < s.size(); ++idx)
					lt.push_back(term((int)s[idx]));
				break;
			default:
				break;
		};
	};

	inline term(unsigned char t, string & s, int n, int c)
	{
		switch (t) {
			case ERL_PORT:
				type = "ERL_PORT";
				type_code = ERL_PORT;
				v.ppr.n = n;
				v.ppr.c = c;
				str = s;
				break;
			case ERL_REF:
				type = "ERL_REF";
				type_code = ERL_REF;
				v.ppr.n = n;
				v.ppr.c = c;
				str = s;
				break;
		}
	};

	inline term(string & _str, int n, int s, int c)
	{
		type = "ERL_PID";
		type_code = ERL_PID;
		v.ppr.n = n;
		v.ppr.s = s;
		v.ppr.c = c;
		str = _str;
	};
};

#endif // TERM_H
