#include "platform.h"
#include "term.h"

string term::print()
{
	string term;

#if 0
	switch(type) {
		case TUPLE:
			term += "{";
			for (size_t idx = 0; idx < lt.size(); ++idx) {
				term += lt[idx].print();
				if (idx+1 < lt.size())
					term += ",";
			}
			term += "}";
			break;
		case LIST:
			if (lt.size() > 0) {
				term += "[";
				for (size_t idx = 0; idx < lt.size(); ++idx) {
					term += lt[idx].print();
					if (idx+1 < lt.size())
						term += ",";
				}
				term += "]";
			} else {
				term += "\"";
				term += str;
				term += "\"";
			}
			break;
		case ATOM:
			term += str;
			break;
		case FLOAT: {
			stringstream ss;
			ss << v.d;
			term += ss.str();
						}
			break;
		case INTEGER: {
			stringstream ss;
			ss << v.i;
			term += ss.str();
						}
			break;
		case U_INTEGER: {
			stringstream ss;
			ss << v.ui;
			term += ss.str();
						}
			break;
		case LONGLONG: {
			stringstream ss;
			ss << v.ll;
			term += ss.str();
						}
			break;
		case U_LONGLONG: {
			stringstream ss;
			ss << v.ull;
			term += ss.str();
						}
			break;
		case BINARY:
			term += "<<";
			for(size_t idx = 0; idx < str_len; ++idx) {
				stringstream ss;
				ss << (unsigned int)str[idx];
				term += ss.str();
				if (idx+1 < str_len)
						term += ",";
			}
			term += ">>";
			break;
		default:
			term += "_";
			break;
	}
#endif

	return term;
}

unsigned long long term::length()
{
	switch (type) {
		case ATOM:			return str_len;
		case FLOAT:			return sizeof(v.d);
		case PID:			return str_len+sizeof(v.ppr);
		case PORT:			return str_len+sizeof(v.ppr);
		case REF:			return str_len+sizeof(v.ppr);
		case BINARY:		return str_len;
		case INTEGER:		return sizeof(v.i);
		case U_INTEGER:		return sizeof(v.ui);
		case U_LONGLONG:	return sizeof(v.ull);
		case LONGLONG:		return sizeof(v.ll);
		case LIST:			return lt.size();
		case TUPLE:			return lt.size();
		default:			return 0;
	}
}

void term::set(Type t, char * s)
{
	type = t;
	if (s) {
		str_len = strlen(s) + 1;
		str.assign(s, s + str_len);
	}
}

void term::set(Type t, char *ns, int n, int c)
{
	type = t;
	v.ppr.n = n;
	v.ppr.c = c;
	if (ns) {
		str_len = strlen(ns) + 1;
		str.assign(ns, ns + str_len);
	}
}

void term::set(Type t, char *ns, int n, int s, int c)
{
	type = t;
	v.ppr.n = n;
	v.ppr.s = s;
	v.ppr.c = c;
	if (ns) {
		str_len = strlen(ns) + 1;
		str.assign(ns, ns + str_len);
	}
}

void term::set(Type t, char *ns, unsigned int *n, int l, int c)
{
	type = t;
	for (int _i = 0; _i < l; ++_i)
		v.ppr.nr[_i] = n[_i];
	v.ppr.c = c;
	if (ns) {
		str_len = strlen(ns) + 1;
		str.assign(ns, ns + str_len);
	}
}

void term::set(Type t, unsigned char * s, int strl)
{
	type = t;
	str_len = strl;
	str.resize(str_len+1);
	if (s && str_len > 0) {
		str.assign(s, s+str_len+1);
		str.push_back('\0');
	}
}

void term::set(Type t, double dbl)
{
	type = t;
	v.d = dbl;
}

void term::set(Type t, int i)
{
	type = t;
	v.i = i;
}

void term::set(Type t, unsigned int ui)
{
	type = t;
	v.ui = ui;
}

void term::set(Type t, long long ll)
{
	type = t;
	v.ll = ll;
}

void term::set(Type t, unsigned long long ull)
{
	type = t;
	v.ull = ull;
}

void term::set(Type t, term & trm, unsigned long long idx)
{
	type = t;
	lt.push_back(trm);
}