#include "platform.h"
#include "term.h"

string term::print()
{
	string term;

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
	if(str)	delete str;
	str_len = strlen(s) + 1;
	str = new char[str_len];
	strcpy(str, s);
}

void term::set(Type t, char *ns, int n, int c)
{
	type = t;
	if(str)	delete str;
	str_len = strlen(ns) + 1;
	str = new char[str_len];
	strcpy(str, ns);
	v.ppr.n = n;
	v.ppr.c = c;
}

void term::set(Type t, char *ns, int n, int s, int c)
{
	type = t;
	if(str)	delete str;
	str_len = strlen(ns) + 1;
	str = new char[str_len];
	strcpy(str, ns);
	v.ppr.n = n;
	v.ppr.s = s;
	v.ppr.c = c;
}

void term::set(Type t, unsigned char * s, int strl)
{
	type = t;
	if(str)	delete str;
	str_len = strl + 1;
	str = new char[str_len];
	strncpy(str, (const char *)s, strl);
	str[strl] = '\0';
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
