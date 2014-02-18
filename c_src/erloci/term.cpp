#include "platform.h"
#include "term.h"

string term::print()
{
	string term;

	switch(type_code) {
		case ERL_TUPLE:
			term += "{";
			for (size_t idx = 0; idx < lt.size(); ++idx) {
				term += lt[idx].print();
				if (idx+1 < lt.size())
					term += ",";
			}
			term += "}";
			break;
		case ERL_LIST:
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
		case ERL_ATOM:
			term += str;
			break;
		case ERL_FLOAT: {
			stringstream ss;
			ss << v.d;
			term += ss.str();
						}
			break;
		case ERL_INTEGER: {
			stringstream ss;
			ss << v.i;
			term += ss.str();
						}
			break;
		case ERL_U_INTEGER: {
			stringstream ss;
			ss << v.ui;
			term += ss.str();
						}
			break;
		case ERL_LONGLONG: {
			stringstream ss;
			ss << v.ll;
			term += ss.str();
						}
			break;
		case ERL_U_LONGLONG: {
			stringstream ss;
			ss << v.ull;
			term += ss.str();
						}
			break;
		case ERL_BINARY:
			term += "<<";
			for(size_t idx = 0; idx < str.size(); ++idx) {
				stringstream ss;
				ss << (unsigned int)str[idx];
				term += ss.str();
				if (idx+1 < str.size())
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
	switch (type_code) {
		case ERL_ATOM:			return str.size();
		case ERL_FLOAT:			return sizeof(v.d);
		case ERL_PID:			return str.size()+sizeof(v.ppr);
		case ERL_PORT:			return str.size()+sizeof(v.ppr);
		case ERL_REF:			return str.size()+sizeof(v.ppr);
		case ERL_BINARY:		return str.size();
		case ERL_INTEGER:		return sizeof(v.i);
		case ERL_U_INTEGER:		return sizeof(v.ui);
		case ERL_U_LONGLONG:	return sizeof(v.ull);
		case ERL_LONGLONG:		return sizeof(v.ll);
		case ERL_LIST:			return lt.size();
		case ERL_TUPLE:			return lt.size();
		default:				return 0;
	}
}

void term::set(unsigned char t, string typestr, char * s)
{
	type_code = t;
	type = typestr;
	str = s;
}

void term::set(unsigned char t, string typestr, char *ns, int n, int c)
{
	type_code = t;
	type = typestr;
	str = ns;
	v.ppr.n = n;
	v.ppr.c = c;
}

void term::set(unsigned char t, string typestr, char *ns, int n, int s, int c)
{
	type_code = t;
	type = typestr;
	str = ns;
	v.ppr.n = n;
	v.ppr.s = s;
	v.ppr.c = c;
}

void term::set(unsigned char t, string typestr, unsigned char * s, int strl)
{
	type_code = t;
	type = typestr;
	str.assign((char*)s, strl);
}

void term::set(unsigned char t, string typestr, double dbl)
{
	type_code = t;
	type = typestr;
	v.d = dbl;
}

void term::set(unsigned char t, string typestr, int i)
{
	type_code = t;
	type = typestr;
	v.i = i;
}

void term::set(unsigned char t, string typestr, unsigned int ui)
{
	type_code = t;
	type = typestr;
	v.ui = ui;
}

void term::set(unsigned char t, string typestr, long long ll)
{
	type_code = t;
	type = typestr;
	v.ll = ll;
}

void term::set(unsigned char t, string typestr, unsigned long long ull)
{
	type_code = t;
	type = typestr;
	v.ull = ull;
}

void term::set(unsigned char t, string typestr, term trm, unsigned long long idx)
{
	type_code = t;
	type = typestr;
	lt.push_back(trm);
}
