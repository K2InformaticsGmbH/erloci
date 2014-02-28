#include "platform.h"
#include "transcoder.h"

using namespace std;

transcoder::transcoder(void)
{
	erl_init(NULL, 0);
    if (INIT_LOCK(transcoder_lock)) {
        return;
    }
}

bool transcoder::lock()
{
	return LOCK(transcoder_lock);
}

void transcoder::unlock()
{
	UNLOCK(transcoder_lock);
}

void transcoder::decode(vector<unsigned char> & buf, term & t)
{
	unsigned long allocated, freed;
	if(lock()) {
		ETERM * etermp = erl_decode(&buf[0]);

		erlterm_to_stl(etermp, t);

		// If allocated != 0 after freeing the term
		// we have a memory leak
		// release the mutex in either case
		erl_free_compound(etermp);
		stats(allocated, freed);
		if (allocated != 0) {
			unlock();
		}
		unlock();
	}
}

vector<unsigned char> transcoder::encode_with_header(term & t)
{
	unsigned long allocated, freed;
	vector<unsigned char> buf;
	if(lock()) {
		ETERM * etermp = stl_to_erlterm(t);

		// If allocated != 0 after freeing the term
		// we have a memory leak
		// release the mutex in either case
		unsigned long len = (unsigned long)erl_term_len(etermp);

		// Header(32bit) : term length in network byte order
		buf.resize(len+4);
		len = htonl(len);
		buf[3] = (unsigned char) ((len >> 24)	& 0x000000FF);
		buf[2] = (unsigned char) ((len >> 16)	& 0x000000FF);
		buf[1] = (unsigned char) ((len >> 8)	& 0x000000FF);
		buf[0] = (unsigned char) (len			& 0x000000FF);

		erl_encode(etermp, &buf[4]);
		erl_free_compound(etermp);
		stats(allocated, freed);
		if (allocated != 0) {
			unlock();
			return buf;
		}
		unlock();
	}
	return buf;
}

vector<unsigned char> transcoder::encode(term & t)
{
	unsigned long allocated, freed;
	vector<unsigned char> buf;
	if(lock()) {
		ETERM * etermp = stl_to_erlterm(t);
		ASSERT(etermp != NULL);

		// If allocated != 0 after freeing the term
		// we have a memory leak
		// release the mutex in either case
		unsigned long len = (unsigned long)erl_term_len(etermp);
		buf.resize(len);

		erl_encode(etermp, &buf[0]);
		erl_free_compound(etermp);
		stats(allocated, freed);
		if (allocated != 0) {
			unlock();
			return buf;
		}
		unlock();
	}
	return buf;
}

void transcoder::erlterm_to_stl(ETERM *et, term & t)
{
	if(0);
	else if(ERL_IS_ATOM(et))				t.set(term::ATOM,		ERL_ATOM_PTR_UTF8(et));
	else if(ERL_IS_FLOAT(et))				t.set(term::FLOAT,		ERL_FLOAT_VALUE(et));
	else if(ERL_IS_PID(et))					t.set(term::PID,		ERL_PID_NODE_UTF8(et),	ERL_PID_NUMBER(et),		ERL_PID_SERIAL(et),		ERL_PID_CREATION(et));
	else if(ERL_IS_PORT(et))				t.set(term::PORT,		ERL_PORT_NODE_UTF8(et),	ERL_PORT_NUMBER(et),	ERL_PORT_CREATION(et));
	else if(ERL_IS_REF(et))					t.set(term::REF,		ERL_REF_NODE_UTF8(et),	ERL_REF_NUMBER(et),		ERL_REF_CREATION(et));
	else if(ERL_IS_BINARY(et))				t.set(term::BINARY,		ERL_BIN_PTR(et),		ERL_BIN_SIZE(et));
	else if(ERL_IS_INTEGER(et))				t.set(term::INTEGER,	ERL_INT_VALUE(et));
	else if(ERL_IS_UNSIGNED_INTEGER(et))	t.set(term::U_INTEGER,	ERL_INT_UVALUE(et));
	else if(ERL_IS_LONGLONG(et))			t.set(term::LONGLONG,	ERL_LL_VALUE(et));
	else if(ERL_IS_UNSIGNED_LONGLONG(et))	t.set(term::U_LONGLONG,	ERL_LL_UVALUE(et));
	else if(ERL_IS_EMPTY_LIST(et))			t.set(term::LIST,		NULL, 0);
	else if(ERL_IS_CONS(et) || ERL_IS_LIST(et)) {
		ETERM *erl_list = et;
		unsigned long long list_idx = 0;
		do {
			ETERM *et1 = ERL_CONS_HEAD(erl_list);
			term t1;
			erlterm_to_stl(et1, t1);
			t.set(term::LIST, t1, list_idx);
			erl_list = ERL_CONS_TAIL(erl_list);
			++list_idx;
		} while(erl_list && !ERL_IS_EMPTY_LIST(erl_list));
	}
	else if(ERL_IS_TUPLE(et)) {
		for(unsigned long long tuple_idx = 0; tuple_idx < (unsigned long long)ERL_TUPLE_SIZE(et); ++tuple_idx) {
			term t1;
			erlterm_to_stl(ERL_TUPLE_ELEMENT(et, tuple_idx), t1);
			t.set(term::TUPLE, t1, tuple_idx);
		}
	}
}

ETERM * transcoder::stl_to_erlterm(term & t)
{
	ETERM * et = NULL;
	switch (t.type) {
		case term::ATOM:
			et = erl_mk_atom(t.str);
			break;
		case term::FLOAT:
			et = erl_mk_float(t.v.d);
			break;
		case term::PID:
			et = erl_mk_pid(t.str, t.v.ppr.n, t.v.ppr.s, t.v.ppr.c);
			break;
		case term::PORT:
			et = erl_mk_port(t.str, t.v.ppr.n, t.v.ppr.c);
			break;
		case term::REF:
			et = erl_mk_ref(t.str, t.v.ppr.n, t.v.ppr.c);
			break;
		case term::BINARY:
			et = erl_mk_binary(t.str, (int)t.str_len);
			break;
		case term::INTEGER:
			et = erl_mk_int(t.v.i);
			break;
		case term::U_INTEGER:
			et = erl_mk_uint(t.v.ui);
			break;
		case term::LONGLONG:
			et = erl_mk_longlong(t.v.ll);
			break;
		case term::U_LONGLONG:
			et = erl_mk_ulonglong(t.v.ull);
			break;
		case term::LIST: {
			ETERM ** et_ms = new ETERM*[t.length()];
			int i=0;
			for (vector<term>::iterator it = t.begin() ; it != t.end(); ++it) {
				et_ms[i] = stl_to_erlterm(*it);
				++i;
			}
			et = erl_mk_list(et_ms, (int)t.length());
            delete et_ms;
					   }
			break;
		case term::TUPLE: {
			ETERM ** et_ms = new ETERM*[t.length()];
			int i=0;
			for (vector<term>::iterator it = t.begin(); it != t.end(); ++it) {
				et_ms[i] = stl_to_erlterm(*it);
				++i;
			}
			et = erl_mk_tuple(et_ms, (int)t.length());
            delete et_ms;
						}
			break;
        default:
            break;
	}
	return et;
}
