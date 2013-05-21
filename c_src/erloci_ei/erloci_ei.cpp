// erloci_ei.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"
#include "ei.h"

static char buf[2048];
static int i = 0;

int _tmain(int argc, _TCHAR* argv[])
{
	char *s = (char*)malloc(BUFSIZ);
	//ei_x_buff xbuf;
	int i=0;

/*	ei_x_new_with_version(&xbuf);

//	ei_x_format(&xbuf, "{~a,~s,~i,~l,~u,~f,~d}", "atom", "string test", 10, 0xFFFFFFFFL, 0xFFFFFFFFUL, 0.01, 0.000001L);

	ei_x_encode_list_header(&xbuf, 3);
	ei_x_encode_atom(&xbuf, "c");
	ei_x_encode_atom(&xbuf, "d");
	ei_x_encode_list_header(&xbuf, 1);
	ei_x_encode_atom(&xbuf, "e");
	ei_x_encode_atom(&xbuf, "f");
	ei_x_encode_empty_list(&xbuf);

	ei_s_print_term(&s, xbuf.buff, &i);

	ei_x_free(&xbuf);*/

	if (ei_encode_version(buf, &i) < 0) goto error_exit;

	if (ei_encode_list_header(buf, &i, 3) < 0) goto error_exit;
	if (ei_encode_atom(buf, &i, "c") < 0) goto error_exit;
	if (ei_encode_atom(buf, &i, "d") < 0) goto error_exit;
	if (ei_encode_list_header(buf, &i, 1) < 0) goto error_exit;
	if (ei_encode_atom(buf, &i, "e") < 0) goto error_exit;
	if (ei_encode_atom(buf, &i, "f") < 0) goto error_exit;
	if (ei_encode_empty_list(buf, &i) < 0) goto error_exit;

	i=0;
	if (ei_s_print_term(&s, buf, &i) < 0) goto error_exit;

	printf("%s\n", s);

error_exit:
	return 0;
}

