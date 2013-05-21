// erloci_con.cpp : D	efines the entry point for the console application.
//

#include "stdafx.h"
#include "oci_lib_intf.h"

#include "string.h"

#include "erl_interface.h"
#include "ei.h"

void append_coldef_to_list(const char * col_name, const char * data_type, const unsigned int max_len, void * list)
{
	printf("\t%s\n", col_name);
}

typedef struct _LIST {
	unsigned long int len;
	ETERM **list;
} LIST;

void string_append(const char * string, void * list)
{
	if (list==NULL)
        return;

	LIST **container_list = (LIST **)list;
	if (*container_list == NULL) {
        *container_list = (LIST *)malloc(sizeof(LIST *));
		(*container_list)->list = NULL;
		(*container_list)->len = 0;
	}

	(*container_list)->list = (ETERM **)realloc((*container_list)->list, ((*container_list)->len + 1) * sizeof(ETERM **));

	(*container_list)->list[(*container_list)->len] = erl_format((char*)"~s", string);
	(*container_list)->len++;
}

void list_append(const void * sub_list, void * list)
{
	if (list==NULL)
        return;

	LIST **container_list = (LIST **)list;
	if (*container_list == NULL) {
        *container_list = (LIST *)malloc(sizeof(LIST *));
		(*container_list)->list = NULL;
		(*container_list)->len = 0;
	}

	(*container_list)->list = (ETERM **)realloc((*container_list)->list, ((*container_list)->len + 1) * sizeof(ETERM **));

	(*container_list)->list[(*container_list)->len] = erl_mk_list(((LIST *)sub_list)->list, ((LIST *)sub_list)->len);
	free(((LIST *)sub_list)->list);

	(*container_list)->len++;
}

unsigned int sizeof_resp(void * resp)
{
	return 0;
}

int _tmain(int argc, _TCHAR* argv[])
{
	const char	*tns = "(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=tcp)(HOST=192.168.1.69)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=SBS0.k2informatics.ch)))",
				*usr = "SBS0",
				*pwd = "sbs0sbs0_4dev",
				*opt = "";
	void * conn_handle = NULL;
	const char * qry = "SELECT * FROM AAABLEVEL";
	void * statement_handle = NULL;
	LIST * rows = NULL;

	erl_init(NULL, 0);

	oci_init();
	oci_create_tns_seesion_pool((const unsigned char *)tns, strlen(tns), (const unsigned char *)usr, strlen(usr), (const unsigned char *)pwd, strlen(pwd), (const unsigned char *)opt, strlen(opt));
	conn_handle = oci_get_session_from_pool();

    /* Columns */
	printf("Columns:\n");
	switch(oci_exec_sql(conn_handle, &statement_handle, (const unsigned char *)qry, strlen(qry), NULL, NULL, append_coldef_to_list)) {
		case SUCCESS:
			printf("Success...!\n");
			break;
		case CONTINUE_WITH_ERROR:
			printf("Continue with error...!\n");
			break;
		case FAILURE:
			printf("Failed...!\n");
			break;
    }

	oci_produce_rows(statement_handle, &rows, string_append, list_append, sizeof_resp);

	ETERM * t = erl_mk_list(rows->list, rows->len);
	erl_print_term(stdout, t);

	free(rows->list);

	return 0;
}

bool log_flag = true;
void log_remote(const char *fmt, ...)
{
    va_list arguments;
    va_start(arguments, fmt);

    vprintf(fmt, arguments);

    va_end(arguments);
}