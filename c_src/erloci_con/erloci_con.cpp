#include "stdafx.h"
#include "oci_lib_intf.h"

#include "string.h"
#include "stdarg.h"

void append_coldef_to_list(const char * col_name, const char * data_type, const unsigned int max_len, void * list)
{
	printf("\t%s\n", col_name);
}

void string_append(const char * string, void * list)
{
	printf("%s\t", string);
}

void list_append(const void * sub_list, void * list)
{
	printf("\n");
}

unsigned int sizeof_resp(void * resp)
{
	return 0;
}

int _tmain(int argc, _TCHAR* argv[])
{
	const char
		*tns = "(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=tcp)(HOST=80.67.144.206)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=XE)))",
		*usr = "bikram",
		*pwd = "abcd123",
		*opt = "";
	void * conn_handle = NULL;
	const char * qry = "SELECT * FROM ALL_TABLES";
	void * statement_handle = NULL;

	oci_init();
	oci_create_tns_seesion_pool(tns, strlen(tns),
								usr, strlen(usr),
								pwd, strlen(pwd),
								opt, strlen(opt));

	intf_ret r = oci_get_session_from_pool(&conn_handle);
	if(r.fn_ret != SUCCESS) {
		printf("oci_get_session_from_pool error... %s!\n", r.gerrbuf);
		return -1;
	}

    /* Columns */
	printf("Columns:\n");
	r = oci_exec_sql(conn_handle, &statement_handle, (const unsigned char *)qry, strlen(qry), NULL, NULL, append_coldef_to_list);
	switch(r.fn_ret) {
		case SUCCESS:
			printf("oci_exec_sql success...!\n");
			break;
		case CONTINUE_WITH_ERROR:
			printf("oci_exec_sql error... %s!\n", r.gerrbuf);
			return -1;
		case FAILURE:
			printf("oci_exec_sql failed... %s!\n", r.gerrbuf);
			return -1;
    }
	r = oci_produce_rows(statement_handle, NULL, string_append, list_append, sizeof_resp, 100);
	switch(r.fn_ret) {
		case SUCCESS:
			printf("oci_produce_rows success...!\n");
			break;
		case CONTINUE_WITH_ERROR:
			printf("oci_produce_rows error... %s!\n", r.gerrbuf);
			return -1;
		case FAILURE:
			printf("oci_produce_rows failed... %s!\n", r.gerrbuf);
			return -1;
    }

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