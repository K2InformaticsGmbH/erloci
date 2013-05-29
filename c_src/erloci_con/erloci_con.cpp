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
	const char	*tns = "(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=tcp)(HOST=80.67.144.206)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=XE)))",
				*usr = "bikram",
				*pwd = "abcd123",
				*opt = "";
	void * conn_handle = NULL;
	const char * qry = "SELECT * FROM ALL_TABLES";
	void * statement_handle = NULL;

	oci_init();
	oci_create_tns_seesion_pool((const unsigned char *)tns, strlen(tns), (const unsigned char *)usr, strlen(usr), (const unsigned char *)pwd, strlen(pwd), (const unsigned char *)opt, strlen(opt));
	conn_handle = oci_get_session_from_pool();

    /* Columns */
	printf("Columns:\n");
	switch(oci_exec_sql(conn_handle, &statement_handle, (const unsigned char *)qry, strlen(qry), NULL, NULL, append_coldef_to_list)) {
		case SUCCESS:
			{
				printf("Success...!\n");
				oci_produce_rows(statement_handle, NULL, string_append, list_append, sizeof_resp, 100);
			}
			break;
		case CONTINUE_WITH_ERROR:
			{
				int err_str_len = 0;
				char * err_str;
				get_last_error(NULL, err_str_len);
				err_str = new char[err_str_len+1];
				get_last_error(err_str, err_str_len);
				printf("Continue with error... %s!\n", err_str);
			}
			break;
		case FAILURE:
			printf("Failed...!\n");
			break;
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