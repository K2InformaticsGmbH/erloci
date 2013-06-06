#include "stdafx.h"
#include "oci_lib_intf.h"

#include "string.h"
#include "stdarg.h"

void append_coldef_to_list(const char * col_name, const char * data_type, const unsigned int max_len, void * list)
{
	printf("\t%s\n", col_name);
}

void string_append(const char * string, int len, void * list)
{
	printf("%.*s\t", len, string);
}

void list_append(const void * sub_list, void * list)
{
	printf("\n");
}

unsigned int sizeof_resp(void * resp)
{
	return 0;
}

int memory_leak_test(const char *tns, const char *usr, const char *pwd, const char *opt)
{
	void * conn_handle = NULL;
	const char * qry = "SELECT * FROM ALL_TABLES";
	void * statement_handle = NULL;
	intf_ret r;

	for(int j = 1; j>0; --j) {
		r = oci_create_tns_seesion_pool(tns, strlen(tns),
										usr, strlen(usr),
										pwd, strlen(pwd),
										opt, strlen(opt));
		if(r.fn_ret != SUCCESS) {
			printf("oci_create_tns_seesion_pool error... %s!\n", r.gerrbuf);
			return -1;
		}

		for(int i = 100; i>0; --i) {
			r = oci_get_session_from_pool(&conn_handle);
			if(r.fn_ret != SUCCESS) {
				printf("oci_get_session_from_pool error... %s!\n", r.gerrbuf);
				return -1;
			}

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
			/*r = oci_close_statement(statement_handle);
			switch(r.fn_ret) {
				case SUCCESS:
					printf("oci_close_statement success...!\n");
					break;
				case CONTINUE_WITH_ERROR:
					printf("oci_close_statement error... %s!\n", r.gerrbuf);
					return -1;
				case FAILURE:
					printf("oci_close_statement failed... %s!\n", r.gerrbuf);
					return -1;
			}//*/
			
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

			r = oci_return_connection_to_pool(conn_handle);
			if(r.fn_ret != SUCCESS) {
				printf("oci_return_connection_to_pool error... %s!\n", r.gerrbuf);
				return -1;
			}
		}
		r = oci_free_session_pool();
		if(r.fn_ret != SUCCESS) {
			printf("oci_create_tns_seesion_pool error... %s!\n", r.gerrbuf);
			return -1;
		}
	}
}

char modqry[1024];
int drop_create_insert_select(const char *tns, const char *usr, const char *pwd, const char *opt, int tid)
{
	void * conn_handle = NULL;
	void * statement_handle = NULL;
	intf_ret r;

	r = oci_create_tns_seesion_pool(tns, strlen(tns),
									usr, strlen(usr),
									pwd, strlen(pwd),
									opt, strlen(opt));
	if(r.fn_ret != SUCCESS) {
		printf("oci_create_tns_seesion_pool error... %s!\n", r.gerrbuf);
		return -1;
	}
	r = oci_get_session_from_pool(&conn_handle);
	if(r.fn_ret != SUCCESS) {
		printf("oci_get_session_from_pool error... %s!\n", r.gerrbuf);
		return -1;
	}

	// Drop can fail so ignoring the errors
	sprintf(modqry, "drop table oci_test_table_%d", tid);
	r = oci_exec_sql(conn_handle, &statement_handle, (const unsigned char *)modqry, strlen(modqry), NULL, NULL, append_coldef_to_list);
	switch(r.fn_ret) {
		case SUCCESS:
			printf("oci_exec_sql success...!\n");
			break;
		case CONTINUE_WITH_ERROR:
			printf("oci_exec_sql error... %s!\n", r.gerrbuf);
			break;
		case FAILURE:
			printf("oci_exec_sql failed... %s!\n", r.gerrbuf);
			break;
	}

	sprintf(modqry, "create table oci_test_table_%d(pkey number,\
                                       publisher varchar2(100),\
                                       rank number,\
                                       hero varchar2(100),\
                                       real varchar2(100),\
                                       votes number,\
                                       votes_first_rank number)", tid);
	r = oci_exec_sql(conn_handle, &statement_handle, (const unsigned char *)modqry, strlen(modqry), NULL, NULL, append_coldef_to_list);
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

	// insert rows
	for(int i=10; i>0; --i) {
		sprintf(modqry, "insert into oci_test_table_%d values (%d,'publisher%d',%d,'hero%d','real%d',%d,%d)", tid, i, i, i, i, i, i, i);
		r = oci_exec_sql(conn_handle, &statement_handle, (const unsigned char *)modqry, strlen(modqry), NULL, NULL, append_coldef_to_list);
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
	}

	// read back rows
	sprintf(modqry, "select * from oci_test_table_%d", tid);
	r = oci_exec_sql(conn_handle, &statement_handle, (const unsigned char *)modqry, strlen(modqry), NULL, NULL, append_coldef_to_list);
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

	r = oci_return_connection_to_pool(conn_handle);
	if(r.fn_ret != SUCCESS) {
		printf("oci_return_connection_to_pool error... %s!\n", r.gerrbuf);
		return -1;
	}

	r = oci_free_session_pool();
	if(r.fn_ret != SUCCESS) {
		printf("oci_create_tns_seesion_pool error... %s!\n", r.gerrbuf);
		return -1;
	}
}

int _tmain(int argc, _TCHAR* argv[])
{
	const char
		*tns = "(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=tcp)(HOST=80.67.144.206)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=XE)))",
		*usr = "bikram",
		*pwd = "abcd123",
		*opt = "";
	intf_ret r;

	// extra create and release pool to stabilize initial memory allocation
	oci_init();

	for(int j = 2; j>0; --j) {
		r = oci_create_tns_seesion_pool(tns, strlen(tns),
										usr, strlen(usr),
										pwd, strlen(pwd),
										opt, strlen(opt));
		if(r.fn_ret != SUCCESS) {
			printf("oci_create_tns_seesion_pool error... %s!\n", r.gerrbuf);
			return -1;
		}
		r = oci_free_session_pool();
		if(r.fn_ret != SUCCESS) {
			printf("oci_create_tns_seesion_pool error... %s!\n", r.gerrbuf);
			return -1;
		}
	}

	// tests for memory leak detection
	int ret;
	//ret = memory_leak_test(tns, usr, pwd, opt);
	ret = drop_create_insert_select(tns, usr, pwd, opt, 0);

	oci_cleanup();

	return ret;
}

bool log_flag = true;
void log_remote(const char *fmt, ...)
{
    va_list arguments;
    va_start(arguments, fmt);

    vprintf(fmt, arguments);

    va_end(arguments);
}