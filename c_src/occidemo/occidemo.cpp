// occidemo.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"

#include <iostream>

#ifdef OTL
#define OTL_ORA10G_R2
#define OTL_STL
#include "otlv4.h"
#else
#include <occi.h>
using namespace oracle::occi;
#endif

using namespace std;

int _tmain(int argc, _TCHAR* argv[])
{
#ifndef OTL
	const char* userName = "bikram";
	const char* password = "abcd123";
	const char* connectString = "(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=tcp)(HOST=80.67.144.206)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=XE)))";
	const char* sql = "SELECT * FROM ALL_TABLES";

	try {
		Environment *env = Environment::createEnvironment("UTF8","UTF8",Environment::DEFAULT);

		Connection *conn = env->createConnection((char*)userName, (char*)password, (char*)connectString);
		Statement *stmt = conn->createStatement(sql);
		ResultSet *rs = stmt->executeQuery();
		rs->next();
		stmt->closeResultSet(rs);
		conn->terminateStatement(stmt);
		env->terminateConnection(conn);
		Environment::terminateEnvironment(env);
	} catch (std::exception const & ex) {
		cout << ex.what() << endl;
	} catch (std::string const & ex) {
		cout << ex << endl;
	} catch (...) {
		// ...
	}
#else
	otl_connect db;
	try {
		db.rlogon( "bikram/abcd123@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=tcp)(HOST=80.67.144.206)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=XE)))" );
		db << "truncate table temp_table";
	}
	catch( otl_exception e ) {
        cout << "Program failed\n" << flush;
        cout << e.msg << flush;
        cout << e.stm_text << flush;
    }
    catch(...) {
        cout << "Program failed\n" << flush;
    }

#endif

	return 0;
}