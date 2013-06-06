// occidemo.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"

#include <iostream>
#include <occi.h>

using namespace std;
using namespace oracle::occi;

int _tmain(int argc, _TCHAR* argv[])
{
	const char* userName = "bikram";
	const char* password = "abcd123";
	const char* connectString = "(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=tcp)(HOST=80.67.144.206)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=XE)))";
	const char* sql = "SELECT * FROM ALL_TABLES";
    int i = 0;
    int rows = 0;

	try {
		Environment *env = Environment::createEnvironment(Environment::THREADED_MUTEXED);
		cout << "createEnvironment" << endl;
		Connection *conn = env->createConnection((char*)userName, (char*)password, (char*)connectString);
		cout << "  createConnection" << endl;

        for (i=100; i>0; --i) {
		    Statement *stmt = conn->createStatement(sql);
			cout << "    createStatement" << endl;

		    ResultSet *rs = stmt->executeQuery();
			cout << "      executeQuery" << endl;

#if 0
			vector<MetaData> listOfColumns = rs->getColumnListMetaData();
			cout << "        getColumnListMetaData" << endl;
            cout << i << " columns " << listOfColumns.size() << endl;
            for (i=0; i < listOfColumns.size(); i++) {
                MetaData columnObj=listOfColumns[i];
                //cout << (columnObj.getString(MetaData::ATTR_NAME))
                //     << "(" << (columnObj.getInt(MetaData::ATTR_DATA_TYPE)) << "), ";
            }
#endif
			rows = 0;
            while(rs->next()) {
                ++rows;
                /*for (i=0; i < listOfColumns.size(); i++)
                    string str = rs->getString(i+1);
                    //cout << rs->getString(j+1) << ", ";
                //cout << endl;*/
            }
            cout << "      itr " << i << " rows " << rows << endl;

			stmt->closeResultSet(rs);
			cout << "      closeResultSet" << endl;

			conn->terminateStatement(stmt);
			cout << "    terminateStatement" << endl;
        }

		env->terminateConnection(conn);
		cout << "  terminateConnection" << endl;

		Environment::terminateEnvironment(env);
		cout << "terminateEnvironment" << endl;

	} catch (std::exception const & ex) {
		cout << ex.what() << endl;
	} catch (std::string const & ex) {
		cout << ex << endl;
	} catch (...) {
		// ...
	}

	return 0;
}