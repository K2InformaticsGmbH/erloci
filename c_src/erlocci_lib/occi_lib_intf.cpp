#include <iostream>
#include "occi_lib_intf.h"

#include <occi.h>

using namespace std;
using namespace oracle::occi;

int main(void)
{
	const char* userName = "bikram";
	const char* password = "abcd123";
	const char* connectString = "(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=tcp)(HOST=80.67.144.206)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=XE)))";
	const char* sql = "SELECT * FROM ALL_TABLES";
    int i = 0;

	try {
		Environment *env = Environment::createEnvironment(Environment::THREADED_MUTEXED);
		Connection *conn = env->createConnection((char*)userName, (char*)password, (char*)connectString);

        for (i=1000; i>0; --i) {
            int rows = 0;
		    Statement *stmt = conn->createStatement(sql);
		    ResultSet *rs = stmt->executeQuery();
            vector<MetaData> listOfColumns = rs->getColumnListMetaData();
            cout << i << " columns " << listOfColumns.size() << endl;
            for (i=0; i < listOfColumns.size(); i++) {
                MetaData columnObj=listOfColumns[i];
                //cout << (columnObj.getString(MetaData::ATTR_NAME))
                //     << "(" << (columnObj.getInt(MetaData::ATTR_DATA_TYPE)) << "), ";
            }
            while(rs->next()) {
                ++rows;
                for (i=0; i < listOfColumns.size(); i++)
                    string str = rs->getString(i+1);
                    //cout << rs->getString(j+1) << ", ";
                //cout << endl;
            }
            cout << i << " rows " << rows << endl;
            cout << "----------------------" << endl;
		    rs->next();
		    stmt->closeResultSet(rs);
		    conn->terminateStatement(stmt);
        }

		env->terminateConnection(conn);
		Environment::terminateEnvironment(env);
		cout << "success!!!!" << endl;
	} catch (std::exception const & ex) {
		cout << ex.what() << endl;
	} catch (std::string const & ex) {
		cout << ex << endl;
	} catch (...) {
		// ...
	}

    return 0;
}
