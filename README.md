# erloci - An Erlang wrapper for the Oracle Call Interface

### Compiling
We assume you have [rebar](https://github.com/basho/rebar) somewhere on your path. Rebar will take care of the Erlang and C++ sources.
<code>rebar compile</code>
Please check the rebar manual for how to add erloci as a dependency to your project.

### 3d party dependencies
#### Threadpool 
The threadpool code (threadpool.cpp/h) is developed by Mathias Brossard mathias@brossard.org. His threadpool library is hosted at https://github.com/mbrossard/threadpool.
This library is unused (not linked) in a Windows environment. For an easier installation process we include the required threadpool files in the erloci repo. So this is NOT a dependency you have to resolve by yourself.

#### Oracle Call Interface (OCI)
OCI provides a high performance, native 'C' language based interface to the Oracle Database. There is no ODBC layer between your application and the database. Since we don't want to distribute the Oracle Code you MUST download the OCI Packages (basic and devel) from the Oracle Website: http://www.oracle.com/technetwork/database/features/instant-client/index-097480.html.

In order to successfully compile and link 'erloci' you must set the variables <code>"CXXFLAGS"</code> and <code>"DRV_LDFLAGS"</code> in rebar.config. 

In our case we just created a folder 'lib' containing symlinks pointing to the downloaded libraries and header files.

Download from [Oracle](http://www.oracle.com/technetwork/database/features/instant-client/index-097480.html) the following libraries (for correct target platfrom)

1. instantclient-basic
2. instantclient-sdk

Unzip and copy them to following directory structure
```
erloci
├── c_src
│   ├── lib
│   │   ├── instantclient
│   │   │   ├── libclntsh.* (linux/mac)
│   │   │   ├── libnnz11.*  (linux/mac)
│   │   │   ├── libocci.*   (linux/mac)
│   │   │   ├── libocci.*   (linux/mac)
│   │   │   ├── occi.lib    (windows)
│   │   │   ├── include
│   │   │   │   ├── *.h     (header files from sdk/include)
```

##### Mac OSX directory structure
```
instantclient
├── libclntsh.dylib.11.1
├── libnnz11.dylib
├── libocci.dylib.11.1
├── libociicus.dylib
├── libocijdbc11.dylib
├── sdk
│   ├── include
```

#### Compile ERLOCI in Windows
Navigate to the Git repo path of ERLOCI from a Visual Studio Command Prompt and run the following:
<code>rebar.bat compile -C rebar.config.win</code>

Please refer to [rebar](https://github.com/basho/rebar) for further details on how to build <code>rebar</code> or <code>rebar.bat</code> in Windows.

### Usage
```
{ok, Pool} = oci_session_pool:start_link("127.0.0.1", 1521, {service, "db.local"}, "dbauser", "supersecret",[]).
%% getting a new session from the pool. (parameterized module) 
Session = oci_session_pool:get_session(Pool).

%% execute_sql can execute INSERT, UPDATE, DELETE, and SELECT statements
MaxRowsToReturn = 10.
ok = Session:execute_sql("select * from test_erloci", [], MaxRowsToReturn).

%% in the case of the select, we can fetch MaxRowsToReturn rows at a time.
FirstTenRows = Session:get_rows().

%% as long as we have an 'open' select statement we can get the column information
Columns = Session:get_columns().

%% we can fetch the rows until we get an empty list
%% Session stays open and we can resuse it for further queries otherwise we close the session
Session:close().

%% if we are really done we shutdown the session pool
oci_session_pool:stop(Pool).

```

### TODOs
1. Better session and pool monitoring
2. Log handling
3. Improve testing
