# erloci - An Erlang wrapper for the Oracle Call Interface

### Setup the development system
Download from [Oracle](http://www.oracle.com/technetwork/database/features/instant-client/index-097480.html) the following libraries (for correct target platfrom)
1. instantclient-basic
2. instantclient-sdk

Unzip and copy them into following directory structure
##### Linux directory structure
```
erloci
├── c_src
│   ├── lib
│   │   ├── instantclient
│   │   │   ├── libclntshcore.so -> libclntshcore.so.12.1
│   │   │   ├── libclntshcore.so.12.1
│   │   │   ├── libclntsh.so -> libclntsh.so.12.1
│   │   │   ├── libclntsh.so.12.1
│   │   │   ├── libocci.so -> libocci.so.12.1
│   │   │   ├── libocci.so.12.1
│   │   │   ├── libocci.so
│   │   │   ├── sdk
│   │   │   │   ├── include
│   │   │   │   │   ├── *.h     (header files from sdk/include)
```

##### Windows directory structure
```
erloci
├── c_src
│   ├── lib
│   │   ├── instantclient
│   │   │   ├── oci.lib
│   │   │   ├── oraocci11.lib
│   │   │   ├── sdk
│   │   │   │   ├── include
│   │   │   │   │   ├── *.h     (header files from sdk/include)
```

##### Mac OSX directory structure (currently not supported)
```
erloci
├── c_src
│   ├── lib
│   │   ├── instantclient
│   │   │   ├── libclntsh.dylib.11.1
│   │   │   ├── libnnz11.dylib
│   │   │   ├── libocci.dylib.11.1
│   │   │   ├── libociicus.dylib
│   │   │   ├── libocijdbc11.dylib
│   │   │   ├── sdk
│   │   │   │   ├── include
│   │   │   │   │   ├── *.h     (header files from sdk/include)
```


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

#### Compile ERLOCI in Windows
Navigate to the Git repo path of ERLOCI from a Visual Studio Command Prompt and run the following:
<code>rebar.bat compile -C rebar.config.win</code>

Please refer to [rebar](https://github.com/basho/rebar) for further details on how to build <code>rebar</code> or <code>rebar.bat</code> in Windows.

### Tests
A basic unit test can be executed after a succesful compile as follows:

1. Change the TNS string, user name and password in erloci.app.src (default_connect_param) to point to a oracle database.
2. <code>rebar compile</code>
3. <code>$ ./start.sh</code>

<code>start.sh</code> esecutes oci_port:run(1,1) in a erlang VM console. The test creates a table erloci_table_1, inserts a row and reads back the same row.
To do some performance tests, change the values in run(Threads, InsertCount) (Threads: number of erloci_table_* tables created and accessed in parallel, InsertCount: Number of rows to insert and read back).

### Work-In-Progess
1. Support Variable binding for Input

### TODOs
1. More test cases
2. In/Out bind variables and arrays
