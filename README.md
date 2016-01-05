# erloci - An Erlang driver for the Oracle Call Interface

### Users
<a href="http://www.k2informatics.ch/">
  <img src="http://www.k2informatics.ch/logo.gif" alt="K2 Informatics GmbH">
</a>
<a href="http://privatbank.ua/">
  <img src="http://privatbank.ua/img/logo.png?v=2828" alt="Privat Bank, Ukraine">
</a>

### Setup the development system
Create a environment variable OTPROOT pointing to erlang installation directory,
e.g. - in linux (if installed from RPM) its usually /usr/lib/erlang.
Download from [Oracle](http://www.oracle.com/technetwork/database/features/instant-client/index-097480.html) the following libraries (for matching os and platfrom for the development system)
  1. instantclient-basic
  2. instantclient-sdk

### Windows
Unzip both into a directory and create the following enviroment variable
E.g. - if your instant client library version is 12.1 and you have unzipped 'instantclient-basic-windows*.zip' to C:\Oracle\instantclient\instantclient_12_1 then the sdk should be at C:\Oracle\instantclient\instantclient_12_1\sdk\
The include headers will be at C:\Oracle\instantclient\instantclient_12_1\sdk\include and static libraries at C:\Oracle\instantclient\instantclient_12_1\sdk\lib\msvc (note the path for VS project configuration later)

### Linux / Mac OS X
Required system libraries
```
libevent
libevent-devel
```
Use rpms (recomended) to install basic and sdk. The default install path is usually (for x86_64 architecture)
For Mac you may use Homebrew (http://brew.sh) as package manager to install it.
```
OCI Headers     : /usr/include/oracle/12.1/client64/
OCI Libraries   : /usr/lib/oracle/12.1/client64/lib/
```

### Create Environment variables
```
INSTANT_CLIENT_LIB_PATH     = path to oci libraries
INSTANT_CLIENT_INCLUDE_PATH = path to oci headers
ERL_INTERFACE_DIR           = path to erl_interface or erlang installation
```

For example:
```
(x64 Fedora)
INSTANT_CLIENT_LIB_PATH     = /usr/lib/oracle/12.1/client64/lib/
INSTANT_CLIENT_INCLUDE_PATH = /usr/include/oracle/12.1/client64/
ERL_INTERFACE_DIR           = /usr/lib64/erlang/lib/erl_interface-3.7.15


(x64 Windows 7)
INSTANT_CLIENT_LIB_PATH     = C:\Oracle\instantclient\instantclient_12_1\
ERL_INTERFACE_DIR           = C:\Program Files\erlang\erl5.10.4\lib\erl_interface-3.7.15
```

### Ubuntu (14.04.2 LTS 'trusty')
```
sudo apt-get install libevent-dev
sudo apt-get install alien dpkg-dev debhelper build-essential
sudo alien oracle-instantclient12.1-basic-12.1.0.2.0-1.x86_64.rpm
sudo alien oracle-instantclient12.1-devel-12.1.0.2.0-1.x86_64.rpm
sudo dpkg -i oracle-instantclient12.1-basic_12.1.0.2.0-2_amd64.deb 
sudo dpkg -i oracle-instantclient12.1-devel_12.1.0.2.0-2_amd64.deb

/etc/environment
INSTANT_CLIENT_LIB_PATH="/usr/lib/oracle/12.1/client64/lib/"
INSTANT_CLIENT_INCLUDE_PATH="/usr/include/oracle/12.1/client64/"
ERL_INTERFACE_DIR="/usr/lib/erlang/lib/erl_interface-3.7.20"
```

### Compiling
We assume you have [rebar](https://github.com/basho/rebar) somewhere on your path. Rebar will take care of the Erlang and C++ sources.
<code>rebar compile</code>
Please check the rebar manual for how to add erloci as a dependency to your project.

#### Compiling C++ port in visual studio (2008)
Change/Add the following:
  * In project properties of erloci_drv and erloci_lib 
    * Configuration Properties -> C/C++ -> General -> Additional Include Directories: path-to-instant-client\sdk\include
    * Configuration Properties -> C/C++ -> General -> Additional Include Directories: path-to-instant-client\sdk\include
  * In project property of erloci_lib 
    * Configuration Properties -> Librarian -> General -> Additional Library Directories: path-to-instant-client\sdk\lib\msvc
    * Configuration Properties -> Librarian -> General -> Additional Dependencies: oraocciXX.lib (replace XX with matching file in path)

#### Compiling C++ port in visual studio (2013)

This is experimental at the moment, a patch need to be applied to update the VS project files in order to make them use the 2013 toolchain:

```
~/erloci $ git apply misc/vs2013.patch
```

Issue `rebar co` as usual; then don't forget to revert temporarily changed vcxproj files: `git reset --hard`.

### 3rd party dependencies
#### Threadpool 
The threadpool code (threadpool.cpp/h) is developed by Mathias Brossard mathias@brossard.org. His threadpool library is hosted at https://github.com/mbrossard/threadpool.
This library is unused (not linked) in a Windows environment. For an easier installation process we include the required threadpool files in the erloci repo. So this is NOT a dependency you have to resolve by yourself.

#### Oracle Call Interface (OCI)
OCI provides a high performance, native 'C' language based interface to the Oracle Database. There is no ODBC layer between your application and the database. Since we don't want to distribute the Oracle Code you MUST download the OCI Packages (basic and devel) from the Oracle Website: http://www.oracle.com/technetwork/database/features/instant-client/index-097480.html.

#### Compile ERLOCI in Windows
Make sure you have <code>vcbuild.exe</code> in path. After that <code>rebar compile</code> will take care the rest. Currently erloci can only be build with VS2008.

### Erlang to/from Oracle datatype mapping (currently)

```
oracle erlang
--------------
SQLT_INT            integer()
SQLT_CHR,SQLT_AFC   binary()
SQLT_FLT            float()
SQLT_IBDOUBLE       float()
SQLT_BIN            binary()
SQLT_DAT            binary()
SQLT_TIMESTAMP      binary()
SQLT_TIMESTAMP_LTZ  binary()
SQLT_INTERVAL_YM    binary()
SQLT_INTERVAL_DS    binary()
SQLT_IBFLOAT        float()
--------------
```

### Eunit test
The Oracle connection information are taken from erloci.app.src. Please change it to point to your database before executing the steps below:
  1. <code>rebar compile</code>
  2. <code>rebar eunit</code>

### CHANGE LOG
#### 0.0.2
1. STL term class for wrapping erlang term
2. Native process redesigned to OO
2. Support Variable binding for Input
3. Concurrent connection and statements
4. Common test for load testing

### Work-In-Progess
1. Testing and stabilization
2. Wiki

### TODOs
1. In/Out bind variables and arrays
