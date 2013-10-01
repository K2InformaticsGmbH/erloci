ERLOCI_LIB_PATH = c_src/erloci_lib
ERLOCI_DRV_PATH = c_src/erloci_drv
OCI_PATH = c_src/lib/instantclient
CXXFLAGS = -I$(ERLANG_HOME)/lib/erl_interface-3.7.11/include -I$(ERLOCI_DRV_PATH) -I$(ERLOCI_LIB_PATH) -I$(OCI_PATH)/include/

# Compiling the common library
priv/liberloci.a: c_src/erloci_lib/ocisession.o c_src/erloci_lib/ocistmt.o c_src/erloci_lib/oci_lib_intf.o
	ar rcs priv/liberloci.a c_src/erloci_lib/oci_lib_intf.o c_src/erloci_lib/ocisession.o c_src/erloci_lib/ocistmt.o

oci_lib_intf.o: c_src/erloci_lib/oci_lib_intf.cpp c_src/erloci_lib/oci_lib_intf.h
	g++ $(CXXFLAGS) -c c_src/erloci_lib/oci_lib_intf.cpp

ocisession.o: c_src/erloci_lib/ocisession.cpp c_src/erloci_lib/ocisession.h
	g++ $(CXXFLAGS) -c c_src/erloci_lib/ocisession.cpp

ocistmt.o: c_src/erloci_lib/ocistmt.cpp c_src/erloci_lib/ocistmt.h
	g++ $(CXXFLAGS) -c c_src/erloci_lib/ocistmt.cpp

# Compiling the driver
cmd_processors.o: c_src/erloci_drv/cmd_processors.cpp c_src/erloci_drv/cmd_processors.h
	g++ $(CXXFLAGS) -c c_src/erloci_drv/cmd_processors.cpp

#all:;echo "$(PATH) ----- erlang home = $(ERLANG_HOME)"

all: priv/liberloci.a c_src/erloci_drv/cmd_processors.o
	@echo "All compiled"

clean:
	rm -rf c_src/erloci_drv/*.o
	rm -rf c_src/erloci_lib/*.o
	rm -rf priv/*.a
