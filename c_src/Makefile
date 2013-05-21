CXXFLAGS := -Ic_src/lib/instantclient/include -Ic_src/erloci_lib

priv/liberloci.a: c_src/erloci_lib/oci_lib_intf.o
	ar rcs priv/liberloci.a c_src/erloci_lib/oci_lib_intf.o

oci_lib_intf.o: c_src/erloci_lib/oci_lib_intf.cpp
	g++ $(CXXFLAGS) -c c_src/erloci_lib/oci_lib_intf.cpp

clean:
	rm -rf c_src/erloci_lib/oci_lib_intf.o
	rm -rf priv/liberloci.a
