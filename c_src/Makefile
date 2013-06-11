CXXFLAGS := -Ic_src/lib/instantclient/include -DOCCI

priv/liberloci.a: c_src/erloci_lib/occi_lib_intf.o c_src/erloci_lib/oci_lib_intf.o
	ar rcs priv/liberloci.a c_src/erloci_lib/oci_lib_intf.o c_src/erloci_lib/occi_lib_intf.o

oci_lib_intf.o: c_src/erloci_lib/oci_lib_intf.cpp c_src/erloci_lib/oci_lib_intf.h
	g++ $(CXXFLAGS) -Ic_src/erloci_lib -c c_src/erloci_lib/oci_lib_intf.cpp

occi_lib_intf.o: c_src/erloci_lib/occi_lib_intf.cpp c_src/erloci_lib/oci_lib_intf.h
	g++ $(CXXFLAGS) -Ic_src/erloci_lib -c c_src/erloci_lib/occi_lib_intf.cpp

clean:
	rm -rf c_src/erloci_drv/*.o
	rm -rf c_src/erloci_lib/*.o
	rm -rf priv/*.a
