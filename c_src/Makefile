CXXFLAGS := -Ic_src/lib/instantclient/include

priv/liberloci.a: c_src/erloci_lib/ocisession.o c_src/erloci_lib/ocistmt.o c_src/erloci_lib/oci_lib_intf.o
	ar rcs priv/liberloci.a c_src/erloci_lib/oci_lib_intf.o c_src/erloci_lib/ocisession.o c_src/erloci_lib/ocistmt.o

ocisession.o: c_src/erloci_lib/ocisession.cpp c_src/erloci_lib/ocisession.h
	g++ $(CXXFLAGS) -Ic_src/erloci_lib -c c_src/erloci_lib/ocisession.cpp

ocistmt.o: c_src/erloci_lib/ocistmt.cpp c_src/erloci_lib/ocistmt.h
	g++ $(CXXFLAGS) -Ic_src/erloci_lib -c c_src/erloci_lib/ocistmt.cpp

clean:
	rm -rf c_src/erloci_drv/*.o
	rm -rf c_src/erloci_lib/*.o
	rm -rf priv/*.a
