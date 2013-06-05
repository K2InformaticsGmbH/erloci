CXXFLAGS := -Ic_src/lib/instantclient/include
LINKS := -Lc_src/lib/instantclient -locci -lclntsh -lnnz11 -lpthread

all: priv/erlocci priv/liberloci.a
	@echo "SUCCESS: liberlocci and liberloci are built!"

oci_lib_intf.o: c_src/erloci_lib/oci_lib_intf.cpp
	g++ $(CXXFLAGS) -Ic_src/erloci_lib -c c_src/erloci_lib/oci_lib_intf.cpp

occi_lib_intf.o: c_src/erlocci_lib/occi_lib_intf.cpp
	g++ $(CXXFLAGS) -Ic_src/erlocci_lib -c c_src/erlocci_lib/occi_lib_intf.cpp

priv/liberloci.a: c_src/erloci_lib/oci_lib_intf.o
	ar rcs priv/liberloci.a c_src/erloci_lib/oci_lib_intf.o

priv/erlocci: c_src/erlocci_lib/occi_lib_intf.o
	@echo `pwd`
	g++ $(CXXFLAGS) $(LINKS) -Lpriv c_src/erlocci_lib/occi_lib_intf.o -o priv/erlocci

clean:
	rm -rf c_src/erloci_lib/*.o
	rm -rf c_src/erlocci_lib/*.o
	rm -rf priv/*.a
