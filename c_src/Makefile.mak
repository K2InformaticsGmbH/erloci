all:
	vcbuild /platform:x64 /logcommands c_src/erloci_lib/erloci_lib.vcproj debug
	vcbuild /platform:x64 /logcommands c_src/erloci/erloci.vcproj debug
	cp c_src/priv/*.* priv/

clean:
	vcbuild /clean /logcommands c_src/erloci/erloci.vcproj debug
	vcbuild /clean /logcommands c_src/erloci_lib/erloci_lib.vcproj debug
