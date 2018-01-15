CPP  = cl.exe
LIB  = lib.exe
LINK = link.exe

LIBCPPFLAGS = /GS /GL /W3 /Gy /Zc:wchar_t /I"%INSTANT_CLIENT_LIB_PATH%\sdk\include" /Zi /Gm- /O2 /Zc:inline /fp:precise /D "WIN32" /D "_LIB" /D "__WIN32__" /D "_UNICODE" /D "UNICODE" /errorReport:prompt /WX- /Zc:forScope /Gd /Oi /MT /EHsc /nologo /diagnostics:classic

LIBFLAGS=/LTCG "oci.lib" /MACHINE:X64 /NODEFAULTLIB:"libcmt.lib" /NODEFAULTLIB:"msvcrt.lib" /NOLOGO /LIBPATH:"%INSTANT_CLIENT_LIB_PATH%\sdk\lib\msvc"

CPPFLAGS = /GS- /GL /W3 /Gy /Zc:wchar_t /I"%ERL_INTERFACE_DIR%\include" /I"%INSTANT_CLIENT_LIB_PATH%\sdk\include" /I"C:\projects\git\K2InformaticsGmbH\erloci\c_src\erloci_drv\..\erloci_lib" /Zi /Gm- /O2 /Zc:inline /fp:precise /D "_WIN32" /D "__WIN32__" /D "WIN32" /D "_CONSOLE" /D "_CRT_SECURE_NO_WARNINGS" /D "_SCL_SECURE_NO_WARNINGS" /D "_UNICODE" /D "UNICODE" /errorReport:prompt /WX- /Zc:forScope /Gd /Oi /MT /EHsc /nologo /diagnostics:classic

LINKFLAGS = /MANIFEST /LTCG:incremental /NXCOMPAT /DYNAMICBASE "ei_md.lib" "erl_interface_md.lib" "legacy_stdio_definitions.lib" "msvcrt.lib" "libcmt.lib" /DEBUG /MACHINE:X64 /OPT:REF /INCREMENTAL:NO /SUBSYSTEM:CONSOLE /MANIFESTUAC:"level='asInvoker' uiAccess='false'" /OPT:ICF /ERRORREPORT:PROMPT /NOLOGO /LIBPATH:"C:\Program Files (x86)\Microsoft Visual Studio\2017\Community\VC\Tools\MSVC\14.11.25503\lib\x64" /LIBPATH:"%ERL_INTERFACE_DIR%\lib" /TLBID:1

# "kernel32.lib" "user32.lib" "gdi32.lib" "winspool.lib" "comdlg32.lib" "advapi32.lib" "shell32.lib" "ole32.lib" "oleaut32.lib" "uuid.lib" "odbc32.lib" "odbccp32.lib" "legacy_stdio_definitions.lib" "msvcrt.lib" "libcmt.lib" 

all: erloci.lib erloci.drv

erloci.drv: priv\transcoder.obj priv\threads.obj priv\term.obj priv\port.obj priv\marshal.obj priv\logger.obj priv\erloci.obj priv\cmd_queue.obj priv\command.obj priv\erloci.lib
	$(LINK) $(LINKFLAGS) /OUT:"ocierl.exe" priv\transcoder.obj priv\threads.obj priv\term.obj priv\port.obj priv\marshal.obj priv\logger.obj priv\erloci.obj priv\cmd_queue.obj priv\command.obj priv\erloci.lib

erloci.lib: priv\ocisession.obj priv\ocistmt.obj
	$(LIB) $(LIBFLAGS) /OUT:"priv/erloci.lib" priv/ocisession.obj priv/ocistmt.obj

{erloci_lib\}.cpp{priv\}.obj:
	$(CPP) $(LIBCPPFLAGS) -c $< -Fo:priv/

{erloci_drv\}.cpp{priv\}.obj:
	$(CPP) $(CPPFLAGS) -c $< -Fo:priv/

