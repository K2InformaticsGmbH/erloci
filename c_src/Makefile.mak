CPP  = cl.exe
LIB  = lib.exe
LINK = link.exe

INSTANT_CLIENT_INCLUDE_PATH = "%INSTANT_CLIENT_LIB_PATH%\sdk\include"

CFLAGS = /GL /W3 /Gy /Zc:wchar_t /Gm- /O2 /Zi /Zc:inline /fp:precise\
	 /errorReport:prompt /WX- /Zc:forScope /Gd /Oi /MT /EHsc /nologo\
	 /diagnostics:classic

DEFS = /D"WIN32" /D"_WIN32" /D"__WIN32__" /D"_CONSOLE" /D"UNICODE" /D"_LIB"\
       /D"_CRT_SECURE_NO_WARNINGS" /D"_SCL_SECURE_NO_WARNINGS" /D"_UNICODE"

LIBCPPFLAGS = $(CFLAGS) /I$(INSTANT_CLIENT_INCLUDE_PATH) $(DEFS)

LIBFLAGS = /LTCG "oci.lib" /MACHINE:X64 /NODEFAULTLIB:"libcmt.lib" /NODEFAULTLIB:"msvcrt.lib" /NOLOGO /LIBPATH:"%INSTANT_CLIENT_LIB_PATH%\sdk\lib\msvc"

DRV_INCLUDES = /I"%ERL_INTERFACE_DIR%\include"\
               /I$(INSTANT_CLIENT_INCLUDE_PATH)\
	       /I"erloci_lib"

DRVCFLAGS = $(CFLAGS) $(DRV_INCLUDES) $(DEFS)

ERL_LIBS = ei_md.lib erl_interface_md.lib
VS_LIBS  = legacy_stdio_definitions.lib msvcrt.lib libcmt.lib
WIN_LIBS = kernel32.lib user32.lib gdi32.lib winspool.lib comdlg32.lib\
	   advapi32.lib shell32.lib ole32.lib oleaut32.lib uuid.lib odbc32.lib\
	   odbccp32.lib

PLATFORM_LIBS = $(ERL_LIBS) $(VS_LIBS) $(WIN_LIBS)
VS_LIB_PATH = "C:\Program Files (x86)\Microsoft Visual Studio\2017\Community\VC\Tools\MSVC\14.11.25503\lib\x64"
WIN_SDK_ROOT = C:\Program Files (x86)\Windows Kits\10\Lib\10.0.16299.0

LINKFLAGS = /LTCG:incremental /NXCOMPAT /DYNAMICBASE /DEBUG /MACHINE:X64\
            /OPT:REF /INCREMENTAL:NO /SUBSYSTEM:CONSOLE /OPT:ICF /NOLOGO\
	    /MANIFESTUAC:"level='asInvoker' uiAccess='false'" /TLBID:1\
	    /ERRORREPORT:PROMPT $(PLATFORM_LIBS)\
	    /LIBPATH:$(VS_LIB_PATH)\
	    /LIBPATH:"$(WIN_SDK_ROOT)\um\x64"\
	    /LIBPATH:"$(WIN_SDK_ROOT)\ucrt\x64"\
	    /LIBPATH:"%ERL_INTERFACE_DIR%\lib"

O = priv
LIBOBJS = $(O)\ocisession.obj $(O)\ocistmt.obj $(O)\checkerror.obj
LIBTARGET = $(O)

all: erloci.lib erloci.drv

erloci.drv: priv\transcoder.obj priv\threads.obj priv\term.obj priv\port.obj priv\marshal.obj priv\logger.obj priv\erloci.obj priv\cmd_queue.obj priv\command.obj priv\erloci.lib
	$(LINK) $(LINKFLAGS) /OUT:"priv/ocierl.exe" priv\transcoder.obj priv\threads.obj priv\term.obj priv\port.obj priv\marshal.obj priv\logger.obj priv\erloci.obj priv\cmd_queue.obj priv\command.obj priv\erloci.lib
	copy "$(INSTANT_CLIENT_LIB_PATH)\oci.dll" 	 priv\oci.dll
	copy "$(INSTANT_CLIENT_LIB_PATH)\oraons.dll" 	 priv\oraons.dll
	copy "$(INSTANT_CLIENT_LIB_PATH)\oraociei12.dll" priv\oraociei12.dll
	del priv\*.obj

erloci.lib: priv\ocisession.obj priv\ocistmt.obj priv\checkerror.obj
	$(LIB) $(LIBFLAGS) /OUT:"priv/erloci.lib" priv/ocisession.obj priv/ocistmt.obj priv\checkerror.obj
	del priv\*.obj

{erloci_lib\}.cpp{priv\}.obj:
	$(CPP) $(LIBCPPFLAGS) -c $< -Fo:priv/

{erloci_drv\}.cpp{priv\}.obj:
	$(CPP) $(DRVCFLAGS) -c $< -Fo:priv/

