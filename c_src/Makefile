ERLOCI_PATH = c_src/erloci
ERLOCI_LIB_PATH = c_src/erloci_lib
OCI_DIR = c_src/lib/instantclient
OCI_INCLUDE_PATH = $(INSTANT_CLIENT_INCLUDE_PATH)
OCI_LIB_PATH = $(INSTANT_CLIENT_LIB_PATH)
ERL_INTERFACE_DIR = $(OTPROOT)/lib/erl_interface-3.7.11

ERLOCI_LIB_SRCS = 	$(ERLOCI_LIB_PATH)/ocisession.cpp	\
				  	$(ERLOCI_LIB_PATH)/ocistmt.cpp		\
					$(ERLOCI_LIB_PATH)/oci_lib_intf.cpp

ERLOCI_LIB_OBJS =	$(ERLOCI_LIB_SRCS:.cpp=.o)

ERLOCI_DRV_SRCS =	$(ERLOCI_PATH)/command.cpp			\
					$(ERLOCI_PATH)/cmd_thread_pool.cpp 	\
					$(ERLOCI_PATH)/erloci.cpp 			\
					$(ERLOCI_PATH)/oci_logger.cpp 		\
					$(ERLOCI_PATH)/oci_marshal.cpp 		\
					$(ERLOCI_PATH)/threadpool.cpp

ERLOCI_DRV_OBJS = $(ERLOCI_DRV_SRCS:.cpp=.o)

CXXFLAGS = -ggdb -Wall -I$(ERL_INTERFACE_DIR)/include  -I$(ERLOCI_PATH) -I$(ERLOCI_LIB_PATH) -I$(OCI_INCLUDE_PATH)
LINKDIRS = -L$(ERL_INTERFACE_DIR)/lib -Lpriv -L$(OCI_LIB_PATH)
LINKFLAGS = -levent -lpthread -lerl_interface -lei -lerloci -locci -lons -lclntshcore -lclntsh -lnnz12

all: priv/erloci
	@echo "erloci compiled"

priv/erloci: priv/liberloci.a $(ERLOCI_DRV_OBJS)
	g++ -Wall $(LINKDIRS) $^ $(LINKFLAGS) -o $@

.cpp.o:
	g++ $(CXXFLAGS) -c $< -o $@

# Compiling the common library
$(ERLOCI_LIB_OBJS): %.o : %.h

priv/liberloci.a: $(ERLOCI_LIB_OBJS)
	ar rcs priv/liberloci.a $(ERLOCI_LIB_OBJS)

# Compiling the driver
$(ERLOCI_PATH)/threadpool.cpp : $(ERLOCI_PATH)/threadpool.h
$(ERLOCI_PATH)/erloci.cpp : $(ERLOCI_PATH)/command.h $(ERLOCI_PATH)/oci_marshal.h
$(ERLOCI_PATH)/oci_marshal.cpp : $(ERLOCI_PATH)/oci_marshal.h
$(ERLOCI_PATH)/command.cpp : $(ERLOCI_PATH)/command.h $(ERLOCI_PATH)/oci_marshal.h $(ERLOCI_LIB_PATH)/ocisession.h $(ERLOCI_LIB_PATH)/ocistmt.h $(ERLOCI_LIB_PATH)/oci_lib_intf.h
$(ERLOCI_PATH)/cmd_thread_pool.cpp : $(ERLOCI_PATH)/command.h $(ERLOCI_PATH)/oci_marshal.h $(ERLOCI_PATH)/threadpool.h

clean:
	rm -rf $(ERLOCI_PATH)/*.o
	rm -rf $(ERLOCI_LIB_PATH)/*.o
	rm -rf priv/*.a
	rm -rf priv/erloci
