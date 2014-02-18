EXE_TARGET = erloci
LIB_TARGET = liberloci.a

ERLOCI_PATH = c_src/erloci
ERLOCI_LIB_PATH = c_src/erloci_lib
OCI_DIR = c_src/lib/instantclient
OCI_INCLUDE_PATH = $(INSTANT_CLIENT_INCLUDE_PATH)
OCI_LIB_PATH = $(INSTANT_CLIENT_LIB_PATH)
ERL_INTERFACE_DIR = $(OTPROOT)/lib/erl_interface-3.7.11

ERLOCI_LIB_HDRS = $(wildcard $(addprefix $(ERLOCI_LIB_PATH)/, *.h))
ERLOCI_LIB_SRCS = $(wildcard $(addprefix $(ERLOCI_LIB_PATH)/, *.cpp))
ERLOCI_LIB_OBJS = $(ERLOCI_LIB_SRCS:.cpp=.o)

ERLOCI_HDRS = $(wildcard $(addprefix $(ERLOCI_PATH)/, *.h))
ERLOCI_SRCS = $(wildcard $(addprefix $(ERLOCI_PATH)/, *.cpp))
ERLOCI_OBJS = $(ERLOCI_SRCS:.cpp=.o)

CXXFLAGS = -ggdb -Wall -I$(ERL_INTERFACE_DIR)/include  -I$(ERLOCI_PATH) -I$(ERLOCI_LIB_PATH) -I$(OCI_INCLUDE_PATH)
LINKDIRS = -L$(ERL_INTERFACE_DIR)/lib -Lpriv -L$(OCI_LIB_PATH)
LINKFLAGS = -levent -lpthread -lerl_interface -lei -lerloci -locci -lons -lclntshcore -lclntsh -lnnz12

all: priv/$(EXE_TARGET)
	@echo "erloci compiled!!!"

priv/$(EXE_TARGET): priv/$(LIB_TARGET) $(ERLOCI_OBJS)
	g++ -Wall $(LINKDIRS) $^ $(LINKFLAGS) -o $@

priv/$(LIB_TARGET): $(ERLOCI_LIB_OBJS)
	ar rcs priv/$(LIB_TARGET) $(ERLOCI_LIB_OBJS)

$(ERLOCI_LIB_SRCS): $(ERLOCI_LIB_HDRS)
	touch $(ERLOCI_LIB_SRCS)

$(ERLOCI_SRCS): $(ERLOCI_HDRS) $(ERLOCI_LIB_HDRS)
	touch $(ERLOCI_LIB_SRCS)
	touch $(ERLOCI_SRCS)

clean:
	rm -rf $(ERLOCI_OBJS)
	rm -rf $(ERLOCI_LIB_OBJS)
	rm -rf priv/$(LIB_TARGET)
	rm -rf priv/$(EXE_TARGET)
