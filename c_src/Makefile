EXE_TARGET = ocierl
LIB_TARGET = liberloci.a

ifeq "$(wildcard c_src)" ""
	SRC_PATH_PREFIX = 
	PRIV_DIR = ../priv
else
	SRC_PATH_PREFIX = c_src/
	PRIV_DIR = priv
endif

ERLOCI_PATH = $(SRC_PATH_PREFIX)erloci_drv
ERLOCI_LIB_PATH = $(SRC_PATH_PREFIX)erloci_lib
OCI_INCLUDE_PATH = $(INSTANT_CLIENT_INCLUDE_PATH)
OCI_LIB_PATH = $(INSTANT_CLIENT_LIB_PATH)

ERLOCI_LIB_HDRS = $(wildcard $(addprefix $(ERLOCI_LIB_PATH)/, *.h))
ERLOCI_LIB_SRCS = $(wildcard $(addprefix $(ERLOCI_LIB_PATH)/, *.cpp))
ERLOCI_LIB_OBJS = $(ERLOCI_LIB_SRCS:.cpp=.o)

ERLOCI_HDRS = $(wildcard $(addprefix $(ERLOCI_PATH)/, *.h))
ERLOCI_SRCS = $(wildcard $(addprefix $(ERLOCI_PATH)/, *.cpp))
ERLOCI_OBJS = $(ERLOCI_SRCS:.cpp=.o)

CXXFLAGS = -ggdb -Wall -I$(ERL_INTERFACE_DIR)/include  -I$(ERLOCI_PATH) -I$(ERLOCI_LIB_PATH) -I$(OCI_INCLUDE_PATH)
LINKDIRS = -L$(ERL_INTERFACE_DIR)/lib -L$(PRIV_DIR) -L$(OCI_LIB_PATH)
LINKFLAGS = -levent -lpthread -lerl_interface -lei -lerloci -locci -lons -lclntshcore -lclntsh -lnnz12 -lipc1 -lmql1

all: $(PRIV_DIR)/$(EXE_TARGET)
	cp -L $(OCI_LIB_PATH)*.so* $(PRIV_DIR)/
	@echo "erloci compiled!!!"

$(PRIV_DIR)/$(EXE_TARGET): $(PRIV_DIR)/$(LIB_TARGET) $(ERLOCI_OBJS)
	g++ -Wall $(LINKDIRS) $^ $(LINKFLAGS) -o $@

$(PRIV_DIR)/$(LIB_TARGET): $(ERLOCI_LIB_OBJS)
	ar rcs $(PRIV_DIR)/$(LIB_TARGET) $(ERLOCI_LIB_OBJS)

$(ERLOCI_LIB_SRCS): $(ERLOCI_LIB_HDRS)
	touch $(ERLOCI_LIB_SRCS)

$(ERLOCI_SRCS): $(ERLOCI_HDRS) $(ERLOCI_LIB_HDRS)
	touch $(ERLOCI_LIB_SRCS)
	touch $(ERLOCI_SRCS)

clean:
	rm -rf $(ERLOCI_OBJS)
	rm -rf $(ERLOCI_LIB_OBJS)
	rm -rf $(PRIV_DIR)/$(LIB_TARGET)
	rm -rf $(PRIV_DIR)/$(EXE_TARGET)
