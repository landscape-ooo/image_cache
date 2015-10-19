
DEV_PATH_ROOT=../../../..
LIB_PATH=$(DEV_PATH_ROOT)/lib
INCLUDE_PATH=$(DEV_PATH_ROOT)/include
BIN_PATH=$(DEV_PATH_ROOT)/bin/ganji

THRIFT_HOME = /usr/local/webserver/thrift

CP=/bin/cp -f
RM=/bin/rm -f
MV=/bin/mv -f
MKDIR=/bin/mkdir -p
RANLIB=ranlib

CC=g++ -ggdb -L$(LIB_PATH) -L$(THRIFT_HOME)/lib -L/usr/lib64 -L/usr/local/webserver/libevent/lib
INCLUDE=-I/usr/include -I/usr/local/webserver/mysql/include -I$(INCLUDE_PATH) -I$(THRIFT_HOME)/include/thrift -I/usr/local/webserver/libevent/include

CPPFLAGS = -D_LINUX_
LIB = libbpt.a
OBJS = disk.o lrucache.o

all: $(LIB)
	rm -rf $(OBJS)

$(LIB): $(OBJS)
	ar cr $@ $(OBJS)
	$(RANLIB) $@

clean:
	rm -f $(LIB) $(OBJS)
	rm -rf unittest

%.o : %.cpp
	$(CC) $(CPPFLAGS) $(INCLUDE) -c $< -o $@ $(INC_PATH)
%.o : %.c
	$(CC) $(CPPFLAGS) $(INCLUDE) -c $< -o $@ $(INC_PATH)
%.o : %.cc
	$(CC) $(CPPFLAGS) $(INCLUDE) -c $< -o $@ $(INC_PATH)

test:
	$(CC) $(CPPFLAGS) $(INCLUDE) unittest.cpp -o unittest $(INC_PATH) $(LIB_PATH) -lbpt
