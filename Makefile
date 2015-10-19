MODNAME=pic_server_fcgi
GANJIPATH=../cc_dev/trunk
CP=/bin/cp -f
RM=/bin/rm -f
MV=/bin/mv -f
MKDIR=/bin/mkdir -p
LIBS=$(GANJIPATH)/lib
INCLUDE_PATH=$(GANJIPATH)/include

MY_LOAD=./deps/leveldb/leveldb-1.14.0/libleveldb.a

LOG_REPLAY=0
ifeq ($(LOG_REPLAY), 1)
LOG_REPLAY_MACRO=-D_LOG_REPLAY_
else
LOG_REPLAY_MACRO=
endif

CFLAGS = -D_LINUX_ -Wall $(FASTDFS_TYPE) $(LOG_REPLAY_MACRO)
CC=g++ -ggdb $(CFLAGS) -L$(LIBS) -L/usr/lib64 -L/usr/local/lib -L/usr/lib
INCLUDE=-I/usr/include -I/usr/local/webserver/boost_1_44_0/include -I/usr/local/webserver/pcre-8.00/include -I/usr/local/webserver/fcgi/include -I/usr/local/webserver/thrift/include/thrift -I$(INCLUDE_PATH) -I./include -I. -I./deps/leveldb/leveldb-1.14.0/include/
CXXFILES = md5.cc global.cc curl_downloader.cc config.cc env.cc blob_file.cc fcgi_function.cc handler.cc leveldb_cache.cc main.cc myutil.cc scribe/scribe_logger_thread.cc fs/file_os.cc crc32c.cc
CPPFILES = scribe/gen-cpp/FacebookService.cpp scribe/gen-cpp/fb303_constants.cpp scribe/gen-cpp/fb303_types.cpp scribe/gen-cpp/scribe_constants.cpp scribe/gen-cpp/scribe.cpp scribe/gen-cpp/scribe_types.cpp

STATIC_LIBS=/usr/local/webserver/fcgi/lib/libfcgi.a /usr/local/webserver/fcgi/lib/libfcgi++.a /usr/local/webserver/thrift/lib/libthrift.a /usr/local/webserver/pcre-8.00/lib/libpcre.a $(LIBS)/libganji_util.a $(MY_LOAD)
CCOBJS = $(CXXFILES:.cc=.o)
CPPOBJS = $(CPPFILES:.cpp=.o)
OBJS = $(CCOBJS) $(CPPOBJS)
TARGET = $(MODNAME)

all: preexec $(TARGET) afterexec

.SUFFIXES: .o .cc .cpp
.cc.o:
	$(CC) -c $(INCLUDE) $< -o $@
.cpp.o:
	$(CC) -c $(INCLUDE) $< -o $@

.PHONY: preexec
preexec:
	#touch main.cc 

.PHONY: afterexec
afterexec:
	$(RM) *~ *.swp

.PHONY: clean
clean:
	$(RM) $(OBJS) $(TARGET)
	$(RM) core.*

$(TARGET):$(OBJS)
	$(CC) $(CFLAGS) $^ -lpthread -lz -ldl -lcurl -o $@ $(STATIC_LIBS)
