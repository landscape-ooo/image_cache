MODNAME=log-replay
GANJIPATH=../../cc_dev/trunk
CP=/bin/cp -f
RM=/bin/rm -f
MV=/bin/mv -f
MKDIR=/bin/mkdir -p
INCLUDE_PATH=$(GANJIPATH)/include

CFLAGS = -D_LINUX_ -Wall $(FASTDFS_TYPE) $(LOG_REPLAY_MACRO) -gstabs+
CC=g++ -ggdb $(CFLAGS) -L$(LIBS) -L/usr/lib64 -L/usr/local/lib -L/usr/lib -L/usr/local/webserver/pcre-8.00/lib
STATIC_LIBS=$(GANJIPATH)/lib/libganji_util.a /usr/local/webserver/pcre-8.00/lib/libpcre.a
INCLUDE=-I/usr/include -I$(INCLUDE_PATH) -I./include -I./../ -I./ -I/usr/local/webserver/pcre-8.00/include
CXXFILES = log_reader.cc replay.cc ../curl_downloader.cc ../myutil.cc ../fs/file_os.cc
CCOBJS = $(CXXFILES:.cc=.o)
OBJS = $(CCOBJS)
TARGET = $(MODNAME)

all: preexec $(TARGET) afterexec

.SUFFIXES: .o .cc
.cc.o:
	#$(CC) -S $(INCLUDE) $<
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
	$(CC) $(CFLAGS) $^ -lpthread -lz -ldl -lpcre -lcurl -o $@ $(STATIC_LIBS)

