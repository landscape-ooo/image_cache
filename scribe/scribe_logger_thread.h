#ifndef _GANJI_PIC_SERVER_SCRIBE_SCRIBE_LOGGER_THREAD_H_
#define _GANJI_PIC_SERVER_SCRIBE_SCRIBE_LOGGER_THREAD_H_
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string>
#include <vector>
#include <boost/shared_ptr.hpp>
#include <transport/TBufferTransports.h>
#include <transport/TSocket.h>
#include <transport/TTransport.h>
#include <protocol/TProtocol.h>
#include <protocol/TBinaryProtocol.h>
#include "scribe/gen-cpp/scribe.h"
#include "scribe/scribe_log.h"
#include "mutex.h"
using ganji::pic_server::Mutex;
using std::string;
using std::vector;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using boost::shared_ptr;
using namespace scribe::thrift;
namespace ganji { namespace pic_server { namespace scribe {
class ScribeLoggerThread {
 public:
  ScribeLoggerThread(const string &host, int port, uint32_t level,
               int connect_timeout, int send_timeout,
               int recv_timeout, int interval = 10)
      : host_(host),
        port_(port),
        level_(level),
        connect_timeout_(connect_timeout),
        send_timeout_(send_timeout),
        recv_timeout_(recv_timeout),
        interval_(interval) {
          mask_ = 0;
          stop_ = false;
          current_ = 0;
        }
  int Create();
  int Destroy();
  int Open();
  bool IsOpen() {
    return transport_->isOpen();
  }
  void Fatal(const char *category, const char *fmt, ...);
  void Warning(const char *category, const char *fmt, ...);
  void Notice(const char *category, const char *fmt, ...);
  void Trace(const char *category, const char *fmt, ...);
  void Debug(const char *category, const char *fmt, ...);
  void Access(const char *category, const char *fmt, ...);

  // return old mask
  uint32_t SetMask(uint32_t mask) {
    uint32_t mask_old = mask_;
    mask_ |= mask;
    return mask_old;
  }

  static void* DoLogThread(void *args);
 private:
  void DoWriteLog(const char *category, const char *prefix,
                  const char *fmt, va_list args);
  void DoLogLoop();
  int Reopen() {
    return Open();
  }
  string host_;
  int port_;
  uint32_t level_;
  int connect_timeout_;
  int send_timeout_;
  int recv_timeout_;
  int interval_;
  uint32_t mask_;
  shared_ptr<TSocket> socket_;
  shared_ptr<TFramedTransport> transport_;
  shared_ptr<TProtocol> protocol_;
  shared_ptr<scribeClient> client_;

  pthread_t tid_;
  vector<LogEntry> buf_[2];
  Mutex buf_mutex_;
  int current_;

  bool stop_;
};
} } }  // namespace ganji::pic_server::scribe
#endif
