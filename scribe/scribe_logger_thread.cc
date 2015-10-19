#include <stdint.h>
#include <stdarg.h>
#include <time.h>
#include <vector>
#include "ulog.h"
#include "scribe/scribe_logger_thread.h"
#include "myutil.h"
using std::vector;
namespace Util = ganji::pic_server::myutil;
namespace ganji { namespace pic_server { namespace scribe {
int ScribeLoggerThread::Open() {
  socket_ = shared_ptr<TSocket>(new TSocket(host_.c_str(), port_));
  transport_ = shared_ptr<TFramedTransport>(new TFramedTransport(socket_));
  protocol_ = shared_ptr<TProtocol>(new TBinaryProtocol(transport_));
  client_ = shared_ptr<scribeClient>(new scribeClient(protocol_));
  try {
    socket_->setNoDelay(true);
    socket_->setConnTimeout(connect_timeout_);
    socket_->setSendTimeout(send_timeout_);
    socket_->setRecvTimeout(recv_timeout_);
    transport_->open();
  } catch (TException &excp) {
    fprintf(stderr, "transport open failed. %s\n", excp.what());
    return -1;
  }
  return 0;
}

int ScribeLoggerThread::Destroy() {
  stop_ = true;
  pthread_join(tid_, NULL);
  return 0;
}

int ScribeLoggerThread::Create() {
  // open thrift socket, create do log thread
  if (Open() != 0) {
    return -1;
  }
  int ret = pthread_create(&tid_, NULL, DoLogThread, (void*)this);
  if (ret != 0) {
    return -1;
  }
  if (pthread_detach(tid_) != 0) {
    return -1;
  }
  return 0;
}

inline void ScribeLoggerThread::DoWriteLog(const char *category,
                                    const char *prefix,
                                    const char *fmt,
                                    va_list args) {
  char fmt_new[1024];
  char buf[65536];
  struct tm vtm;
  time_t tt;
  ::time(&tt);
  localtime_r(&tt, &vtm);
  snprintf(fmt_new, sizeof(fmt_new),
           "%s%04d-%02d-%02d %02d:%02d:%02d %s\n",
           prefix, vtm.tm_year + 1900, vtm.tm_mon + 1, vtm.tm_mday,
           vtm.tm_hour, vtm.tm_min, vtm.tm_sec, fmt);
  vsnprintf(buf, sizeof(buf), fmt_new, args);
  LogEntry entry;
  entry.category = category;
  entry.message = buf;
  MutexGuard l(&buf_mutex_);
  buf_[current_].push_back(entry);
}

void ScribeLoggerThread::Fatal(const char *category, const char *fmt, ...) {
  if (!stop_ && !(kScribeLogFatal & mask_) && level_ <= kScribeLogFatal) {
    va_list args;
    va_start(args, fmt);
    DoWriteLog(category, "FATAL: ", fmt, args);
    va_end(args);
  }
}

void ScribeLoggerThread::Warning(const char *category, const char *fmt, ...) {
  if (!stop_ && !(kScribeLogWarning & mask_) && level_ <= kScribeLogWarning) {
    va_list args;
    va_start(args, fmt);
    DoWriteLog(category, "WARNING: ", fmt, args);
    va_end(args);
  }
}

void ScribeLoggerThread::Notice(const char *category, const char *fmt, ...) {
  if (!stop_ && !(kScribeLogNotice & mask_) && level_ <= kScribeLogNotice) {
    va_list args;
    va_start(args, fmt);
    DoWriteLog(category, "NOTICE: ", fmt, args);
    va_end(args);
  }
}

void ScribeLoggerThread::Trace(const char *category, const char *fmt, ...) {
  if (!stop_ && !(kScribeLogTrace & mask_) && level_ <= kScribeLogTrace) {
    va_list args;
    va_start(args, fmt);
    DoWriteLog(category, "TRACE: ", fmt, args);
    va_end(args);
  }
}

void ScribeLoggerThread::Debug(const char *category, const char *fmt, ...) {
  if (!stop_ && !(kScribeLogDebug & mask_) && level_ <= kScribeLogDebug) {
    va_list args;
    va_start(args, fmt);
    DoWriteLog(category, "DEBUG: ", fmt, args);
    va_end(args);
  }
}

void ScribeLoggerThread::Access(const char *category, const char *fmt, ...) {
  if (!stop_ && !(kScribeLogAccess & mask_) && level_ <= kScribeLogAccess) {
    va_list args;
    va_start(args, fmt);
    DoWriteLog(category, "", fmt, args);
    va_end(args);
  }
}

void* ScribeLoggerThread::DoLogThread(void *args) {
  ScribeLoggerThread *scribe_logger_thread = (ScribeLoggerThread*)args;
  scribe_logger_thread->DoLogLoop();
  return NULL;
}

void ScribeLoggerThread::DoLogLoop() {
  while (!stop_) {
    buf_mutex_.Lock();
    int target = current_;
    current_ = (target + 1) % 2;
    buf_mutex_.Unlock();
    if (buf_[target].size() != 0) {
      try {
        client_->Log(buf_[target]);
        buf_[target].clear();
      } catch (TException &excp) {
        Warning("scribe log failed. %s", excp.what());
        Reopen();
      }
    }
    Util::DoSleep(interval_);
  }
  UWarning("scribe logger thread exiting...");
  pthread_exit(NULL);
}

} } }  // namespace ganji::pic_server::scribe
