/*
 * multithread scribe logger, thread number can be 0
 * round robin
 */
#ifndef _GANJI_PIC_SERVER_SCRIBE_SCRIBE_LOGGER_H_
#define _GANJI_PIC_SERVER_SCRIBE_SCRIBE_LOGGER_H_
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdarg.h>
#include <string>
#include <vector>
#include "scribe_logger_thread.h"
using std::string;
using std::vector;

namespace ganji { namespace pic_server { namespace scribe {
class ScribeLogger {
 public:
  ScribeLogger(const string &host, int port, uint32_t level,
               int connect_timeout, int send_timeout,
               int recv_timeout, int interval = 10,
               int thread_number = 1) {
    host_ = host;
    port_ = port;
    level_ = level;
    connect_timeout_ = connect_timeout;
    send_timeout_ = send_timeout;
    recv_timeout_ = recv_timeout;
    interval_ = interval;
    thread_number_ = thread_number;

    current_ = 0;
  }

  int Create() {
    for (int i = 0; i < thread_number_; ++i) {
      ScribeLoggerThread *thread_logger = NULL;
      thread_logger = new ScribeLoggerThread(host_, port_, level_,
                                             connect_timeout_,
                                             send_timeout_,
                                             recv_timeout_,
                                             interval_);
      if (thread_logger->Create() != 0) {
        return -1;
      }
      thread_loggers_.push_back(thread_logger);
    }
    return 0;
  }

  int Destroy() {
    for (int i = 0; i < thread_number_; ++i) {
      thread_loggers_[i]->Destroy();
      delete thread_loggers_[i];
      thread_loggers_[i] = NULL;
    }
    return 0;
  }
  
  void Fatal(const char *category, const char *fmt, ...) {
    if (thread_number_ == 0) {
      return;
    }
    va_list args;
    va_start(args, fmt);
    mutex_.Lock();
    int index = current_;
    current_ = (current_ + 1) % thread_number_;
    mutex_.Unlock();
    thread_loggers_[index]->Fatal(category, fmt, args);
  }

  void Warning(const char *category, const char *fmt, ...) {
    if (thread_number_ == 0) {
      return;
    }
    va_list args;
    va_start(args, fmt);
    mutex_.Lock();
    int index = current_;
    current_ = (current_ + 1) % thread_number_;
    mutex_.Unlock();
    thread_loggers_[index]->Warning(category, fmt, args);
  }

  void Notice(const char *category, const char *fmt, ...) {
    if (thread_number_ == 0) {
      return;
    }
    va_list args;
    va_start(args, fmt);
    mutex_.Lock();
    int index = current_;
    current_ = (current_ + 1) % thread_number_;
    mutex_.Unlock();
    thread_loggers_[index]->Notice(category, fmt, args);
  }

  void Trace(const char *category, const char *fmt, ...) {
    if (thread_number_ == 0) {
      return;
    }
    va_list args;
    va_start(args, fmt);
    mutex_.Lock();
    int index = current_;
    current_ = (current_ + 1) % thread_number_;
    mutex_.Unlock();
    thread_loggers_[index]->Trace(category, fmt, args);
  }

  void Debug(const char *category, const char *fmt, ...) {
    if (thread_number_ == 0) {
      return;
    }
    va_list args;
    va_start(args, fmt);
    mutex_.Lock();
    int index = current_;
    current_ = (current_ + 1) % thread_number_;
    mutex_.Unlock();
    thread_loggers_[index]->Debug(category, fmt, args);
  }

  void Access(const char *category, const char *fmt, ...) {
    if (thread_number_ == 0) {
      return;
    }
    va_list args;
    va_start(args, fmt);
    mutex_.Lock();
    int index = current_;
    current_ = (current_ + 1) % thread_number_;
    mutex_.Unlock();
    thread_loggers_[index]->Access(category, fmt, args);
  }

  uint32_t SetMask(uint32_t mask) {
    uint32_t old_mask;
    for (int i = 0; i < thread_number_; ++i) {
      old_mask = thread_loggers_[i]->SetMask(mask);
    }
    return old_mask;
  }

 private:
  string host_;
  int port_;
  uint32_t level_;
  int connect_timeout_;
  int recv_timeout_;
  int send_timeout_;
  int interval_;
  int thread_number_;

  int current_;
  Mutex mutex_;

  vector<ScribeLoggerThread*> thread_loggers_;
};
} } }  // namespace ganji::pic_server::scribe
#endif
