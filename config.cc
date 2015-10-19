#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <string>
#include <vector>
#include <map>
#include "ganji/util/log/thread_fast_log.h"
#include "ganji/util/text/text.h"
#include "config.h"
#include "myutil.h"
#include "scribe/scribe_logger.h"
using std::string;
using std::vector;
using std::map;
namespace Text = ganji::util::text::Text;
namespace myutil = ganji::pic_server::myutil;
namespace Log = ganji::util::log::ThreadFastLog;

namespace ganji { namespace pic_server {
int int_set(const string &cmd, const string &value,
            void *ptr, string *reason) {
  *((int*)ptr) = atoi(value.c_str());
  return 0;
}

int uint32_set(const string &cmd, const string &value,
               void *ptr, string *reason) {
  *((uint32_t*)ptr) = (uint32_t)atol(value.c_str());
  return 0;
}

int size_t_set(const string &cmd, const string &value,
               void *ptr, string *reason) {
  *((size_t*)ptr) = (size_t)(atoll(value.c_str()));
  return 0;
}

int off_t_set(const string &cmd, const string &value,
              void *ptr, string *reason) {
  *((off_t*)ptr) = (off_t)(atoll(value.c_str()));
  return 0;
}

int long_set(const string &cmd, const string &value,
             void *ptr, string *reason) {
  *((long*)ptr) = atol(value.c_str());
  return 0;
}

int float_set(const string &cmd, const string &value,
              void *ptr, string *reason) {
  *((float*)ptr) = (float)atof(value.c_str());
  return 0;
}

int string_set(const string &cmd, const string &value,
               void *ptr, string *reason) {
  *((string*)ptr) = value;
  return 0;
}

int bool_set(const string &cmd, const string &value,
             void *ptr, string *reason) {
  if (value == "1" || value == "on" || value == "true") {
    *((bool*)ptr) = true;
    return 0;
  } else if (value == "0" || value == "off" || value == "false") {
    *((bool*)ptr) = false;
    return 0;
  } else {
    *reason = value + string(": invalid value for bool type. shoule in [0, 1, on, off, true, false]");
    return -1;
  }
}

int Time_set(const string &cmd, const string &value,
             void *ptr, string *reason) {
  string why;
  Time tmp(value);
  if (tmp.Init(&why) != 0) {
    *reason = why;
    return -1;
  }
  *((Time*)ptr) = tmp;
  return 0;
}

int log_level_set(const string &cmd, const string &value,
                  void *ptr, string *reason) {
  if (value == "debug") {
    *((uint32_t*)ptr) = Log::kLogDebug;
  } else if (value == "trace") {
    *((uint32_t*)ptr) = Log::kLogTrace;
  } else if (value == "notice") {
    *((uint32_t*)ptr) = Log::kLogNotice;
  } else if (value == "warning") {
    *((uint32_t*)ptr) = Log::kLogWarning;
  } else if (value == "fatal") {
    *((uint32_t*)ptr) = Log::kLogFatal;
  } else if (value == "none") {
    *((uint32_t*)ptr) = Log::kLogNone;
  } else if (value == "all") {
    *((uint32_t*)ptr) = Log::kLogAll;
  } else {
    *reason = string("undefined log level, should in [debug, info, notice, warning, fatal, access]");
    return -1;
  }
  return 0;
}

// absolute path, seperated by ;
int leveldbs_set(const string &cmd, const string &value,
                 void *ptr, string *reason) {
  vector<string> &leveldbs = *((vector<string>*)ptr);
  vector<string> tmp;
  Text::Segment(value, ';', &tmp);
  for (size_t i = 0; i < tmp.size(); ++i) {
    Text::Trim(&tmp[i]);
    if (tmp[i] == ";") {
      continue;
    }
    leveldbs.push_back(tmp[i]);
  }
  return 0;
}

struct Command FindCommand(struct Command *commands, const string &cmd) {
  for (int i = 0;; ++i) {
    struct Command command = commands[i];
    if (command.command == "") {
      return command;
    }
    if (command.command == cmd) {
      return command;
    }
  }
}

int Config::Init(const string &file) {
  file_ = file;
  struct Command commands[] = {
    {"leveldb.lru_capacity",
     &this->leveldb_lru_capacity_,
     &size_t_set
    },
    {"leveldb.cache_days",
      &this->leveldb_cache_days_,
      &int_set
    },
    {"leveldb.cache_switch_time",
      &this->leveldb_cache_switch_time_,
      &Time_set
    },
    {"leveldb.cache_max_size",
      &this->leveldb_cache_max_size_,
      &size_t_set
    },
    {"leveldb.dbs",
      &this->leveldbs_,
      &leveldbs_set
    },
    {"blob_disk_reserve",
      &this->blob_disk_reserve_,
      &float_set
    },
    {"blob_cache_disk_reserve",
      &this->blob_cache_disk_reserve_,
      &float_set
    },
    {"check_interval",
      &this->check_interval_,
      &int_set
    },
    {"copy_limit",
      &this->copy_limit_,
      &int_set
    },
    {"process.worker_thread_number",
      &this->worker_thread_number_,
      &int_set
    },
    {"process.log_path",
      &this->log_path_,
      &string_set
    },
    {"process.read_only_mode",
      &this->read_only_mode_,
      &bool_set
    },
    {"process.proxy_pass",
      &this->proxy_pass_,
      &string_set
    },
    {"process.proxy_timeout",
      &this->proxy_timeout_,
      &long_set
    },
    {"scribe.host",
      &this->scribe_host_,
      &string_set
    },
    {"scribe.port",
      &this->scribe_port_,
      &int_set
    },
    {"scribe.connect_timeout",
      &this->scribe_connect_timeout_,
      &int_set
    },
    {"scribe.send_timeout",
      &this->scribe_send_timeout_,
      &int_set
    },
    {"scribe.recv_timeout",
      &this->scribe_recv_timeout_,
      &int_set
    },
    {"scribe.interval",
      &this->scribe_interval_,
      &int_set
    },
    {"scribe.thread_number",
      &this->scribe_thread_number_,
      &int_set
    },
    {"log_level",
      &this->log_level_,
      &log_level_set
    },
    {"blob_file_size",
      &this->blob_file_size_,
      &off_t_set
    },
    {"mime_file",
      &this->mime_file_,
      &string_set
    },
    {"use_blob_cache",
      &this->use_blob_cache_,
      &bool_set
    },
    {"", NULL, NULL}};
  map<string, string> conf_map;
  string reason;
  int ret = myutil::ParseConf(file_, "=", &conf_map, &reason);
  if (ret != 0) {
    fprintf(stderr, "%s\n", reason.c_str());
    return -1;
  }
  map<string, string>::iterator it = conf_map.begin();
  for (; it != conf_map.end(); ++it) {
    struct Command command = FindCommand(commands, it->first);
    if (command.command == "") {
      fprintf(stderr, "%s not define\n", it->first.c_str());
      return -1;
    }
    reason.clear();
    ret = command.handler(it->first, it->second, command.ptr, &reason);
    if (ret != 0) {
      fprintf(stderr, "%s\n", reason.c_str());
      return -1;
    }
  }
  reason.clear();
  if (Verify(&reason) != 0) {
    fprintf(stderr, "%s\n", reason.c_str());
    return -1;
  }
  inited_ = true;
  return 0;
}
int Config::Verify(string *reason) {
  // leveldbs >= 1
  if (!(leveldbs_.size() >= 1)) {
    *reason = string("must define at least one instance of leveldb");
    return -1;
  }
  // leveldb_lru_capacity_ >= 0
  if (!(leveldb_lru_capacity_ >= 0)) {
    *reason = string("leveldb.lru_capacity should >= 0");
    return -1;
  }
  // leveldb_cache_days_ >= 0
  if (!(leveldb_cache_days_ >= 0)) {
    *reason = string("leveldb.cache_days should >= 0");
    return -1;
  }
  // disk_reserve_ >= 0.05
  if (!(blob_disk_reserve_ >= 0.05)) {
    *reason = string("blob_disk_reserve should in [0.05, 0.5]");
    return -1;
  }
  if (!(blob_cache_disk_reserve_ >= 0.05)) {
    *reason = string("blob_cache_disk_reserve should > 0.05");
    return -1;
  }
  if (!(check_interval_ > 0)) {
    *reason = string("check_interval should be defined and value > 0");
    return -1;
  }
  // worker_thread_number_ > 0
  if (!(worker_thread_number_ > 0)) {
    *reason = string("worker_thread_number should > 0");
    return -1;
  }
  if (log_path_ == "") {
    *reason = string("must define process.log_path");
    return -1;
  }

  if (proxy_pass_ == "") {
    *reason = string("must define process.proxy_pass");
    return -1;
  }
  if (proxy_pass_[proxy_pass_.size() - 1] != '/') {
    proxy_pass_.push_back('/');
  } else {
    size_t pos = proxy_pass_.find_last_not_of('/');
    if (pos == string::npos) {
      *reason = string("invalid process.proxy_pass: ") + proxy_pass_;
      return -1;
    } else {
      proxy_pass_ = proxy_pass_.substr(0, pos + 1);
    }
  }
  if (proxy_timeout_ < 0 || proxy_timeout_ > 100000) {
    *reason = string("must define process.proxy_timeout when use read only model, and it's value should in [0, 100000]");
    return -1;
  }

  if (scribe_host_ == "") {
    *reason = string("must define scribe.host");
    return -1;
  }
  // scribe_port in (1024, 65535)
  if (!(scribe_port_ > 1024 && scribe_port_ < 65535)) {
    *reason = string("scribe.port should in (1024, 65535)");
    return -1;
  }
  // scribe_connect_timeout > 0
  if (!(scribe_connect_timeout_ > 0)) {
    *reason = string("scribe.connect_timeout should > 0");
    return -1;
  }
  // scribe_send_timeout > 0
  if (!(scribe_send_timeout_ > 0)) {
    *reason = string("scribe.send_timeout should > 0");
    return -1;
  }
  // scribe_recv_timeout > 0
  if (!(scribe_recv_timeout_ > 0)) {
    *reason = string("scribe.recv_timeout should > 0");
    return -1;
  }
  // scribe log interval [0-1000]
  if (!(scribe_interval_ > 0 && scribe_interval_ < 1000)) {
    *reason = string("scribe.interval should in (0, 1000)");
    return -1;
  }
  // scribe thread number [0-1000]
  if (!(scribe_thread_number_ >= 0 && scribe_thread_number_ <= 1000)) {
    *reason = string("scribe.thread_number should in [0, 1000]");
    return -1;
  }
  if (!(blob_file_size_ > 0)) {
    *reason = string("must define blob_file_size");
    return -1;
  }

  struct stat st;
  if (stat(mime_file_.c_str(), &st) != 0) {
    *reason = string("mime_file:") + mime_file_ + string(" ") + strerror(errno);
    return -1;
  }
  return 0;
}
} }  // namespace ganji::pic_server
