#ifndef _GANJI_PIC_SERVER_CONFIG_H_
#define _GANJI_PIC_SERVER_CONFIG_H_
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include "types.h"
using std::string;
using std::vector;

namespace ganji { namespace pic_server {
class Config;
struct Command {
  string command;
  void *ptr;
  int (*handler)(const string &cmd, const string &value,
       void *ptr, string *reason);
};

class Config {
 public:
  Config() {
    inited_ = false;
    file_ = "";
    leveldb_lru_capacity_ = 0;
    leveldb_cache_days_ = 0;
    blob_disk_reserve_ = 0.2;
    blob_cache_disk_reserve_ = 0.2;
    leveldb_cache_max_size_ = 0;
    check_interval_ = 0;
    copy_limit_ = 10;

    worker_thread_number_ = -1;
    log_path_ = "";
    scribe_host_ = "";
    scribe_port_ = 0;
    scribe_connect_timeout_ = -1;
    scribe_send_timeout_ = -1;
    scribe_recv_timeout_ = -1;
    scribe_interval_ = -1;
    scribe_thread_number_ = -1;

    blob_file_size_ = 0;

    read_only_mode_ = false;
    proxy_pass_ = "";
    proxy_timeout_ = -1;
    
    mime_file_ = "";
    use_blob_cache_ = true;
  }
  int Init(const string &file);

  size_t leveldb_lru_capacity() {
    return leveldb_lru_capacity_;
  }

  vector<string> leveldbs() {
    return leveldbs_;
  }

  int leveldb_cache_days() {
    return leveldb_cache_days_;
  }

  Time leveldb_cache_switch_time() {
    return leveldb_cache_switch_time_;
  }

  float blob_disk_reserve() {
    return blob_disk_reserve_;
  }

  float blob_cache_disk_reserve() {
    return blob_cache_disk_reserve_;
  }

  size_t leveldb_cache_max_size() {
    return leveldb_cache_max_size_;
  }

  int check_interval() {
    return check_interval_;
  }

  int copy_limit() {
    return copy_limit_;
  }

  int worker_thread_number() {
    return worker_thread_number_;
  }

  string log_path() {
    return log_path_;
  }

  string scribe_host() {
    return scribe_host_;
  }

  int scribe_port() {
    return scribe_port_;
  }

  int scribe_connect_timeout() {
    return scribe_connect_timeout_;
  }

  int scribe_send_timeout() {
    return scribe_send_timeout_;
  }

  int scribe_recv_timeout() {
    return scribe_recv_timeout_;
  }

  int scribe_interval() {
    return scribe_interval_;
  }

  int scribe_thread_number() {
    return scribe_thread_number_;
  }

  uint32_t log_level() {
    return log_level_;
  }

  off_t blob_file_size() {
    return blob_file_size_;
  }

  bool read_only_mode() {
    return read_only_mode_;
  }

  string proxy_pass() {
    return proxy_pass_;
  }

  long proxy_timeout() {
    return proxy_timeout_;
  }

  string mime_file() {
    return mime_file_;
  }

  bool use_blob_cache() {
    return use_blob_cache_;
  }

 private:
  int Verify(string *reason);
  string file_;
  bool inited_;
  int core_number_;
  // leveldb 
  size_t leveldb_lru_capacity_;
  vector<string> leveldbs_;
  int leveldb_cache_days_;
  Time leveldb_cache_switch_time_;
  float blob_disk_reserve_;
  float blob_cache_disk_reserve_;
  size_t leveldb_cache_max_size_;
  int check_interval_;
  int copy_limit_;

  // process
  int worker_thread_number_;
  string log_path_;
  bool read_only_mode_;
  string proxy_pass_;
  long proxy_timeout_;  // curl_easy_setopt need long type for timeout

  // scribe
  string scribe_host_;
  int scribe_port_;
  int scribe_connect_timeout_;
  int scribe_send_timeout_;
  int scribe_recv_timeout_;
  int scribe_interval_;
  int scribe_thread_number_;
  uint32_t log_level_;

  // blob file size
  off_t blob_file_size_;

  string mime_file_;

  bool use_blob_cache_;
};
} } // namespace ganji::pic_server
#endif
