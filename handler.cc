#include <stdio.h>
#include <stdlib.h>
#include <string>
#include "ulog.h"
#include "handler.h"
#include "msg.h"
#include "curl_downloader.h"
extern uint64_t g_timestamp;
extern string g_ok_msg;
namespace ganji { namespace pic_server {
inline int Handler::GetCache() {
  int ret = 0;
  uint64_t start = myutil::GetUs();
  if (proc_env_->caches()->Get(url_, &data_) == 0) {
    log_item_->hit_cache = 1;
    ret = 0;
  } else {
    ret = -1;
  }
  log_item_->leveldb_time = myutil::GetUs() - start;
  return ret;
}

inline int Handler::PutCache() {
  int ret = 0;
  uint64_t start = myutil::GetUs();
  if (proc_env_->caches()->Put(url_, data_) == 0) {
    ret = 0;
  } else {
    ret = -1;
  }
  log_item_->put_time = myutil::GetUs() - start;
  return ret;
}

inline int Handler::Download() {
  uint64_t start = myutil::GetUs();
  int ret;
  map<string, string> header_out;
  map<string, string> header_in;
  ret = Curl::Download(proc_env_->config()->proxy_pass() + url_,
                       proc_env_->config()->proxy_timeout(),
                       header_out,
                       &header_in,
                       &data_);
  if (header_in["HttpCode"] != "200") {
    ret = -1;
  }
  log_item_->cgi_time = myutil::GetUs() - start;
  if (ret != 0) {
    UTrace("download failed. %s", url_.c_str());
  }
  return ret;
}

inline int Handler::FinishRequest() {
  FCGX_PutStr(g_ok_msg.c_str(), g_ok_msg.size(), request_->out);
  string header;
  string content_type = "Content-Type: ";
  content_type += proc_env_->GetMime(ext_) + string("\r\n");
  if (log_item_->hit_cache != 0) {
    FCGX_PutStr(g_cache_hit_msg.c_str(),
                g_cache_hit_msg.size(),
                request_->out);
  } else {
    FCGX_PutStr(g_cache_miss_msg.c_str(),
                g_cache_miss_msg.size(),
                request_->out);
  }
  char content_length[128];
  snprintf(content_length, sizeof(content_length),
           "Content-Length: %lu\r\n", data_.size());
  header = content_type + content_length + string("\r\n");

  FCGX_PutStr(header.c_str(), header.size(), request_->out);
  FCGX_PutStr(data_.c_str(), data_.size(), request_->out);
  FCGX_Finish_r(request_);
  log_item_->status = 200;
  log_item_->bytes = data_.size();
  return 0;
}

int Handler::Run() {
  map<string, string>::const_iterator it = params_.find("key");
  if (it != params_.end()) {
    url_ = it->second;
  } else {
    return -1;
  }
  ext_ = myutil::GetExtension(url_);
  UDebug("url:%s ext:%s", url_.c_str(), ext_.c_str());
  if (GetCache() != 0) {
    if (Download() != 0) {
      return -1;
    }
    if (!proc_env_->config()->read_only_mode()) {
      PutCache();
    }
  }
  FinishRequest();
  return 0;
}
} }  // namespace ganji::pic_server
