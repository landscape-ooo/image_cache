#include <errno.h>
#include <string.h>
#include <pthread.h>
#include "env.h"
#include "types.h"
#include "myutil.h"
#include "ulog.h"
namespace Util = ganji::pic_server::myutil;
namespace ganji { namespace pic_server {
const string g_conf_file = "conf/worker.conf";
int ProcEnv::Init() {
  // init conf
  config_ = new Config;
  assert(config_ != NULL);
  if (config_->Init(g_conf_file) != 0) {
    return -1;
  }

  // read mime
  if (ReadMime(config_->mime_file(), " ", &mime_) != 0) {
    fprintf(stderr, "read mime %s failed.\n", config_->mime_file().c_str());
    return -1;
  }

  // read config and init leveldb cache manager
  // read leveldb_lru_capacity 
  vector<string> dbs = config_->leveldbs();
  struct LevelDBCacheInfo cache_info;
  cache_info.lru_capacity = config_->leveldb_lru_capacity();
  cache_info.blob_file_size = config_->blob_file_size();
  cache_info.blob_disk_reserve = config_->blob_disk_reserve();
  cache_info.release_point = config_->leveldb_cache_switch_time();
  cache_info.cache_days = config_->leveldb_cache_days();
  cache_info.copy_limit = config_->copy_limit();
  cache_info.blob_cache_disk_reserve = config_->blob_cache_disk_reserve();
  cache_info.use_blob_cache = config_->use_blob_cache();
  vector<LevelDBCacheInfo> cache_infos;
  for (size_t i = 0; i < dbs.size(); ++i) {
    vector<string> db_info;
    Text::Segment(dbs[i], ':', &db_info);
    if (db_info.size() != 4) {
      fprintf(stderr, "leveldb config error; %s\n", dbs[i].c_str());
      return -1;
    }
    cache_info.leveldb_path = db_info[0];
    cache_info.blob_path = db_info[1];
    cache_info.blob_cache_path = db_info[2];
    cache_info.blob_cache_days = atoi(db_info[3].c_str());
    cache_infos.push_back(cache_info);
  }
  caches_ = new CacheManager(cache_infos, config_->check_interval());
  if (caches_->Init() != 0) {
    return -1;
  }

  // init scribe logger thread
  uint32_t scribe_log_level;
  uint32_t log_level = config_->log_level();
  if (log_level == Log::kLogFatal) {
    scribe_log_level = kScribeLogFatal;
  } else if (log_level == Log::kLogWarning) {
    scribe_log_level = kScribeLogWarning;
  } else if (log_level == Log::kLogNotice) {
    scribe_log_level = kScribeLogNotice;
  } else if (log_level == Log::kLogTrace) {
    scribe_log_level = kScribeLogTrace;
  } else if (log_level == Log::kLogDebug) {
    scribe_log_level = kScribeLogDebug;
  } else {
    // default
    scribe_log_level = kScribeLogTrace;
  }
  logger_ = new ScribeLogger(config_->scribe_host(),
                             config_->scribe_port(),
                             scribe_log_level,
                             config_->scribe_connect_timeout(),
                             config_->scribe_send_timeout(),
                             config_->scribe_recv_timeout(),
                             config_->scribe_interval(),
                             config_->scribe_thread_number());
  if (logger_->Create() != 0) {
    return -1;
  }

  return 0;
}

int ProcEnv::ReadMime(const string &file, const string &sep,
                      unordered_map<string, string> *pmime) {
  assert(pmime != NULL);
  unordered_map<string, string> &mime = *pmime;
  map<string, string> mime_map;
  string reason;
  if (Util::ParseConf(file, sep, &mime_map, &reason, true) == 0) {
    map<string, string>::iterator it = mime_map.begin();
    for (; it != mime_map.end(); ++it) {
      // mime_type and extensions are all trimed by ParseConf, this
      // is very important
      string mime_type = it->second;
      string extensions = it->first;
      size_t start = 0;
      for (size_t i = 0; i < extensions.size(); ++i) {
        if (extensions[i] == ' ') {
          mime[extensions.substr(start, i - start)] = mime_type;
          while (extensions[++i] == ' ') {
          }
          start = i;
        }
      }
      mime[extensions.substr(start)] = mime_type;
    }
    return 0;
  } else {
    fprintf(stderr, "read mime file: %s failed. %s\n",
            file.c_str(), reason.c_str());
    return -1;
  }
}

string ProcEnv::GetMime(const string &ext) {
  unordered_map<string, string>::iterator it = mime_.find(ext);
  if (it != mime_.end()) {
    return it->second;
  } else {
    return "application/octet-stream";
  }
}

int ProcEnv::Destroy() {
  delete config_;

  logger_->Destroy();

  delete logger_;

  delete caches_;

  return 0;
}
} }  // namespace ganji::pic_server
