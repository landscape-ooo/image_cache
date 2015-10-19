#ifndef _GANJI_PIC_SERVER_LEVELDB_CACHE_H_
#define _GANJI_PIC_SERVER_LEVELDB_CACHE_H_
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include "leveldb/db.h"
#include "leveldb/cache.h"
#include "types.h"
#include "mutex.h"
#include "blob_file.h"

using std::string;
using std::vector;
using leveldb::Slice;
using leveldb::Status;
using leveldb::DB;
using leveldb::Options;
using leveldb::WriteOptions;
using leveldb::ReadOptions;
using leveldb::Iterator;
using leveldb::Snapshot;

// leveldb and blob implementation will ensure thread safe, so this file
// need not to use any external synchronize facilities
namespace ganji { namespace pic_server {
class LevelDBCache {
 public:
  LevelDBCache(LevelDBCacheInfo info) : info_(info) { }
  ~LevelDBCache() {
    delete db_lru_cache_;
    delete db_;
    delete blob_;
  }
  int Init();
  void DoRelease(const string &date);

  void DoReleaseBlobCache(bool force) {
    blob_->ReleaseBlobCache(force);
  }

  int Put(const string &key, const string &value);
  int Get(const string &key, string *value);
  string StartDate() {
    return blob_->StartDate();
  }

  size_t BlobSize() {
    return blob_->BlobSize();
  }

 private:
  // leveldb and blob files are all thread safe
  leveldb::DB *db_;
  leveldb::Cache *db_lru_cache_;
  BlobFile *blob_;
  LevelDBCacheInfo info_;
  string leveldb_path_;
};

// function: route keys to a certain leveldb cache instance
// monitor disk free percentage and time point to release some cache
class CacheManager {
 public:
  CacheManager(const vector<LevelDBCacheInfo> &leveldb_infos,
               int check_interval)
      : leveldb_infos_(leveldb_infos), check_interval_(check_interval) {
        size_ = leveldb_infos_.size();
        stop_ = false;
  }
  ~CacheManager();
  int Init();
  int Put(const string &key, const string &value);
  int Get(const string &key, string *data);

 private:
  static void* Monitor(void *args);
  void DoMonitor();
  int GetIndex(const string &key);
  vector<LevelDBCacheInfo> leveldb_infos_;
  vector<LevelDBCache*> leveldb_caches_;
  int size_;
  int check_interval_;
  pthread_t monitor_thread_;

  volatile bool stop_;
};
} }  // namespace ganji::pic_server
#endif
