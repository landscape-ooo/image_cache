#include <signal.h>
#include <errno.h>
#include <string.h>
#include <tr1/functional>
#include "leveldb_cache.h"
#include "myutil.h"
#include "ulog.h"
using std::tr1::hash;
namespace Log = ganji::util::log::ThreadFastLog;

namespace ganji { namespace pic_server {
int LevelDBCache::Init() {
  // create&&open leveldb and blob files
  string leveldb_path = info_.leveldb_path;
  leveldb_path_ = leveldb_path;
  string blob_path = info_.blob_path;
  string blob_cache_path = info_.blob_cache_path;
  int blob_cache_days = info_.blob_cache_days;
  db_lru_cache_ = leveldb::NewLRUCache(info_.lru_capacity);
  leveldb::Options options;
  options.block_cache = db_lru_cache_;
  options.write_buffer_size = 8 * 1024 * 1024;
  options.create_if_missing = true;
  options.max_open_files = 1024 * 8;
  leveldb::Status status = leveldb::DB::Open(options, leveldb_path, &db_);
  if (!status.ok()) {
    UFatal("open leveldb failed. %s", leveldb_path.c_str());
    return -1;
  }
  UTrace("open leveldb %s", leveldb_path.c_str());

  blob_ = new BlobFile(blob_path, blob_cache_path, blob_cache_days,
                       info_.blob_file_size, info_.copy_limit,
                       info_.use_blob_cache);
  if (blob_->Build() != 0) {
    return -1;
  }
  UTrace("open blob file %s", blob_path.c_str());
  return 0;
}

// first put blob data file, and then put leveldb
inline int LevelDBCache::Put(const string &key, const string &value) {
  BlobInfo info;

  if (blob_->Put(value, &info) != 0) {
    return -1;
  }
  UTrace("%s %s blob put: %ld %ld %u %u", leveldb_path_.c_str(),
         key.c_str(), info.timestamp, info.index.offset,
         info.index.length, info.index.suffix);

  Slice slice_value((char*)&info, sizeof(info));
  Status status = db_->Put(WriteOptions(), Slice(key), slice_value);
  if (!status.ok()) {
    UWarning("%s %s put leveldb failed", leveldb_path_.c_str(), key.c_str());
    return -1;
  }
  UTrace("%s %s leveldb put ok", leveldb_path_.c_str(), key.c_str());
  return 0;
}

// first get leveldb index, then get data from blob data file
inline int LevelDBCache::Get(const string &key, string *value) {
  string index;
  BlobInfo *info;

  Status status = db_->Get(ReadOptions(), Slice(key), &index);

  if (!status.ok()) {
    UTrace("%s %s leveldb get failed.", leveldb_path_.c_str(), key.c_str());
    return -1;
  }

  info = (BlobInfo*)(index.c_str());

  UTrace("%s %s leveldb get: %ld %ld %u %u", leveldb_path_.c_str(),
         key.c_str(), info->timestamp, info->index.offset,
         info->index.length, info->index.suffix);

  if (blob_->Get(info->index, value) != 0) {
    UWarning("%s %s leveldb cached, no blob file found.",
             leveldb_path_.c_str(),key.c_str());
    return -1;
  }
  UTrace("%s %s blob file get ok.", leveldb_path_.c_str(), key.c_str());
  return 0;
}

void LevelDBCache::DoRelease(const string &date) {
  // iterator and delete
  time_t end;
  if (myutil::GetDateTimeRange(date, NULL, &end) != 0) {
    UFatal("%s get date time range failed. %s",
           leveldb_path_.c_str(), date.c_str());
    return;
  } else {
    UWarning("%s get date range: %s, release time end: %ld",
             leveldb_path_.c_str(), date.c_str(), end);
  }
  time_t tt_start, tt_end;
  time(&tt_start);
  UWarning("%s deleting leveldb index until %s %ld",
           leveldb_path_.c_str(), date.c_str(), end);
  ReadOptions read_options;
  WriteOptions write_options;
  Iterator *it = db_->NewIterator(read_options);
  it->SeekToFirst();
  while (true) {
    if (!it->Valid()) {
      break;
    }
    Slice key = it->key();
    Slice value = it->value();
    BlobInfo *info = (BlobInfo*)value.data();
    if (info->timestamp <= end) {
      Status status = db_->Delete(write_options, key);
      UDebug("%s leveldb delete key %s result:%d", info_.leveldb_path.c_str(),
             string(key.data(), key.size()).c_str(), status.ok());
    }
    it->Next();
  }
  delete it;
  time(&tt_end);
  UWarning("%s delete leveldb index until %s %ld done. timeuse: %lds",
           info_.leveldb_path.c_str(), date.c_str(), end, tt_end - tt_start);
  blob_->Release(date);
}

// init leveldb caches and create monitor thread
int CacheManager::Init() {
  for (size_t i = 0; i < leveldb_infos_.size(); ++i) {
    LevelDBCache *leveldb_cache = new LevelDBCache(leveldb_infos_[i]);
    if (leveldb_cache->Init() != 0) {
      return -1;
    }
    leveldb_caches_.push_back(leveldb_cache);
  }

  // create monitor thread
  int ret = pthread_create(&monitor_thread_, NULL, Monitor, (void*)this);
  if (ret != 0) {
    UFatal("create Monitor thread failed. %d %s", errno, strerror(errno));
    return -1;
  }
  return 0;
}

void* CacheManager::Monitor(void *args) {
  CacheManager *cache_manager = (CacheManager*)args;
  cache_manager->DoMonitor();
  return NULL;
}

void CacheManager::DoMonitor() {
  // 每天定点检测是否需要滚动缓存
  time_t check_point;
  check_point = leveldb_infos_[0].release_point.Next();

  while (!stop_) {
    time_t cur;
    time(&cur);

    // get a copy and shuffle it
    vector<LevelDBCacheInfo> tmp_infos(leveldb_infos_);
    std::random_shuffle( tmp_infos.begin(), tmp_infos.end());

  // 检测磁盘使用情况
  // HACK：我们在force的情况下一次只删除一个文件，因此需要多重复几次
  for(int retry=0;retry<10;retry++) {
    for (size_t i = 0; i < tmp_infos.size(); ++i) {
      LevelDBCacheInfo& info = tmp_infos[i];
      string &blob_path = info.blob_path;
      string &blob_cache_path = info.blob_cache_path;
      float disk_free = myutil::GetDiskFree(blob_path.c_str());
      if (disk_free < 0) {
        UFatal("get disk free %s failed. %s", blob_path.c_str(), strerror(errno));
        abort();
      }
      if (disk_free <= info.blob_disk_reserve) {
        UWarning("%s disk_free %f < %f, release cache", blob_path.c_str(),
                 disk_free, info.blob_disk_reserve);
        string date = leveldb_caches_[i]->StartDate();
        leveldb_caches_[i]->DoRelease(date);
        continue;
      }

      disk_free = myutil::GetDiskFree(blob_cache_path.c_str());
      if (disk_free < 0) {
        UFatal("get disk free %s failed. %s", blob_cache_path.c_str(), strerror(errno));
        abort();
      }
      if (disk_free <= info.blob_cache_disk_reserve) {
        UWarning("%s disk free %f < %f release blob cache",
                 blob_cache_path.c_str(), 
                 disk_free,
                 info.blob_cache_disk_reserve);
        leveldb_caches_[i]->DoReleaseBlobCache(true);
      }
    }
  }
    if (cur > check_point) {
      // 检测是否需要滚动缓存
      for (size_t i = 0; i < leveldb_infos_.size(); ++i) {
        while (true) {
          size_t blob_cache_days = leveldb_caches_[i]->BlobSize();
          string start_date = leveldb_caches_[i]->StartDate();
          UDebug("%s start_date:%s cache_days:%lu",
                 leveldb_infos_[i].blob_path.c_str(), 
                 start_date.c_str(),
                 blob_cache_days);
          if ((int)blob_cache_days > leveldb_infos_[i].cache_days) {
            UTrace("%s start_date:%s cache_days:%lu > %d, release cache",
                   leveldb_infos_[i].blob_path.c_str(), 
                   start_date.c_str(),
                   blob_cache_days, 
                   leveldb_infos_[i].cache_days);
            leveldb_caches_[i]->DoRelease(start_date);
          } else {
            break;
          }
        }
      }

      // 滚动大文件ssd cache
      for (size_t i = 0; i < leveldb_infos_.size(); ++i) {
        leveldb_caches_[i]->DoReleaseBlobCache(false);
      }

      // update check point
      check_point = leveldb_infos_[0].release_point.Next();
    }

    for (int i = 0; i < check_interval_ && !stop_; ++i) {
      sleep(1);
    }
  }
  return;
}

int CacheManager::Put(const string &key, const string &value) {
  return leveldb_caches_[GetIndex(key)]->Put(key, value);
}

int CacheManager::Get(const string &key, string *data) {
  return leveldb_caches_[GetIndex(key)]->Get(key, data);
}

inline int CacheManager::GetIndex(const string &key) {
  return hash<string>()(key) % size_;
}

CacheManager::~CacheManager() {
  // stop monitor thread
  stop_ = true;
  pthread_join(monitor_thread_, NULL);

  // delete all leveldb caches
  for (size_t i = 0; i < leveldb_caches_.size(); ++i) {
    delete leveldb_caches_[i];
  }
  leveldb_infos_.clear();
  leveldb_caches_.clear();
}
} }  // namespace ganji::pic_server
