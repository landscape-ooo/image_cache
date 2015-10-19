/*
 * 所有程序内部的文件名交互，禁止使用决定路径，因为程序有可能
 * 会被拷贝部署到另外的地方
 *
 * 在release的瞬间，会存在读取已经删除的fd的情况，但是对服务没有影响
 * Put调用pwrite写文件时，没有加锁(不加锁可以尽可能的解放cpu)。会出现一种情况为：另外一个线程读还没有写完成的数据. 如下：
 *           thread-1                thread-2
 *       Put(data, index)
 *                                Get(index, pdata)
 *                                    pread
 *           pwrite
 *后续可以考虑增加crc校验
 *current_file不进入manifest
 *
 * blob.manifest
 *   date suffix type
 *   type: 0 : blob file, 1 : blob cache file
 *   for example:
 *   20131018 109 1
 *   20131018 100 0
 *
 * 这个程序是cpu性能不敏感的，所以为了可靠简洁，在有些地方是使用遍历的方式来进行查找
 *
 */
#ifndef _GANJI_PIC_SERVER_BLOB_FILE_H_
#define _GANJI_PIC_SERVER_BLOB_FILE_H_
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <deque>
#include <tr1/unordered_map>
#include <utility>
#include "mutex.h"
#include "rwlock.h"
#include "types.h"
#include "ulog.h"
using std::string;
using std::deque;
using std::pair;
using std::tr1::unordered_map;
using ganji::pic_server::Mutex;
namespace ganji { namespace pic_server {
const uint32_t kMaxSuffix = 1000000;
// blob.current will be protected by put_mutex_
class BlobFile {
 public:
  BlobFile(const string &path, const string &cache_path,
           const int &cache_days, off_t max_size,
           int copy_limit = 10, bool use_blob_cache = true)
      : path_(path), cache_path_(cache_path),
        cache_days_(cache_days), max_size_(max_size),
        copy_limit_(copy_limit), use_blob_cache_(use_blob_cache) {
    last_timestamp_ = 0;
  }
  ~BlobFile();
  int Build();
  int Get(BlobIndex index, string *data);
  int Put(const string &data, BlobInfo *info);
  int GetReleaseTimeRange(time_t *start_p, time_t *end_p);
  int GetReleaseTimeEnd(time_t *tt);
  int Release(const string &date);
  int ReleaseBlobCache(bool force);
  int ReleaseBlobCache(const string &date, uint32_t suffix);

  string StartDate() {
    MutexGuard guard(&put_mutex_);
    if (date_files_.empty()) {
      UWarning("date_files_ is empty");
      return "";
    } else {
      return date_files_.begin()->first;
    }
  }

  size_t BlobSize() {
    return date_files_.size();
  }

  inline string GetCurrentPath() {
    if (use_blob_cache_) {
      return cache_path_;
    } else {
      return path_;
    }
  }

 private:
  void DumpDateFiles();
  void DumpCacheFiles();
  int DoRelease(map<string, vector<uint32_t> >::iterator map_it);
  int BuildManifestInfo();
  int BuildCurrentInfo();
  int ConsistManifest(const string &path);
  int CreateCurrent();
  BlobFile(const BlobFile&);
  void operator=(const BlobFile&);
  int SetCurrent(const string &fn, int file_type);
  int GetCurrent(string *fn, int *file_type);
  int HandleChange(time_t timestamp);
  int AddManifest(const string &date, uint32_t suffix, int type);
  int DeleteManifest(const string &date, uint32_t suffix);
  int DeleteManifest(const string &date);

  int ModifyManifestCache(const string &date);
  int ModifyManifestCache(const string &date, uint32_t suffix);

  string path_;
  string cache_path_;
  int cache_days_;
  off_t max_size_;
  int copy_limit_;

  Mutex put_mutex_;
  uint32_t current_suffix_;
  off_t current_size_;
  string current_date_;
  int current_type_;

  Rwlock fds_rwlock_;
  unordered_map<uint32_t, int> fds_;

  // map<string, pair<vector<uint32_t>,int> > date_files_;
  map<string, vector<uint32_t> > date_files_;
  map<string, set<uint32_t> > cache_files_;

  time_t last_timestamp_;

  bool use_blob_cache_;
};
} }  // namespace ganji::pic_server
#endif
