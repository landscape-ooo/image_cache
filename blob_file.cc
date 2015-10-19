#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>
#include <string.h>
#include <utility>
#include <vector>
#include <tr1/unordered_set>
#include "ulog.h"
#include "ganji/util/text/text.h"
#include "blob_file.h"
#include "fs/file_os.h"
#include "crc32c.h"
using std::pair;
using std::vector;
using std::tr1::unordered_set;
using ganji::pic_server::FileOS;
namespace Text = ganji::util::text::Text;
namespace ganji { namespace pic_server {

BlobFile::~BlobFile() {
  // close all fds
  unordered_map<uint32_t, int>::iterator it = fds_.begin();
  for (; it != fds_.end(); ++it) {
    close(it->second);
  }
}

int BlobFile::BuildManifestInfo() {
  FileOS file_os(path_);
  string data;
  if (file_os.Get("blob.manifest", &data) != 0) {
    file_os.Put("blob.manifest", "");
    return 0;
  }

  vector<string> lines;
  Text::Segment(data, '\n', &lines);

  for (size_t i = 0; i < lines.size(); ++i) {
    string line = lines[i];
    Text::Trim(&line);
    if (line == "") {
      continue;
    }
    vector<string> columns;
    Text::Segment(line, ' ', &columns);
    if (columns.size() != 3) {
      UFatal("%s blob.manifest format error. %s", path_.c_str(), line.c_str());
      continue;
    }
    string date = columns[0];
    uint32_t suffix = atol(columns[1].c_str());
    int file_type = atoi(columns[2].c_str());
    char buf[64];
    snprintf(buf, sizeof(buf), "blob.data.%u", suffix);
    int fd = -1;
    if (file_type == 1) {
      FileOS cache_file_os(cache_path_);
      fd = cache_file_os.Open(buf, O_RDONLY);
    } else {
      fd = file_os.Open(buf, O_RDONLY);
    }
    if (fd == -1) {
      UFatal("%s can not open %s. delete. %d, %s",
             file_type == 1 ? cache_path_.c_str() : path_.c_str(),
             buf, errno, strerror(errno));
      DeleteManifest(date, suffix);
      continue;
    }
    fds_[suffix] = fd;
    if (date_files_.find(date) == date_files_.end()) {
      vector<uint32_t> suffixs;
      date_files_[date] = suffixs;
    }
    date_files_[date].push_back(suffix);
    if (file_type == 1) {
      if (cache_files_.find(date) == cache_files_.end()) {
        set<uint32_t> suffixs;
        cache_files_[date] = suffixs;
      }
      cache_files_[date].insert(suffix);
    }
  }
  return 0;
}

int BlobFile::BuildCurrentInfo() {
  char date_buf[16];
  struct tm ctm;
  time_t ctt;
  time(&ctt);
  localtime_r(&ctt, &ctm);
  snprintf(date_buf, sizeof(date_buf), "%04d%02d%02d",
           ctm.tm_year + 1900, ctm.tm_mon + 1, ctm.tm_mday);
  current_date_ = string(date_buf);
  
  string current_file;
  int file_type = use_blob_cache_? 1 : 0;;
  if (GetCurrent(&current_file, &file_type) != 0) {
    uint32_t suffix = 0;
    if (date_files_.size() != 0) {
      suffix = (date_files_.rbegin()->second.back() + 1) % kMaxSuffix;
    }
    char fn_buf[64];
    snprintf(fn_buf, sizeof(fn_buf), "blob.data.%u", suffix);
    SetCurrent(string(fn_buf), use_blob_cache_? 1 : 0);
    current_file = string(fn_buf);

    current_suffix_ = suffix;
    current_size_ = 0;
    current_type_ = use_blob_cache_? 1 : 0;
  } else {
    string full_path = (file_type == 1 ? cache_path_ : path_) + string("/") + current_file;
    struct stat st;
    if (stat(full_path.c_str(), &st) != 0) {
      UFatal("stat current blob data file %s failed. %d, %s", 
             full_path.c_str(), errno, strerror(errno));
      return -1;
    }
    current_suffix_ = atol(current_file.substr(10).c_str());
    current_size_ = st.st_size;
    current_type_ = file_type;
  }

  UDebug("current:%u %lu %d", current_suffix_, current_size_, current_type_);
  FileOS file_os(file_type == 1 ? cache_path_: path_);

  // open current blob file
  int fd = file_os.Open(current_file.c_str(), O_CREAT | O_RDWR, 0644);
  if (fd == -1) {
    UFatal("open %s %s failed. %d, %s", cache_path_.c_str(),
           current_file.c_str(), errno, strerror(errno));
    return -1;
  }

  fcntl(fd, F_SETFD, 1);
  fds_[current_suffix_] = fd;

  return 0;
}

int BlobFile::Build() {
  // read manifest and open all blob.data files
  if (BuildManifestInfo() != 0) {
    return -1;
  }
  if (BuildCurrentInfo() != 0) {
    return -1;
  }
  UDebug("-------------------- after building manifest ----------------");
  DumpDateFiles();
  DumpCacheFiles();
  // consist manifest
  ConsistManifest(path_);
  ConsistManifest(cache_path_);
  UDebug("------------------ after consisting manifest -----------------");
  DumpDateFiles();
  DumpCacheFiles();
  return 0;
}

int BlobFile::ConsistManifest(const string &path) {
  FileOS file_os(path);
  vector<string> files;
  string reason;
  string pattern = "^blob\\.data\\.\\d+$";
  if (file_os.Find(pattern, &files, &reason) != 0) {
    UFatal("%s find %s failed. %s", path.c_str(), pattern.c_str(), reason.c_str());
    return -1;
  }
  for (size_t i = 0; i < files.size(); ++i) {
    string fn = files[i];
    uint32_t suffix = atol(fn.substr(10).c_str());
    if (fds_.find(suffix) == fds_.end()) {
      UFatal("%s %s not in manifest, delete", path.c_str(), fn.c_str());
      file_os.Delete(fn);
    }
  }
  return 0;
}

int BlobFile::SetCurrent(const string &fn, int file_type) {
  FileOS file_os(path_);
  char buf[128];
  snprintf(buf, sizeof(buf), "%s %d\n", fn.c_str(), file_type);
  return file_os.Put("blob.current", string(buf));
}

int BlobFile::GetCurrent(string *fn, int *file_type) {
  FileOS file_os(path_);
  int ret = file_os.Get("blob.current", fn);
  if (ret != 0) {
    return ret;
  }
  Text::Trim(fn);
  size_t pos = fn->find(' ');
  if (pos == string::npos) {
    UFatal("%s blob current format error. no blankspace found. %s",
           path_.c_str(), fn->c_str());
    abort();
  }
  *file_type = atoi(fn->c_str() + pos + 1);
  *fn = fn->substr(0, pos);
  return ret;
}

int BlobFile::AddManifest(const string &date, uint32_t suffix, int type) {
  FileOS file_os(path_);
  char buf[64];
  snprintf(buf, sizeof(buf), "%s %u %d\n", date.c_str(), suffix, type);
  return file_os.Add("blob.manifest", buf);
}

int BlobFile::DeleteManifest(const string &date) {
  FileOS file_os(path_);
  string data;
  if (file_os.Get("blob.manifest", &data) != 0) {
    UFatal("get blob.manifest failed. %s %d %s ", path_.c_str(), errno, strerror(errno));
    return -1;
  }

  string to_put;
  vector<string> lines;
  Text::Segment(data, '\n', &lines);
  for (size_t i = 0; i < lines.size(); ++i) {
    if (lines[i] != "" && strncmp(lines[i].c_str(), date.c_str(), 8) != 0) {
      to_put += lines[i] + "\n";
    }
  }
  return file_os.Put("blob.manifest", to_put);
}

int BlobFile::DeleteManifest(const string &date, uint32_t suffix) {
  FileOS file_os(path_);
  string data;
  if (file_os.Get("blob.manifest", &data) != 0) {
    return -1;
  }
  char buf[64];
  snprintf(buf, sizeof(buf), "%s %u", date.c_str(), suffix);
  size_t line_pos = data.find(buf);
  if (line_pos == string::npos) {
    UFatal("%s find %s in blob.manifest failed. %s", path_.c_str(), buf, data.c_str());
    return -1;
  }
  size_t line_end_pos = data.find('\n', line_pos);
  if (line_end_pos == string::npos) {
    UFatal("%s find %s in blob.mainfest failed. %s", path_.c_str(), buf, data.c_str());
    return -1;
  }
  string new_data = data.substr(0, line_pos);
  new_data += data.substr(line_end_pos + 1);
  return file_os.Put("blob.manifest", new_data);
}

int BlobFile::ModifyManifestCache(const string &date) {
  FileOS file_os(path_);
  string data;
  if (file_os.Get("blob.manifest", &data) != 0) {
    UFatal("get blob.manifest failed. %s %d %s", path_.c_str(),
           errno, strerror(errno));
    return -1;
  }
  vector<string> lines;
  Text::Segment(data, '\n', &lines);
  data.clear();
  for (size_t i = 0; i < lines.size(); ++i) {
    if (lines[i] != "" && strncmp(lines[i].c_str(), date.c_str(), 8) == 0) {
      lines[i][lines[i].size() - 1] = '0';
    }
    data += lines[i] + "\n";
  }
  return file_os.Put("blob.manifest", data);
}

int BlobFile::ModifyManifestCache(const string &date, uint32_t suffix) {
  FileOS file_os(path_);
  string data;
  if (file_os.Get("blob.manifest", &data) != 0) {
    UFatal("get blob.manifest failed. %s %d %s", path_.c_str(),
           errno, strerror(errno));
    return -1;
  }
  char buf[128];
  snprintf(buf, sizeof(buf), "%s %u", date.c_str(), suffix);
  size_t pos = data.find(string(buf));
  if (pos == string::npos) {
    UFatal("%s can not found %s in %s", path_.c_str(), buf, data.c_str());
    return -1;
  }
  data[pos + string(buf).size() + 1] = '0';
  return file_os.Put("blob.manifest", data);
}

int BlobFile::Get(BlobIndex index, string *data) {
  int fd = 0;
  RdGuard guard(&fds_rwlock_);
  unordered_map<uint32_t, int>::iterator it = fds_.find(index.suffix);
  if (it == fds_.end()) {
    UFatal("%s can not found suffix %u's fd", path_.c_str(), index.suffix);
    return -1;
  } else {
    fd = it->second;
  }
  uint32_t left = index.length + 4;
  off_t offset = index.offset;
  while (left > 0) {
    char buf[65536];
    size_t max_read = (left < 65536)? left : 65536;
    ssize_t count = pread(fd, buf, max_read, offset);
    if (count <= 0 && errno != EINTR) {
      UWarning("%s pread failed. count: %ld, %s, %ld, %u, %u",
               path_.c_str(), count, strerror(errno), index.offset,
               index.length, index.suffix);
      return -1;
    }
    data->append(buf, count);
    left -= count;
    offset += count;
  }
  uint32_t crc32 = crc32c::Value(data->c_str(), data->size() - 4);
  uint32_t crc32_masked = *(uint32_t*)(data->c_str() + data->size() - 4);
  uint32_t crc32_read = crc32c::Unmask(crc32_masked);
  if (crc32 != crc32_read) {
    UWarning("%s crc32 checkout failed. %u %u %lu crc32:%u crc32_read:%u",
             path_.c_str(), index.suffix, index.length, index.offset, crc32, crc32_read);
    return -1;
  }
  return 0;
}

int BlobFile::HandleChange(time_t timestamp) {
  // put_mutex_ is already locked
  fds_rwlock_.Rdlock();
  unordered_map<uint32_t, int>::iterator it = fds_.find(current_suffix_);
  if (it == fds_.end()) {
    fds_rwlock_.Unlock();
    UFatal("%s can not find current suffix %u's fd",
           path_.c_str(), current_suffix_);
    return -1;
  }
  int old_fd = it->second;
  fds_rwlock_.Unlock();
  struct stat st;
  time_t time_now = timestamp, time_last_modify;
  struct tm tm_now, tm_last_modify;
  if (fstat(old_fd, &st) == -1) {
    UFatal("%s fstat old file fd failed. %s", path_.c_str(), strerror(errno));
    return -1;
  }
  if (last_timestamp_ == 0) {
    time_last_modify = st.st_mtime;
  } else {
    time_last_modify = last_timestamp_;
  }

  last_timestamp_ = timestamp;

  localtime_r(&time_now, &tm_now);
  localtime_r(&time_last_modify, &tm_last_modify);

  // time change file
  if (tm_now.tm_mday != tm_last_modify.tm_mday) {
    char date[16];
    snprintf(date, sizeof(date), "%04d%02d%02d", tm_now.tm_year + 1900,
             tm_now.tm_mon + 1, tm_now.tm_mday);
    current_date_ = date;

    char old_date[16];
    snprintf(old_date, sizeof(old_date), "%04d%02d%02d",
             tm_last_modify.tm_year + 1900, tm_last_modify.tm_mon + 1,
             tm_last_modify.tm_mday);

    map<string, vector<uint32_t> >::iterator it = date_files_.find(string(old_date));
    if (it == date_files_.end()) {
      vector<uint32_t> tmp;
      date_files_[string(old_date)] = tmp;
      date_files_[string(old_date)].push_back(current_suffix_);
    } else {
      it->second.push_back(current_suffix_);
    }
    AddManifest(string(old_date), current_suffix_, current_type_);
    if (current_type_ == 1) {
      if (cache_files_.find(old_date) == cache_files_.end()) {
        cache_files_[old_date] = set<uint32_t>();
      }
      cache_files_[old_date].insert(current_suffix_);
    }
    return CreateCurrent();
  }

  if (st.st_size >= max_size_) {
    map<string, vector<uint32_t> >::iterator it = date_files_.find(current_date_);
    if (it == date_files_.end()) {
      vector<uint32_t> tmp;
      date_files_[current_date_] = tmp;
      date_files_[current_date_].push_back(current_suffix_);
    } else {
      it->second.push_back(current_suffix_);
    }
    AddManifest(current_date_, current_suffix_, current_type_);
    if (current_type_ == 1) {
      if (cache_files_.find(current_date_) == cache_files_.end()) {
        cache_files_[current_date_] = set<uint32_t>();
      }
      cache_files_[current_date_].insert(current_suffix_);
    }
    return CreateCurrent();
  }
  return 0;
}

int BlobFile::CreateCurrent() {
  // add old file to date_files_, open new current blob file and update
  // current infomations
  char new_current[128];
  snprintf(new_current, sizeof(new_current), "blob.data.%u",
           (current_suffix_ + 1) % kMaxSuffix);
  FileOS file_os(GetCurrentPath());
  int fd = file_os.Open(new_current, O_RDWR | O_CREAT, 0664);
  if (fd == -1) {
    UFatal("%s open %s failed. %s", path_.c_str(), new_current, strerror(errno));
    return -1;
  }
  fcntl(fd, F_SETFD, 1);

  current_suffix_ = (current_suffix_ + 1) % kMaxSuffix;

  fds_rwlock_.Wrlock();
  fds_[current_suffix_] =fd;
  fds_rwlock_.Unlock();

  current_size_ = 0;
  current_type_ = use_blob_cache_? 1 : 0;

  SetCurrent(new_current, use_blob_cache_? 1 : 0);
  return 0;
}

// 零点换文件会存在的bug
// step-1: last_modify 23:59 99.8
// step-2: time_now 23:59 99.9
// step-3: compare, need not change file
// step-4: write at 00:00 0.1
// when next write, last_modify will be 00:00 0.1, this file will never change
// 记录last_timestamp可以解决部分问题，但是启动的时候是有问题的。
//
// first check whether to change file according to size and date
int BlobFile::Put(const string &data, BlobInfo *info) {
  put_mutex_.Lock();

  info->timestamp = time(NULL);
  if (HandleChange(info->timestamp) != 0) {
    put_mutex_.Unlock();
    return -1;
  }
  int fd = -1;

  fds_rwlock_.Rdlock();
  unordered_map<uint32_t, int>::iterator it = fds_.find(current_suffix_);
  if (it == fds_.end()) {
    fds_rwlock_.Unlock();
    put_mutex_.Unlock();
    UFatal("%s can not found current suffix %u's fd", path_.c_str(), current_suffix_);
    return -1;
  } else {
    fd = it->second;
    fds_rwlock_.Unlock();
  }

  // set BlobInfo first
  info->index.offset = current_size_;
  info->index.length = data.size();
  info->index.suffix = current_suffix_;

  string put_data = data;
  char buf[4];
  uint32_t crc32 = crc32c::Value(data.c_str(), data.size());
  uint32_t masked = crc32c::Mask(crc32);
  memcpy(buf, &masked, 4);
  put_data.append(buf, 4);
  
  off_t write_offset = current_size_;
  current_size_ += put_data.size();
  put_mutex_.Unlock();

  // pwrite to write data to blob data file
  uint32_t left = put_data.size();
  while (left > 0) {
    ssize_t count = pwrite(fd, put_data.c_str() + put_data.size() - left, left, write_offset);
    if (count == -1 && errno != EINTR) {
      UFatal("%s pwrite failed. %s", path_.c_str(), strerror(errno));
      return -1;
    }
    left -= count;
    write_offset += count;
  }
  return 0;
}

int BlobFile::GetReleaseTimeEnd(time_t *tt) {
  string date;
  put_mutex_.Lock();
  if (date_files_.size() == 0) {
    put_mutex_.Unlock();
    return -1;
  } else {
    date = date_files_.begin()->first;
    put_mutex_.Unlock();
  }

  struct tm end;
  int year, month, day;
  char tmp;
  tmp = date[4];
  date[4] = '\0';
  year = atoi(date.c_str());
  date[4] = tmp;
  tmp = date[6];
  date[6] = '\0';
  month = atoi(date.c_str() + 4);
  date[6] = tmp;
  day = atoi(date.c_str() + 6);
  end.tm_year = year - 1900;
  end.tm_mon = month - 1;
  end.tm_mday = day;
  end.tm_hour = 23;
  end.tm_min = end.tm_sec = 59;
  end.tm_isdst = 0;
  if ((*tt = mktime(&end)) == -1) {
    return -1;
  }
  return 0;
}

int BlobFile::GetReleaseTimeRange(time_t *start_p, time_t *end_p) {
  string date;
  put_mutex_.Lock();
  if (date_files_.size() == 0) {
    put_mutex_.Unlock();
    return -1;
  } else {
    date = date_files_.begin()->first;
    put_mutex_.Unlock();
  }

  struct tm start, end;
  int year, month, day;
  char tmp;
  tmp = date[4];
  date[4] = '\0';
  year = atoi(date.c_str());
  date[4] = tmp;
  tmp = date[6];
  date[6] = '\0';
  month = atoi(date.c_str() + 4);
  date[6] = tmp;
  day = atoi(date.c_str() + 6);
  start.tm_year = end.tm_year = year - 1900;
  start.tm_mon = end.tm_mon = month - 1;
  start.tm_mday = end.tm_mday = day;
  start.tm_hour = start.tm_min = start.tm_sec = 0;
  end.tm_hour = 23;
  end.tm_min = end.tm_sec = 59;
  start.tm_isdst = end.tm_isdst = 0;
  if ((*start_p = mktime(&start)) == -1) {
    return -1;
  }
  if ((*end_p = mktime(&end)) == -1) {
    return -1;
  }
  return 0;
}

int BlobFile::ReleaseBlobCache(const string &date, uint32_t suffix) {
  // step-1: copy
  // step-2: modify blob.manifest
  // step-3: replace fd
  // step-4: delete
  uint64_t start = myutil::GetUs();
  char buf[128];
  snprintf(buf, sizeof(buf), "blob.data.%u", suffix);
  string fn = string(buf);
  ssize_t ret = FileOS::CopyEx(cache_path_, fn, path_, fn, copy_limit_);
  if (ret < 0) {
    UFatal("Copy failed. %d %s, %s %s %s", errno, strerror(errno),
           cache_path_.c_str(), path_.c_str(), fn.c_str());
    return -1;
  }

  if (ModifyManifestCache(date, suffix) != 0) {
    return -1;
  }

  FileOS file_os(path_);
  int fd = file_os.Open(fn, O_RDONLY);
  if (fd < 0) {
    UFatal("open %s %s failed. %d %s", path_.c_str(), fn.c_str(),
           errno, strerror(errno));
    return -1;
  }

  fds_rwlock_.Wrlock();
  unordered_map<uint32_t, int>::iterator it = fds_.find(suffix);
  assert(it != fds_.end());
  close(it->second);
  it->second = fd;
  fds_rwlock_.Unlock();

  FileOS cache_os(cache_path_);
  cache_os.Delete(fn);
  UNotice("ReleaseBlobCache %s %s %u done. use:%lums", cache_path_.c_str(),
          date.c_str(), suffix, (myutil::GetUs() - start)/ 1000);
  return 0;
}

// blob cache的转移和leveldb索引没有关系，所以在blob_file中自行管理
// 每次转移1天的文件，具体的转移操作一个文件一个文件的进行，中间出错不影响
// 首先找到所有需要移动的文件
int BlobFile::ReleaseBlobCache(bool force) {
  while ((force && cache_files_.size() > 0) ||
         (int)cache_files_.size() > cache_days_) {
    UTrace("%s ReleaseBlobCache force:%d cached:%lu require:%d", path_.c_str(),
           force, cache_files_.size(), cache_days_);
    set<uint32_t> files;
    string date;
    {
      MutexGuard guard(&put_mutex_);
      map<string, set<uint32_t> >::iterator it = cache_files_.begin();
      if (it == cache_files_.end()) {
        return 0;
      }
      files = it->second;
      date = it->first;
      cache_files_.erase(it);
    }

    for (set<uint32_t>::iterator it = files.begin(); it != files.end(); ++it) {
      if (ReleaseBlobCache(date, *it) != 0) {
        UFatal("%s release %s %u failed.", path_.c_str(), date.c_str(), *it);
        return -1;
      } else {
        UTrace("%s release %s %u", path_.c_str(), date.c_str(), *it);
        if(force) // force only process 1 file
          return 0;
      }
    }
    if (force) {
      break;
    }
  }
  return 0;
}

// release until date
int BlobFile::Release(const string &date) {
  uint64_t start = myutil::GetUs();
  UWarning("%s ReleaseCache until %s(included)", path_.c_str(), date.c_str());
  vector<pair<uint32_t, int> > files;
  while (date_files_.size() > 0) {
    MutexGuard guard(&put_mutex_);
    map<string, vector<uint32_t> >::iterator map_it = date_files_.begin();
    if (strcmp(map_it->first.c_str(), date.c_str()) <= 0) {
      string delete_date = map_it->first;
      UWarning("%s ReleaseCache date %s", path_.c_str(), delete_date.c_str());
      vector<uint32_t> suffixs = map_it->second;
      date_files_.erase(map_it);

      DeleteManifest(delete_date);

      for (size_t i = 0; i < suffixs.size(); ++i) {
        // step 1: delete and close fd
        // step 2: delete blob data file
        fds_rwlock_.Wrlock();
        unordered_map<uint32_t, int>::iterator it = fds_.find(suffixs[i]);
        if (it == fds_.end()) {
          UFatal("%s can not find suffix %d's fd", path_.c_str(), suffixs[i]);
        } else {
          close(it->second);
          fds_.erase(it);
        }
        fds_rwlock_.Unlock();

        int file_type = 0;
        map<string, set<uint32_t> >::iterator cache_it = cache_files_.find(date);
        if (cache_it != cache_files_.end()) {
          if (cache_it->second.find(suffixs[i]) != cache_it->second.end()) {
            file_type = 1;
            cache_it->second.erase(suffixs[i]);
            if (cache_it->second.empty()) {
              cache_files_.erase(cache_it);
            }
          }
        }
        files.push_back(pair<uint32_t, int>(suffixs[i], file_type));
      }
    } else {
      break;
    }
  }

  // delete files
  for (size_t i = 0; i < files.size(); ++i) {
    uint64_t start;
    char fn[64];
    snprintf(fn, sizeof(fn), "blob.data.%u", files[i].first);
    if (files[i].second == 0) {
      FileOS file_os(path_);
      file_os.Delete(fn);
    } else {
      FileOS file_os(cache_path_);
      file_os.Delete(fn);
    }
    UTrace("%s delete %s use:%lums", path_.c_str(), fn, (myutil::GetUs() - start) / 1000);
  }
  UNotice("Release util %s done. use:%lums", date.c_str(), (myutil::GetUs() - start) / 1000);
  return 0;
}

void BlobFile::DumpDateFiles() {
  map<string, vector<uint32_t> >::iterator it = date_files_.begin();
  for (; it != date_files_.end(); ++it) {
    for (size_t i = 0; i < it->second.size(); ++i) {
      UDebug("date_files %s %u", it->first.c_str(), it->second[i]);
    }
  }
}

void BlobFile::DumpCacheFiles() {
  map<string, set<uint32_t> >::iterator it = cache_files_.begin();
  for (; it != cache_files_.end(); ++it) {
    for (set<uint32_t>::iterator sit = it->second.begin(); sit != it->second.end(); ++sit) {
      UDebug("cache_files %s %u", it->first.c_str(), *sit);
    }
  }
}
} }  // namespace ganji::pic_server
