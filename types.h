#ifndef _GANJI_PIC_SERVER_TYPES_H_
#define _GANJI_PIC_SERVER_TYPES_H_
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <errno.h>
#include <string.h>
#include <string>
#include <vector>
#include <map>
#include "ganji/util/text/text.h"
using std::string;
using std::vector;
using std::map;
namespace Text = ganji::util::text::Text;
namespace ganji { namespace pic_server {
struct Time {
  int hour;
  int minute;
  int second;
  string time_str;
  Time(const string &time_str_in) : time_str(time_str_in) {
    string reason;
    if (Init(&reason) != 0) {
      fprintf(stderr, "init time failed. %s\n", reason.c_str());
      exit(-1);
    }
  }
  Time() : time_str("00:00:00") {
    string reason;
    if (Init(&reason) != 0) {
      fprintf(stderr, "init time failed. %s\n", reason.c_str());
      exit(-1);
    }
  }

  // next UTC seconds
  time_t Next() {
    struct tm ctm;
    time_t tt;
    time(&tt);
    localtime_r(&tt, &ctm);
    ctm.tm_sec = second;
    ctm.tm_min = minute;
    ctm.tm_hour = hour;
    time_t ntt = mktime(&ctm);
    if (ntt < tt) {
      ntt = ntt + 3600 * 24;
    }
    return ntt;
  }

  // next UTC with beginning date
  time_t Next(const string &date) {
    if (date.size() != 8) {
      return -1;
    }
    struct tm ctm;
    time_t tt;
    time(&tt);
    localtime_r(&tt, &ctm);
    ctm.tm_sec = second;
    ctm.tm_min = minute;
    ctm.tm_hour = hour;
    ctm.tm_year = atoi(date.substr(0, 4).c_str()) - 1900;
    ctm.tm_mon = atoi(date.substr(4, 2).c_str()) - 1;
    ctm.tm_mday = atoi(date.substr(6, 2).c_str());
    time_t ntt = mktime(&ctm);
    return ntt + (time_t)3600 * (time_t)24;
  }

  // next UTC days after beginning date 
  time_t Next(const string &date, int days) {
    time_t ntt = Next(date);
    return ntt + (time_t)3600 * (time_t)24 * (time_t)days;
  }

  time_t Next(int days) {
    time_t ntt = Next();
    return ntt + (time_t)3600 * (time_t)24 * (time_t)days;
  }

  // seconds to next time point
  time_t ToNext() {
    struct tm ctm;
    time_t tt;
    time(&tt);
    localtime_r(&tt, &ctm);
    ctm.tm_sec = second;
    ctm.tm_min = minute;
    ctm.tm_hour = hour;
    time_t ntt = mktime(&ctm);
    if (ntt < tt) {
      ntt = ntt + 3600 * 24;
    }
    return ntt - tt;
  }

  int Init(string *reason) {
    vector<string> segs;
    Text::Segment(time_str, ':', &segs);
    if (segs.size() != 3) {
      *reason = time_str + string("time config error. HH:MM:SS");
      return -1;
    }
    hour = atoi(segs[0].c_str());
    if (!(hour >= 0 && hour <= 23)) {
      *reason = time_str + string("hour should in [0, 23]");
      return -1;
    }
    minute = atoi(segs[1].c_str());
    if (!(minute >= 0 && minute <= 59)) {
      *reason = time_str + string("minute should in [0, 59]");
      return -1;
    }
    second = atoi(segs[2].c_str());
    if (!(second >= 0 && second <= 59)) {
      *reason = time_str + string("second should in [0, 59]");
      return -1;
    }
    return 0;
  }
};

struct BlobIndex {
  off_t offset;
  uint32_t length;
  uint32_t suffix;
};

struct BlobInfo {
  struct BlobIndex index;
  time_t timestamp;
};

struct LevelDBCacheInfo {
  size_t lru_capacity;
  off_t blob_file_size;
  float blob_disk_reserve;
  Time release_point;
  int cache_days;
  string leveldb_path;
  string blob_path;
  string blob_cache_path;
  int blob_cache_days;
  float blob_cache_disk_reserve;
  int copy_limit;
  bool use_blob_cache;
};

struct ScribeLoggerInfo {
  string ip;
  int port;
  int connect_timeout;
  int send_timeout;
  int recv_timeout;
};

struct LocalLoggerInfo {
  string path;
  int max_size;
};

struct LogItem {
  LogItem() {
    url = "";
    bytes = 0;
    status = 404;
    hit_cache = 0;
    leveldb_time = 0;
    cgi_time = 0;
    put_time = 0;
    referer = "";
  }

  string AsString() {
    char buf[4096];
    snprintf(buf, sizeof(buf), "url:%s, status:%d, bytes:%d, hit_cache:%d, leveldb_time:%.6f, cgi_time:%.6f, put_time:%.6f, referer:%s", url.c_str(), status, bytes, hit_cache, ((float)leveldb_time) / 1000000, ((float)cgi_time) / 1000000, ((float)put_time) / 1000000, referer.c_str());
    return string(buf);
  }

  string url;
  int status;
  int bytes;
  int hit_cache;
  int leveldb_time;
  int cgi_time;
  int put_time;
  string referer;
};
} }  // namespace ganji::pic_server
#endif
