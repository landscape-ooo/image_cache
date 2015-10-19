#ifndef _GANJI_PIC_SERVER_LOG_REPLAY_LOG_READER_H_
#define _GANJI_PIC_SERVER_LOG_REPLAY_LOG_READER_H_
#include <stdio.h>
#include <stdlib.h>
#include <pcre.h>
#include <string>
#include <vector>
#include <set>
#include <utility>
#include "mutex.h"
using std::string;
using std::vector;
using std::set;
using std::pair;

namespace ganji { namespace pic_server {
struct LogItem {
  string log_time;
  string path;
  string status;
  string hit;
};

class LogItemCmp {
 public:
  bool operator()(const pair<struct LogItem, int> &lh,
                  const pair<struct LogItem, int> &rh) const {
    int ret = strcmp(lh.first.log_time.c_str(), rh.first.log_time.c_str());
    if (ret != 0) {
      return ret < 0;
    }
    ret = strcmp(lh.first.path.c_str(), rh.first.path.c_str());
    if (ret != 0) {
      return ret < 0;
    }
    return lh.second < rh.second;
  }
};

class File {
 public:
  File(const string &root, const string &fn, const string &pattern);
  ~File();
  int Next(struct LogItem *pItem);
 private:
  string root_;
  string fn_;
  string pattern_;
  pcre *p_;
  pcre_extra *extra_;
  FILE *fp_;
};

class FileMgr {
 public:
  int Init(const string &root,
           const vector<string> &dirs,
           const string &fn_pattern,
           const string &log_pattern);
  int ReInit(const string &root,
             const vector<string> &dirs,
             const string &fn_pattern,
             const string &log_pattern);
  FileMgr();
  ~FileMgr();
  int Next(struct LogItem *pItem);
  int SetSysTime(const string &log_time);

 public:
  string root_;
  vector<string> dirs_;
  string fn_pattern_;
  string item_pattern_;
  vector<vector<string> > dir_files_;
  vector<int> dir_file_index_;
  vector<File*> dir_Files_;
  set<pair<struct LogItem, int>, LogItemCmp> items_;

  Mutex mutex_;
  string cur_time_;
};
} }  // namespace ganji::pic_server
#endif
