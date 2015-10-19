#include <time.h>
#include <sys/time.h>
#include <errno.h>
#include <string.h>
#include "ulog.h"
#include "log_replay/log_reader.h"
#include "fs/file_os.h"
#include "mutex.h"
namespace ganji { namespace pic_server {
File::File(const string &root, const string &fn, const string &pattern) {
  root_ = root;
  fn_ = fn;
  pattern_ = pattern;
  FileOS file_os(root_);
  fp_ = file_os.Fopen(fn, "r");
  if (fp_ == NULL) {
    UFatal("open file %s failed.", fn.c_str());
    abort();
  }

  const char *errptr;
  int erroffset;
  p_ = pcre_compile(pattern_.c_str(), 0, &errptr, &erroffset, NULL);
  if (p_ == NULL) {
    fprintf(stderr, "pcre_compile failed. %s. %s\n", pattern_.c_str(), errptr);
    abort();
  }

  extra_ = pcre_study(p_, 0, &errptr);
  if (errptr != NULL) {
    fprintf(stderr, "pcre_study failed. %s\n", errptr);
    abort();
  }
}

File::~File() {
  fclose(fp_);
  pcre_free(p_);
  pcre_free(extra_);
}

int File::Next(struct LogItem *pItem) {
  char *line = NULL;
  size_t n = 0;
  ssize_t len;
  while ((len = getline(&line, &n, fp_)) != -1) {
    int ovector[60];
    if (line[len - 1] == '\n') {
      line[len - 1] = '\0';
      len--;
    }
    int ret = pcre_exec(p_, extra_, line, len, 0, 0, ovector, 60);
    if (ret < 0) {
      free(line);
      line = NULL;
      continue;
    } else {
      const char *named_string = NULL;
      int get_ret = pcre_get_named_substring(p_, line, ovector, ret,
                                             "path", &named_string);
      if (get_ret < 0) {
        fprintf(stderr, "no substring named path found\n");
        abort();
      }
      pItem->path = string(named_string, get_ret);
      pcre_free_substring(named_string);
      named_string = NULL;

      get_ret = pcre_get_named_substring(p_, line, ovector, ret,
                                         "status", &named_string);
      if (get_ret < 0) {
        fprintf(stderr, "no substring named status found\n");
        abort();
      }
      pItem->status = string(named_string, get_ret);
      pcre_free_substring(named_string);
      named_string = NULL;

      get_ret = pcre_get_named_substring(p_, line, ovector, ret,
                                         "log_time", &named_string);
      if (get_ret < 0) {
        fprintf(stderr, "no substring named log_time found\n");
        abort();
      }
      pItem->log_time = string(named_string, get_ret);
      pcre_free_substring(named_string);
      named_string = NULL;

      get_ret = pcre_get_named_substring(p_, line, ovector, ret,
                                         "hit", &named_string);
      if (get_ret < 0) {
        fprintf(stderr, "no substring named log_time found\n");
        abort();
      }
      pItem->hit = string(named_string, get_ret);
      pcre_free_substring(named_string);
      named_string = NULL;
      free(line);
      line = NULL;
      return 0;
    }
  }
  if (line) {
    free(line);
  }
  return -1;
}

FileMgr::FileMgr() {
}

// find all files in all dirs and init vars
int FileMgr::Init(const string &root,
                  const vector<string> &dirs,
                  const string &fn_pattern,
                  const string &item_pattern) {
  root_ = root;
  dirs_ = dirs;
  fn_pattern_ = fn_pattern;
  item_pattern_ = item_pattern;
  
  // find all files in all dirs
  for (size_t i = 0; i < dirs_.size(); ++i) {
    FileOS file_os(root_ + string("/") + dirs_[i]);
    string errmsg;
    vector<string> files;
    if (file_os.Find(fn_pattern_, &files, &errmsg) != 0) {
      UFatal("find %s failed. %s", fn_pattern_.c_str(), errmsg.c_str());
      return -1;
    } else {
      for (size_t j = 0; j < files.size(); ++j) {
        UFatal("%s/%s %s --> %s", root_.c_str(), dirs_[i].c_str(),
              fn_pattern_.c_str(), files[j].c_str());
      }
    }
    dir_files_.push_back(files);
    if (files.size() > 0) {
      dir_file_index_.push_back(0);
      File *pFile = new File(root_ + string("/") + dirs_[i], files[0], item_pattern_);
      dir_Files_.push_back(pFile);
      struct LogItem log_item;
      // TODO BUG
      //-----------------------------
      int dir_index = i;
      while (dir_Files_[dir_index]->Next(&log_item) != 0) {
        delete dir_Files_[dir_index];
        dir_Files_[dir_index] = NULL;
        if (dir_file_index_[dir_index] + 1 >= (int)dir_files_[dir_index].size()) {
          dir_file_index_[dir_index] = -1;
          break;
        } else {
          dir_file_index_[dir_index]++;
          File *pFile = new File(root_ + string("/") + dirs_[dir_index],
                                 dir_files_[dir_index][dir_file_index_[dir_index]],
                                 item_pattern_);
          dir_Files_[dir_index] = pFile;
        }
      }
      if (dir_file_index_[dir_index] != -1) {
        items_.insert(pair<struct LogItem, int>(log_item, dir_index));
      }
    } else {
      dir_file_index_.push_back(-1);
      dir_Files_.push_back(NULL);
    }
  }

  // set cur_time_
  set<pair<struct LogItem, int>, LogItemCmp>::iterator it = items_.begin();
  if (it == items_.end()) {
    UFatal("items_.begin() == items_.end())");
    return -1;
    // abort();
  }
  cur_time_ = it->first.log_time;
  SetSysTime(cur_time_);
  return 0;
}

int FileMgr::ReInit(const string &root,
                    const vector<string> &dirs,
                    const string &fn_pattern,
                    const string &log_pattern) {
  MutexGuard guard(&mutex_);
  for (size_t i = 0; i < dir_Files_.size(); ++i) {
    if (dir_Files_[i]) {
      delete dir_Files_[i];
    }
  }
  dir_files_.clear();
  dir_file_index_.clear();
  dir_Files_.clear();
  items_.clear();
  return Init(root, dirs, fn_pattern, log_pattern);
}

FileMgr::~FileMgr() {
  for (size_t i = 0; i < dir_Files_.size(); ++i) {
    if (dir_Files_[i]) {
      delete dir_Files_[i];
    }
  }
}

// log_time: 02-18 15:14:13
int FileMgr::SetSysTime(const string &log_time) {
  return 0;
  time_t ctt;
  struct tm ctm;
  time(&ctt);
  localtime_r(&ctt, &ctm);
  int month = atoi(log_time.substr(0, 2).c_str());
  int mday = atoi(log_time.substr(3, 2).c_str());
  int hour = atoi(log_time.substr(6, 2).c_str());
  int minute = atoi(log_time.substr(9, 2).c_str());
  int second = atoi(log_time.substr(12, 2).c_str());

  ctm.tm_mon = month - 1;
  ctm.tm_mday = mday;
  ctm.tm_hour = hour;
  ctm.tm_min = minute;
  ctm.tm_sec = second;

  time_t cur = mktime(&ctm);
  if (cur == (time_t)-1) {
    fprintf(stderr, "mktime failed. %s\n", strerror(errno));
    abort();
  }

  struct timeval tv;
  tv.tv_sec = cur;
  tv.tv_usec = 0;
  if (settimeofday(&tv, NULL) != 0) {
    fprintf(stderr, "settimeofday failed. %s\n", strerror(errno));
    abort();
  }
  return 0;
}

int FileMgr::Next(struct LogItem *pItem) {
  MutexGuard guard(&mutex_);
  if (items_.size() == 0) {
    return -1;
  }
  set<pair<struct LogItem, int>, LogItemCmp>::iterator it = items_.begin();
  string log_time = it->first.log_time;
  if (log_time != cur_time_) {
    SetSysTime(log_time);
    cur_time_ = log_time;
  }
  *pItem = it->first;
  UTrace("download_url: %s", pItem->path.c_str());

  int dir_index = it->second;
  items_.erase(it);

  struct LogItem log_item;
  while (dir_Files_[dir_index]->Next(&log_item) != 0) {
    delete dir_Files_[dir_index];
    dir_Files_[dir_index] = NULL;
    if (dir_file_index_[dir_index] + 1 >= (int)dir_files_[dir_index].size()) {
      dir_file_index_[dir_index] = -1;
      return 0;
    } else {
      dir_file_index_[dir_index]++;
      File *pFile = new File(root_ + string("/") + dirs_[dir_index],
                             dir_files_[dir_index][dir_file_index_[dir_index]],
                             item_pattern_);
      dir_Files_[dir_index] = pFile;
    }
  }
  items_.insert(pair<struct LogItem, int>(log_item, dir_index));
  return 0;
}
} }  // namespace ganji::pic_server
