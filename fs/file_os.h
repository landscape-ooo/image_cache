#ifndef _GANJI_PIC_SERVER_FS_FILE_OS_H_
#define _GANJI_PIC_SERVER_FS_FILE_OS_H_
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <assert.h>
#include <string>
#include <vector>
using std::string;
using std::vector;
// os file operation, include NFS and local file system
// set root dir, and all operations will be done based on root_
// the api below all will create dir when needed
namespace ganji { namespace pic_server {
class FileGuard {
 public:
  FileGuard(int fd) {
    fd_ = fd;
    is_fd_ = true;
  }
  FileGuard(FILE *fp) {
    fp_ = fp;
    is_fd_ = false;
  }
  ~FileGuard() {
    if (is_fd_) {
      close(fd_);
    } else {
      fclose(fp_);
    }
  }

 private:
  FileGuard(const FileGuard&);
  void operator=(const FileGuard&);
  int fd_;
  FILE *fp_;
  bool is_fd_;
};

class FileOS {
 public:
  FileOS(const string root = "./") : root_(root) { }
  int Open(const string &path_i, int flags, mode_t mode);
  int Open(const string &path_i, int flags);
  int Get(const string &url, string *data);
  int Put(const string &dest, const string &data);
  int Move(const string &src, const string &dest, bool del_src = true);
  int Delete(const string &url);
  int Copy(const string &src, const string &dest);
  int Add(const string &url, const string &data);
  int Mkdir(const string &path_i, mode_t mode);

  // 限速copy, limit M/s
  static ssize_t CopyEx(const string &srcpath, const string &srcfile,
                        const string &destpath, const string &destfile,
                        int limit);

  // find all files in root_ that match pcre regex(pattern)
  int Find(const string &pattern, vector<string> *pret, string *preason);

  string Url(const string &url) {
    if (url[0] == '/') {
      return url;
    }
    return root_ + string("/") + url;
  }

  FILE* Fopen(const string &path, const char *mode);

  static uint64_t GetMs() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (uint64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
  }

 private:
  int Mkdir_p(const string &path, mode_t mode = 0755);
  int NewUrl(const string &old_url, string *new_url) {
    if (old_url.size() == 0) {
      return -1;
    }
    if (old_url[0] == '/') {
      *new_url = old_url;
    } else {
      *new_url = root_ + string("/") + old_url;
    }
    return 0;
  }

  string root_;
};
} }  // namespace pic::fs
#endif
