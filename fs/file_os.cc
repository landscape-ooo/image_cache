#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <pcre.h>
#include "fs/file_os.h"
#include "myutil.h"
namespace ganji { namespace pic_server {
// will never mkdir or create new file
int FileOS::Get(const string &url, string *pdata) {
  assert(pdata != NULL);
  string &data = *pdata;
  string new_path;
  if (NewUrl(url, &new_path) != 0) {
    return -1;
  }
  FILE *fp = fopen(new_path.c_str(), "rb");
  if (fp == NULL) {
    return -1;
  } else {
    char buf[65536];
    int read_len = 0;
    while ((read_len = fread(buf, 1, sizeof(buf), fp)) > 0) {
      data.append(buf, read_len);
    }
    fclose(fp);
    return 0;
  }
}

// mkdir when needed
int FileOS::Put(const string &dest, const string &data) {
  FILE *fp = Fopen(dest, "wb");
  if (fp == NULL) {
    return -1;
  }
  size_t write_len = 0;
  while (write_len != data.size()) {
    write_len += fwrite(data.c_str() + write_len, 1, data.size() - write_len, fp);
  }
  fclose(fp);
  return 0;
}

// mkdir when needed
int FileOS::Add(const string &dest, const string &data) {
  FILE *fp = Fopen(dest, "ab");
  if (fp == NULL) {
    return -1;
  }
  size_t write_len = 0;
  while (write_len != data.size()) {
    write_len += fwrite(data.c_str() + write_len, 1, data.size() - write_len, fp);
  }
  fclose(fp);
  return 0;
}

// try to create dir when needed
int FileOS::Move(const string &src, const string &dest, bool del_src) {
  string new_dest, new_src;
  if (NewUrl(dest, &new_dest) != 0 || NewUrl(src, &new_src) != 0) {
    return -1;
  }
  if (rename(new_src.c_str(), new_dest.c_str()) != 0) {
    struct stat buf;
    if (stat(new_src.c_str(), &buf) != 0) {
      return -1;
    }
    char *slash = strrchr(new_dest.c_str(), '/');
    if (slash == new_dest.c_str()) {
      return -1;
    }
    *slash = '\0';
    if (Mkdir_p(new_dest) != 0) {
      return -1;
    }
    *slash = '/';
    if (rename(new_src.c_str(), new_dest.c_str()) != 0) {
      return -1;
    }
    return 0;
  }
  return 0;
}

int FileOS::Delete(const string &url) {
  string path;
  if (NewUrl(url, &path) != 0) {
    return -1;
  }
  if (remove(path.c_str()) == 0) {
    return 0;
  } else {
    return -1;
  }
}

int FileOS::Mkdir(const string &path_i, mode_t mode) {
  // if path_i is dir name, must end with /
  // if path_i is file name(not ended by /), Mkdir will split pathname and
  // filename by the last /
  // Mkdir return 0 if the dir eventually exists
  string path;
  if (NewUrl(path_i, &path) != 0) {
    return -1;
  }
  struct stat st;
  char *slash = strrchr(path.c_str(), '/');
  if (slash != NULL && slash != path.c_str()) {
    *slash = '\0';
    if (stat(path.c_str(), &st) != 0) {
      if (Mkdir_p(path.c_str()) != 0) {
        return -1;
      }
    }
    *slash = '/';
  }
  return 0;
}

int FileOS::Mkdir_p(const string &path_i, mode_t mode) {
  // Mkdir_p return 0 if path_i eventually exists
  string path = path_i;
  struct stat buf;
  int size = path.size();
  // state 0 looking for subpath. 1 subpath found
  for (int i = 0; i < size; ++i) {
    if (path[i] == '/') {
      while (path[++i] == '/') {
      }
      if (i != 1) {
        path[i - 1] = '\0';
        if (stat(path.c_str(), &buf) != 0) {
          if (mkdir(path.c_str(), mode) != 0) {
            return -1;
          }
        }
        path[i - 1] = '/';
      }
    }
  }
  if (stat(path.c_str(), &buf) != 0) {
    if (mkdir(path.c_str(), mode) != 0) {
      return -1;
    }
  }
  return 0;
}

FILE* FileOS::Fopen(const string &path_i, const char *mode) {
  if (Mkdir(path_i, 0755) != 0) {
    return NULL;
  }
  string path;
  if (NewUrl(path_i, &path) != 0) {
    return NULL;
  }
  return fopen(path.c_str(), mode);
}

int FileOS::Open(const string &path_i, int flags, mode_t mode) {
  if (flags & O_CREAT) {
    if (Mkdir(path_i, 0755) != 0) {
      return -1;
    }
  }
  string path;
  if (NewUrl(path_i, &path) != 0) {
    return -1;
  }
  return open(path.c_str(), flags, mode);
}

int FileOS::Open(const string &path_i, int flags) {
  if (flags & O_CREAT) {
    if (Mkdir(path_i, 0755) != 0) {
      return -1;
    }
  }
  string path;
  if (NewUrl(path_i, &path) != 0) {
    return -1;
  }
  return open(path.c_str(), flags);
}

int FileOS::Copy(const string &src_i, const string &dest_i) {
  string src, dest;
  if (NewUrl(src_i, &src) != 0 || NewUrl(dest_i, &dest) != 0) {
    return -1;
  }
  string data;
  if (Get(src, &data) != 0) {
    return -1;
  }
  if (Put(dest, data) != 0) {
    return -1;
  }
  return 0;
}

int FileOS::Find(const string &pattern, vector<string> *pret, string *preason) {
  pret->clear();
  preason->clear();
  pcre *p;
  pcre_extra *extra;
  const char *errptr;
  int erroffset;
  int ret_code = 0;
  int count;
  p = pcre_compile(pattern.c_str(), 0, &errptr, &erroffset, NULL);
  if (p == NULL) {
    *preason = string("pcre_compile failed. ") + string(errptr);
    ret_code = -1;
    goto free_pcre_res;
  }
  extra = pcre_study(p, 0, &errptr);
  if (errptr != NULL) {
    *preason = string("pcre_study failed. ") + string(errptr);
    ret_code = -1;
    goto free_pcre_res;
  }

  DIR *p_dir;
  p_dir = opendir(root_.c_str());
  if (p_dir == NULL) {
    *preason = strerror(errno);
    ret_code = -1;
    closedir(p_dir);
    goto free_pcre_res;
  }
  struct dirent **ent_list;
  count = scandir(root_.c_str(), &ent_list, NULL, alphasort);
  if (count < 0) {
    *preason = strerror(errno);
    closedir(p_dir);
    ret_code = -1;
    goto free_pcre_res;
  }

  for (int i = 0; i < count; ++i) {
    int ovector[60];
    int ret = pcre_exec(p, extra, ent_list[i]->d_name,
                        strlen(ent_list[i]->d_name), 0, 0, ovector, 60);
    if (!(ret < 0)) {
      pret->push_back(string(ent_list[i]->d_name));
    }
    free(ent_list[i]);
  }
  free(ent_list);

  closedir(p_dir);

  // open dir and scan files
free_pcre_res:
  pcre_free(p);
  pcre_free(extra);
  return ret_code;
}

// 每次尝试读写64K数据
ssize_t FileOS::CopyEx(const string &srcpath, const string &srcfile,
                       const string &destpath, const string &destfile,
                       int limit) {
  ssize_t total = 0;
  string srcfull = srcpath + "/" + srcfile;
  string destfull = destpath + "/" + destfile;
  int src = open(srcfull.c_str(), O_RDONLY);
  if (src == -1) {
    // fprintf(stderr, "open %s failed. %d %s\n", srcfull.c_str(), errno, strerror(errno));
    return -1;
  }
  FileGuard srcGuard(src);
  int dest = open(destfull.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
  if (dest == -1) {
    // fprintf(stderr, "open %s failed. %d %s\n", destfull.c_str(), errno, strerror(errno));
    return -1;
  }
  FileGuard destGuard(dest);
  // 每次64K
  char buf[65536];
  size_t buf_size = 0;

  // sleep 10ms
  int bytems = limit * 1024 * 1024 / 1000;
  bool eof = false;
  while (true) {
    if (!eof) {
      ssize_t read_count = read(src, buf + buf_size, sizeof(buf) - buf_size);
      if (read_count < 0) {
        return -1;
      } else if (read_count == 0) {
        eof = true;
      } else {
        buf_size += read_count;
        total += read_count;
      }
    }
    if (buf_size == 0) {
      break;
    } else {
      ssize_t write_count = write(dest, buf, buf_size);
      if (write_count < 0) {
        return -1;
      } else {
        if (write_count != (ssize_t)buf_size) {
          memmove(buf, buf + write_count, buf_size - write_count);
          buf_size -= write_count;
        } else {
          buf_size = 0;
          if (eof) {
            break;
          }
        }
        int sleep_ms = (sizeof(buf) - buf_size) / bytems;
        if (sleep_ms == 0) {
          sleep_ms = 1;
        }
        myutil::DoSleep(sleep_ms);
        continue;
      }
    }
  }
  return total;
}
} }  // namespace ganji::pic_server
