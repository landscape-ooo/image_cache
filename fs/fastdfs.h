#ifndef _GANJI_PIC_SERVER_FS_FASTDFS_H_
#define _GANJI_PIC_SERVER_FS_FASTDFS_H_
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <map>
#include <utility>
#include <tr1/unordered_map>
#include <fastdfs/fdfs_client.h>
#include <fastdfs/fdfs_global.h>
#include <fastcommon/logger.h>
using std::string;
using std::map;
using std::pair;
using std::tr1::unordered_map;

namespace ganji { namespace pic_server {
// use long connection, when network error occured, fastdfs will close
// connection automatically, so we need to check if sock in
// TrackerServerInfo is -1, if so, reconnect
// This file is strongly bind to fastdfs source code, so be carefully if
// you monidify it, remember to refer to fastdfs source code
class FastDFS {
 public:
  // init tracker server group and get one tracker server
  FastDFS() {
    conf_file_ = "";
  }
  int Init(const string &conf_file);
  ~FastDFS();
  int Get(const string &url, string *data);
  int Get(const string &url, string *data, map<string, string> *meta);
  int SetMeta(const string &url, const map<string, string> &mata);
  int GetMeta(const string &url, map<string, string> *meta);
  int Put(const string &data, const string &ext, string *url);
  int Put(const string &data,
          const string &ext,
          const map<string, string> &meta,
          string *url);
  int PutByFile(const string &file, string *url);

  int PutByFile(const string &file,
                const map<string, string> &meta,
                string *url);
  int Delete(const string &url);
  int GetFileInfo(const string &fileid, FDFSFileInfo *pInfo) {
    return fdfs_get_file_info_ex1(fileid.c_str(), false, pInfo);
  }
  static int LogInit() {
    if (!log_inited_) {
      log_inited_ = true;
      log_init();
    }
    return 0;
  }
 private:
  int ConnectTracker();
  int GetStorageConnection(TrackerServerInfo &storage,
                           TrackerServerInfo **storage_pp);
  string GenString(const char *ip, int port);
  // upload
  int GetStorageStore(TrackerServerInfo **storage_pp,
                      int *store_path_index);

  // download and get meta
  int GetStorageFetch(const string &url, TrackerServerInfo **storage_pp);

  // delete and set meta
  int GetStorageUpdate(const string &url, TrackerServerInfo **storage_pp);
  TrackerServerGroup tracker_group_;
  TrackerServerInfo *tracker_;
  unordered_map<string, TrackerServerInfo*> storages_;
  int count_;
  int interval_;
  string conf_file_;

  static bool log_inited_;
};
} }  // namespace ganji::pic_server
#endif
