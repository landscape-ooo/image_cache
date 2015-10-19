#include <stdlib.h>
#include <pthread.h>
#include "fs/fastdfs.h"
#include "fs/file_os.h"
#include "myutil.h"
#include "ulog.h"
using ganji::pic_server::myutil::DoSleep;
extern pthread_mutex_t g_fdfs_init_mutex;
// 包装fastdfs的长连接，对于出现网络错误，fastdfs会将连接关掉，并设置
// sock = -1。所以，每次获取缓存的连接的时候，都要判断sock是否为-1来确定
// 此连接是否仍然有效
namespace ganji { namespace pic_server {
bool FastDFS::log_inited_ = false;
inline string FastDFS::GenString(const char *ip, int port) {
  char buf[128];
  snprintf(buf, sizeof(buf), "%s-%d", ip, port);
  return string(buf);
}

int FastDFS::Init(const string &conf_file) {
  conf_file_ = conf_file;
  count_ = 0;
  string file_content;
  FileOS file_os("./");
  if (file_os.Get(conf_file, &file_content) != 0) {
    Fatal("get %s content failed. %s", conf_file.c_str(), strerror(errno));
    return -1;
  }
  if (pthread_mutex_lock(&g_fdfs_init_mutex) != 0) {
    Fatal("lock g_fdfs_init_mutex failed.");
    abort();
  }
  // int result = fdfs_client_init_ex(&tracker_group_, conf_file.c_str());
  int result = fdfs_client_init_from_buffer_ex(&tracker_group_,
                                               file_content.c_str());
  if (pthread_mutex_unlock(&g_fdfs_init_mutex) != 0) {
    Fatal("unlock g_fdfs_init_mutex failed");
    abort();
  }
  if (result != 0) {
    Fatal("init fdfs client failed");
    exit(-1);
  }

  Warning("using long connection fastdfs...");
  return ConnectTracker();
}

inline int FastDFS::ConnectTracker() {
  tracker_group_.server_index = random() % tracker_group_.server_count;
  tracker_ = tracker_get_connection_ex(&tracker_group_);
  if (tracker_ == NULL) {
    Fatal("get connection to tracker failed");
    return -1;
  }
  return 0;
}

// quit connections and destroy client
FastDFS::~FastDFS() {
  fdfs_quit(tracker_);
  tracker_disconnect_server(tracker_);
  unordered_map<string, TrackerServerInfo*>::iterator it = storages_.begin();
  while (it != storages_.end()) {
    fdfs_quit(it->second);
    tracker_disconnect_server(it->second);
    free(it->second);
    ++it;
  }
  fdfs_client_destroy_ex(&tracker_group_);
}

// query tracker and get storage server to upload
int FastDFS::GetStorageStore(TrackerServerInfo **storage_pp,
                             int *store_path_index) {
  TrackerServerInfo storage;
  if (tracker_->sock == -1 && ConnectTracker() != 0) {
    return -1;
  }
  int ret = tracker_query_storage_store(tracker_,
                                        &storage,
                                        store_path_index);
  if (ret != 0) {
    return -1;
  }
  return GetStorageConnection(storage, storage_pp);
}

int FastDFS::GetStorageConnection(TrackerServerInfo &storage,
                                  TrackerServerInfo **storage_pp) {
  unordered_map<string, TrackerServerInfo*>::iterator it =
    storages_.find(GenString(storage.ip_addr, storage.port));
  if (it != storages_.end()) {
    TrackerServerInfo *storage_server_ptr = it->second;
    if (storage_server_ptr->sock == -1) {
      // connection invalid, delete it and then reconnect
      free(storage_server_ptr);
      storages_.erase(it);
    } else {
      *storage_pp = storage_server_ptr;
      return 0;
    }
  }

  // connect and insert to storages_
  int ret = tracker_connect_server(&storage);
  if (ret != 0) {
    fdfs_quit(&storage);
    tracker_disconnect_server(&storage);
    return -1;
  }
  TrackerServerInfo *storage_ptr =
    (TrackerServerInfo*)malloc(sizeof(TrackerServerInfo));
  memcpy(storage_ptr, &storage, sizeof(storage));
  storages_[GenString(storage.ip_addr, storage.port)] = storage_ptr;
  *storage_pp = storage_ptr;
  return 0;
}

inline int FastDFS::GetStorageFetch(const string &url,
                             TrackerServerInfo **storage_pp) {
  TrackerServerInfo storage;
  if (tracker_->sock == -1 && ConnectTracker() != 0) {
    return -1;
  }
  int ret = tracker_query_storage_fetch1(tracker_, &storage, url.c_str());
  if (ret != 0) {
    return -1;
  }
  return GetStorageConnection(storage, storage_pp);
}

inline int FastDFS::GetStorageUpdate(const string &url,
                              TrackerServerInfo **storage_pp) {
  TrackerServerInfo storage;
  if (tracker_->sock == -1 && ConnectTracker() != 0) {
    return -1;
  }
  int ret = tracker_query_storage_update1(tracker_, &storage, url.c_str());
  if (ret != 0) {
    return -1;
  }
  return GetStorageConnection(storage, storage_pp);
}

int FastDFS::Get(const string &url, string *data) {
  TrackerServerInfo *storage;
  if (GetStorageFetch(url, &storage) != 0) {
    return -1;
  }
  char *buf = NULL;
  int64_t buf_size;
  int result = storage_download_file_to_buff1(tracker_,
                                              storage,
                                              url.c_str(),
                                              &buf,
                                              &buf_size);
  if (result != 0) {
    free(buf);
    return -1;
  }
  *data = string(buf, buf_size);
  free(buf);
  return 0;
}

int FastDFS::Get(const string &url,
                 string *data,
                 map<string, string> *meta) {
  TrackerServerInfo *storage;
  if (GetStorageFetch(url, &storage) != 0) {
    return -1;
  }
  char *buf = NULL;
  int64_t buf_size = 0;
  int result = storage_download_file_to_buff1(tracker_,
                                              storage,
                                              url.c_str(),
                                              &buf,
                                              &buf_size);
  if (result != 0) {
    free(buf);
    return -1;
  }
  *data = string(buf, buf_size);
  free(buf);

  FDFSMetaData *meta_list = NULL;
  int meta_count;
  if (storage_get_metadata1(tracker_, storage, url.c_str(),
        &meta_list, &meta_count) == 0) {
    for (int i = 0; i < meta_count; ++i) {
      (*meta)[meta_list[i].name] = meta_list[i].value;
    }
  }
  free(meta_list);
  return 0;
}

int FastDFS::Put(const string &data, const string &ext, string *url) {
  TrackerServerInfo *storage;
  int store_path_index;
  if (GetStorageStore(&storage, &store_path_index) != 0) {
    return -1;
  }
  char file_id[128];
  if (storage_upload_by_filebuff1(tracker_,
                                  storage,
                                  store_path_index,
                                  data.c_str(),
                                  data.size(),
                                  ext.c_str(),
                                  NULL,
                                  0,
                                  NULL,
                                  file_id) != 0) {
    return -1;
  }
  *url = string(file_id);
  return 0;
}

int FastDFS::PutByFile(const string &file, string *url) {
  TrackerServerInfo *storage;
  int store_path_index;
  if (GetStorageStore(&storage, &store_path_index) != 0) {
    return -1;
  }
  char file_id[128];
  if (storage_upload_by_filename1(tracker_,
                                  storage,
                                  store_path_index,
                                  file.c_str(),
                                  NULL,
                                  NULL,
                                  0,
                                  NULL,
                                  file_id) != 0) {
    return -1;
  }
  *url = string(file_id);
  return 0;
}

int FastDFS::PutByFile(const string &file,
                       const map<string, string> &meta,
                       string *url) {
  TrackerServerInfo *storage;
  int store_path_index;
  if (GetStorageStore(&storage, &store_path_index) != 0) {
    return -1;
  }
  FDFSMetaData meta_list[meta.size()];
  map<string, string>::const_iterator it = meta.begin();
  size_t index = 0;
  while (it != meta.end() && index < meta.size()) {
    snprintf(meta_list[index].name, sizeof(meta_list[index].name),
             "%s", it->first.c_str());
    snprintf(meta_list[index].value, sizeof(meta_list[index].value),
             "%s", it->second.c_str());
    ++it;
    index++;
  }
  char file_id[128];
  if (storage_upload_by_filename1(tracker_,
                                  storage,
                                  store_path_index,
                                  file.c_str(),
                                  NULL,
                                  meta_list,
                                  meta.size(),
                                  NULL,
                                  file_id) != 0) {
    return -1;
  }
  *url = string(file_id);
  return 0;
}

int FastDFS::Put(const string &data,
                 const string &ext,
                 const map<string, string> &meta,
                 string *url) {
  TrackerServerInfo *storage;
  int store_path_index;
  if (GetStorageStore(&storage, &store_path_index) != 0) {
    return -1;
  }
  FDFSMetaData meta_list[meta.size()];
  map<string, string>::const_iterator it = meta.begin();
  size_t index = 0;
  while (it != meta.end() && index < meta.size()) {
    snprintf(meta_list[index].name, sizeof(meta_list[index].name),
             "%s", it->first.c_str());
    snprintf(meta_list[index].value, sizeof(meta_list[index].value),
             "%s", it->second.c_str());
    ++it;
    index++;
  }
  char file_id[128];
  if (storage_upload_by_filebuff1(tracker_,
                                  storage,
                                  store_path_index,
                                  data.c_str(),
                                  data.size(),
                                  ext.c_str(),
                                  meta_list,
                                  meta.size(),
                                  NULL,
                                  file_id) != 0) {
    return -1;
  }
  *url = string(file_id);
  return 0;
}

int FastDFS::Delete(const string &url) {
  TrackerServerInfo *storage;
  if (GetStorageUpdate(url, &storage) != 0) {
    return -1;
  }
  if (storage_delete_file1(tracker_, storage, url.c_str()) != 0) {
    return -1;
  }
  return 0;
}

int FastDFS::SetMeta(const string &url, const map<string, string> &meta) {
  TrackerServerInfo *storage;
  if (GetStorageUpdate(url, &storage) != 0) {
    return -1;
  }
  FDFSMetaData meta_list[meta.size()];
  map<string, string>::const_iterator it = meta.begin();
  size_t index = 0;
  while (it != meta.end() && index < meta.size()) {
    snprintf(meta_list[index].name, sizeof(meta_list[index].name),
             "%s", it->first.c_str());
    snprintf(meta_list[index].value, sizeof(meta_list[index].value),
             "%s", it->second.c_str());
    ++it;
    index++;
  }
  if (storage_set_metadata1(tracker_,
                            storage,
                            url.c_str(),
                            meta_list,
                            meta.size(),
                            STORAGE_SET_METADATA_FLAG_OVERWRITE) != 0) {
    return -1;
  }
  return 0;
}

int FastDFS::GetMeta(const string &url, map<string, string> *meta) {
  TrackerServerInfo *storage;
  if (GetStorageFetch(url, &storage) != 0) {
    return -1;
  }
  FDFSMetaData *meta_list;
  int meta_count;
  if (storage_get_metadata1(tracker_,
                            storage,
                            url.c_str(),
                            &meta_list,
                            &meta_count) != 0) {
    free(meta_list);
    return -1;
  }
  for (int i = 0; i < meta_count; ++i) {
    (*meta)[meta_list[i].name] = meta_list[i].value;
  }
  free(meta_list);
  return 0;
}
} }  // namespace ganji::pic_server
