#include <stdlib.h>
#include "fs/fastdfs_short.h"
#include "fs/file_os.h"
#include "myutil.h"
#include "ulog.h"
using ganji::pic_server::myutil::DoSleep;
extern pthread_mutex_t g_fdfs_init_mutex;

namespace ganji { namespace pic_server {
bool FastDFS::log_inited_ = false;
int FastDFS::Init(const string &conf_file) {
  // init conf file and connect to all trackers
  UDebug("init fastdfs........");
  conf_file_ = conf_file;
  // BUG: fdfs_client_init_ex是非线程安全的函数，这个函数会调用chdir
  //  use fdfs_client_init_from_buffer_ex instead
  string file_content;
  FileOS file_os("./");
  if (file_os.Get(conf_file, &file_content) != 0) {
    UFatal("get %s content failed. %s", conf_file.c_str(), strerror(errno));
    return -1;
  }
  // int result = fdfs_client_init_ex(&tracker_group_, conf_file.c_str());
  if (pthread_mutex_lock(&g_fdfs_init_mutex) != 0) {
    UFatal("lock g_fdfs_init_mutex failed.");
    abort();
  }
  int result = fdfs_client_init_from_buffer_ex(&tracker_group_,
                                               file_content.c_str());
  if (pthread_mutex_unlock(&g_fdfs_init_mutex) != 0) {
    UFatal("unlock g_fdfs_init_mutex failed");
    abort();
  }
  if (result != 0) {
    UFatal("fdfs_client_init_ex failed");
    exit(-1);
  }

  // result = tracker_get_all_connections_ex(&tracker_group_);
  // if (result != 0) {
  //   UFatal("tracker_get_all_connections_ex failed.");
  //   exit(-1);
  // }

  // UWarning("using short connection fastdfs..");
  return 0;
}

// quit connections and destroy client
FastDFS::~FastDFS() {
  // close all tracker connections
  UDebug("destructor fastdfs......");
  tracker_close_all_connections_ex(&tracker_group_);
  fdfs_client_destroy_ex(&tracker_group_);
}

int FastDFS::Get(const string &url, string *data) {
  TrackerServerInfo tracker;
  if (tracker_get_connection_r_ex(&tracker_group_, &tracker) != 0) {
    UTrace("tracker_get_connection_r_ex failed.");
    return -1;
  }
  char *buf = NULL;
  int64_t buf_size;
  int result = storage_download_file_to_buff1(&tracker,
                                              NULL,
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
  TrackerServerInfo tracker;
  if (tracker_get_connection_r_ex(&tracker_group_, &tracker) != 0) {
    UTrace("get connection to tracker failed.");
    return -1;
  }
  char *buf = NULL;
  int64_t buf_size = 0;
  int result = storage_download_file_to_buff1(&tracker,
                                              NULL,
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
  if (storage_get_metadata1(&tracker, NULL, url.c_str(), &meta_list, &meta_count) == 0) {
    for (int i = 0; i < meta_count; ++i) {
      (*meta)[meta_list[i].name] = meta_list[i].value;
    }
  }
  free(meta_list);
  return 0;
}

int FastDFS::Put(const string &data, const string &ext, string *url) {
  TrackerServerInfo storage;
  int store_path_index;
  TrackerServerInfo tracker;
  if (tracker_get_connection_r_ex(&tracker_group_, &tracker) != 0) {
    UTrace("tracker_get_connection_r_ex failed.");
    return -1;
  }
  if (tracker_query_storage_store(&tracker, &storage, &store_path_index) != 0) {
    UTrace("tracker_query_storage_store failed.");
    return -1;
  }
  char file_id[128];
  if (storage_upload_by_filebuff1(&tracker,
                                  &storage,
                                  store_path_index,
                                  data.c_str(),
                                  data.size(),
                                  ext.c_str(),
                                  NULL,
                                  0,
                                  NULL,
                                  file_id) != 0) {
    UTrace("storage_upload_by_filebuff1 failed.");
    return -1;
  }
  *url = string(file_id);
  return 0;
}

int FastDFS::PutByFile(const string &file, string *url) {
  TrackerServerInfo storage;
  int store_path_index;
  TrackerServerInfo tracker;
  if (tracker_get_connection_r_ex(&tracker_group_, &tracker) != 0) {
    UTrace("tracker_get_connection_r_ex failed");
    return -1;
  }
  if (tracker_query_storage_store(&tracker, &storage, &store_path_index) != 0) {
    UTrace("tracker_query_storage_store failed.");
    return -1;
  }
  char file_id[128];
  if (storage_upload_by_filename1(&tracker,
                                  &storage,
                                  store_path_index,
                                  file.c_str(),
                                  NULL,
                                  NULL,
                                  0,
                                  NULL,
                                  file_id) != 0) {
    UTrace("storage_upload_by_filename1 failed.");
    return -1;
  }
  *url = string(file_id);
  return 0;
}

int FastDFS::PutByFile(const string &file,
                       const map<string, string> &meta,
                       string *url) {
  TrackerServerInfo storage;
  int store_path_index;
  TrackerServerInfo tracker;
  if (tracker_get_connection_r_ex(&tracker_group_, &tracker) != 0) {
    UTrace("tracker_get_connection_r_ex.");
    return -1;
  }
  if (tracker_query_storage_store(&tracker, &storage, &store_path_index) != 0) {
    UTrace("tracker_query_storage_store failed.");
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
  if (storage_upload_by_filename1(&tracker,
                                  &storage,
                                  store_path_index,
                                  file.c_str(),
                                  NULL,
                                  meta_list,
                                  meta.size(),
                                  NULL,
                                  file_id) != 0) {
    UTrace("storage_upload_by_filename1 failed.");
    return -1;
  }
  *url = string(file_id);
  return 0;
}

int FastDFS::Put(const string &data,
                 const string &ext,
                 const map<string, string> &meta,
                 string *url) {
  TrackerServerInfo storage;
  int store_path_index;
  TrackerServerInfo tracker;
  if (tracker_get_connection_r_ex(&tracker_group_, &tracker) != 0) {
    UTrace("tracker_get_connection_r_ex failed.");
    return -1;
  }
  if (tracker_query_storage_store(&tracker, &storage, &store_path_index) != 0) {
    UTrace("tracker_query_storage_store failed.");
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
  if (storage_upload_by_filebuff1(&tracker,
                                  &storage,
                                  store_path_index,
                                  data.c_str(),
                                  data.size(),
                                  ext.c_str(),
                                  meta_list,
                                  meta.size(),
                                  NULL,
                                  file_id) != 0) {
    UTrace("storage_upload_by_filebuff1 failed.");
    return -1;
  }
  *url = string(file_id);
  return 0;
}

int FastDFS::Delete(const string &url) {
  TrackerServerInfo tracker;
  if (tracker_get_connection_r_ex(&tracker_group_, &tracker) != 0) {
    UTrace("tracker_get_connection_r_ex failed.");
    return -1;
  }
  if (storage_delete_file1(&tracker, NULL, url.c_str()) != 0) {
    UTrace("storage_delete_file1 failed.");
    return -1;
  }
  return 0;
}

int FastDFS::SetMeta(const string &url, const map<string, string> &meta) {
  TrackerServerInfo tracker;
  if (tracker_get_connection_r_ex(&tracker_group_, &tracker) != 0) {
    UTrace("tracker_get_connection_r_ex failed.");
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
  if (storage_set_metadata1(&tracker,
                            NULL,
                            url.c_str(),
                            meta_list,
                            meta.size(),
                            STORAGE_SET_METADATA_FLAG_OVERWRITE) != 0) {
    UTrace("storage_set_metadata1 failed.");
    return -1;
  }
  return 0;
}

int FastDFS::GetMeta(const string &url, map<string, string> *meta) {
  TrackerServerInfo tracker;
  if (tracker_get_connection_r_ex(&tracker_group_, &tracker) != 0) {
    UTrace("tracker_get_connection_r_ex failed.");
    return -1;
  }
  FDFSMetaData *meta_list;
  int meta_count;
  if (storage_get_metadata1(&tracker,
                            NULL,
                            url.c_str(),
                            &meta_list,
                            &meta_count) != 0) {
    free(meta_list);
    UTrace("storage_get_metadata1 failed.");
    return -1;
  }
  for (int i = 0; i < meta_count; ++i) {
    (*meta)[meta_list[i].name] = meta_list[i].value;
  }
  free(meta_list);
  return 0;
}
} }  // namespace ganji::pic_server
