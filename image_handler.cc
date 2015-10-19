#include <Magick++.h>
#include <tr1/unordered_map>
#include "ulog.h"
#include "image_handler.h"
#include "curl_downloader.h"
#include "myutil.h"
#include "msg.h"
#include "md5.h"
using std::tr1::unordered_map;
using Magick::Image;
using Magick::Geometry;
using Magick::Color;
namespace Util = ganji::pic_server::myutil;

extern uint64_t g_timestamp;

namespace ganji { namespace pic_server {
const int fill_r = 64;
const int fill_g = 64;
const int fill_b = 64;

void ImageHandler::Dump() {
  UDebug("url: %s", url_.c_str());
  UDebug("width:%d height:%d cut:%d fill:%d strict:%d quality:%d version:%d",
         url_info_.width, url_info_.height, url_info_.cut, url_info_.fill,
         url_info_.strict, url_info_.quality, url_info_.version);
  UDebug("url: %s", url_info_.url.c_str());
  UDebug("path : %s", url_info_.path.c_str());
  UDebug("ext : %s", url_info_.ext.c_str());
  UDebug("expect_ext : %s", url_info_.expect_ext.c_str());
  UDebug("data: %s", data_.c_str());
  UDebug("hit_cache: %d", hit_cache_);
}

int ImageHandler::SetUrlInfo() {
  ImageUrlInfo &info = url_info_;

  map<string, string>::const_iterator key_it = params_.find("key");
  if (key_it != params_.end()) {
    info.url = key_it->second;
  } else {
    info.url = "";
  }

  info.url = match_.subject;

  // set fastdfs url info, fastdfs url is regular
  // get sizew sizeh cut quality version ext expect_ext
  string value;
  match_.Get("sizew", &value);
  info.width = atoi(value.c_str());
  match_.Get("sizeh", &value);
  info.height = atoi(value.c_str());
  match_.Get("quality", &value);
  info.quality = atoi(value.c_str()) * 11 + 1;
  match_.Get("ver", &value);
  info.version = atoi(value.c_str());
  match_.Get("ext", &value);
  info.ext = value;
  match_.Get("path", &value);
  info.path = value + string(".") + info.ext;
  match_.Get("expect_ext", &value);
  if (value == "") {
    info.expect_ext = "jpg";
  } else {
    info.expect_ext = value;
  }

  // get cut
  match_.Get("cut", &value);
  if (value == "c") {
    info.cut = true;
    info.fill = false;
    info.strict = false;
  } else if (value == "f") {
    info.cut = false;
    info.fill = true;
    info.strict = false;
  } else if (value == "s") {
    info.cut = false;
    info.fill = false;
    info.strict = true;
  } else {
    info.cut = false;
    info.fill = false;
    info.strict = false;
  }
  return 0;
}

inline int ImageHandler::GetCache() {
  int ret = 0;
  struct timeval tmv;
  gettimeofday(&tmv, NULL);
  uint64_t start = (uint64_t)tmv.tv_sec * 1000000 + tmv.tv_usec;
  if (env_->proc_env->caches->Get(url_info_.url, &data_) == 0) {
    log_item_->hit_cache = 1;
    ret = 0;
  } else {
    ret = -1;
  }
  gettimeofday(&tmv, NULL);
  log_item_->leveldb_time = (uint64_t)tmv.tv_sec * 1000000 + tmv.tv_usec - start;
  return ret;
}

inline int ImageHandler::PutCache() {
  if (env_->proc_env->caches->Put(url_info_.url, data_) == 0) {
    return 0;
  } else {
    return -1;
  }
}

ssize_t ImageHandler::EncodeString(const string &data,
                                   char *buf,
                                   size_t buf_size) {
  size_t value_size = data.size();
  size_t total_size = sizeof(value_size) + value_size;
  if (total_size <= buf_size) {
    memcpy(buf, (void*)&value_size, sizeof(value_size));
    memcpy(buf + sizeof(value_size), (void*)data.c_str(), value_size);
    return total_size;
  } else {
    return -1;
  }
}

// encode blob data and meta data to char array for storage
ssize_t ImageHandler::Encode(const string &data,
                         const map<string, string> &meta,
                         char *buf, size_t buf_size) {
  size_t size = 0;
  ssize_t ret;
  ret = EncodeString(data, buf + size, buf_size - size);
  if (ret == -1) {
    return -1;
  } else {
    size += ret;
  }
  map<string, string>::const_iterator it = meta.begin();
  for (; it != meta.end(); ++it) {
    ret = EncodeString(it->first, buf + size, buf_size - size);
    if (ret == -1) {
      return -1;
    } else {
      size += ret;
    }
    ret = EncodeString(it->second, buf + size, buf_size - size);
    if (ret == -1) {
      return -1;
    } else {
      size += ret;
    }
  }
  return size;
}

// decode one string
// return
//   0  end of string
//   positive decoded char number
ssize_t ImageHandler::DecodeString(const char *buf,
                                   size_t size,
                                   string *data) {
  size_t value_size;
  size_t total_size;
  if (sizeof(value_size) > size) {
    return -1;
  }
  memcpy((void*)&value_size, buf, sizeof(value_size));
  total_size = value_size + sizeof(value_size);
  if (total_size > size) {
    return -1;
  }
  *data = string(buf + sizeof(value_size), value_size);
  return total_size;
}

// decode blob data and meta data from char array
ssize_t ImageHandler::Decode(const char *buf, size_t buf_size, string *data,
                             map<string, string> *meta) {
  size_t size = 0;
  ssize_t ret;
  if ((ret = DecodeString(buf + size, buf_size - size, data)) == -1) {
    return -1;
  }
  size += ret;
  string key, value;
  while (size < buf_size) {
    if ((ret = DecodeString(buf + size, buf_size - size, &key)) == -1) {
      return -1;
    }
    size += ret;
    if ((ret = DecodeString(buf + size, buf_size - size, &value)) == -1) {
      return -1;
    }
    size += ret;
    (*meta)[key] = value;
  }
  return 0;
}

inline int ImageHandler::Download() {
  // download from fifo mem cache
  string data;
  struct MemCacheItem cache_item;
  if (env_->proc_env->fifo_cache->LookUp(url_info_.path, &cache_item) == 0) {
    origin_data_ = cache_item.data;
    meta_ = cache_item.meta;
    log_item_->hit_cache = 2;
    return 0;
  }
  // download from fastdfs
  struct timeval tmv;
  gettimeofday(&tmv, NULL);
  uint64_t start = (uint64_t)tmv.tv_sec * (uint64_t)1000000 + tmv.tv_usec;
#ifndef _LOG_REPLAY_
  if (env_->thread_env->fastdfs->Get(url_info_.path,
                                     &origin_data_,
                                     &meta_) != 0) {
    UTrace("down fdfs failed. %s", url_info_.path.c_str());
    return -1;
  }
  gettimeofday(&tmv, NULL);
  log_item_->fdfs_time =  (uint64_t)tmv.tv_sec * (uint64_t)1000000 + tmv.tv_usec - start;
  cache_item.data = origin_data_;
  cache_item.meta = meta_;
  env_->proc_env->fifo_cache->Insert(url_info_.path, cache_item, cache_item.data.size());
#else
  // read status from get param. get fdfs file size
  FDFSFileInfo file_info;
  // if (env_->thread_env->fastdfs->GetFileInfo(url_info_.path, &file_info) != 0) {
  //   UTrace("fdfs get file info failed. %s", url_info_.path.c_str());
  //   return -1;
  // }
  file_info.file_size = 20000;
  // cache_item.data = url_info_.path;
  env_->proc_env->fifo_cache->Insert(url_info_.path,
                                     cache_item,
                                     file_info.file_size);
  // map<string, string>::const_iterator params_it = params_.find("status");
  // if (params_it != params_.end() && params_it->second == "200") {
  //   env_->proc_env->fifo_cache->Insert(url_info_.path,
  //                                      cache_item,
  //                                      file_info.file_size);
  // } else {
  //   UTrace("status not 200. %s", url_info_.url.c_str());
  //   return -1;
  // }
#endif
  return 0;
}

// process origin_data_ and meta_, store final data in data_
// for images:
//   resize, crop, watermark, sharpen
int ImageHandler::Process() {
#ifndef _LOG_REPLAY_
  try {
    map<string, string>::const_iterator param_it = params_.find("okey");
    if (param_it != params_.end()) {
      if (Md5::GetMd5Str(url_info_.url + string("ankp@gc")) == param_it->second) {
        data_ = origin_data_;
        return 0;
      }
    }
    Blob blob(origin_data_.c_str(), origin_data_.size());
    Image img(blob);
    bool cut = false, fill = false;
    int imageW = img.columns();
    int imageH = img.rows();
    int srcW = imageW, srcH = imageH;
    int destW = url_info_.width, destH = url_info_.height;
    if (!url_info_.strict) {
      if (destW > srcW) {
        destW = srcW;
      }
      if (destH > srcH) {
        destH = srcH;
      }
    }
    if (destW == 0 && destH == 0) {
      destW = srcW;
      destH = srcH;
      // 老的接口，如果是gif图片且请求长宽都为0，那么输出为gif
      if (url_info_.ext == "gif") {
        url_info_.expect_ext = "gif";
      }
    } else if (destW == 0) {
      destW = destH * srcW / srcH;
    } else if (destH == 0) {
      destH = destW * srcH / srcW;
    } else if (!url_info_.strict) {
      int x = destW * imageH;
      int y = destH * imageW;
      if ((x > y && url_info_.cut && (cut = true)) ||
          (x < y && url_info_.fill && (fill = true))) {
        // change srcH
        srcH = destH * imageW / destW;
      } else if ((x > y && url_info_.fill && (fill = true)) ||
                 (x < y && url_info_.cut && (cut = true))) {
        // change srcW
        srcW = destW * imageH / destH;
      }
    }

    // crop
    if (cut) {
      int x_offset, y_offset;
      if (srcH != imageH) {
        x_offset = 0;
        y_offset = (imageH - srcH) / 2;
      }
      if (srcW != imageW) {
        x_offset = (imageW - srcW) / 2;
        y_offset = 0;
      }
      img.crop(Geometry(srcW, srcH, x_offset, y_offset));
    }

    // fill
    if (fill) {
      int x_offset, y_offset;
      if (srcH != imageH) {
        x_offset = 0;
        y_offset = (srcH - imageH) / 2;
      }
      if (srcW != imageW) {
        x_offset = (srcW - imageW) / 2;
        y_offset = 0;
      }
      // create empty image with color (fill_r, fill_g, fill_b)
      Image tmp(Geometry(srcW, srcH), Magick::Color(fill_r, fill_g, fill_b));
      tmp.composite(img, x_offset, y_offset, Magick::OverCompositeOp);
      img = tmp;
    }

    // resize
    if (url_info_.strict) {
      Geometry geo(destW, destH);
      geo.aspect(true);
      img.zoom(geo);
    } else {
      img.zoom(Geometry(destW, destH));
    }

    WaterMarkInfo info;
    string category;
    map<string, string>::iterator meta_it = meta_.find("category");
    if (meta_it == meta_.end()) {
      category = "default";
    } else {
      category = meta_it->second;
    }
    UDebug("category: %s", category.c_str());
    unordered_map<string, WaterMarkInfo>::iterator cat_wm_it = env_->proc_env->cat_watermark.find(category);
    if (cat_wm_it == env_->proc_env->cat_watermark.end()) {
      cat_wm_it = env_->proc_env->cat_watermark.find("default");
      if (cat_wm_it == env_->proc_env->cat_watermark.end()) {
        return -1;
      }
      // info = env_->proc_env->cat_watermark["default"];
      info = cat_wm_it->second;
    } else {
      info = cat_wm_it->second;
    }
    UDebug("info(file, position, sharpen)=(%s, %d, %d)",
          info.file.c_str(), info.position, info.sharpen);

    if (!(srcW < 150 || srcH < 100)) {
      // watermark
      int x_margin, y_margin;
      map<string, Image>::iterator img_it;
      if (srcW <= 340 || srcH<= 220) {
        img_it = env_->proc_env->watermarks.find(info.small_file);
        x_margin = 7;
        y_margin = 5;
      } else {
        img_it = env_->proc_env->watermarks.find(info.file);
        x_margin = 10;
        y_margin = 10;
      }
      // map<string, Image>::iterator img_it = env_->proc_env->watermarks.find(info.file);
      if (img_it != env_->proc_env->watermarks.end()) {
        Image &watermark_img = img_it->second;
        int x_offset, y_offset;
        int watermark_width = watermark_img.columns();
        int watermark_height = watermark_img.rows();
        if (info.position == 3) {
          x_offset = srcW - watermark_width - x_margin;
          y_offset = srcH - watermark_height - y_margin;
        } else if (info.position == 7) {
          x_offset = x_margin;
          y_offset = y_margin;
        }
        img.composite(watermark_img, x_offset, y_offset,
            Magick::OverCompositeOp);
      }
    }

    if (info.sharpen) {
      img.sharpen(2, 1);
    }
    // srcW < 150 || srcH < 100, no watermark
    // write img to data
    Blob eventual_data;
    // fprintf(stderr, "set image quality %d\n", url_info_.quality);
    img.quality(url_info_.quality);
    img.write(&eventual_data, url_info_.expect_ext);
    data_ = string((char*)eventual_data.data(), eventual_data.length());
    return 0;
  } catch (Exception &e) {
    UTrace("process image exception catched. %s", e.what());
    return -1;
  }
#else
  // set data_ to url_info_.path
  data_ = url_info_.url;
  return 0;
#endif
}

int ImageHandler::FinishRequest() {
  FCGX_PutStr(g_ok_msg.c_str(), g_ok_msg.size(), request_->out);
  // set content type, and set
  unordered_map<string, string>::iterator it =
    env_->proc_env->mime.find(url_info_.expect_ext);
  if (it != env_->proc_env->mime.end()) {
    string content_type = string("Content-Type: ") +
                          it->second +
                          string("\r\n");
    FCGX_PutStr(content_type.c_str(), content_type.size(), request_->out);
  }
  if (log_item_->hit_cache == 1) {
    FCGX_PutStr(g_cache_hit_msg.c_str(),
                g_cache_hit_msg.size(),
                request_->out);
  } else if (log_item_->hit_cache == 2) {
    FCGX_PutStr(g_cache_hit_mem_msg.c_str(),
                g_cache_hit_mem_msg.size(),
                request_->out);
  } else {
    FCGX_PutStr(g_cache_miss_msg.c_str(),
                g_cache_miss_msg.size(),
                request_->out);
  }

  char content_length[128];
  int content_length_size = snprintf(content_length, sizeof(content_length),
                                     "Content-Length: %lu\r\n",
                                     data_.size());
  FCGX_PutStr(content_length, content_length_size, request_->out);
  FCGX_PutStr("\r\n", 2, request_->out);
  FCGX_PutStr(data_.c_str(), data_.size(), request_->out);
  FCGX_Finish_r(request_);
  log_item_->status = 200;
  log_item_->bytes = data_.size();
  return 0;
}

int ImageHandler::Run() {
  if (SetUrlInfo() != 0) {
    return -1;
  }
  Dump();
  if (GetCache() != 0) {
    bool has_error = false;;
    uint64_t start_stamp = myutil::GetUs();
    if (!env_->proc_env->config.read_only_mode()) {
      // read-only, just proxy request to proxy_pass
      if (!env_->proc_env->config.proxy_mode()) {
        // proxy-mode, cache miss, proxy request to proxy_pass, insert
        // response into cache
        if (Download() != 0 || Process() != 0) {
          has_error = true;
        }
      } else {
        map<string, string> header_out;
        map<string, string> header_in;
        int ret = Curl::Download(env_->proc_env->config.proxy_pass() + url_info_.url,
                                 env_->proc_env->config.proxy_timeout(),
                                 header_out,
                                 &header_in,
                                 &data_);
        if (ret == 0) {
          map<string, string>::iterator it = header_in.find("HttpCode");
          if (it == header_in.end()) {
            UWarning("no http code found. %s", url_info_.url.c_str());
            has_error = true;
          } else if (it->second != "200") {
            UWarning("httpcode %s not 200. %s", it->second.c_str(), url_info_.url.c_str());
            has_error = true;
          }
        } else {
          has_error = true;
        }
      }
      PutCache();
    } else {
      // curl download from proxy_pass
      map<string, string> header_out;
      map<string, string> header_in;
      int ret = Curl::Download(env_->proc_env->config.proxy_pass() + url_info_.url,
                               env_->proc_env->config.proxy_timeout(),
                               header_out,
                               &header_in,
                               &data_);
      has_error = ret == 0 ? false : true;
    }
    log_item_->cgi_time = GetUs() - start_stamp;
    if (has_error) {
      return -1;
    }
  } else {
    hit_cache_ = true;
  }
  UDebug("url: %s", url_info_.url.c_str());
  FinishRequest();
  return 0;
}
} }  // namespace ganji::pic_server
