#ifndef _GANJI_PIC_SERVER_IMAGE_HANDLER_H_
#define _GANJI_PIC_SERVER_IMAGE_HANDLER_H_
#include <fastcgi.h>
#include <fcgiapp.h>
#include <string>
#include <map>
#include "types.h"
#include "env.h"
#include "url_regex.h"
#include "handler.h"
using std::string;
using std::map;

namespace ganji { namespace pic_server {
// to gen picture thumbnail, image handler needs watermark info and
// thumbnail info
class ImageHandler {
 public:
  ImageHandler(const RegexMatch &match, Env *env,
               FCGX_Request *request, LogItem *log_item,
               const map<string, string> &params)
      : match_(match), env_(env), request_(request),
        log_item_(log_item), params_(params) {
    hit_cache_ = false;
  }
  ~ImageHandler() { };
  int Run();

  // code blob data and meta info to string
  // format: [len][blob data]([len][key][len][value])*
  ssize_t Encode(const string &data, const map<string, string> &meta,
             char *buf, size_t size);
  ssize_t Decode(const char *buf, size_t size, string *data,
                 map<string, string> *meta);
  ssize_t EncodeString(const string &data, char *buf, size_t buf_size);
  ssize_t DecodeString(const char *buf, size_t size, string *data);

  void Dump();

 private:
  // get info form url regex match and set url info
  int SetUrlInfo();

  // read cache
  int GetCache();

  // download nfs/fastdfs
  int Download();

  // process downloaded data
  int Process();

  // put cache
  int PutCache();

  int FinishRequest();

  WaterMarkInfo watermark_info_;
  ThumbnailInfo thumbnail_info_;
  ImageUrlInfo url_info_;
  string url_;
  string data_;
  string origin_data_;
  map<string, string> meta_;
  bool hit_cache_;

  RegexMatch match_;
  Env *env_;
  FCGX_Request *request_;

  LogItem *log_item_;
  const map<string, string> &params_;
};
} }  // namespace ganji::pic_server
#endif
