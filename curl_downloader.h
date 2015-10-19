#ifndef _GANJI_PIC_SERVER_CURL_DOWNLOADER_
#define _GANJI_PIC_SERVER_CURL_DOWNLOADER_
#include <curl/curl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <map>
using std::string;
using std::map;
namespace ganji { namespace pic_server {
class Curl {
 public:
  // init and destory libcurl environment, should be called only once
  static int Init();
  static int Destroy();
  
  static int Download(const string &url, const long timeout,
               const map<string, string> &header_out,
               map<string, string> *pheader_in, string *pbody,
               const string &body_out = "");
  static long write_call_back(void *data, size_t size, size_t nmemb, void *stream);
  
  static int SetOpt(CURLcode code);
};
} }  // namespace ganji::pic_server
#endif
