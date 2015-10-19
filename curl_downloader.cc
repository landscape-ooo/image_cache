#include <pthread.h>
#include "ulog.h"
#include "curl_downloader.h"
#include "myutil.h"
namespace Util = ganji::pic_server::myutil;
namespace Log = ganji::util::log::ThreadFastLog;
namespace ganji { namespace pic_server {
int Curl::Init() {
  static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
  static bool inited = false;
  pthread_mutex_lock(&mutex);
  if (!inited) {
    if (curl_global_init(CURL_GLOBAL_ALL) != 0) {
      UFatal("curl_global_init failed");
      abort();
    }
    inited = true;
  }
  pthread_mutex_unlock(&mutex);
  return 0;
}

int Curl::Destroy() {
  static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
  static bool destroyed = false;
  pthread_mutex_lock(&mutex);
  if (!destroyed) {
    curl_global_cleanup();
    destroyed = true;
  }
  pthread_mutex_unlock(&mutex);
  return 0;
}

int Curl::Download(const string &url, const long timeout,
                   const map<string, string> &header_out,
                   map<string, string> *pheader_in,
                   string *pbody,
                   const string &body_out) {
  CURL *curl = curl_easy_init();
  CURLcode code;
  string data_recv;
  struct curl_slist *headers = NULL;
  int ret_code = -1;
  map<string, string>::const_iterator map_it = header_out.begin();

  if (curl == NULL) {
    UWarning("curl_easy_init failed. %s", url.c_str());
    goto failed;
  }

  // set options
  if (SetOpt(curl_easy_setopt(curl, CURLOPT_URL, url.c_str())) != 0) {
    goto failed;
  }

  for (; map_it != header_out.end(); ++map_it) {
    char buf[1024];
    snprintf(buf, sizeof(buf), "%s: %s", map_it->first.c_str(), map_it->second.c_str());
    headers = curl_slist_append(headers, buf);
    if (headers == NULL) {
      UWarning("curl_slist_append header failed. %s", url.c_str());
      goto failed;
    }
  }

  headers = curl_slist_append(headers, "Expect:");
  if (headers == NULL) {
    UWarning("curl_slist_append Except failed. %s", url.c_str());
    goto failed;
  }

  if (SetOpt(curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers)) != 0) {
    goto failed;
  }
  if (body_out != "") {
    if (SetOpt(curl_easy_setopt(curl, CURLOPT_POST, 1)) != 0) {
      goto failed;
    }

    if (SetOpt(curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body_out.c_str())) != 0) {
      goto failed;
    }

    if (SetOpt(curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, body_out.size())) != 0) {
      goto failed;
    }
  }

  // libcurl will cache connection for reuse, if download mainly do short
  // connection works, just forbid reuse. if not, there will be many many
  // CLOSE_WAIT state
  if (SetOpt(curl_easy_setopt(curl, CURLOPT_FORBID_REUSE, 1)) != 0) {
    goto failed;
  }
  if (SetOpt(curl_easy_setopt(curl, CURLOPT_TIMEOUT, (long)timeout)) != 0) {
    goto failed;
  }
  if (SetOpt(curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1)) != 0) {
    goto failed;
  }
  if (SetOpt(curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_call_back)) != 0) {
    goto failed;
  }
  if (SetOpt(curl_easy_setopt(curl, CURLOPT_WRITEDATA, &data_recv)) != 0) {
    goto failed;
  }
  if (SetOpt(curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, write_call_back)) != 0) {
    goto failed;
  }
  if (SetOpt(curl_easy_setopt(curl, CURLOPT_HEADERDATA, &data_recv)) != 0) {
      goto failed;
  }

  code = curl_easy_perform(curl);
  if (code != CURLE_OK) {
    UWarning("curl_easy_perform failed. %s", curl_easy_strerror(code));
    goto failed;
  }

  if (Util::ParseHttpResponse(data_recv, pheader_in, pbody) != 0) {
    UWarning("curl parse http response failed. %s %s", url.c_str(), data_recv.c_str());
    goto failed;
  }
  ret_code = 0;
failed:
  if (headers) {
    curl_slist_free_all(headers);
  }
  curl_easy_cleanup(curl);
  return ret_code;
}

long Curl::write_call_back(void *data, size_t size,
                                     size_t nmemb, void *stream) {
  string *content = (string*)stream;
  *content += string((char*)data, size * nmemb);
  return size * nmemb;
}

int Curl::SetOpt(CURLcode code) {
  if (code != CURLE_OK) {
    UNotice("curl_easy_setopt failed. %s", curl_easy_strerror(code));
    return -1;
  }
  return 0;
}
} }  // namespace ganji::pic_server
