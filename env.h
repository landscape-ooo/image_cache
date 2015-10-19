#ifndef _GANJI_PIC_SERVER_ENV_H_
#define _GANJI_PIC_SERVER_ENV_H_
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string>
#include <tr1/unordered_map>
#include "types.h"
#include "scribe/scribe_logger.h"
#include "config.h"
#include "leveldb_cache.h"
using std::string;
using std::tr1::unordered_map;
using namespace ganji::pic_server::scribe;

namespace ganji { namespace pic_server {
struct ProcEnv {
 public:
  ProcEnv() {
    caches_ = NULL;
    logger_ = NULL;
  }

  int Init();
  int Destroy();

  string GetMime(const string &ext);

  int ReadMime(const string &file, const string &sep,
               unordered_map<string, string> *pmime);

  CacheManager* caches() {
    return caches_;
  }

  ScribeLogger* logger() {
    return logger_;
  }

  Config* config() {
    return config_;
  }

 private:
  Config *config_;

  CacheManager *caches_;

  ScribeLogger *logger_;

  unordered_map<string, string> mime_;
};
} }  // namespace ganji::pic_server
#endif
