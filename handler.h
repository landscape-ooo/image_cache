#ifndef _GANJI_PIC_SERVER_HANDLER_H_
#define _GANJI_PIC_SERVER_HANDLER_H_
#include <fastcgi.h>
#include <fcgiapp.h>
#include <string>
#include <map>
#include "env.h"
#include "url_regex.h"
#include "types.h"
using std::string;
using std::map;
namespace ganji { namespace pic_server {
class Handler {
 public:
  Handler(ProcEnv *proc_env, FCGX_Request *request, LogItem *log_item,
          const map<string, string> &params)
      : proc_env_(proc_env), request_(request),
        log_item_(log_item), params_(params) {  }
  virtual ~Handler() { }
  int Run();

 private:
  int Download();
  int FinishRequest();
  int GetCache();
  int PutCache();

  string data_;
  string url_;
  string ext_;

  ProcEnv *proc_env_;
  FCGX_Request *request_;

  LogItem *log_item_;
  const map<string, string> params_;
};
} }  // namespace ganji::pic_server
#endif
