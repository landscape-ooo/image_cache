#include <pthread.h>
#include <signal.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <fcgi_config.h>
#include <fcgiapp.h>
#include <map>
#include "ulog.h"
#include "fcgi_function.h"
#include "fs/file_os.h"
#include "types.h"
#include "env.h"
#include "msg.h"
#include "handler.h"
#include "myutil.h"
#include "scribe/scribe_logger_thread.h"
namespace Util = ganji::pic_server::myutil;
using std::map;
extern sig_atomic_t g_stop;
namespace ganji { namespace pic_server {
const string& GetErrorMsg() {
  static bool nopic_readed = false;
  static string nopic;
  static string content_length;
  static string content_type = "Content-Type: image/gif\r\n";
  static string error_msg;
  if (nopic_readed == false) {
    FileOS file_os("./");
    if (file_os.Get("images/nopic.gif", &nopic) != 0) {
      UFatal("read images/nopic.gif failed. %s", strerror(errno));
      exit(-1);
    }
    char buf[128];
    snprintf(buf, sizeof(buf), "Content-Length: %lu\r\n", nopic.size());
    content_length = string(buf);
    error_msg += g_error_msg + content_type + content_length +
                 g_head_end_msg + nopic;
    nopic_readed = true;
  }
  return error_msg;
}

void HandleError(FCGX_Request *request) {
  const string &error_msg = GetErrorMsg();
  FCGX_PutStr(error_msg.c_str(), error_msg.size(), request->out);
  FCGX_Finish_r(request);
}

void ThreadSigHandler(int signo) {
}

void* FCGXMainLoop(void *args) {
  // block signals
  sigset_t set;
  sigaddset(&set, SIGTERM);
  sigaddset(&set, SIGINT);
  sigaddset(&set, SIGKILL);
  sigaddset(&set, SIGPIPE);
  sigaddset(&set, SIGCHLD);
  pthread_sigmask(SIG_BLOCK, &set, NULL);
  signal(SIGUSR2, ThreadSigHandler);

  ProcEnv *proc_env = (ProcEnv*)args;
  ScribeLogger *logger = proc_env->logger();

  int rc;
  FCGX_Request request;
  FCGX_InitRequest(&request, 0, FCGI_FAIL_ACCEPT_ON_INTR);

  while (!g_stop) {
    rc =  FCGX_Accept_r(&request);
    struct LogItem log_item;
    if (rc != 0) {
      UWarning("FCGX_Accept_r failed");
      continue;
    }
    string query_string;
    map<string, string> param_map;
    char *param = FCGX_GetParam("QUERY_STRING", request.envp);
    if (param) {
      query_string += string(param);
      Util::ParseHttpQuery(query_string, &param_map);
    }

    param = FCGX_GetParam("HTTP_REFERER", request.envp);
    if (param) {
      log_item.referer = string(param);
    }

    param = FCGX_GetParam("CONTENT_LENGTH", request.envp);
    if (param) {
      query_string.clear();
      size_t content_length = atol(param);
      if (content_length >= 1) {
        char buf[65536];
        size_t count = 0;
        while (count < content_length) {
          int ret = FCGX_GetStr(buf, sizeof(buf), request.in);
          count += ret;
          query_string.append(buf, ret);
        }
        Util::ParseHttpQuery(query_string, &param_map);
      }
    }
    map<string, string>::iterator act_it = param_map.find("act");
    map<string, string>::iterator key_it = param_map.find("key");
    if (act_it == param_map.end() || key_it == param_map.end()) {
      UWarning("query invalid. %s", query_string.c_str());
    } else {
      log_item.url = key_it->second;
      if (act_it->second == "read") {
        Handler handler(proc_env, &request, &log_item, param_map);
        if (handler.Run() == 0) {
          goto do_log;
        } else {
          UNotice("handler failed. %s", key_it->second.c_str());
        }
      } else {
        UWarning("undefined action. %s", query_string.c_str());
      }
    }
    HandleError(&request);
do_log:
    UNotice("%s", log_item.AsString().c_str());
    // logger->Notice("img_thumbnail", log_item.AsString().c_str());
  }
  return NULL;
}
} }   // namespace ganji::pic_server
