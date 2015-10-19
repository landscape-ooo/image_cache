#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <errno.h>
#include <string.h>
#include <signal.h>
#include <string>
#include <vector>
#include <fastcgi.h>
#include <fcgiapp.h>
#include "ulog.h"
#include "env.h"
#include "types.h"
#include "fcgi_function.h"
#include "curl_downloader.h"
using std::string;
using std::vector;
using namespace ganji::pic_server;
namespace Log = ganji::util::log::ThreadFastLog;
extern sig_atomic_t g_stop;

void SigHandler(int signo) {
  g_stop = 1;
}

int main(int argc, char **argv) {
  // set resource limit
  struct rlimit rlim;
  rlim.rlim_cur = RLIM_INFINITY;
  rlim.rlim_max = RLIM_INFINITY;
  if (setrlimit(RLIMIT_CORE, &rlim) != 0) {
    fprintf(stderr, "setrlimit(RLIMIT_CORE) failed. %d, %s\n",
            errno, strerror(errno));
    return -1;
  }

  rlim.rlim_cur = 20 * 1024 * 1024;
  rlim.rlim_max = 20 * 1024 * 1024;
  if (setrlimit(RLIMIT_STACK, &rlim) != 0) {
    fprintf(stderr, "setrlimit(RLIMIT_STACK) failed. %d, %s\n",
           errno, strerror(errno));
    return -1;
  }

  signal(SIGTERM, SigHandler);
  signal(SIGKILL, SigHandler);
  signal(SIGINT, SigHandler);
  signal(SIGPIPE, SIG_IGN);
  signal(SIGCHLD, SIG_IGN);

  srand(time(0));

  // init fastcgi lib
  FCGX_Init();

  // init libcurl
  Curl::Init();

  // init error msg
  GetErrorMsg();

  ProcEnv *proc_env = new ProcEnv();
  if (proc_env == NULL) {
    fprintf(stderr, "new ProcEnv() failed\n");
    exit(-1);
  }
  if (proc_env->Init() != 0) {
    fprintf(stderr, "init proc_env failed\n");
    return -1;
  }

  // open log
  uint32_t log_level = proc_env->config()->log_level();
  Log::FastLogStat log_stat = {log_level,
                               Log::kLogNone,
                               Log::kLogSizeSplit};
  Log::OpenLog(proc_env->config()->log_path().c_str(), "picserver",
               2048, &log_stat);

  // create threads
  int thread_number = proc_env->config()->worker_thread_number();
  vector<pthread_t> tids;
  for (int i = 0; i < thread_number; ++i) {
    pthread_t tid;
    if (pthread_create(&tid, NULL, FCGXMainLoop, (void*)proc_env) != 0) {
      Log::WriteLog(Log::kLogFatal, "create thread failed. %d, %s",
                    errno, strerror(errno));
      return -1;
    }
    tids.push_back(tid);
  }

  sigset_t set;
  sigaddset(&set, SIGTERM);
  sigaddset(&set, SIGINT);
  sigaddset(&set, SIGKILL);
  if (sigprocmask(SIG_BLOCK, &set, NULL) == -1) {
    UFatal("sigprocmask failed. %d %s", errno, strerror(errno));
    return -1;
  }
  sigemptyset(&set);
  while (true) {
    sigsuspend(&set);
    if (g_stop == 1) {
      UWarning("stoping service.........");
      UWarning("cancelling all threads..");
      UWarning("close listening socket 0. ret:%d", close(0));
      for (size_t i = 0; i < tids.size(); ++i) {
        pthread_kill(tids[i], SIGUSR2);
      }
      for (size_t i = 0; i < tids.size(); ++i) {
        pthread_join(tids[i], NULL);
      }
      UWarning("all threads stopped.......");
      break;
    }
  }
  proc_env->Destroy();
  delete proc_env;

  // destroy libcurl
  Curl::Destroy();

  return 0;
}
