#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <errno.h>
#include <string.h>
#include <string>
#include <vector>
#include "ganji/util/log/thread_fast_log.h"
#include "ulog.h"
#include "log_replay/log_reader.h"
#include "curl_downloader.h"
#include "fs/file_os.h"
using std::string;
using std::vector;
namespace Log = ganji::util::log::ThreadFastLog;
using namespace ganji::pic_server;

string pattern = "^NOTICE: (?P<log_time>\\S+ \\S+) log_info: url:(?P<path>\\S+), status:(?<status>\\d+), bytes:(?P<size>\\d+), hit_cache:(?<hit>\\d), leveldb_time:(?P<leveldb_time>\\S+), cgi_time:(?P<gen_time>\\S+),.*$";


string start_date = "20130116";
string end_date = "20130223";

int thread_number = 100;


uint64_t leveldb_hit_count = 0;
uint64_t mem_hit_count = 0;
uint64_t total_count = 0;
uint64_t error_count = 0;
uint64_t code_404_count = 0;

void* DownloadThread(void* args) {
  FileMgr *mgr = (FileMgr*)args;
  struct LogItem log_item;
  while (mgr->Next(&log_item) == 0) {
    char url[1024];
    snprintf(url, sizeof(url), "http://192.168.115.58/%s?status=%s",
             log_item.path.c_str(), log_item.status.c_str());
    map<string, string> header_out;
    // header_out["Host:"] = "image.ganjistatic1.com";
    map<string, string> header_in;
    string body;
    int ret = Curl::Download(string(url), 1000, header_out, &header_in, &body);
    if (ret != 0) {
      UFatal("download %s failed.", url);
      // fprintf(stderr, "download %s failed.\n", url);
      error_count++;
      continue;
    }
    map<string, string>::iterator it;
    it = header_in.find("HttpCode");
    if (it == header_in.end()) {
      UFatal("no HttpCode found. %s", url);
      error_count++;
      continue;
    }
    if ((it->second == "404" && log_item.status == "404") ||
        strncmp(log_item.path.c_str(), "gjfs", 4) != 0) {
      code_404_count++;
      continue;
    }
    it = header_in.find("Ganji-Cache");
    if (it == header_in.end()) {
      UFatal("no Ganji-Cache found. %s", url);
      error_count++;
      continue;
    }
    if (it->second == "Hit-1") {
      leveldb_hit_count++;
    } else if (it->second == "Hit-2") {
      mem_hit_count++;
    }
    if (strcmp(body.c_str(), log_item.path.c_str()) != 0) {
      UFatal("%s content invalid: %s", log_item.path.c_str(), body.c_str());
      error_count++;
      continue;
    }
    total_count++;
  }
  pthread_exit(NULL);
}

void* DepressThread(void *args) {
  vector<string> &cmds = *((vector<string>*)args);
  for (size_t i = 0; i < cmds.size(); ++i) {
    fprintf(stderr, "thread cmd: %s\n", cmds[i].c_str());
    system(cmds[i].c_str());
  }
  pthread_exit(NULL);
}

int main(int argc, char **argv) {
  Log::FastLogStat log_stat = {Log::kLogAll,
                               Log::kLogNone,
                               Log::kLogSizeSplit};
  Log::OpenLog("./log", "replay", 2048, &log_stat);

  Curl::Init();

  vector<string> dirs;
  dirs.push_back("img-web-01");
  dirs.push_back("img-web-02");
  dirs.push_back("img-web-03");
  dirs.push_back("img-web-04");
  dirs.push_back("img-web-07");

  vector<string> fn_patterns;
  fn_patterns.push_back("^picserverlog.20130116\\d{6}$");
  fn_patterns.push_back("^picserverlog.20130117\\d{6}$");
  fn_patterns.push_back("^picserverlog.20130118\\d{6}$");
  fn_patterns.push_back("^picserverlog.20130119\\d{6}$");
  fn_patterns.push_back("^picserverlog.20130120\\d{6}$");
  fn_patterns.push_back("^picserverlog.20130121\\d{6}$");
  fn_patterns.push_back("^picserverlog.20130122\\d{6}$");
  fn_patterns.push_back("^picserverlog.20130123\\d{6}$");
  fn_patterns.push_back("^picserverlog.20130124\\d{6}$");
  fn_patterns.push_back("^picserverlog.20130125\\d{6}$");
  fn_patterns.push_back("^picserverlog.20130126\\d{6}$");
  fn_patterns.push_back("^picserverlog.20130127\\d{6}$");
  fn_patterns.push_back("^picserverlog.20130128\\d{6}$");
  fn_patterns.push_back("^picserverlog.20130129\\d{6}$");
  fn_patterns.push_back("^picserverlog.20130130\\d{6}$");
  fn_patterns.push_back("^picserverlog.20130131\\d{6}$");
  fn_patterns.push_back("^picserverlog.20130201\\d{6}$");
  fn_patterns.push_back("^picserverlog.20130202\\d{6}$");
  fn_patterns.push_back("^picserverlog.20130203\\d{6}$");
  fn_patterns.push_back("^picserverlog.20130204\\d{6}$");
  fn_patterns.push_back("^picserverlog.20130205\\d{6}$");
  fn_patterns.push_back("^picserverlog.20130206\\d{6}$");
  fn_patterns.push_back("^picserverlog.20130207\\d{6}$");
  fn_patterns.push_back("^picserverlog.20130208\\d{6}$");
  fn_patterns.push_back("^picserverlog.20130209\\d{6}$");
  fn_patterns.push_back("^picserverlog.20130210\\d{6}$");
  fn_patterns.push_back("^picserverlog.20130211\\d{6}$");
  fn_patterns.push_back("^picserverlog.20130212\\d{6}$");
  fn_patterns.push_back("^picserverlog.20130213\\d{6}$");
  fn_patterns.push_back("^picserverlog.20130214\\d{6}$");
  fn_patterns.push_back("^picserverlog.20130215\\d{6}$");
  fn_patterns.push_back("^picserverlog.20130216\\d{6}$");
  fn_patterns.push_back("^picserverlog.20130217\\d{6}$");
  fn_patterns.push_back("^picserverlog.20130218\\d{6}$");
  fn_patterns.push_back("^picserverlog.20130219\\d{6}$");
  fn_patterns.push_back("^picserverlog.20130220\\d{6}$");
  fn_patterns.push_back("^picserverlog.20130221\\d{6}$");
  fn_patterns.push_back("^picserverlog.20130222\\d{6}$");
  fn_patterns.push_back("^picserverlog.20130223\\d{6}$");

  for (size_t i = 0; i < fn_patterns.size(); ++i) {
    vector<string> depressed_files;
    vector<vector<string> > depressed_vec;
    // 解压文件
    for (size_t j = 0; j < dirs.size(); ++j) {
      string basename_path = string("/data/log-replay/") + dirs[j];
      string tar_file_pattern = fn_patterns[i].substr(0, fn_patterns[i].size() -1) + string(".tar.gz$");
      fprintf(stderr, "tar_file_pattern %s\n", tar_file_pattern.c_str());
      FileOS file_os_tmp(basename_path);
      string errmsg;
      vector<string> tar_files;
      file_os_tmp.Find(tar_file_pattern, &tar_files, &errmsg);

      vector<string> cmd_vec_tmp;
      for (size_t k = 0; k < tar_files.size(); ++k) {
        char cmd[1024];
        snprintf(cmd, sizeof(cmd), "tar -zxvf %s/%s -C %s/",
                 basename_path.c_str(), tar_files[k].c_str(),
                 basename_path.c_str());
        depressed_files.push_back(basename_path + string("/") + tar_files[k].substr(0, 27));
        cmd_vec_tmp.push_back(cmd);
      }
      depressed_vec.push_back(cmd_vec_tmp);
    }

    vector<pthread_t> depress_tids;
    for (size_t j = 0; j < depressed_vec.size(); ++j) {
      pthread_t depress_tid;
      if (pthread_create(&depress_tid, NULL, DepressThread, &depressed_vec[j]) != 0) {
        fprintf(stderr, "create depress thread failed. %s\n", strerror(errno));
        abort();
      }
      depress_tids.push_back(depress_tid);
    }

    for (size_t j = 0; j < depressed_vec.size(); ++j) {
      pthread_join(depress_tids[j], NULL);
    }

    FileOS file_os;
    string fn_pattern = fn_patterns[i];
    UWarning("begin to simulate %s", fn_pattern.c_str());
    FileMgr *mgr = new FileMgr();
    mgr->Init("/data/log-replay", dirs, fn_pattern, pattern);

    vector<pthread_t> tids;
    for (int j = 0; j < thread_number; ++j) {
      pthread_t tid;
      if (pthread_create(&tid, NULL, DownloadThread, (void*)mgr) != 0) {
        fprintf(stderr, "pthread_create failed. %s\n", strerror(errno));
        return -1;
      }
      tids.push_back(tid);
    }

    for (int j = 0; j < thread_number; ++j) {
      pthread_join(tids[j], NULL);
    }

    UFatal("%s result:", fn_pattern.c_str());
    UFatal("%s leveldb_hit_count: %lu", fn_pattern.c_str(), leveldb_hit_count);
    UFatal("%s mem_hit_count: %lu", fn_pattern.c_str(), mem_hit_count);
    UFatal("%s total_count: %lu", fn_pattern.c_str(), total_count);
    if (total_count != 0) {
      UFatal("%s leveldb_hit_ratio: %f", fn_pattern.c_str(), leveldb_hit_count / (float)total_count);
      UFatal("%s mem_hit_ratio: %f", fn_pattern.c_str(), mem_hit_count / (float)total_count);
    }
    UFatal("%s code_404_count: %lu", fn_pattern.c_str(), code_404_count);
    UFatal("%s error_count: %lu", fn_pattern.c_str(), error_count);
    delete mgr;

    leveldb_hit_count = 0;
    mem_hit_count = 0;
    total_count = 0;
    code_404_count = 0;
    error_count = 0;

    // delete depressed file
    for (size_t j = 0; j < depressed_files.size(); ++j) {
      char cmd[1024];
      snprintf(cmd, sizeof(cmd), "rm -f %s", depressed_files[j].c_str());
      UFatal("delete file. %s", cmd);
      fprintf(stderr, "%s\n", cmd);
      system(cmd);
    }
  }
}
