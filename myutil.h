#ifndef _GANJI_PIC_SERVER_MYUTIL_H_
#define _GANJI_PIC_SERVER_MYUTIL_H_
#include <stdint.h>
#include <sys/types.h>
#include <string>
#include <vector>
#include <map>
#include <set>
#include <sstream>
#include <utility>
#include <dirent.h>
using std::string;
using std::vector;
using std::set;
using std::map;
using std::pair;

namespace ganji { namespace pic_server { namespace myutil {
time_t GetHourTime(time_t time, int hour);

///tm到str的转换
std::string TmToStr(const tm& tmTime);
///str到tm的转换:字符串不合法,则返回false
bool StrToTm(const std::string& strTime, tm& tmTime);

///time_t到tm的转换
void TimeToTm(const time_t &tTime, tm &tmTime);
///tm到time_t的转换
time_t TmToTime(struct tm &tmTime);

///time_t向str的转换
std::string TimeToStr(const time_t &tTime);
///str向time_t的转换:字符串不合法,则返回0
time_t StrToTime(const std::string &strTime);

uint64_t GetCurTimeUs();

uint32_t GetCurTimeMs();

bool isExistDir( const string strDir);

// get disk free space percentage
float GetDiskFree(const char *path);
void DoSleep(int32_t milliseconds);
void DoSleepUsec(int32_t usec);

// read "command sep value" config file to map
// support # comment
int ParseConf(const string &file,
              const string &sep,
              map<string, string> *conf_map_p,
              string *reason,
              bool reverse = false);

string IntToStr(int value);
string IntToStr(uint64_t value);

int ParseHttpQuery(const string &query, map<string, string> *param_map);
int GetDateTimeRange(const string &date, time_t *pstart, time_t *pend);

int ParseHttpResponse(const string &data,
                     map<string, string> *pheader,
                     string *pbody);
char Dec2HexChar(short int n);
short int HexChar2Dec(char c);
string EncodeUrl(const string &strUrl);
string DecodeUrl(const std::string &strUrl);

pid_t GetTid();
uint64_t GetUs();
string GetExtension(const string &name);
}}}
#endif
