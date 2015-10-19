#include <stdio.h>
#include <stdlib.h>
#include <sys/statfs.h>
#include <sys/types.h>
#include <errno.h>
#include <dirent.h>
#include <assert.h>
#include <iconv.h>
#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/select.h>
#include <sys/syscall.h>
#include <fstream>
#include <stack>
#include <iostream>
#include <algorithm>

#include "myutil.h"
using std::string;
using std::vector;
using std::map;
using std::set;
using std::stack;
using std::ifstream;

#include "ganji/util/text/text.h"
#include "ganji/util/net/http_opt.h"
namespace Text = ganji::util::text::Text;
namespace Http = ganji::util::net::Http;

namespace ganji { namespace pic_server { namespace  myutil {


time_t GetHourTime(time_t time, int hour){
  struct tm tm_time;     
	localtime_r(&time, &tm_time);
  tm_time.tm_hour = hour;
  tm_time.tm_min=0;
  tm_time.tm_sec=0;
  time_t hour_time = mktime(&tm_time);
  return hour_time; 
}

string TmToStr(const tm& tmTime)
{
	char buf[20];
	snprintf(buf,sizeof(buf),"%04d-%02d-%02d-%02d:%02d:%02d",tmTime.tm_year+1900,tmTime.tm_mon+1,tmTime.tm_mday,
		tmTime.tm_hour,tmTime.tm_min,tmTime.tm_sec);
	return buf;
}

bool StrToTm(const string& strTime, tm& tmTime)
{
	sscanf(strTime.c_str(),"%d-%d-%d-%d:%d:%d",&tmTime.tm_year, &tmTime.tm_mon, &tmTime.tm_mday ,
		&tmTime.tm_hour , &tmTime.tm_min, &tmTime.tm_sec );		
	if(!(tmTime.tm_year>=0&&tmTime.tm_year<=3000) || !(tmTime.tm_mon>=1&&tmTime.tm_mon<=12) || !(tmTime.tm_mday>=1&&tmTime.tm_mday<=31) || !(tmTime.tm_hour>=0&&tmTime.tm_hour<=59) || !(tmTime.tm_min>=0&&tmTime.tm_min<=59) || !(tmTime.tm_sec>=0&&tmTime.tm_sec<=59))
		return false;
	tmTime.tm_year -= 1900;
	tmTime.tm_mon -=  1;
	return true;
}


void TimeToTm(const time_t &tTime,tm &tmTime)
{
#ifdef WIN32
	tmTime=*localtime(&tTime);
#else
	localtime_r(&tTime,&tmTime);
	bool bGMTZone=false;
	#ifdef  __USE_BSD
	if(strcmp(tmTime.tm_zone,"GMT")==0)
		bGMTZone=true;		
	#else
	if(strcmp(tmTime.__tm_zone,"GMT")==0)
		bGMTZone=true;		
	#endif
	//if(bGMTZone)
	  //  AppendFile("./SysAlarm.log","系统严重错误:时区被改成GMT!\n");
#endif		
}

time_t TmToTime(tm &tmTime)
{
	time_t tTime=mktime(&tmTime);
	return tTime;
}

string TimeToStr(const time_t &tTime)
{
	struct tm tmTime;
	TimeToTm(tTime,tmTime);		
	return TmToStr(tmTime);
}

time_t StrToTime(const string &strTime)
{
	tm tmTime;
	if(StrToTm(strTime,tmTime))
		return TmToTime(tmTime);
	else
		return 0;
}

uint64_t GetCurTimeUs() {
  struct timeval  tv;
  struct timezone tz;
  tz.tz_minuteswest = 0;
  tz.tz_dsttime     = 0;
  gettimeofday(&tv, &tz);
  uint64_t time = tv.tv_sec * (uint64_t)1000000 + tv.tv_usec;
  return time;
}

//get current time of ms
uint32_t GetCurTimeMs() {
  struct timeval  tv;
  struct timezone tz;
  tz.tz_minuteswest = 0;
  tz.tz_dsttime = 0;
  gettimeofday(&tv, &tz);
  uint32_t time = tv.tv_sec * 1000 + tv.tv_usec / 1000;
  return time;
}

bool isExistDir(const string strDir) {
  DIR * tmp = opendir(strDir.c_str());
  bool ret;
  ret = tmp != NULL ? true : false;
  if (tmp) {
    closedir(tmp);
  }
  return ret;
}

float GetDiskFree(const char *path) {
  struct statfs buf;
  int ret = statfs(path, &buf);
  if (ret != 0) {
    return -1;
  }
  float free_p = float(buf.f_bavail) / buf.f_blocks;
  return free_p;
}

void DoSleep(int32_t milliseconds) {
  if (milliseconds <= 0) {
    return;
  }
  struct timeval tv;
  tv.tv_sec = milliseconds / 1000;
  tv.tv_usec = (milliseconds % 1000) * 1000;
  select(0, 0, 0, 0, &tv);
}

void DoSleepUsec(int32_t usec) {
  if (usec <= 0) {
    return;
  }
  struct timeval tv;
  tv.tv_sec = usec / 1000000;
  tv.tv_usec = usec % 1000000;
  select(0, 0, 0, 0, &tv);
}

// used by ParseConf to format error msg
string ErrorPrefix(const string &file, int line_index) {
  char buf[1024];
  snprintf(buf, sizeof(buf), "%s:%d error: ", file.c_str(), line_index);
  return buf;
}
int ParseConf(const string &file,
              const string &sep,
              map<string, string> *conf_map_p,
              string *reason,
              bool reverse) {
  assert(conf_map_p != NULL);
  map<string, string> &conf_map = *conf_map_p;
  FILE *fp = fopen(file.c_str(), "r");
  if (fp == NULL) {
    *reason = strerror(errno);
    fclose(fp);
    return -1;
  }
  int line_index = 0;
  char *line_p = NULL;
  size_t len = 0;
  ssize_t read_count;
  while ((read_count = getline(&line_p, &len, fp)) != -1) {
    // first trim comment, then trim, then sep
    line_index++;
    string line = line_p;
    Text::Trim(&line);
    size_t comment_pos = line.find('#');
    if (comment_pos != string::npos) {
      line = line.substr(0, comment_pos);
    }
    Text::Trim(&line);
    if (line.size() == 0) {
      continue;
    }
    size_t sep_pos = line.find(sep);
    if (sep_pos == string::npos) {
      *reason = ErrorPrefix(file, line_index) + string("no sep found");
      fclose(fp);
      return -1;
    }
    string cmd = line.substr(0, sep_pos);
    string value = line.substr(sep_pos + sep.size());
    Text::Trim(&cmd);
    Text::Trim(&value);
    if (reverse) {
      string tmp = cmd;
      cmd = value;
      value = tmp;
    }
    if (cmd.size() == 0) {
      *reason = ErrorPrefix(file, line_index) + string("command is empty");
      fclose(fp);
      return -1;
    }
    if (conf_map.find(cmd) != conf_map.end()) {
      *reason = ErrorPrefix(file, line_index) +
                string("duplicate command ") + cmd;
      fclose(fp);
      return -1;
    }
    conf_map[cmd] = value;
  }
  fclose(fp);
  return 0;
}

string IntToStr(int value) {
  char buf[100];
  snprintf(buf, sizeof(buf), "%d", value);
  return buf;
}

string IntToStr(uint64_t value) {
  char buf[100];
  snprintf(buf, sizeof(buf), "%lu", value);
  return buf;
}

// // BUG
// // act=read&key=gjfs05/M04/DE/4D/wKhzUlIY1,3wIB46AAF,XnoxRH8877_600-400_8-0.jpg&&sec_sig=8c67cfe7e57c02cd2efa2e5b4db42033c9f577c7d04f21e2b0b7392818a50c67776af77bd4e9441a56295c5443b4a2939f658269b2e2d4c6b61d8adffc5f4e2a461721cf99088144d3c59764ae2dbd917867633b0032bbf61eda45fac90d199a15aefd862643ae359b4e9a940ae4f62d
// int ParseHttpQuery(const string &query, map<string, string> *param_map) {
//   size_t sharp_pos = query.find("#");
//   string tmp = query;
//   if (sharp_pos != string::npos) {
//     tmp = query.substr(0, sharp_pos);
//   }
//   size_t start = 0, end = string::npos;
//   while (start != string::npos) {
//     end = tmp.find('&', start);
//     size_t equal_pos = tmp.find('=', start);
//     if (equal_pos >= end) {
//       start = end;
//       continue;
//     }
//     string key = tmp.substr(start, equal_pos - start);
//     if (end == string::npos) {
//       string value = tmp.substr(equal_pos + 1, end);
//       Text::TrimAll("\t ", &key);
//       Text::TrimAll("\t ", &value);
//       (*param_map)[key] = DecodeUrl(value);
//       break;
//     } else {
//       string value = tmp.substr(equal_pos + 1, end - equal_pos - 1);
//       Text::TrimAll("\t ", &key);
//       Text::TrimAll("\t ", &value);
//       (*param_map)[key] = DecodeUrl(value);
//       start = end + 1;
//     }
//   }
//   return 0;
// }

int ParseHttpQuery(const string &query, map<string, string> *p_param_map) {
  assert(p_param_map != NULL);
  map<string, string> &query_map = *p_param_map;
  query_map.clear();
  vector<string> vecSeg;
  Text::Segment(query, "&", &vecSeg);
  for (size_t i=0; i < vecSeg.size(); i++) {
    const string& strSeg = vecSeg[i];
    size_t nPos = strSeg.find('=');
    if (nPos != string::npos) {
      const string& strName = strSeg.substr(0,nPos);
      if (query_map.find(strName) == query_map.end()) {
          query_map[strName] = Http::DeescapeURL(strSeg.substr(nPos+1));
      }
    }
  }
  return 0;
}

int GetDateTimeRange(const string &date_in, time_t *pstart, time_t *pend) {
  string date = date_in;
  if (date.size() != 8) {
    return -1;
  }
  struct tm start, end;
  int year, month, day;
  char tmp;
  tmp = date[4];
  date[4] = '\0';
  year = atoi(date.c_str());
  date[4] = tmp;
  tmp = date[6];
  date[6] = '\0';
  month = atoi(date.c_str() + 4);
  date[6] = tmp;
  day = atoi(date.c_str() + 6);
  start.tm_year = end.tm_year = year - 1900;
  start.tm_mon = end.tm_mon = month - 1;
  start.tm_mday = end.tm_mday = day;
  start.tm_hour = start.tm_min = start.tm_sec = 0;
  end.tm_hour = 23;
  end.tm_min = end.tm_sec = 59;
  start.tm_isdst = end.tm_isdst = 0;
  if (pstart != NULL) {
    if ((*pstart = mktime(&start)) == -1) {
      return -1;
    }
  }
  if (pend != NULL) {
    if ((*pend = mktime(&end)) == -1) {
      return -1;
    }
  }
  return 0;
}

// set headers to pheader map
// special header key: Version, Httpcode, CodeMsg
int ParseHttpResponse(const string &data,
                      map<string, string> *pheader,
                      string *pbody) {
  size_t header_end_pos = data.find("\r\n\r\n");
  if (header_end_pos == string::npos) {
    return -1;
  }
  *pbody = data.substr(header_end_pos + 4);
  
  // parse response line
  size_t head_line_end_pos = data.find("\r\n");
  string head_line = data.substr(0, head_line_end_pos);
  size_t start = 0, i = 0;
  while (i < head_line.size() && head_line[i] != ' ') {
    i++;
  }
  (*pheader)["Version"] = head_line.substr(start, i - start);
  i++;
  start = i;
  if (start < head_line.size()) {
    while (i < head_line.size() && head_line[i] != ' ') {
      i++;
    }
    (*pheader)["HttpCode"] = head_line.substr(start, i - start);
    if (i + 1 < head_line.size()) {
      (*pheader)["CodeMsg"] = head_line.substr(i + 1);
    }
  }
  vector<string> header_lines;
  Text::Segment(data.substr(head_line_end_pos + 2, header_end_pos - head_line_end_pos - 2), "\r\n", &header_lines);
  for (size_t j = 0; j < header_lines.size(); ++j) {
    size_t colon_pos = header_lines[j].find(':');
    if (colon_pos == string::npos) {
      return -1;
    }
    string key = header_lines[j].substr(0, colon_pos);
    string value = header_lines[j].substr(colon_pos + 1);
    Text::Trim(&key);
    Text::Trim(&value);
    (*pheader)[key] = value;
  }
  return 0;
}
char Dec2HexChar(short int n) {
  if(0 <= n && n <= 9) {
     return char( short('0') + n );
   }else if ( 10 <= n && n <= 15 ) {
     return char( short('A') + n - 10 );
   }else {
     return char(0);
   }
}

short int HexChar2Dec(char c) {
 if ( '0'<=c && c<='9' ) {
        return short(c-'0');
    } else if ( 'a'<=c && c<='f' ) {
        return ( short(c-'a') + 10 );
    } else if ( 'A'<=c && c<='F' ) {
        return ( short(c-'A') + 10 );
    } else {
        return -1;
    }
}

string EncodeUrl(const string &strUrl)
{
    const static unsigned char urlchr_table[256] =
    {
        1,  1,  1,  1,   1,  1,  1,  1,   /* N1L SOH STX ETX  EOT ENQ ACK BEL */
        1,  1,  1,  1,   1,  1,  1,  1,   /* BS  HT  LF  VT   FF  C1  SO  SI  */
        1,  1,  1,  1,   1,  1,  1,  1,   /* DLE DC1 DC2 DC3  DC4 NAK SYN ETB */
        1,  1,  1,  1,   1,  1,  1,  1,   /* CAN EM  S1B ESC  FS  GS  1S  1S  */
        1,  0,  1,  1,   0,  1,  1,  0,   /* SP  !   "   #    $   %   &   '   */
        0,  0,  0,  1,   0,  0,  0,  1,   /* (   )   *   +    ,   -   .   /   */
        0,  0,  0,  0,   0,  0,  0,  0,   /* 0   1   2   3    4   5   6   7   */
        0,  0,  1,  1,   1,  1,  1,  1,   /* 8   9   :   ;    <   =   >   ?   */
        1,  0,  0,  0,   0,  0,  0,  0,   /* @   A   B   C    D   E   F   G   */
        0,  0,  0,  0,   0,  0,  0,  0,   /* H   I   J   K    L   M   N   O   */
        0,  0,  0,  0,   0,  0,  0,  0,   /* P   Q   R   S    T   1   V   W   */
        0,  0,  0,  1,   1,  1,  1,  0,   /* X   Y   Z   [    \   ]   ^   _   */
        1,  0,  0,  0,   0,  0,  0,  0,   /* `   a   b   c    d   e   f   g   */
        0,  0,  0,  0,   0,  0,  0,  0,   /* h   i   j   k    l   m   n   o   */
        0,  0,  0,  0,   0,  0,  0,  0,   /* p   q   r   s    t   u   v   w   */
        0,  0,  0,  1,   1,  1,  1,  1,   /* x   y   z   {    |   }   ~   DEL */

        1, 1, 1, 1,  1, 1, 1, 1,  1, 1, 1, 1,  1, 1, 1, 1,
        1, 1, 1, 1,  1, 1, 1, 1,  1, 1, 1, 1,  1, 1, 1, 1,
        1, 1, 1, 1,  1, 1, 1, 1,  1, 1, 1, 1,  1, 1, 1, 1,
        1, 1, 1, 1,  1, 1, 1, 1,  1, 1, 1, 1,  1, 1, 1, 1,

        1, 1, 1, 1,  1, 1, 1, 1,  1, 1, 1, 1,  1, 1, 1, 1,
        1, 1, 1, 1,  1, 1, 1, 1,  1, 1, 1, 1,  1, 1, 1, 1,
        1, 1, 1, 1,  1, 1, 1, 1,  1, 1, 1, 1,  1, 1, 1, 1,
        1, 1, 1, 1,  1, 1, 1, 1,  1, 1, 1, 1,  1, 1, 1, 1,
    };
    string strResult;
    for (size_t i=0;i<strUrl.size(); i++ ) {
        unsigned char c = strUrl[i];
        if(urlchr_table[c]==1){   
            strResult += '%';       
            strResult += Dec2HexChar(c >> 4);
            strResult += Dec2HexChar(c & 0xf) ;
        }
        else{
            strResult+=c;
        }
    }
    return strResult;
}

string DecodeUrl(const std::string &strUrl) {
    string result = "";
    for ( unsigned int i=0; i<strUrl.size(); i++ ) {
        char c = strUrl[i];
        if ( c != '%' ) {
            if(c == '+')
                result += ' ';
            else
                result += c;
        }
        else if( (i+2)<strUrl.size())
        {
            char c1 = strUrl[++i];
            char c0 = strUrl[++i];
            int num = 0;
            num += HexChar2Dec(c1) * 16 + HexChar2Dec(c0);
            result += char(num);
        }
    }
    return result;
} 
pid_t GetTid() {
  static __thread pid_t tid = 0;
  if (tid == 0) {
    tid = syscall(SYS_gettid);
  }
  return tid;
}
uint64_t GetUs() {
  struct timeval tm;
  uint64_t usec;
  gettimeofday(&tm, NULL);
  usec = (uint64_t)tm.tv_sec * 1000000 + tm.tv_usec;
  return usec;
}

string GetExtension(const string &name) {
  for (size_t i = name.size() - 1; i >= 0; i--) {
    if (name[i] == '.') {
      return name.substr(i + 1);
    }
  }
  return name;
}
}}}
