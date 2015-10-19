#ifndef _GANJI_PIC_SERVER_URL_REGEX_H_
#define _GANJI_PIC_SERVER_URL_REGEX_H_
#include <string.h>
#include <string>
#include <pcre.h>
using std::string;
namespace ganji { namespace pic_server {
class RegexMatch {
 public:
  int Get(const string &name, string *value) {
    value->clear();
    const char *named_string = NULL;
    int ret = pcre_get_named_substring(p, subject.c_str(), ovector, count,
                                       name.c_str(), &named_string);
    if (ret < 0) {
      return -2;
    }
    *value = string(named_string, ret);
    pcre_free_substring(named_string);
    return 0;
  }
  pcre *p;
  pcre_extra *extra;
  int ovector[60];
  int count;
  string subject;
};

class UrlRegex {
 public:
  UrlRegex(const string &pattern) : pattern_(pattern) {
    p_ = NULL;
    extra_ = NULL;
  }
  int Init(string *reason) {
    const char *errptr;
    int erroffset;
    p_ = pcre_compile(pattern_.c_str(), 0, &errptr, &erroffset, NULL);
    if (p_ == NULL) {
      *reason = string("pcre_compile failed. ") + string(errptr);
      return -1;
    }
    extra_ = pcre_study(p_, 0, &errptr);
    if (errptr != NULL) {
      *reason = string("pcre_study failed. ") + string(errptr);
      return -1;
    }
    return 0;
  }
  ~UrlRegex() {
    if (p_) {
      pcre_free(p_);
    }
    if (extra_) {
      pcre_free(extra_);
    }
  }
  string Pattern() {
    return pattern_;
  }
  int Match(const string &subject, RegexMatch *match) {
    int ovector[60];
    int ret;
    ret = pcre_exec(p_, extra_, subject.c_str(), subject.size(), 0, 0,
                    ovector, 60);
    if (ret < 0) {
      return -1;
    }
    match->p = p_;
    match->extra = extra_;
    match->count = ret;
    match->subject = subject;
    memcpy(match->ovector, ovector, sizeof(ovector));
    return 0;
  }

 private:
  UrlRegex(const UrlRegex&);
  void operator=(const UrlRegex);
  string pattern_;
  pcre *p_;
  pcre_extra *extra_;
};
} }  // namespace ganji::pic_server
#endif
