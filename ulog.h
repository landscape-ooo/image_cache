#include "ganji/util/log/thread_fast_log.h"
#include "myutil.h"
namespace Log = ganji::util::log::ThreadFastLog;
using ganji::pic_server::myutil::GetTid;
using ganji::pic_server::myutil::GetUs;

#define UFatal(fmt, arg...) \
  Log::WriteLog(Log::kLogFatal, "[%d %s:%d %lu] " fmt, GetTid(), __FILE__, __LINE__, GetUs(), ## arg)

#define UWarning(fmt, arg...) \
  Log::WriteLog(Log::kLogWarning, "[%d %s:%d %lu] " fmt, GetTid(), __FILE__, __LINE__, GetUs(), ## arg)


#define UTrace(fmt, arg...) \
  Log::WriteLog(Log::kLogTrace, "[%d %s:%d %lu] " fmt, GetTid(), __FILE__, __LINE__, GetUs(), ## arg)

//#define UNotice(fmt, arg...) Log::WriteLog(Log::kLogNotice, "[%d %s:%d %lu] " fmt, GetTid(), __FILE__, __LINE__, GetUs(), ## arg)

#define UNotice(fmt, arg...) \
  Log::WriteLog(Log::kLogNotice, fmt, ## arg)

#define UDebug(fmt, arg...) \
  Log::WriteLog(Log::kLogDebug, "[%d %s:%d %lu] " fmt, GetTid(), __FILE__, __LINE__, GetUs(), ## arg)
