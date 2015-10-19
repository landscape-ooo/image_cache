#include "ganji/util/log/thread_fast_log.h"
//#include "ganji/util/log/fast_log.h"
//using namespace ganji::util::log;
using namespace ganji::util::log::ThreadFastLog;

#define U_LOG_MONITOR(fmt, arg...)  \
    WriteLog(kLogFatal, "[%s:%d]" fmt,  __FILE__, __LINE__, ## arg)

#define U_LOG_FATAL(fmt, arg...) do { \
    WriteLog(kLogFatal, "[%s:%d]" fmt,  __FILE__, __LINE__, ## arg);\
} while (0)

#define U_LOG_WARNING(fmt, arg...)  \
    WriteLog(kLogWarning, "[%s:%d]" fmt,  __FILE__, __LINE__, ## arg )

#define U_LOG_NOTICE(fmt, arg...) do { \
    WriteLog(kLogNotice, fmt, ## arg);\
} while (0)

#define U_LOG_TRACE(fmt, arg...)  \
    WriteLog(kLogTrace, fmt, ## arg)

#ifndef NO_UB_DEBUG
#define U_LOG_DEBUG(fmt, arg...) \
    WriteLog(kLogDebug, fmt, ## arg)
#else
#define U_LOG_DEBUG(fmt, arg...) \
    do {} while(0)
#endif

