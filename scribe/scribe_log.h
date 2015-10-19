#ifndef _GANJI_PIC_SERVER_SCRIBE_LOG_H_
#define _GANJI_PIC_SERVER_SCRIBE_LOG_H_
namespace ganji { namespace pic_server { namespace scribe {
enum {
  kScribeLogDebug = 0x1,
  kScribeLogTrace = 0x2,
  kScribeLogNotice = 0x4,
  kScribeLogWarning = 0x8,
  kScribeLogFatal = 0x10,
  kScribeLogAccess = 0x20
};
}}}  // end of namespace ganji::pic_server::scribe
#endif
