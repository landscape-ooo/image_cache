#ifndef _GANJI_PIC_FCGI_FUNCTION_H_
#define _GANJI_PIC_FCGI_FUNCTION_H_
#include <string>
using std::string;
namespace ganji { namespace pic_server {
void* FCGXMainLoop(void *args);

// this could be call before any fcgi thread creation
const string& GetErrorMsg();

void HandleError(FCGX_Request *request);
} }  // namespace ganji::pic_server
#endif
