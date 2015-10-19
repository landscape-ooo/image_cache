#include "msg.h"
namespace ganji { namespace pic_server {
string g_ok_msg = "Status: 200 OK\r\n";
string g_error_msg = "Status: 404 Not Found\r\n";
string g_cache_hit_msg = "Ganji-Cache: Hit\r\n";
string g_cache_miss_msg = "Ganji-Cache: Miss\r\n";
string g_head_end_msg = "\r\n";
string g_content_type_plain = "Content-Type: text/plain; charset=utf-8\r\n";
} }  // namespace ganji::pic_server
