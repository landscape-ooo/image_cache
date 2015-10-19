#include <stdint.h>
#include <pthread.h>
#include <signal.h>
#include <string>
using std::string;
uint64_t g_timestamp;
pthread_t g_time_thread_id;
sig_atomic_t g_stop = 0;
string g_ok_msg = "Status: 200 OK\r\n";
string g_error_msg = "Status: 404 Not Found\r\n";
string g_cache_hit_msg = "Ganji-Cache: Hit-1\r\n";
string g_cache_hit_mem_msg = "Ganji-Cache: Hit-2\r\n";
string g_cache_miss_msg = "Ganji-Cache: Miss\r\n";
string g_head_end_msg = "\r\n";
string g_content_type_plain = "Content-Type: text/plain; charset=utf-8\r\n";

pthread_mutex_t g_fdfs_init_mutex = PTHREAD_MUTEX_INITIALIZER;
