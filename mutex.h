#ifndef _GANJI_PIC_SERVER_MUTEX_H_
#define _GANJI_PIC_SERVER_MUTEX_H_
#include <string.h>
#include <pthread.h>
namespace ganji { namespace pic_server {
class Mutex {
 public:
  Mutex() {
    PthreadCall("init mutex", pthread_mutex_init(&mutex_, NULL));
  }

  ~Mutex() {
    PthreadCall("destroy mutex", pthread_mutex_destroy(&mutex_));
  }

  void Lock() {
    if (pthread_mutex_lock(&mutex_) != 0) {
      abort();
    }
  }

  void Unlock() {
    if (pthread_mutex_unlock(&mutex_) != 0) {
      abort();
    }
  }

 private:
  void PthreadCall(const char *label, int result) {
    if (result != 0) {
      fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
      abort();
    }
  }
  pthread_mutex_t mutex_;
  
  // no copy allowed
  Mutex(const Mutex&);
  void operator=(const Mutex&);
};

class MutexGuard {
 public:
  explicit MutexGuard(Mutex *mutex) {
    mutex_ = mutex;
    mutex_->Lock();
  }

  ~MutexGuard() {
    mutex_->Unlock();
  }

 private:
  MutexGuard(const MutexGuard&);
  void operator=(const MutexGuard&);
  Mutex *mutex_;
};
} }  // namespace ganji::pic_server
#endif
