#ifndef _GANJI_PIC_SERVER_RWLOCK_H_
#define _GANJI_PIC_SERVER_RWLOCK_H_
#include <errno.h>
#include <string.h>
#include <pthread.h>
namespace ganji { namespace pic_server {
class Rwlock {
 public:
  Rwlock() {
    PthreadCall("init rwlock", pthread_rwlock_init(&rwlock_, NULL));
  }

  ~Rwlock() {
    PthreadCall("destroy rwlock", pthread_rwlock_destroy(&rwlock_));
  }

  void Rdlock() {
    if (pthread_rwlock_rdlock(&rwlock_) != 0) {
      abort();
    }
  }

  void Wrlock() {
    if (pthread_rwlock_wrlock(&rwlock_) != 0) {
      abort();
    }
  }

  void Unlock() {
    if (pthread_rwlock_unlock(&rwlock_) != 0) {
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
  pthread_rwlock_t rwlock_;

  Rwlock(const Rwlock&);
  void operator=(const Rwlock&);
};

class RdGuard {
 public:
  explicit RdGuard(Rwlock *rwlock) : rwlock_(rwlock) {
    rwlock_->Rdlock();
  }

  ~RdGuard() {
    rwlock_->Unlock();
  }
 
 private:
  Rwlock *rwlock_;

  RdGuard(const RdGuard&);
  void operator=(const RdGuard&);
};

class WrGuard {
 public:
  explicit WrGuard(Rwlock *rwlock) : rwlock_(rwlock) {
    rwlock_->Wrlock();
  }

  ~WrGuard() {
    rwlock_->Unlock();
  }

 private:
  Rwlock *rwlock_;

  WrGuard(const WrGuard&);
  void operator=(const WrGuard&);
};
} }  // namespace ganji::pic_server
#endif
