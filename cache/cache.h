#ifndef _GANJI_PIC_SERVER_CACHE_CACHE_H_
#define _GANJI_PIC_SERVER_CACHE_CACHE_H_
namespace ganji { namespace pic_server {
template<typename Key, typename Value>
struct DoubleList {
  Key key;
  Value value;
  size_t charge;
  DoubleList *prev;
  DoubleList *next;
  uint32_t refs;
};

template<typename Key, typename Value, typename KeyHash>
class Cache {
 public:
  Cache() { }
  virtual ~Cache() { }
  virtual int Insert(const Key &key, const Value &value, size_t charge) = 0;
  virtual int Delete(const Key &key) = 0;
  virtual int LoopUp(const Key &key, Value *value) = 0;
  virtual int SetCapacity(size_t capacity) = 0;
  virtual int SetMaxSize(size_t max) = 0;
};
} }  // namespace ganji::pic_server
#endif
