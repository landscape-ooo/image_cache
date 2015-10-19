#ifndef _GANJI_PIC_SERVER_CACHE_FIFOCACHE_H_
#define _GANJI_PIC_SERVER_CACHE_FIFOCACHE_H_
#include <stdio.h>
#include <stdlib.h>
#include <tr1/functional>
#include <iostream>
#include <string>
#include <vector>
#include <tr1/unordered_map>

#include "cache/cache.h"
#include "mutex.h"
using std::string;
using std::vector;
using std::tr1::hash;
using std::tr1::unordered_map;
using ganji::pic_server::Mutex;
using ganji::pic_server::MutexGuard;

namespace ganji { namespace pic_server {
enum {
  FIFOCacheOK = 0,
  FIFOCacheToBig = -1,
  FIFOCacheNotFound = -2,
  FIFOCacheFailed = -3
};

template<typename Key, typename Value, typename KeyHash = hash<Key> >
class FIFOCache {
 public:
  FIFOCache() : usage_(0) {
    fifo_.prev = &fifo_;
    fifo_.next = &fifo_;
  }
  ~FIFOCache() {
    DoubleList<Key, Value> *node = fifo_.next;
    while (node != &fifo_) {
      node = node->next;
      delete node->prev;
    }
  }
  int Insert(const Key &key, const Value &value, size_t charge);
  int LookUp(const Key &key, Value *value);
  int Delete(const Key &key);
  string Stat();
  bool Empty() {
    return map_.size() == 0 && fifo_.next == &fifo_ && fifo_.prev == &fifo_;
  }
  void dump() {
    fprintf(stderr, "------fifo cache dump--------\n");
    fprintf(stderr, "Usage: %lu\n", usage_);
    fprintf(stderr, "capacity: %lu\n", capacity_);
    fprintf(stderr, "max size: %lu\n", max_);
    fprintf(stderr, "map size: %lu\n", map_.size());
    int list_node_count = 0;
    DoubleList<Key, Value> *node = fifo_.next, *head = &fifo_;
    while (node != head) {
      list_node_count++;
      Key key = node->key;
      std::cout<<key<<"\t"<<node->value<<"\t"<<node->charge<<std::endl;
      if (map_.find(key) == map_.end()) {
        fprintf(stderr, "can not find in map\n");
      }
      node = node->next;
    }
    fprintf(stderr, "list count: %lu\n", list_node_count);
  }

  void SetCapacity(size_t capacity) { capacity_ = capacity; }
  void SetMaxSize(size_t max) { max_ = max; }
 private:
  void FIFO_Append(DoubleList<Key, Value> *node);
  void FIFO_Remove(DoubleList<Key, Value> *node);
  KeyHash key_hash_;
  size_t capacity_;
  unordered_map<Key, DoubleList<Key, Value>*, KeyHash> map_;
  size_t usage_;
  size_t max_;
  Mutex mutex_;
  DoubleList<Key, Value> fifo_; // fifo_.prev is newest, fifo_.next is oldest
};

template<typename Key, typename Value, typename KeyHash>
string FIFOCache<Key, Value, KeyHash>::Stat() {
  char buf[128];
  MutexGuard guard(&mutex_);
  snprintf(buf, sizeof(buf), "capacity: %lu, usage: %lu %.2f",
           capacity_, usage_, (float)((double)usage_ / capacity_));
  return string(buf);
}

template<typename Key, typename Value, typename KeyHash>
void FIFOCache<Key, Value, KeyHash>::FIFO_Append(DoubleList<Key, Value> *node) {
  node->next = &fifo_;
  node->prev = fifo_.prev;
  node->prev->next = node;
  node->next->prev = node;
}

template<typename Key, typename Value, typename KeyHash>
void FIFOCache<Key, Value, KeyHash>::FIFO_Remove(DoubleList<Key, Value> *node) {
  node->next->prev = node->prev;
  node->prev->next = node->next;
}

template<typename Key, typename Value, typename KeyHash>
int FIFOCache<Key, Value, KeyHash>::LookUp(const Key &key, Value *value) {
  MutexGuard guard(&mutex_);
  typename unordered_map<Key, DoubleList<Key, Value>*, KeyHash>::iterator it = map_.find(key);
  if (it == map_.end()) {
    return FIFOCacheNotFound;
  }
  *value = it->second->value;
  return FIFOCacheOK;
}

template<typename Key, typename Value, typename KeyHash>
int FIFOCache<Key, Value, KeyHash>::Insert(const Key &key, const Value &value, size_t charge) {
  if (charge > max_) {
    return FIFOCacheToBig;
  }
  DoubleList<Key, Value> *node = new DoubleList<Key, Value>();
  if (node == NULL) {
    return FIFOCacheFailed;
  }
  node->key = key;
  node->value = value;
  node->charge = charge;
  MutexGuard guard(&mutex_);
  FIFO_Append(node);
  usage_ += charge;
  typename unordered_map<Key, DoubleList<Key, Value>*, KeyHash>::iterator it = map_.find(key);
  if (it != map_.end()) {
    FIFO_Remove(it->second);
    usage_ -= it->second->charge;
    delete it->second;
    // map_.erase(it);
  }
  map_[key] = node;
  while (usage_ > capacity_ && fifo_.next != &fifo_) {
    DoubleList<Key, Value> *old = fifo_.next;
    FIFO_Remove(old);
    map_.erase(old->key);
    usage_ -= old->charge;
    delete old;
  }
  return FIFOCacheOK;
}

template<typename Key, typename Value, typename KeyHash>
int FIFOCache<Key, Value, KeyHash>::Delete(const Key &key) {
  MutexGuard guard(&mutex_);
  typename unordered_map<Key, DoubleList<Key, Value>*, KeyHash>::iterator it = map_.find(key);
  if (it == map_.end()) {
    return FIFOCacheNotFound;
  }
  FIFO_Remove(it->second);
  usage_ -= it->second->charge;
  delete it->second;
  map_.erase(it);
  return FIFOCacheOK;
}

template<typename Key, typename Value, typename KeyHash = hash<Key> >
class ShardedFIFO {
 public:
  explicit ShardedFIFO(size_t capacity, size_t max_size, int core_number) {
    shard_number_ = (int)(core_number * 1.5);
    max_size_ = max_size;
    per_shard_ = (capacity + shard_number_ - 1) / shard_number_;
    for (int i = 0; i < shard_number_; ++i) {
      shards_.push_back(new FIFOCache<Key, Value, KeyHash>());
      shards_[i]->SetCapacity(per_shard_);
      shards_[i]->SetMaxSize(max_size_);
    }
  }

  ~ShardedFIFO() {
    for (size_t i = 0; i < shards_.size(); ++i) {
      delete shards_[i];
    }
  }
  
  int Insert(const Key &key, const Value &value, size_t charge) {
    int index = key_hash(key) % shard_number_;
    return shards_[index]->Insert(key, value, charge);
  }

  int Delete(const Key &key) {
    int index = key_hash(key) % shard_number_;
    return shards_[index]->Delete(key);
  }

  int LookUp(const Key &key, Value *value) {
    int index = key_hash(key) % shard_number_;
    return shards_[index]->LookUp(key, value);
  }

  void Stat(string *pstat) {
    assert(pstat != NULL);
    for (int i = 0; i < shard_number_; ++i) {
      char buf[128];
      snprintf(buf, sizeof(buf), "shard-%d   %s\n", i,
               shards_[i]->Stat().c_str());
      *pstat += string(buf);
    }
  }

  void dump() {
    fprintf(stderr, "------sharded fifo dump------\n");
    fprintf(stderr, "shard_number: %d\n", shard_number_);
    fprintf(stderr, "per_shard: %lu\n", per_shard_);
    fprintf(stderr, "max size: %lu\n", max_size_);
    for(int i = 0; i < shards_.size(); ++i) {
      shards_[i]->dump();
    }
  }
 private:
  KeyHash key_hash;
  int shard_number_;
  size_t per_shard_;
  int max_size_;
  vector<FIFOCache<Key, Value, KeyHash>*> shards_;
};
} }  // namespace ganji::pic_server
#endif
