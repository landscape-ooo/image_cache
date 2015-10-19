/***************************************************************************
 *
 * Copyright (c) 2010 Baidu.com, Inc. All Rights Reserved
 * $Id: lrucache.h,v 1.0 2010/05/19 11:16:11 wantao Exp $
 *
 **************************************************************************/

/**
 * @file lrucache.h
 * @author wantao
 * @date 2010/05/20 17:04:50
 * @version $Revision: 1.0 $
 * @brief lru cache module
 *
 **/

#ifndef _LRUCACHE_H_
#define _LRUCACHE_H_

#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "ulog.h"
#include "ganji/util/thread/mutex.h"

using ganji::util::thread::Mutex;
using ganji::util::thread::MutexGuard;

typedef uint32_t pgno_t;

static const char *LRUCache_ERR_STR[] = { 
	"LRUCache: Success",
	"LRUCache: Inner Error",
	"LRUCache: Not Enough Memory",
	"LRUCache: Access Memory Error",
	"LRUCache: ARG Error",
	"LRUCache: Unknown Error"
};

class LRUCache {

public:
	enum RET_TYPE {
		RET_FOUND = 2,
		RET_NOTFOUND = 1,
		RET_SUCCESS = 0,
		RET_INNER_ERR = -1,
		RET_NOSPACE_ERR = -2,
		RET_MEM_ACCESS_ERR = -3,
		RET_ARG_ERR = -4,
		RET_ALL
	};
	
	LRUCache(pgno_t mcp, pgno_t pgs, uint32_t mhs) 
		: maxcachepages_(mcp), pagesize_(pgs), maxhashsize_(mhs), 
		cacheleft_(mcp), cache_mpool_(NULL), hash_(NULL) {}

	~LRUCache() {

		if (NULL != hash_) {
            WriteLog(kLogTrace, "LRUCache Hash destruction!");
			free(hash_);
			hash_ = NULL;
		}
		
		if (NULL != cache_mpool_) {
            WriteLog(kLogTrace, "LRUCache Cache destruction!");
			cache_node_t *ptr = cache_mpool_;
			while (ptr) {
				if (NULL != ptr->page_) {
                    WriteLog(kLogTrace, "LRUCache Cache2 destruction!");
					free(ptr->page_);
					ptr->page_ = NULL;
				}
				ptr = ptr->next_;
			}
			free(cache_mpool_);
			cache_mpool_ = NULL;
		}
		
		// destory cache mutex
		//cache_mutex_.destroy();
	}
	
	int init();
	int get(pgno_t pageno, void *data);
	int push(pgno_t pageno, void *data);

	static const char *perror(int err) {
		if (err > 0 || err < RET_ALL) {
			err = RET_ALL;
		}
		return LRUCache_ERR_STR[-err];
	}

#ifdef _LRUCACHE_STATISTICS_
	void print_stat() {
		printf("hit: %u, miss: %u, hit rate: %.3lf\n", 
			cachehit_, cachemiss_, (double)cachehit_ / (cachehit_ + cachemiss_) * 100);
	}
#endif

#ifdef _LRUCACHE_DEBUG_
	int print_lru() {
		cache_node_t *ptr = cache_head_;
		while (ptr != cache_tail_) {
			printf("<%u, %u> - ", ptr->pageno_, *((pgno_t *)(ptr->page_)));
			ptr = ptr->next_;
		}
		printf("\n");
		return 0;
	}

	int print_hash() {

		for (uint32_t i = 0; i < maxhashsize_; i++) {
			printf("bucket[%u]:", i);
			if (hash_[i].cache_ptr_) {
				printf(" %u\n", i);
			} else {
				printf("NULL\n");
			}
		}
		return 0;
	}
#endif

private:
	pgno_t maxcachepages_;
	pgno_t pagesize_;
	uint32_t	maxhashsize_;
	uint32_t	cacheleft_;
	
	typedef struct _cache_node_t {
		void *page_;
		pgno_t pageno_;
		uint8_t flags_;
		_cache_node_t *prev_;
		_cache_node_t *next_;
	} cache_node_t;

	typedef struct _hash_node_t {
		pgno_t pageno_;
		_cache_node_t *cache_ptr_;
	} hash_node_t;
	
	cache_node_t *cache_mpool_;
	cache_node_t *cache_head_;
	cache_node_t *cache_tail_;

	hash_node_t *hash_;

	// private methods for cache
	bool ishitcache(pgno_t pageno, cache_node_t **cache_ptr );
	bool isfull();
	int adjust_cache(cache_node_t *cache_ptr);
	int pop();

#ifdef _LRUCACHE_STATISTICS_
	uint32_t	cachehit_;
	uint32_t	cachemiss_;
#endif

	ganji::util::thread::Mutex cache_mutex_;
};


#endif
