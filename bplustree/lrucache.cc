#include "lrucache.h"

// public methods implementation

/**
 * @brief initialize lru cache
 *
 * @return  int
 * @retval
 *    success : LRUCache::RET_SUCCESS (=0)
 *       fail : < 0
 * @see
 * @note
 * @author wantao
 * @date 2010/05/20 17:10:24
**/
int LRUCache::init() {

	int ret = 0;
	pgno_t i;

	// alloc mpool for cache, tha last mem is redundant one
	cache_mpool_ = (cache_node_t *)calloc(maxcachepages_+1, sizeof(cache_node_t));
	if (NULL == cache_mpool_) {
		U_LOG_FATAL("Alloc memory for cache mpool err![maxpage:%u][size:%zu]",
				maxcachepages_, (maxcachepages_+1)*sizeof(cache_node_t));
		ret = RET_NOSPACE_ERR;
		goto err;
	}

	for (i = 0; i <= maxcachepages_; i++) {

		cache_mpool_[i].page_ = NULL;

		if (0 == i) {
			cache_mpool_[i].prev_ = NULL;
		} else {
			cache_mpool_[i].prev_ = &cache_mpool_[i-1];
		}

		if (maxcachepages_ == i) {
			cache_mpool_[i].next_ = NULL;
		} else {
			cache_mpool_[i].next_ = &cache_mpool_[i+1];
		}

	}

	// initialize cache_head_ & cache_tail_ pointers
	cache_head_ = cache_mpool_;
	cache_tail_ = cache_mpool_;

	// alloc memory for hash table
	hash_ = (hash_node_t *)calloc(maxhashsize_, sizeof(hash_node_t));
	if (NULL == hash_) {
		U_LOG_FATAL("Alloc memory for hash array err![maxhashsize:%u][size:%zu]",
				maxhashsize_, (maxhashsize_) * sizeof(hash_node_t));
		ret = RET_NOSPACE_ERR;
		goto err;
	}

  /*
	if (!cache_mutex_.init()) {
		U_LOG_FATAL("Cache mutex init err!");
		ret = RET_INNER_ERR;
		goto err;
	}
  */

#ifdef _LRUCACHE_STATISTICS_
		cachehit_ = 0;
		cachemiss_ = 0;
#endif

	return RET_SUCCESS;

err:

	if (NULL != cache_mpool_) {
		free(cache_mpool_);
		cache_mpool_ = NULL;
	}

	if (NULL != hash_) {
		free(hash_);
		hash_ = NULL;
	}

	return ret;
}


/**
 * @brief seek page in lru cache
 *
 * @return  int
 * @retval
 *    success : LRUCache::RET_SUCCESS (=0)
 *       fail : < 0
 * @see
 * @note
 * @author wantao
 * @date 2010/05/20 17:11:28
**/
int LRUCache::get(pgno_t pageno, void *data) {

	cache_node_t *cache_ptr = NULL;
	bool hit;

	// arg check
	if (NULL == data) {
		U_LOG_FATAL("pointer to res data buffer err!");
		return RET_ARG_ERR;
	}

	if (pageno >= maxhashsize_) {
		U_LOG_FATAL("pageno exceed maxhashsize![pgno:%u][maxhashsize:%u]",
			pageno, maxhashsize_);
		return RET_ARG_ERR;
	}

	// lock the whole cache
  ganji::util::thread::MutexGuard guard(&cache_mutex_);

	hit = ishitcache(pageno, &cache_ptr);
	if (hit) {

		if (NULL == cache_ptr) {
			U_LOG_FATAL("Invalid cache page address.[pageno:%u]", pageno);
			return RET_INNER_ERR;
		}

#ifdef _LRUCACHE_STATISTICS_
		++cachehit_;
#endif

		if (NULL == cache_ptr->page_) {
			U_LOG_FATAL("Null pointer to page in cache node!");
			return RET_INNER_ERR;
		}

		memcpy(data, cache_ptr->page_, (size_t)pagesize_);

		adjust_cache(cache_ptr);

	} else {

#ifdef _LRUCACHE_STATISTICS_
		++cachemiss_;
#endif

		return RET_NOTFOUND;
	}
	return RET_FOUND;
}

/**
 * @brief put page into lru cache
 *
 * @return  int
 * @retval
 *    success : LRUCache::RET_SUCCESS (=0)
 *       fail : < 0
 * @see
 * @note
 * @author wantao
 * @date 2010/05/20 17:12:27
**/
int LRUCache::push(pgno_t pageno, void *data) {

	// put the data into cache, and then reset hash node

	int ret = 0;
	cache_node_t *cache_ptr;

	// arg check
	if (NULL == data) {
		U_LOG_FATAL("pointer to res data buffer err!");
		return RET_ARG_ERR;
	}

	if (pageno >= maxhashsize_) {
		U_LOG_FATAL("pageno exceed maxhashsize![pgno:%u][maxhashsize:%u]",
			pageno, maxhashsize_);
		return RET_ARG_ERR;
	}

	// lock the whole cache
	//qtl::Mutex::Lock guard(cache_mutex_);
  ganji::util::thread::MutexGuard guard(&cache_mutex_);

	// whether it is already in lru cache
	if (ishitcache(pageno, &cache_ptr)) {

		// if so, replace data first, and then adjust the order of lru cache
		if (NULL == cache_ptr->page_) {
			U_LOG_FATAL("Null pointer to page in cache node!");
			return RET_INNER_ERR;
		}

		memcpy(cache_ptr->page_, data, pagesize_);
		adjust_cache(cache_ptr);

		goto suc;
	}

	// whether lru cache is full
	if (isfull()) {

		// if so, pop the oldest used page out of lru cache
		ret = pop();
		if (0 != ret) {
			U_LOG_FATAL("Pop old page out of cache err![ret:%d]", ret);
			return RET_INNER_ERR;
		}
	}

	// put the data into cache
	cache_ptr = cache_tail_;
	if (NULL == cache_ptr) {
		U_LOG_FATAL("Null pointer to new cache node!");
		return RET_MEM_ACCESS_ERR;
	}

	cache_tail_ = cache_tail_->next_;

	if (NULL == cache_ptr->page_) {
		// alloc space for page
		cache_ptr->page_ = calloc(1, pagesize_);
		if (NULL == cache_ptr->page_) {
			U_LOG_FATAL("alloc space for page when pushing page into lrucache![pagesize:%u]", pagesize_);
			return RET_NOSPACE_ERR;
		}
	}

	// copy data into cache
	memcpy(cache_ptr->page_, data, (size_t)pagesize_);
	cache_ptr->pageno_ = pageno;

	// adjust cache order
	adjust_cache(cache_ptr);

	// cacheleft_ decrease by 1
	--cacheleft_;

	// reset hash node
	hash_[pageno].cache_ptr_ = cache_ptr;

suc:
	return RET_SUCCESS;
}

// private methods implementation

bool LRUCache::ishitcache(pgno_t pageno, cache_node_t **cache_ptr) {
	*cache_ptr = hash_[pageno].cache_ptr_;
	return (NULL != *cache_ptr);
}

bool LRUCache::isfull() {
	return (cacheleft_ == 0x0);
}

int LRUCache::adjust_cache(cache_node_t *ptr) {

	if (NULL == ptr || NULL == cache_head_) {
		U_LOG_FATAL("Null pointer to cache node when adjusting cache order!");
		return RET_MEM_ACCESS_ERR;
	}

	if (NULL == ptr->prev_) {
		// it is already at the first position
		goto out;
	}

	ptr->prev_->next_ = ptr->next_;

	if (NULL != ptr->next_) {
		ptr->next_->prev_ = ptr->prev_;
	}

	cache_head_->prev_ = ptr;
	ptr->prev_ = NULL;
	ptr->next_ = cache_head_;

	cache_head_ = ptr;

out:
	return RET_SUCCESS;
}

int LRUCache::pop() {

	pgno_t pageno;

	// reset hash node first, and then collect cache node
	if (NULL == cache_tail_->prev_) {
		U_LOG_FATAL("Access invalid memory when pop old page out of cache!");
		return RET_MEM_ACCESS_ERR;
	}

	// reset hash node
	pageno = cache_tail_->prev_->pageno_;
	hash_[pageno].cache_ptr_ = NULL;

	// collect cache node
	cache_tail_ = cache_tail_->prev_;
	++cacheleft_;

	return RET_SUCCESS;
}
