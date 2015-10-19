#include "disk.h"
#include <errno.h>

/**
 * @brief init disk class & cache
 *
 * @return  int
 * @retval
 *    success : Disk::RET_SUCCESS (=0)
 *       fail : < 0
 * @see
 * @note
 * @author wantao
 * @date 2010/06/03 17:16:08
**/
int Disk::init() {

	int ret = 0;
	uint32_t i;

	if (0 == levelnum_) {
		U_LOG_FATAL("Invalid level num![levelnum:%u]", levelnum_);
		ret = RET_ARG_ERR;
		goto err;
	}

	maxhashsize_each_ = maxhashsize_ / levelnum_ + 1;
	maxcachepages_each_ = maxcachepages_ / levelnum_ + 1;

	lru_ = (LRUCache **)calloc(levelnum_, sizeof(LRUCache *));
	if (NULL == lru_) {
		U_LOG_FATAL("Alloc LRUCache pointers array err![size:%zu]",
			levelnum_ * sizeof(LRUCache *));
		ret = RET_NOSPACE_ERR;
		goto err;
	}

	for (i = 0; i < levelnum_; i++) {

		// new each level cache
		lru_[i] = new LRUCache(maxcachepages_each_, pagesize_, maxhashsize_each_);
		if (NULL == lru_[i]) {
			U_LOG_FATAL("Instantiate %u-th level LRUCache err!", i);
			ret = RET_NOSPACE_ERR;
			goto err;
		}

		// init each level cache
		ret = lru_[i]->init();
		if (LRUCache::RET_SUCCESS != ret) {
			U_LOG_FATAL("Initialize %u-th level LRUCache err![%s]",
				i, lru_[i]->perror(ret));
			ret = RET_INNER_ERR;
			goto err;
		}
	}

	return RET_SUCCESS;

err:

	if (NULL != lru_) {
		for (i = 0; i < levelnum_; i++) {
			if (NULL != lru_[i]) {
				delete lru_[i];
				lru_[i] = NULL;
			}
		}
		free(lru_);
		lru_ = NULL;
	}
	return ret;
}

/**
 * @brief read page from cache or disk
 *
 * @param [in] pageno: page no.
 * @param [out] data: page content
 * @return  int
 * @retval
 *    success : Disk::RET_SUCCESS (=0)
 *       fail : < 0
 * @see
 * @note
 * @author wantao
 * @date 2010/05/20 16:54:36
**/
int Disk::read(pgno_t pageno, void *data) {

	int ret;
	ssize_t readlen;
	uint32_t levelno = 0;
	pgno_t pgno_in_level = 0;

	if (NULL == data) {
		U_LOG_FATAL("Null pointer to data when reading page into disk!");
		return RET_MEM_ACCESS_ERR;
	}

	// calc levelno, pgno_in_level
	ret = calc_pgno(pageno, levelno, pgno_in_level);
	if (RET_SUCCESS != ret) {
		U_LOG_FATAL("Calculate pageno err!");
		return RET_INNER_ERR;
	}

	if (levelno >= levelnum_ || pgno_in_level >= maxhashsize_each_) {
		U_LOG_FATAL("Invalid leveno or pgno in level![lelvenum:%u][pgno:%u]"
			"[total_levelnum:%u][maxhashsize_each:%u]",
			levelno, pgno_in_level, levelnum_, maxhashsize_each_);
		return RET_INNER_ERR;
	}

	// seek page in lru cache first
	ret = lru_[levelno]->get(pgno_in_level, data);
	if (ret < 0) {
		U_LOG_FATAL("Seeking page in cache err![pageno:%u][ret:%d][%s]",
			pageno, ret, LRUCache::perror(ret));
		return RET_INNER_ERR;
	}

	// whether it is in lru cache
	if (LRUCache::RET_FOUND == ret) {
		goto found;
	}

	// if not, read page from disk
	readlen = pread(fd_, data, pagesize_, pageno * pagesize_);
	if (pagesize_ != readlen) {
		U_LOG_FATAL("reading page from disk err![fd:%d][readlen:%zd][pagesize:%u]", fd_, readlen, pagesize_);
		return RET_INNER_ERR;
	}

	// push page into cache
	ret = lru_[levelno]->push(pgno_in_level, data);
	if (LRUCache::RET_SUCCESS != ret) {
		U_LOG_FATAL("Cannot push page into lru cache![ret:%d][%s]", ret, LRUCache::perror(ret));
		return RET_INNER_ERR;
	}

found:

	return RET_SUCCESS;
}

/**
 * @brief write page into cache and disk
 *
 * @param [in] pageno: page no.
 * @param [in] data: page content
 * @return  int
 * @retval
 *    success : Disk::RET_SUCCESS (=0)
 *       fail : < 0
 * @see
 * @note
 * @author wantao
 * @date 2010/05/20 16:54:36
**/
int Disk::write(pgno_t pageno, void *data) {

	int ret = 0;
	ssize_t writelen;
	uint32_t levelno = 0;
	pgno_t pgno_in_level = 0;

	if (NULL == data) {
		U_LOG_FATAL("Null pointer to data when writing page into disk!");
		return RET_MEM_ACCESS_ERR;
	}

	// calc levelno, pgno_in_level
	ret = calc_pgno(pageno, levelno, pgno_in_level);
	if (RET_SUCCESS != ret) {
		U_LOG_FATAL("Calculate pageno err!");
		return RET_INNER_ERR;
	}

	if (levelno >= levelnum_ || pgno_in_level >= maxhashsize_each_) {
		U_LOG_FATAL("Invalid leveno or pgno in level![lelvenum:%u][pgno:%u]"
			"[total_levelnum:%u][maxhashsize_each:%u]",
			levelno, pgno_in_level, levelnum_, maxhashsize_each_);
		return RET_INNER_ERR;
	}

	// then write page into disk
	writelen = pwrite(fd_, data, pagesize_, pagesize_ * pageno);
	if (pagesize_ != writelen) {
		U_LOG_FATAL("writing page into disk err![fd:%d][writelen:%zd][pagesize:%u][err:%d-%m] ", fd_, writelen, pagesize_, errno);
		return RET_INNER_ERR;
	}

	// push page into lru cache first
	ret = lru_[levelno]->push(pgno_in_level, data);
	if (LRUCache::RET_SUCCESS != ret) {
		U_LOG_FATAL("Cannot push page into lru cache![ret:%d][%s]", ret, LRUCache::perror(ret));
		return RET_INNER_ERR;
	}

	return RET_SUCCESS;
}

/**
 * @brief alloc a new page from disk
 *
 * @param [in] cur_pgno: page no.
 * @param [out] data: page content
 * @return  int
 * @retval
 *    success : Disk::RET_SUCCESS (=0)
 *       fail : < 0
 * @see
 * @note
 * @author wantao
 * @date 2010/05/20 16:55:55
**/
int Disk::alloc(pgno_t cur_pgno, void *data) {

	int ret;
	struct stat st;
	ssize_t readlen;
	uint32_t levelno = 0;
	pgno_t pgno_in_level = 0;
	off_t pg_off = cur_pgno * pagesize_;

	if (cur_pgno >= maxhashsize_) {
		U_LOG_FATAL("current pageno exceed maximum page number of B+ Tree when alloc new page"
			"[cur_pageno:%u][max_page:%u]", cur_pgno, maxhashsize_);
		ret = RET_NOSPACE_ERR;
		goto err;
	}

	ret = fstat(fd_, &st);
	if (ret < 0) {
		U_LOG_FATAL("fstat file err![ret:%d][pgn:%u]", ret, cur_pgno);
		ret = RET_INNER_ERR;
		goto err;
	}

	if (pg_off + pagesize_ > st.st_size) {
		ret = ftruncate(fd_, pg_off + pagesize_);
		if (ret < 0) {
			U_LOG_FATAL("ftrucate file err![trunc_pos:%u]", (pgno_t)pg_off + pagesize_);
			ret = RET_INNER_ERR;
			goto err;
		}
	}

	// read page from disk directly
	readlen = pread(fd_, data, pagesize_, pg_off);
	if (pagesize_ != readlen) {
		U_LOG_FATAL("reading page from disk err![fd:%d][readlen:%zd][pagesize:%u]", fd_, readlen, pagesize_);
		return RET_INNER_ERR;
	}

	// calc levelno, pgno_in_level
	ret = calc_pgno(cur_pgno, levelno, pgno_in_level);
	if (RET_SUCCESS != ret) {
		U_LOG_FATAL("Calculate pageno err!");
		ret = RET_INNER_ERR;
		goto err;
	}

	if (levelno >= levelnum_ || pgno_in_level >= maxhashsize_each_) {
		U_LOG_FATAL("Invalid leveno or pgno in level![lelvenum:%u][pgno:%u]"
			"[total_levelnum:%u][maxhashsize_each:%u]",
			levelno, pgno_in_level, levelnum_, maxhashsize_each_);
		ret = RET_INNER_ERR;
		goto err;
	}

	// push page into cache
	ret = lru_[levelno]->push(pgno_in_level, data);
	if (ret < 0) {
		ret = RET_INNER_ERR;
		goto err;
	}

	return RET_SUCCESS;

err:
	return ret;
}

// private methods implementation
int Disk::calc_pgno(pgno_t pgno, uint32_t &levelno, pgno_t &pgno_in_level) {

	if (0 == levelnum_) {
		U_LOG_FATAL("Invalid level num![levelnum:%u]", levelnum_);
		return RET_INNER_ERR;
	}

	levelno = pgno % levelnum_;
	pgno_in_level = pgno / levelnum_;

	return RET_SUCCESS;
}
