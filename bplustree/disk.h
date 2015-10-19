/***************************************************************************
 *
 * Copyright (c) 2010 Baidu.com, Inc. All Rights Reserved
 * $Id: disk.h,v 1.0 2010/05/19 11:16:11 wantao Exp $
 *
 **************************************************************************/


/**
 * @file disk.h
 * @author wantao
 * @date 2010/05/20 16:57:27
 * @version $Revision: 1.0 $
 * @brief interface encapsulation of lru cache module
 *
 **/
#ifndef _DISK_H
#define _DISK_H

#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <math.h>
#include "ulog.h"
#include "lrucache.h"

typedef uint32_t pgno_t;

static const char *DISK_ERR_STR[] = { 
	"DISK: Success",
	"DISK: Inner Error",
	"LRUCache: Not Enough Memory",
	"DISK: Access Memory Error",
	"DISK: ARG Error",
	"DISK: Unknown Error"
};

class Disk {
public:
	enum RET_TYPE {
		RET_SUCCESS = 0,
		RET_INNER_ERR = -1,
		RET_NOSPACE_ERR = -2,
		RET_MEM_ACCESS_ERR = -3,
		RET_ARG_ERR = -4,
		RET_ALL
	};

	Disk(int fd, pgno_t pgs, uint32_t ln, uint32_t mhs, pgno_t mcp)
		: fd_(fd), pagesize_(pgs), levelnum_(ln), maxhashsize_(mhs), maxcachepages_(mcp), 
			maxhashsize_each_(0), maxcachepages_each_(0), lru_(NULL) {};
	~Disk() {;
        WriteLog(kLogTrace, "Disk destruction!");
		if (NULL != lru_) {
			for (uint32_t i = 0; i < levelnum_; i++) {
				if (NULL != lru_[i]) {
					delete lru_[i];
					lru_[i] = NULL;
				}
			}
			free(lru_);
			lru_ = NULL;
		}
	}
	int init();
	int write(pgno_t pageno, void *data);
	int read(pgno_t pageno, void *data);
	int alloc(pgno_t cur_pgno, void *data);

	static const char *perror(int err) {
		if (err > 0 || err < RET_ALL) {
			err = RET_ALL;
		}
		return DISK_ERR_STR[-err];
	}

private:
	int fd_;                         /*< file descriptor */
	pgno_t pagesize_;                /*< page size */
	uint32_t levelnum_;              /*< number of levels of cache */
	uint32_t maxhashsize_;           /*< max hash size totally */
	pgno_t maxcachepages_;           /*< max cache pages totally */
	uint32_t maxhashsize_each_;      /*< max hash size for each level of cache */
	pgno_t maxcachepages_each_;      /*< max cache pages for each level of cache */
	LRUCache **lru_;                 /*< lru array */

	int calc_pgno(pgno_t pgno, uint32_t &level_num, pgno_t &pgno_in_level);
};

#endif // _DISK_H
