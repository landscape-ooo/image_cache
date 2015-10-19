/***************************************************************************
 *
 * Copyright (c) 2010 Baidu.com, Inc. All Rights Reserved
 * $Id: bplustree.h,v 1.0 2010/05/19 11:16:11 wantao Exp $
 *
 **************************************************************************/

/**
 * @file bplustree.h
 * @author wantao
 * @date 2010/05/26 10:00:06
 * @version $Revision: 1.0 $
 * @brief implementation of B+ Tree
 *
 **/

#ifndef _BPLUSTREE_H_
#define _BPLUSTREE_H_

#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdint.h>
#include <sys/mman.h>
#include <errno.h>
#include "ulog.h"
#include "disk.h"
#include "ganji/util/thread/mutex.h"
#include <string>

using ganji::util::thread::Mutex;
using ganji::util::thread::MutexGuard;
using namespace std;

#define BPT_OFFSET ((size_t)((_bpt_node_t *)0)->data)

typedef uint32_t pgno_t;
typedef uint16_t offset_t;

const uint64_t INF = 1000000000000000ll;

/* structure of B+ Tree node header */
typedef struct _bpt_node_t {
	uint16_t knum;              /**< number of keys in current node */
	uint16_t leftbytes;         /**< remaining bytes of current node */
	uint16_t type;				/**< whether it is a leaf node or inner one */
	pgno_t prev;                /**< previous page no. (if it is a leaf page) */
	pgno_t next;                /**< next page no. (if it is a leaf page) */

	char data[0];               /**< data buffer start position */
} bpt_node_t;

/* structure of B+ Tree META-PAGE */
typedef struct bpt_meta_node_t {
	pgno_t root_pgno;
	pgno_t unused_pgno;
	uint32_t pagesize;
} bpt_meta_node_t;

template <typename T>
class DefaultSerial{
public:
	ssize_t load(T &obj, const void *buf, size_t buf_size) {

		size_t size = sizeof(obj);
		if (NULL == buf) {
			U_LOG_FATAL("Null pointer to buffer when dumping data.");
			return -1;
		}

		if (size > buf_size) {
			U_LOG_FATAL("No space when loading data.[size:%zu][bufsize:%zu]", size, buf_size);
			size = buf_size ;
		}

		memcpy(&obj, buf, size);

		return size;
	}

	ssize_t dump(const T &obj, void *buf, size_t buf_size) {

		size_t size = sizeof(obj);
		if (NULL == buf) {
			U_LOG_FATAL("Null pointer to buffer when dumping data.");
			return -1;
		}

		if (size > buf_size) {
			U_LOG_FATAL("No space when dumping data.[size:%zu][bufsize:%zu]", size, buf_size);
			size = buf_size;
		}

		memcpy(buf, &obj, size);

		return size;
	}
	ssize_t get_size(const T &obj) { return sizeof(obj); }
};

template <typename T>
class MySerial{
public:
	ssize_t load(string &obj, const void *buf, size_t buf_size) {
        obj.resize(0);

		size_t size = *(size_t*)(buf);
        if( size <=0){
			U_LOG_FATAL("No size when dumping data.");
			return -1;
        }
		if (NULL == buf) {
			U_LOG_FATAL("Null pointer to buffer when dumping data.");
			return -1;
		}

		if (size > buf_size) {
			U_LOG_FATAL("No space when loading data.[size:%zu][bufsize:%zu]", size, buf_size);
			size = buf_size ;
		}

		obj.append((char*)buf+sizeof(size_t), size);

		size_t resize = size+sizeof(size_t);
        U_LOG_TRACE("load.[str:%s][size:%zu][bufsize:%zu][resize:%d]", obj.c_str(), size, buf_size, resize);
        return resize;
	}

	ssize_t dump(const string &obj, void *buf, size_t buf_size) {

		size_t size = obj.size();
		if (NULL == buf) {
			U_LOG_FATAL("Null pointer to buffer when dumping data.");
			return -1;
		}

		if (size > buf_size) {
			U_LOG_FATAL("No space when dumping data.[size:%zu][bufsize:%zu]", size, buf_size);
			size = buf_size;
		}

        *(size_t*)buf = size;
		memcpy((char*)buf+sizeof(size_t), obj.c_str(), size);

		size_t resize = size+sizeof(size_t);
        U_LOG_TRACE("dump.[str:%s][size:%zu][bufsize:%zu][resize:%d]", obj.c_str(), size, buf_size, resize);
        return resize;
	}

    ssize_t get_size(const string& obj) { 
        return sizeof(size_t) + obj.size();
    }
};

template <typename T>
class MyStrKeyCompare {
  public:
    bool operator() (const string &a, const string &b) const {
      return strcmp(a.c_str(), b.c_str()) < 0;
    }

    bool operator() (const char* &a, const char* &b) const {
      return strcmp(a, b) < 0;
    }

    bool operator() ( char* &a,  char* &b) const {
      return strcmp(a, b) < 0;
    }
};


static const int32_t STR_MAX_KEY_LEN = 33;
typedef struct str_t {
  char val[STR_MAX_KEY_LEN];//md5
} str_t;

template <typename T>
class StrSerial{
public:
	ssize_t load(str_t &obj, const void *buf, size_t buf_size) {

		size_t size = sizeof(obj);
		if (NULL == buf) {
			U_LOG_FATAL("Null pointer to buffer when dumping data.");
			return -1;
		}

		if (size > buf_size) {
			U_LOG_FATAL("No space when loading data.[size:%zu][bufsize:%zu]", size, buf_size);
			size = buf_size ;
		}

		memcpy(&obj, buf, size);
    //U_LOG_FATAL("load--%s--%s---%d",(char*)obj,(char*) buf, (int)size);

		return size;
	}

	ssize_t dump(const str_t &obj, void *buf, size_t buf_size) {
		size_t size = sizeof(obj);
		if (NULL == buf) {
			U_LOG_FATAL("Null pointer to buffer when dumping data.");
			return -1;
		}

		if (size > buf_size) {
			U_LOG_FATAL("No space when dumping data.[size:%zu][bufsize:%zu]", size, buf_size);
			size = buf_size;
		}

		memcpy(buf, (char*)(&obj), size);
    //U_LOG_FATAL("dump--%s---%s--%d", (str_t*)buf->val, (str_t)obj.val, (int)size);
		return size;
	}

	ssize_t get_size(const str_t &obj) { return sizeof(obj); }

	ssize_t get_size(const char* &obj) { return strlen(obj)+1; }

	ssize_t dump(const char* &obj, void *buf, size_t buf_size) {
		size_t size = strlen(obj)+1;
		if (NULL == buf) {
			U_LOG_FATAL("Null pointer to buffer when dumping data.");
			return -1;
		}

		if (size > buf_size) {
			U_LOG_FATAL("No space when dumping data.[size:%zu][bufsize:%zu]", size, buf_size);
			size = buf_size;
		}

		memcpy(buf, obj, size);
		return size;
	}
	ssize_t load(const char* &obj, const void *buf, size_t buf_size) {

		size_t size = strlen(obj)+1;
		if (NULL == buf) {
			U_LOG_FATAL("Null pointer to buffer when dumping data.");
			return -1;
		}

		if (size > buf_size) {
			U_LOG_FATAL("No space when loading data.[size:%zu][bufsize:%zu]", size, buf_size);
			size = buf_size ;
		}

		memcpy(&obj, buf, size);
    //U_LOG_FATAL("load--%s--%s---%d",(char*)obj,(char*) buf, (int)size);

		return size;
	}
};


template <typename T>
class DefaultKeyCompare {
  public:
    bool operator() (const T &a, const T &b) const {
      return a < b;
    }
};

template <typename T>
class StrKeyCompare {
  public:
    bool operator() (const char* &a, const char* &b) const {
      return strcmp(a, b) < 0;
    }

    bool operator() (const str_t &a, const str_t &b) const {
      return strcmp(a.val, b.val) < 0;
    }
};

static const char *BPlusTree_ERR_STR[] = {
	"BPlusTree: Success",
	"BPlusTree: Inner Error",
	"BPlusTree: Duplicate Data in Cache",
	"BPlusTree: No Such Data in Cache",
	"BPlusTree: Not Enough Memory",
	"BPlusTree: Access Memory Error",
	"BPlusTree: ARG Error",
	"BPlusTree: Invalid Value Size Error",
	"BPlusTree: Unknown Error"
};

template < typename KeyT, typename ValueT, typename KeyCompareT = DefaultKeyCompare<KeyT>, typename KeySerial = DefaultSerial<KeyT>, typename ValueSerial = DefaultSerial<ValueT> >
class BPlusTree {
public:

	static const int32_t BPT_MAX_HEIGHT = 16;

	enum RET_TYPE {

		RET_DUPDATA = 3,
		RET_NOTFOUND = 2,
		RET_FOUND = 0,
		RET_SUCCESS = 0,

		RET_INNER_ERR = -1,
		RET_DUPDATA_ERR= -2,
		RET_NODATA_ERR= -3,
		RET_NOSPACE_ERR = -4,
		RET_MEM_ACCESS_ERR = -5,
		RET_ARG_ERR = -6,
		RET_SIZE_ERR = -7,
		RET_ALL
	};

	enum PAGE_NO {
		META_PAGE = 0,
		ROOT_PAGE
	};

	enum TREE_NODE_TYPE {
		TREE_INNER_NODE = 0x0,
		TREE_LEAF_NODE = 0x1
	};

	BPlusTree(const KeyCompareT &comp = KeyCompareT(), const KeySerial &ks = KeySerial(),
		const ValueSerial &vs = ValueSerial())
			: bpt_meta_(NULL), fd_(-1), disk_(NULL), comp_(comp), keyserial_(ks), valueserial_(vs) {}

	~BPlusTree() {

        WriteLog(kLogTrace, "BPlusTree destruction!");
		if (NULL != bpt_meta_) {
			munmap((void *)bpt_meta_, sizeof(bpt_meta_node_t));
			bpt_meta_ = NULL;
		}

		//modify_mutex_.destroy();

		ptrdel(&disk_);
		fdclose(&fd_);
	};

	int open(const char *fname, pgno_t pgs, uint32_t ln, pgno_t mcp, uint32_t mp);
	int get(const KeyT &key, ValueT &value);
	int put(const KeyT &key, const ValueT &value);
	int delet(const KeyT &key);
	int update(const KeyT &key, const ValueT &value);

	static const char *perror(int err) {
		if (err > 0 || err < RET_ALL) {
			err = RET_ALL;
		}
		return BPlusTree_ERR_STR[-err];
	}

#ifdef _BPLUSTREE_DEBUG_
	int print_node_info(const pgno_t pageno);
	int validate_whole_tree();
	int validate_node(pgno_t pageno);
	int check_key(pgno_t pageno, uint64_t key1, uint64_t key2);
#endif // _BPLUSTREE_DEBUG_

private:
	pgno_t pagesize_;
	uint32_t level_num_;
	uint32_t maxhashsize_;
	pgno_t maxcachepages_;
	bpt_meta_node_t *bpt_meta_;
	int fd_;
	Disk *disk_;
	KeyCompareT comp_;
	KeySerial keyserial_;
	ValueSerial valueserial_;

	typedef struct _collect_array_t {
		pgno_t col_pg_[BPT_MAX_HEIGHT];
		int32_t cur_;
	} collect_array_t;

	ganji::util::thread::Mutex modify_mutex_;

	// get, put, delet
	int search(pgno_t pageno, const KeyT &key, ValueT &value);
	int insert(pgno_t pageno, const KeyT &key, const ValueT &value, KeyT **newkey, pgno_t *leftpg, pgno_t *rightpg,
			collect_array_t *col_ar);
	int remove(pgno_t pageno, const KeyT &key, bool &empty);
	int modify(pgno_t pageno, const KeyT &key, const ValueT &value);

	// insert inner node
	int insert_inner_node(pgno_t pageno, void *cpage, const KeyT &key, const pgno_t lpn, const pgno_t rpn,
		KeyT **newkey, pgno_t *leftpg, pgno_t *rightpg, collect_array_t *col_ar);
	int insert_nonfull_inner_node(pgno_t pageno, void *cpage, const KeyT &key, const pgno_t lgn, const pgno_t rpn);
	int split_inner_node(pgno_t pageno, void *cpage, const KeyT &key, const pgno_t lpn, const pgno_t rpn,
		KeyT **newkey, pgno_t *leftpg, pgno_t *rightpg, collect_array_t *col_ar);

	// insert leaf node
	int insert_leaf_node(pgno_t pageno, void *cpage, const KeyT &key, const ValueT &value,
			KeyT **newkey, pgno_t *leftpg, pgno_t *rightpg, collect_array_t *col_ar);
	int insert_nonfull_leaf_node(pgno_t pageno, void *cpage, const KeyT &key, const ValueT &value);
	int split_leaf_node(pgno_t pageno, void *cpage, const KeyT &key, const ValueT &value,
			KeyT **newkey, pgno_t *leftpg, pgno_t *rightpg, collect_array_t *col_ar);

	// remove leaf node
	int remove_leaf_node(pgno_t pageno, void *cpage, const KeyT &key, bool &empty);

	// page manager func
	int alloc_new_page(pgno_t *pgn, void *page);
	int collect_old_page(const pgno_t pgn, void *cpage);
	int collect_old_page(const pgno_t pgn);

	// binary search func
	offset_t* bsearch_lower(const KeyT &key, offset_t *left, offset_t *right, void *base);

	offset_t* bsearch_upper(const KeyT &key, offset_t *left, offset_t *right, void *base);

	offset_t* bsearch_equal(const KeyT &key, offset_t *left, offset_t *right, void *base);


	bool validate_leftbytes(void *cpage) {

		uint16_t leftbytes = pagesize_;
		bpt_node_t *bpt;
		offset_t *off;

		bpt = (bpt_node_t *)cpage;
		off = (offset_t *)bpt->data;

		if (TREE_LEAF_NODE == bpt->type) {
			leftbytes = leftbytes - sizeof(bpt_node_t) - bpt->knum * sizeof(offset_t);
			if (bpt->knum > 0) {
				leftbytes -= pagesize_ - (off[bpt->knum-1]);
			}
		} else {
			leftbytes = leftbytes - sizeof(bpt_node_t) - bpt->knum * sizeof(offset_t) - sizeof(pgno_t);
			if (bpt->knum > 0) {
				leftbytes -= pagesize_ - (off[bpt->knum-1]);
			}
		}

		return (leftbytes == bpt->leftbytes);
	}


	template <class T>
	void ptrfree(T **p);

	template <class T>
	void ptrdel(T **p);

	void fdclose(int *fd);

};

// public methods implementation

/**
 * @brief create a B+ Tree or reopen it on an existing file
 *
 * @param [in] fname: B+ Tree Storage File
 * @param [in] pgs  : Page Size
 * @param [in] ln   : Level of LRUCache
 * @param [in] mcp  : Maximum Cache Pages
 * @param [in] mp   : Maximum Pages for B+ Tree
 * @return  int
 * @retval
 *    success : BPlusTree::RET_SUCCESS (=0)
 *       fail : < 0
 * @see
 * @note
 * @author wantao
 * @date 2010/05/26 10:07:15
*>*/
template < typename KeyT, typename ValueT, typename KeyCompareT, typename KeySerial, typename ValueSerial>
int BPlusTree<KeyT, ValueT, KeyCompareT, KeySerial, ValueSerial>::open(const char *fname, pgno_t pgs, uint32_t ln, pgno_t mcp, uint32_t mp) {

	// init private var
	pagesize_ = pgs;
	level_num_ = ln;
	maxcachepages_ = mcp;
	maxhashsize_ = mp;

	int ret;
	char page[pagesize_];
	bpt_node_t *bpt;

	if (NULL == fname) {
		U_LOG_WARNING("Null pointer to file name when creating B+ Tree!");
		ret = RET_ARG_ERR;
		goto err;
	}

  /*
	// init modify mutex
	if (!modify_mutex_.init()) {
		U_LOG_FATAL("B+ Tree modify mutex init err!");
		ret = RET_INNER_ERR;
		goto err;
	}
  */


	// whether the file exists
	ret = access(fname, R_OK | W_OK);
	if (0 == ret) {

		// B+ Tree file exists
		fd_ = ::open(fname, O_RDWR);
		if (fd_ < 0) {
			U_LOG_FATAL("Cannot open storage file when init B+ Tree!");
			ret = RET_INNER_ERR;
			goto err;
		}

		// new Disk
		disk_ = new Disk(fd_, pagesize_, level_num_, maxhashsize_, maxcachepages_);
		if (NULL == disk_) {
			U_LOG_FATAL("Instantiate Disk class err when opening B+ Tree file!");
			ret = RET_INNER_ERR;
			goto err;
		}

		// init Disk
		ret = disk_->init();
		if (Disk::RET_SUCCESS != ret) {
			U_LOG_FATAL("Initialize Disk class err when opening B+ Tree file![%s]",
				Disk::perror(ret));
			ret = RET_INNER_ERR;
			goto err;
		}

		bpt_meta_ = (bpt_meta_node_t *)mmap(NULL, sizeof(bpt_meta_node_t), PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
		if (MAP_FAILED == (void *)bpt_meta_) {
			U_LOG_FATAL("Memory mapping meta-pagefailed!");
			ret = RET_INNER_ERR;
			goto err;
		}

		if (0 == bpt_meta_->root_pgno || 0 == bpt_meta_->unused_pgno) {
			U_LOG_FATAL("root pgno or unused pgno err![root_pgno:%u][unused_pgno: %u]", 
				bpt_meta_->root_pgno, bpt_meta_->unused_pgno);
			ret = RET_INNER_ERR;
			goto err;
		}

		if (pagesize_ != bpt_meta_->pagesize) {
			U_LOG_FATAL("Invalid pagesize, not equal to the one in B+ Tree storage file!"
				"[pagesize:%u][pagesize_file:%u]", pagesize_, bpt_meta_->pagesize);
			ret = RET_ARG_ERR;
			goto err;
		}

		ret = disk_->read(bpt_meta_->root_pgno, page);
		if (Disk::RET_SUCCESS != ret) {
			U_LOG_FATAL("Reading root-page from disk err![pageno:%u][ret:%d][%s]",
				bpt_meta_->root_pgno, ret, Disk::perror(ret));
			ret = RET_INNER_ERR;
			goto err;
		}

	} else {

		fd_ = ::open(fname, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);
		if (fd_ < 0) {
			U_LOG_FATAL("Cannot create storage file when init B+ Tree!");
			ret = RET_INNER_ERR;
			goto err;
		} else if( fd_ <=2){
			U_LOG_TRACE("bplus filename:%s!", fname);
        } else {
            U_LOG_TRACE("bplus filename:%s! fd:%d", fname, fd_);
        }

		// new Disk
		disk_ = new Disk(fd_, pagesize_, level_num_, maxhashsize_, maxcachepages_);
		if (NULL == disk_) {
			U_LOG_FATAL("Instantiate Disk class err when opening B+ Tree file!");
			ret = RET_INNER_ERR;
			goto err;
		}

		// init Disk
		ret = disk_->init();
		if (Disk::RET_SUCCESS != ret) {
			U_LOG_FATAL("Initialize Disk class err when opening B+ Tree file![%s]",
				Disk::perror(ret));
			ret = RET_INNER_ERR;
			goto err;
		}

		// alloc meta-page in disk
		ret = disk_->alloc(META_PAGE, page);
		if (Disk::RET_SUCCESS != ret) {
			U_LOG_FATAL("Alloc memory for meta-page err![%s]", Disk::perror(ret));
			ret = RET_INNER_ERR;
			goto err;
		}

		bpt_meta_ = (bpt_meta_node_t *)mmap(NULL, sizeof(bpt_meta_node_t), PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
		if (MAP_FAILED == (void *)bpt_meta_) {
			U_LOG_FATAL("Memory mapping meta-page failed!");
			ret = RET_INNER_ERR;
			goto err;
		}

		// set meta-page
		bpt_meta_->root_pgno = ROOT_PAGE;
		bpt_meta_->unused_pgno = ROOT_PAGE + 1;
		bpt_meta_->pagesize = pagesize_;

		bzero(page, pagesize_);
		bpt = (bpt_node_t *)page;

		bpt->knum = 0;
		bpt->leftbytes = pagesize_ - BPT_OFFSET;
		bpt->type = TREE_LEAF_NODE;
		bpt->prev = 0;
		bpt->next = 0;

		ret = disk_->write(bpt_meta_->root_pgno, page);
		if (Disk::RET_SUCCESS != ret) {
			U_LOG_FATAL("Writing root-page into disk err![ret:%d][%s]",
				ret, Disk::perror(ret));
			ret = RET_INNER_ERR;
			goto err;
		}
	}

	return RET_SUCCESS;

err:

	ptrdel(&disk_);
	return ret;
}

/**
 * @brief get the value of given key in B+ Tree
 *
 * @param [in] key: key
 * @param [out] value: value
 * @return  int
 * @retval
 *    success : BPlusTree::RET_SUCCESS (=0)
 *       fail : < 0
 * @see
 * @note
 * @author wantao
 * @date 2010/05/26 10:09:56
**/
template <typename KeyT, typename ValueT, typename KeyCompareT, typename KeySerial, typename ValueSerial>
int BPlusTree<KeyT, ValueT, KeyCompareT, KeySerial, ValueSerial>::get(const KeyT &key, ValueT &value) {

	int ret;
	ssize_t key_size;

	// arg check
	key_size = keyserial_.get_size(key);
	if (key_size < 0) {
		U_LOG_WARNING("Invalid size of given key when searching in B+ Tree!");
		return RET_ARG_ERR;
	}
/*
	if (NULL == value) {
		U_LOG_WARNING("Null pointer to value buf when searching in B+ Tree!");
		return RET_ARG_ERR;
	}
*/
	ret = search(bpt_meta_->root_pgno, key, value);
	if (ret < 0) {
		U_LOG_FATAL("Searching key in B+ Tree err![%s]", perror(ret));
		return RET_INNER_ERR;
	}

	return ret;
}


/**
 * @brief put a key/value pair into B+ Tree
 *
 * @param [in] key: key
 * @param [in] value: value
 * @return  int
 * @retval
 *    success : BPlusTree::RET_SUCCESS (=0)
 *       fail : < 0
 * @see
 * @note
 * @author wantao
 * @date 2010/05/26 10:10:59
**/
template <typename KeyT, typename ValueT, typename KeyCompareT, typename KeySerial, typename ValueSerial>
int BPlusTree<KeyT, ValueT, KeyCompareT, KeySerial, ValueSerial>::put(const KeyT &key, const ValueT &value) {

	int ret = 0;
	ssize_t newkey_size, key_size, value_size;
	ssize_t dump_size;

	KeyT *newkey = NULL;
	pgno_t pgn, leftpg, rightpg;
	offset_t *off;
	bpt_node_t *bpt;

	char cpage[pagesize_];

	collect_array_t col_ar;

	col_ar.cur_ = 0;

	// Lock
  ganji::util::thread::MutexGuard guard(&modify_mutex_);

	// arg check
	key_size = keyserial_.get_size(key);
	if (key_size < 0) {
		U_LOG_WARNING("Invalid size of given key when inserting key/value pair into B+ Tree!");
		ret = RET_ARG_ERR;
		goto err;
	}

	value_size = valueserial_.get_size(value);
	if (value_size < 0) {
		U_LOG_WARNING("Invalid size of given value when inserting key/value pair into B+ Tree!");
		ret = RET_ARG_ERR;
		goto err;
	}

	// insert the key/value pair recursively from the root of B+ Tree
	ret = insert(bpt_meta_->root_pgno, key, value, &newkey, &leftpg, &rightpg, &col_ar);
	if (ret < 0) {
		U_LOG_FATAL("Inserting a key into B+ Tree failed![%s]", perror(ret));
		ret = RET_INNER_ERR;
		goto err;
	}

	// The key is already in B+ Tree
	if (RET_DUPDATA == ret) {
		goto err;
	}

	if (NULL != newkey) {

		// the root node is splitted and the height of B+ tree increase by 1
		ret = alloc_new_page(&pgn, cpage);
		if (RET_SUCCESS != ret) {
			U_LOG_FATAL("allocate new page err![pgn:%u]", pgn);
			ret = RET_INNER_ERR;
			goto err;
		}

		newkey_size = keyserial_.get_size(*newkey);
		if (newkey_size < 0) {
			U_LOG_FATAL("Invalid key size.[size:%zd]", newkey_size);
			ret = RET_INNER_ERR;
			goto err;
		}

		bpt = (bpt_node_t *)cpage;
		off = (offset_t *)bpt->data;

		bpt->knum = 1;
		bpt->type = TREE_INNER_NODE;
		bpt->prev = 0;
		bpt->next = 0;
		bpt->leftbytes = pagesize_ - sizeof(bpt_node_t)
			- sizeof(offset_t) - sizeof(pgno_t) * 2 - newkey_size;

		*off = pagesize_ - sizeof(pgno_t) - newkey_size;

		// copy old pgno into new node

		memcpy((char *)cpage + pagesize_ - sizeof(pgno_t), &leftpg, sizeof(pgno_t));

		// copy new key into new node
		dump_size = keyserial_.dump(*newkey, (char *)cpage + *off, newkey_size);
		if (dump_size < 0 || dump_size != keyserial_.get_size(*newkey)) {
			U_LOG_FATAL("Dump key object err![dump_size:%zd][size:%zd]",
				dump_size, keyserial_.get_size(*newkey));
			ret = RET_INNER_ERR;
			goto err;
		}

		// copy new pgno into new node
		memcpy((char *)cpage + *off - sizeof(pgno_t), &rightpg, sizeof(pgno_t));

		ret = disk_->write(pgn, cpage);
		if (Disk::RET_SUCCESS != ret) {
			U_LOG_FATAL("write page into disk err![pgn:%u][%s]", pgn, Disk::perror(ret));
			ret = RET_INNER_ERR;
			goto err;
		}

		//reset new root page no.
		bpt_meta_->root_pgno = pgn;

		// release memory for newchild
		// FIX BUG:
		//free(newkey);
		delete newkey;
		newkey = NULL;
	}

	// collect old pages
	while (--col_ar.cur_ >= 0) {
		ret = collect_old_page(col_ar.col_pg_[col_ar.cur_]);
		if (RET_SUCCESS != ret) {
			U_LOG_WARNING("Collect %u-th page err!", 
					col_ar.col_pg_[col_ar.cur_]);
			ret = RET_INNER_ERR;
			goto err;
		}
	}

	return RET_SUCCESS;
err:
	return ret;
}


/**
 * @brief delete the given key
 *
 * @param [in] key: key
 * @return  int
 * @retval
 *    success : BPlusTree::RET_SUCCESS (=0)
 *       fail : < 0
 * @see
 * @note
 * @author wantao
 * @date 2010/05/26 10:12:16
**/
template <typename KeyT, typename ValueT, typename KeyCompareT, typename KeySerial, typename ValueSerial>
int BPlusTree<KeyT, ValueT, KeyCompareT, KeySerial, ValueSerial>::delet(const KeyT &key) {

	int ret;
	bool empty = false;
	ssize_t key_size;
	char cpage[pagesize_];

	pgno_t pgn;
	bpt_node_t *bpt;

	// Lock
	//qtl::Mutex::Lock guard(modify_mutex_);
  ganji::util::thread::MutexGuard guard(&modify_mutex_);

	// arg check
	key_size = keyserial_.get_size(key);
	if (key_size < 0) {
		U_LOG_WARNING("Invalid size of given key when deleting key in B+ Tree!");
		return RET_ARG_ERR;
	}

	ret = remove(bpt_meta_->root_pgno, key, empty);
	if (ret < 0) {
		U_LOG_FATAL("Deleting data in B+ Tree err![%s]", perror(ret));
		return RET_INNER_ERR;
	}

	// if not found
	if (RET_NOTFOUND == ret) {
		return ret;
	}

	if (empty) {

		// the root node is collected
		ret = alloc_new_page(&pgn, cpage);
		if (RET_SUCCESS != ret) {
			U_LOG_FATAL("allocate new page err![pgn:%u]", pgn);
			return RET_INNER_ERR;
		}

		bpt = (bpt_node_t *)cpage;
		bpt->knum = 0;
		bpt->leftbytes = pagesize_ - BPT_OFFSET;
		bpt->type = TREE_LEAF_NODE;
		bpt->prev = 0;
		bpt->next = 0;

		ret = disk_->write(pgn, cpage);
		if (Disk::RET_SUCCESS != ret) {
			U_LOG_FATAL("Writing root-page into disk when deleting key/value pair err![%s]",
				Disk::perror(ret));
			return RET_INNER_ERR;
		}

		// reset root_pgno_;
		bpt_meta_->root_pgno = pgn;
	}

	return RET_SUCCESS;
}

template <typename KeyT, typename ValueT, typename KeyCompareT, typename KeySerial, typename ValueSerial>
int BPlusTree<KeyT, ValueT, KeyCompareT, KeySerial, ValueSerial>::update(const KeyT &key, const ValueT &value) {

	int ret = 0;
	ssize_t key_size, value_size;

	// Lock
  ganji::util::thread::MutexGuard guard(&modify_mutex_);

	// arg check
	key_size = keyserial_.get_size(key);
	if (key_size < 0) {
		U_LOG_WARNING("Invalid size of given key when inserting key/value pair into B+ Tree!");
		ret = RET_ARG_ERR;
		goto err;
	}

	value_size = valueserial_.get_size(value);
	if (value_size < 0) {
		U_LOG_WARNING("Invalid size of given value when inserting key/value pair into B+ Tree!");
		ret = RET_ARG_ERR;
		goto err;
	}

	ret = modify(bpt_meta_->root_pgno, key, value);
	if (ret < 0) {
		U_LOG_FATAL("Updating key/value in B+ Tree err![%s]", perror(ret));
		ret = RET_INNER_ERR;
	}

	if (RET_NOTFOUND == ret) {
		goto err;
	}

	return RET_SUCCESS;

err:
	return ret;
}

// private methods implementation
template <typename KeyT, typename ValueT, typename KeyCompareT, typename KeySerial, typename ValueSerial>
int BPlusTree<KeyT, ValueT, KeyCompareT, KeySerial, ValueSerial>::search(pgno_t pageno, const KeyT &key, ValueT &value) {

	int ret = 0;
	ssize_t load_size;
	offset_t *off, *hit;
	pgno_t *pgn;

	char cpage[pagesize_];

	KeyT key_obj;

	bpt_node_t *bpt;

	if (0 == pageno) {
		return RET_NOTFOUND;
	}
/*
	if (NULL == value) {
		U_LOG_FATAL("Null pointer to value buf when searching in B+ Tree!");
		return RET_MEM_ACCESS_ERR;
	}
*/
	// get current page
	ret = disk_->read(pageno, cpage);
	if (Disk::RET_SUCCESS != ret) {
		U_LOG_FATAL("Reading page from disk err![ret:%d][%s]", ret, Disk::perror(ret));
		return RET_INNER_ERR;
	}

	bpt= (bpt_node_t *)cpage;
	off = (offset_t *)bpt->data;

	if (!validate_leftbytes(cpage)) {
		U_LOG_FATAL("leftbytes err![leftbytes:%u][knum:%u]", bpt->leftbytes, bpt->knum);
		return -1;
	}

	if (TREE_LEAF_NODE == bpt->type) {

		if (0 == bpt->knum) {
			// number of key/value pair in leaf node shouldn't be zero.
			// maybe another thread has just delet key/value out of current leaf node
			U_LOG_WARNING("There is no data in current leaf node![pgn:%u]", pageno);
			return RET_NOTFOUND;
		}

		hit = bsearch_equal(key, off, off + bpt->knum, cpage);

		if (NULL == hit) {
			return RET_NOTFOUND;
		} else {

			load_size = keyserial_.load(key_obj, ((char *)cpage + *hit), pagesize_);
			if (load_size < 0) {
				U_LOG_FATAL("Load key object err![load_size:%zd]", load_size);
				return RET_INNER_ERR;
			}

			load_size = valueserial_.load(value, ((char *)cpage + *hit + keyserial_.get_size(key_obj)), pagesize_);
			if (load_size < 0) {
				U_LOG_FATAL("Load value object err![load_size:%zd]", load_size);
				return RET_INNER_ERR;
			}
			return RET_FOUND;
		}

	} else {

		if (0 == bpt->knum) {

			// number of key(s) in inner node should be zero
			pgn = (pgno_t *)((char *)cpage + pagesize_ - sizeof(pgno_t));

		} else {

			hit = bsearch_upper(key, off, off + bpt->knum, cpage);
			if (hit != off + bpt->knum) {

				load_size = keyserial_.load(key_obj, ((char *)cpage + *hit), pagesize_);
				if (load_size < 0) {
					U_LOG_FATAL("Load key object err![load_size:%zd]", load_size);
					return RET_INNER_ERR;
				}

				pgn = (pgno_t *)((char *)cpage + *hit + keyserial_.get_size(key_obj));

			} else {

				pgn = (pgno_t *)((char *)cpage + off[bpt->knum-1] - sizeof(pgno_t));
			}

		}

		return search(*pgn, key, value);
	}

	return RET_SUCCESS;
}


template <typename KeyT, typename ValueT, typename KeyCompareT, typename KeySerial, typename ValueSerial>
int BPlusTree<KeyT, ValueT, KeyCompareT, KeySerial, ValueSerial>::modify(pgno_t pageno, const KeyT &key, const ValueT &value) {

	int ret = 0;
	ssize_t load_size, dump_size;
	ssize_t key_size, value_size;
	offset_t *off, *hit;
	pgno_t *pgn;

	bpt_node_t *bpt;

	char cpage[pagesize_];

	KeyT key_obj;
	ValueT value_obj;

	value_size = valueserial_.get_size(value);
	if (value_size < 0) {
		U_LOG_WARNING("Invalid size of value!");
		return RET_ARG_ERR;
	}


	if (0 == pageno) {
		return RET_NOTFOUND;
	}

	// get current page
	ret = disk_->read(pageno, cpage);
	if (Disk::RET_SUCCESS != ret) {
		U_LOG_FATAL("Reading page from disk err![ret:%d][%s]", ret, Disk::perror(ret));
		return RET_INNER_ERR;
	}

	bpt= (bpt_node_t *)cpage;
	off = (offset_t *)bpt->data;

	if (!validate_leftbytes(cpage)) {
		U_LOG_FATAL("leftbytes err![leftbytes:%u][knum:%u]", bpt->leftbytes, bpt->knum);
		return -1;
	}

	if (TREE_LEAF_NODE == bpt->type) {

		if (0 == bpt->knum) {

			U_LOG_WARNING("There is no data in current leaf node![pgn:%u]", pageno);
			return RET_NOTFOUND;
		}

		hit = bsearch_equal(key, off, off + bpt->knum, cpage);

		if (NULL == hit) {
			return RET_NOTFOUND;
		} else {
/*
			kt = (KeyT *)((char *)cpage + *hit);
			vt = (ValueT *)((char *)kt + get_size(*kt));
*/
			load_size = keyserial_.load(key_obj, ((char *)cpage + *hit), pagesize_);
			if (load_size < 0) {
				U_LOG_FATAL("Load key object err![load_size:%zd]", load_size);
				return RET_INNER_ERR;
			}

			key_size = keyserial_.get_size(key_obj);
			if (key_size < 0) {
				U_LOG_WARNING("Invalid size of key!");
				return RET_INNER_ERR;
			}

			load_size = valueserial_.load(value_obj, ((char *)cpage + *hit + key_size), pagesize_);
			if (load_size < 0) {
				U_LOG_FATAL("Load value object err![load_size:%zd]", load_size);
				return RET_INNER_ERR;
			}

			if (value_size != valueserial_.get_size(value_obj)) {
				U_LOG_WARNING("Size of values are not equal.[value_size:%zd][modify_size:%zd]",
					valueserial_.get_size(value_obj), value_size);
				return RET_SIZE_ERR;
			}
/*
			dump_size = dump(value, ((char *)cpage + *hit + keyserial_.get_size(key_obj)), value_size);
			if (dump_size < 0 || dump_size != value_size) {
				U_LOG_FATAL("dumping value into page buffer failed![dump_size:%zd][value_size:%zd]",
					dump_size, value_size);
				return RET_INNER_ERR;
			}
*/

			dump_size = valueserial_.dump(value, (char *)cpage + *hit + key_size, value_size);
			if (dump_size < 0) {
				U_LOG_FATAL("dumping value into page buffer failed![dump_size:%zd][value_size:%zd]",
						dump_size, value_size);
				return RET_INNER_ERR;
			}

			// rewrite page into disk
			ret = disk_->write(pageno, cpage);
			if (Disk::RET_SUCCESS != ret) {
				U_LOG_FATAL("Write page into disk when collect old page err![pgn:%u][%s]",
						pageno, Disk::perror(ret));
				return  RET_INNER_ERR;
			}

			return RET_SUCCESS;

		}
	} else {

		// it is an inner-node
		if (0 == bpt->knum) {

			// number of key(s) in inner node should be zero
			pgn = (pgno_t *)((char *)cpage + pagesize_ - sizeof(pgno_t));

		} else {

			hit = bsearch_upper(key, off, off + bpt->knum, cpage);
			if (hit != off + bpt->knum) {

				load_size = keyserial_.load(key_obj, ((char *)cpage + *hit), pagesize_);
				if (load_size < 0) {
					U_LOG_FATAL("Load key object err![load_size:%zd]", load_size);
					return RET_INNER_ERR;
				}
				pgn = (pgno_t *)((char *)cpage + *hit + keyserial_.get_size(key_obj));

			} else {
				pgn = (pgno_t *)((char *)cpage + off[bpt->knum-1] - sizeof(pgno_t));
			}
		}

		return modify(*pgn, key, value);
	}

	return RET_SUCCESS;
}

template <typename KeyT, typename ValueT, typename KeyCompareT, typename KeySerial, typename ValueSerial>
int BPlusTree<KeyT, ValueT, KeyCompareT, KeySerial, ValueSerial>::insert(pgno_t pageno, const KeyT &key, const ValueT &value,
	KeyT **newkey, pgno_t *leftpg, pgno_t *rightpg, collect_array_t *col_ar)
{
	int ret;
	char cpage[pagesize_];
	ssize_t load_size;

	offset_t *off, *hit;
	pgno_t *npage_no, pop_lpn, pop_rpn;
	bpt_node_t *bpt;
//	KeyT *kt, *pop_key = NULL;
	KeyT key_obj, *pop_key = NULL;

	// read page
	ret = disk_->read(pageno, cpage);
	if (Disk::RET_SUCCESS != ret) {
		U_LOG_FATAL("read page from disk err![ret:%d][%s]", ret, Disk::perror(ret));
		ret = RET_INNER_ERR;
		goto err;
	}

	bpt = (bpt_node_t *)cpage;
	off = (offset_t *)bpt->data;

	if (!validate_leftbytes(cpage)) {
		U_LOG_FATAL("leftbytes err![leftbytes:%u][knum:%u]", bpt->leftbytes, bpt->knum);
		return -1;
	}


	if (TREE_INNER_NODE == bpt->type) {

		if (0 == bpt->knum) {
			// there is only one page no. in current inner node
			npage_no = (pgno_t *)((char *)cpage + pagesize_ - sizeof(pgno_t));

		} else {

			// find the interval
			hit = bsearch_upper(key, off, off + bpt->knum, cpage);
			if (hit != off + bpt->knum) {
/*
				kt = (KeyT *)((char *)cpage + *hit);
				npage_no = (pgno_t *)((char *)kt + get_size(*kt));
*/
				load_size = keyserial_.load(key_obj, ((char *)cpage + *hit), pagesize_);
				if (load_size < 0) {
					U_LOG_FATAL("Load key object err![load_size:%zd]", load_size);
					return RET_INNER_ERR;
				}

				npage_no = (pgno_t *)((char *)cpage + *hit + keyserial_.get_size(key_obj));

			} else {
/*
				kt = (KeyT *)((char *)cpage + off[bpt->knum-1]);
				npage_no = (pgno_t *)((char *)kt - sizeof(pgno_t));
*/
				npage_no = (pgno_t *)((char *)cpage + off[bpt->knum-1] - sizeof(pgno_t));
			}

		}

		// insert the key recursively
		ret = insert(*npage_no, key, value, newkey, leftpg, rightpg, col_ar);
		if (ret < 0) {
			U_LOG_FATAL("Insert a key/value into B+Tree err![%s][npage_no:%u]",
				perror(ret), *npage_no);
			goto err;
		}

		// The key is already in B+ Tree
		if (RET_DUPDATA == ret) {
			goto err;
		}

		// child node is splitted
		if (NULL != *newkey) {

			pop_key = *newkey;
			pop_lpn = *leftpg;
			pop_rpn = *rightpg;

			*newkey = NULL;
			*leftpg= 0;
			*rightpg = 0;

			ret = insert_inner_node(pageno, cpage, *pop_key, pop_lpn, pop_rpn, newkey, leftpg, rightpg, col_ar);

			if (ret < 0) {
				U_LOG_FATAL("Insert a key into inner node err![%s]", perror(ret));
				goto err;
			}

			//free(pop_key);
			// Fix bug:
			delete pop_key;
			pop_key = NULL;

		} else {

			// if there is no pop key, collect all old pages
			while (--col_ar->cur_ >= 0) {
				ret = collect_old_page(col_ar->col_pg_[col_ar->cur_]);
				if (RET_SUCCESS != ret) {
					U_LOG_WARNING("Collect %u-th page err!", 
							col_ar->col_pg_[col_ar->cur_]);
					ret = RET_INNER_ERR;
					goto err;
				}
			}
		}

	} else if (TREE_LEAF_NODE == bpt->type) {

		ret = insert_leaf_node(pageno, cpage, key, value, newkey, leftpg, rightpg, col_ar);
		if (ret < 0) {
			U_LOG_FATAL("Insert a key/value pair into leaf node err![%s]", perror(ret));
			ret = RET_INNER_ERR;
			goto err;
		}

		// The key is already in B+ Tree
		if (RET_DUPDATA == ret) {
			goto err;
		}
	}

	return RET_SUCCESS;
err:
	return ret;
}

template <typename KeyT, typename ValueT, typename KeyCompareT, typename KeySerial, typename ValueSerial>
int BPlusTree<KeyT, ValueT, KeyCompareT, KeySerial, ValueSerial>::remove(pgno_t pageno, const KeyT &key, bool &empty) {

	int ret;
	ssize_t remove_size , load_size;
	char *st, *end;

	offset_t *off, *hit;
	offset_t i;
	bpt_node_t *bpt;
	pgno_t *pgn;

	KeyT key_obj;

	char cpage[pagesize_];

	// get page
	ret = disk_->read(pageno, cpage);
	if (Disk::RET_SUCCESS != ret) {
		U_LOG_FATAL("Reading page from disk err![%s]", Disk::perror(ret));
		ret = RET_INNER_ERR;
		goto err;
	}

	bpt = (bpt_node_t *)cpage;
	off = (offset_t *)bpt->data;

	if (!validate_leftbytes(cpage)) {
		U_LOG_FATAL("leftbytes err![leftbytes:%u][knum:%u]", bpt->leftbytes, bpt->knum);
		ret = RET_INNER_ERR;
		goto err;
	}

	if (TREE_LEAF_NODE == bpt->type) {

		ret = remove_leaf_node(pageno, cpage, key, empty);
		if (ret < 0) {
			U_LOG_FATAL("Removing a key/value pair from B+ Tree err![%s]", perror(ret));
			ret = RET_INNER_ERR;
			goto err;
		}

		// if not found
		if (RET_NOTFOUND == ret) {
			goto err;
		}

		goto suc;

	} else if (TREE_INNER_NODE == bpt->type) {

		if (0 == bpt->knum) {

			// there is only one pageno in current inner node
			pgn = (pgno_t *)((char *)cpage + pagesize_ - sizeof(pgno_t));
			ret = remove(*pgn, key, empty);
			if (ret < 0) {
				U_LOG_FATAL("Removing a key/value pair from B+ Tree err![%s]", perror(ret));
				ret = RET_INNER_ERR;
				goto err;
			}

			// if not found
			if (RET_NOTFOUND == ret) {
				goto err;
			}

			if (empty) {

				// collect current page
				ret = collect_old_page(pageno, cpage);
				if (RET_SUCCESS != ret) {
					U_LOG_FATAL("Collect old page err![pgn:%u][%s]", pageno, perror(ret));
					ret = RET_INNER_ERR;
					goto err;
				}
				goto suc;

			} else {
				// no modification on current node
				goto suc;

			}
		} else {

			hit = bsearch_upper(key, off, off + bpt->knum, cpage);
			if (hit != off + bpt->knum) {
				load_size = keyserial_.load(key_obj, ((char *)cpage + *hit), pagesize_);
				if (load_size < 0) {
					U_LOG_FATAL("Load key object err![load_size:%zd]", load_size);
					ret = RET_INNER_ERR;
					goto err;
				}
				pgn = (pgno_t *)((char *)cpage + *hit + keyserial_.get_size(key_obj));
			} else {
				pgn = (pgno_t *)((char *)cpage + off[bpt->knum-1] - sizeof(pgno_t));
			}

			ret = remove(*pgn, key, empty);
			if (ret < 0) {
				U_LOG_FATAL("Removing a key/value pair from B+ Tree err![%s]", perror(ret));
				ret = RET_INNER_ERR;
				goto err;
			}

			// if not found
			if (RET_NOTFOUND == ret) {
				goto err;
			}

			if (empty) {

				empty = false;

				// delete current key and left pgno.
				if (hit != off + bpt->knum) {

					load_size = keyserial_.load(key_obj, ((char *)cpage + *hit), pagesize_);
					if (load_size < 0) {
						U_LOG_FATAL("Load key object err![load_size:%zd]", load_size);
						return RET_INNER_ERR;
					}

					remove_size = keyserial_.get_size(key_obj) + sizeof(pgno_t);

					st = (char *)cpage + off[bpt->knum-1] - sizeof(pgno_t);
					end = (char *)cpage + *hit;

					memmove(st + remove_size, st, end - st);

					--(bpt->knum);
					for (i = hit-off; i < bpt->knum; i++) {
						off[i] = off[i+1] + remove_size;
					}
					bpt->leftbytes += (sizeof(offset_t) + remove_size);

				} else if (hit == off + bpt->knum) {

					load_size = keyserial_.load(key_obj, ((char *)cpage + off[bpt->knum-1]), pagesize_);
					if (load_size < 0) {
						U_LOG_FATAL("Load key object err![load_size:%zd]", load_size);
						ret = RET_INNER_ERR;
						goto err;
					}
					remove_size = keyserial_.get_size(key_obj) + sizeof(pgno_t);

					--(bpt->knum);
					bpt->leftbytes += (sizeof(offset_t) + remove_size);
				}


			}

		}
	}

	// write current page into disk & cache
	ret = disk_->write(pageno, cpage);
	if (Disk::RET_SUCCESS != ret) {
		U_LOG_FATAL("Write page into disk when collect old page err![pgn:%u][%s]",
				pageno, Disk::perror(ret));
		ret = RET_INNER_ERR;
		goto err;
	}

suc:
	return RET_SUCCESS;

err:
	return ret;
}

template <typename KeyT, typename ValueT, typename KeyCompareT, typename KeySerial, typename ValueSerial>
int BPlusTree<KeyT, ValueT, KeyCompareT, KeySerial, ValueSerial>::insert_inner_node(pgno_t pageno, void *cpage,
	const KeyT &key, const pgno_t lpn, const pgno_t rpn, KeyT **newkey,
		pgno_t *leftpg, pgno_t *rightpg, collect_array_t *col_ar)
{

	int ret = 0;
	ssize_t insert_size, key_size;

	bpt_node_t *bpt = (bpt_node_t *)cpage;

	key_size = keyserial_.get_size(key);
	if (key_size < 0) {
		U_LOG_WARNING("Invalid size of key!");
		return RET_INNER_ERR;
	}

	insert_size = key_size + sizeof(pgno_t);

	if (!validate_leftbytes(cpage)) {
		U_LOG_FATAL("leftbytes err![leftbytes:%u][knum:%u]", bpt->leftbytes, bpt->knum);
		return -1;
	}

	if (insert_size + sizeof(offset_t) > bpt->leftbytes) {

		// need to split current inner node
		ret = split_inner_node(pageno, cpage, key, lpn, rpn, newkey, leftpg, rightpg, col_ar);
		if (ret < 0) {
			U_LOG_FATAL("Split inner node err![%s]", perror(ret));
			goto err;
		}

	} else {
		*newkey = NULL;
		ret = insert_nonfull_inner_node(pageno, cpage, key, lpn, rpn);
		if (ret < 0) {
			U_LOG_FATAL("Insert key/pgn pair into non-full inner node err![%s]", perror(ret));
			goto err;
		}
	}

	return RET_SUCCESS;
err:
	return ret;
}

template <typename KeyT, typename ValueT, typename KeyCompareT, typename KeySerial, typename ValueSerial>
int BPlusTree<KeyT, ValueT, KeyCompareT, KeySerial, ValueSerial>::insert_nonfull_inner_node(pgno_t pageno, void *cpage,
	const KeyT &key, const pgno_t lpn, const pgno_t rpn)
{

	int ret = 0;
	ssize_t insert_size, key_size;
	ssize_t load_size, dump_size;

	offset_t *off, *hit, *itr;
	//pgno_t *pgn;
	bpt_node_t *bpt;
	char *st, *end;
	KeyT key_obj;

	bpt = (bpt_node_t *)cpage;
	off = (offset_t *)bpt->data;

	if (!validate_leftbytes(cpage)) {
		U_LOG_FATAL("leftbytes err![leftbytes:%u][knum:%u]", bpt->leftbytes, bpt->knum);
		ret = RET_INNER_ERR;
		goto err;
	}

	key_size = keyserial_.get_size(key);
	if (key_size < 0) {
		U_LOG_WARNING("Invalid size of key!");
		ret = RET_INNER_ERR;
		goto err;
	}
	insert_size = key_size + sizeof(pgno_t);

	if (insert_size + sizeof(offset_t) > bpt->leftbytes) {
		U_LOG_FATAL("There is not enough space to insert one key into an inner node!");
		ret = RET_INNER_ERR;
		goto err;
	}

	if (0 == bpt->knum) {
		// there is only one page no. in current inner node
		*off = pagesize_ - sizeof(pgno_t) - key_size;

		dump_size = keyserial_.dump(key, (char *)cpage + *off, key_size);
		if (dump_size < 0 || dump_size != key_size) {
			U_LOG_FATAL("Dump key object err![dump_size:%zd][size:%zd]",
				dump_size, key_size);
			ret = RET_INNER_ERR;
			goto err;
		}

		// dump right pageno.
		memcpy((char *)cpage + *off - sizeof(pgno_t), &rpn, sizeof(pgno_t));

		// dump left pageno.
		memcpy((char *)cpage + pagesize_ - sizeof(pgno_t), &lpn, sizeof(pgno_t));

		// key num increase by 1
		++(bpt->knum);

		// page leftbytes decrease by 'insert_size + offset_t'
		bpt->leftbytes -= (insert_size + sizeof(offset_t));

		goto suc;
	}

	hit = bsearch_upper(key, off, off + bpt->knum, cpage);

	if (hit == off + bpt->knum) {

		*hit = *(hit-1) - sizeof(pgno_t) - key_size;

		// dump key
		dump_size = keyserial_.dump(key, (char *)cpage + *hit, key_size);
		if (dump_size < 0 || dump_size != key_size) {
			U_LOG_FATAL("Dump key object err![dump_size:%zd][size:%zd]", 
				dump_size, key_size);
			ret = RET_INNER_ERR;
			goto err;
		}

		// dump right pageno.
		memcpy((char *)cpage + *hit - sizeof(pgno_t), &rpn, sizeof(pgno_t));

		// dump left pageno.
		memcpy((char *)cpage + *hit + key_size, &lpn, sizeof(pgno_t));

		// key num increase by 1
		++(bpt->knum);

		// page leftbytes decrease by 'insert_size + offset_t'
		bpt->leftbytes -= (insert_size + sizeof(offset_t));

	} else {

//		kt = (KeyT *)((char *)cpage + *hit);

		load_size = keyserial_.load(key_obj, ((char *)cpage + *hit), pagesize_);
		if (load_size < 0) {
			U_LOG_FATAL("Load key object err![load_size:%zd]", load_size);
			ret = RET_INNER_ERR;
			goto err;
		}

		key_size = keyserial_.get_size(key_obj);
		if (key_size < 0) {
			U_LOG_WARNING("Invalid size of key!");
			ret = RET_INNER_ERR;
			goto err;
		}

		st = (char *)cpage + off[bpt->knum-1] - sizeof(pgno_t);
		end = (char *)cpage + *hit + key_size;

		// move memory ('st' to 'end')
		memmove(st - insert_size, st, end - st);


		// TODO: need more check or debug

		// dump key
		dump_size = keyserial_.dump(key, (char *)end - key_size, key_size);
		if (dump_size < 0 || dump_size != key_size) {
			U_LOG_FATAL("Dump object err![dump_size:%zd][size:%zd]", 
				dump_size, key_size);
			ret = RET_INNER_ERR;
			goto err;
		}

		// dump right pageno.
		memcpy((char *)end - insert_size, &rpn, sizeof(pgno_t));

		// dump left pageno.
		memcpy((char *)end, &lpn, sizeof(pgno_t));

		// modify offset
		for (itr = off + bpt->knum ; itr != hit; itr--) {
			*itr = *(itr-1) - insert_size;
		}

		*hit = end - key_size - (char *)cpage;

		// key num increase by 1
		++(bpt->knum);

		// page leftbytes decrease by 'insert_size + offset_t'
		bpt->leftbytes -= (insert_size + sizeof(offset_t));

	}

suc:
	// rewrite the page into disk & cache
	ret = disk_->write(pageno, cpage);
	if (Disk::RET_SUCCESS != ret) {
		U_LOG_FATAL("Write page into disk when split leaf node err![pgn:%u][%s]",
				pageno, Disk::perror(ret));
		ret = RET_INNER_ERR;
		goto err;
	}
	return RET_SUCCESS;

err:
	return ret;
}

template <typename KeyT, typename ValueT, typename KeyCompareT, typename KeySerial, typename ValueSerial>
int BPlusTree<KeyT, ValueT, KeyCompareT, KeySerial, ValueSerial>::split_inner_node(pgno_t pageno, void *cpage,
	const KeyT &key, const pgno_t lpn, const pgno_t rpn, KeyT **newkey,
		pgno_t *leftpg, pgno_t *rightpg, collect_array_t *col_ar)
{

	int ret = 0;
	ssize_t load_size;
	ssize_t insert_size, key_size;

	offset_t left_knum, right_knum;
	offset_t *off, *left_off, *right_off;
	pgno_t curpos, i, j, pop_mid;

	bpt_node_t *bpt, *left_bpt, *right_bpt;

	char left_page[pagesize_];
	char right_page[pagesize_];

	KeyT key_obj;

	key_size = keyserial_.get_size(key);
	if (key_size < 0) {
		U_LOG_WARNING("Invalid size of key!");
		ret = RET_INNER_ERR;
		goto err;
	}
	insert_size = key_size + sizeof(pgno_t);

	ret = alloc_new_page(leftpg, left_page);
	if (ret < 0) {
		U_LOG_FATAL("alloc new page when split leaf node err!");
		ret = RET_INNER_ERR;
		goto err;
	}

	ret = alloc_new_page(rightpg, right_page);
	if (ret < 0) {
		U_LOG_FATAL("alloc new page when split leaf node err!");
		ret = RET_INNER_ERR;
		goto err;
	}

	// copy current page content into left page
	memcpy(left_page, cpage, pagesize_);

	bpt = (bpt_node_t *)cpage;
	off = (offset_t *)bpt->data;

	if (!validate_leftbytes(cpage)) {
		U_LOG_FATAL("leftbytes err![leftbytes:%u][knum:%u]", bpt->leftbytes, bpt->knum);
		ret = RET_INNER_ERR;
		goto err;
	}

	left_bpt = (bpt_node_t *)left_page;
	left_off = (offset_t *)left_bpt->data;

	right_bpt = (bpt_node_t *)right_page;
	right_off = (offset_t *)right_bpt->data;

	// TODO: maybe the sizeof key/value pair is very large
	if (bpt->knum < 3) {
		U_LOG_FATAL("num of key is invalid when split inner node![knum:%u][leftbytes:%u]", 
			bpt->knum, bpt->leftbytes);
		ret = RET_INNER_ERR;
		goto err;
	}

	pop_mid = bpt->knum / 2;
	left_knum = pop_mid;
	right_knum = bpt->knum - left_knum - 1;

	if (0 == left_knum || 0 == right_knum) {
		U_LOG_FATAL("Num of key is zero when split inner node![left_knum:%u][right_knum:%u]",
			left_knum, right_knum);
		ret = RET_INNER_ERR;
		goto err;
	}

	curpos = pagesize_;
	for (i = pop_mid + 1, j = 0; i < bpt->knum; i++, j++) {
/*
		kt = (KeyT *)((char *)left_page + off[i]);
		key_size = get_size(*kt);
*/
		load_size = keyserial_.load(key_obj, ((char *)left_page + off[i]), pagesize_);
		if (load_size < 0) {
			U_LOG_FATAL("Load key object err![load_size:%zd]", load_size);
			ret = RET_INNER_ERR;
			goto err;
		}
		key_size = keyserial_.get_size(key_obj);
		if (key_size < 0) {
			U_LOG_WARNING("Invalid size of key!");
			ret = RET_INNER_ERR;
			goto err;
		}
		right_off[j] = curpos - key_size - sizeof(pgno_t);

		// move key
		memcpy((char *)right_page + right_off[j], (char *)left_page + off[i], key_size);

		// move pageno.
		memcpy((char *)right_page + right_off[j] + key_size,
			(char *)left_page + off[i] + key_size, sizeof(pgno_t));


		curpos -= (key_size + sizeof(pgno_t));
	}


	memcpy((char *)right_page + right_off[right_knum-1] - sizeof(pgno_t),
			(char *)left_page + off[bpt->knum-1] - sizeof(pgno_t), sizeof(pgno_t));

	// reset both bpts
	left_bpt->knum = left_knum;
	left_bpt->leftbytes = pagesize_ - sizeof(bpt_node_t) - left_knum * sizeof(offset_t)
		- (pagesize_ - off[left_knum-1] + sizeof(pgno_t));

	right_bpt->knum = right_knum;
	right_bpt->type = TREE_INNER_NODE;
	right_bpt->leftbytes = pagesize_ - sizeof(bpt_node_t) - right_knum * sizeof(offset_t)
		- (pagesize_ - off[right_knum-1] + sizeof(pgno_t));

	right_bpt->prev = 0;
	right_bpt->next = 0;


	// rewrite both pages into disk & cache

	ret = disk_->write(*leftpg, left_page);
	if (Disk::RET_SUCCESS != ret) {
		U_LOG_FATAL("Write page into disk when split leaf node err![pgn:%u][%s]",
				*leftpg, Disk::perror(ret));
		ret = RET_INNER_ERR;
		goto err;
	}

	ret = disk_->write(*rightpg, right_page);
	if (Disk::RET_SUCCESS != ret) {
		U_LOG_FATAL("Write page into disk when split leaf node err![pgn:%u][%s]",
				*rightpg, Disk::perror(ret));
		ret = RET_INNER_ERR;
		goto err;
	}

	// insert a key/pgn into nonfull inner node
	//*newkey = (KeyT *)calloc(1, key_size);
	*newkey = new KeyT();
	if (NULL == *newkey) {
		U_LOG_FATAL("alloc space for one new key err!");
		ret = RET_NOSPACE_ERR;
		goto err;
	}

	// This is a BUG, newkey must be loaded not memcpy
//	memcpy(*newkey, (char *)left_page + off[pop_mid], key_size);

	load_size = keyserial_.load(**newkey, (char *)left_page + off[pop_mid], pagesize_);
	if (load_size < 0 ) {
		U_LOG_FATAL("Load new key object err![load_size:%zd]", load_size);
		ret = RET_INNER_ERR;
		goto err;
	}

//FIX BUG:
//	if (comp_(key, key_obj)) {
	if (comp_(key, **newkey)) {
		ret = insert_nonfull_inner_node(*leftpg, left_page, key, lpn, rpn);
		if (ret < 0) {
			U_LOG_FATAL("Insert key/pgn pair into non-full inner node err![%s]", perror(ret));
			goto err;
		}
	} else {
		ret = insert_nonfull_inner_node(*rightpg, right_page, key, lpn, rpn);
		if (ret < 0) {
			U_LOG_FATAL("Insert key/pgn pair into non-full inner node err![%s]", perror(ret));
			goto err;
		}
	}


	// push current pageno. into collect array
	if (col_ar->cur_ >= BPT_MAX_HEIGHT) {
		U_LOG_FATAL("Collect array is full![cur:%u]", col_ar->cur_);
		ret = RET_INNER_ERR;
		goto err;
	}

	col_ar->col_pg_[col_ar->cur_++] = pageno;

	return RET_SUCCESS;

err:
	return ret;
}

template <typename KeyT, typename ValueT, typename KeyCompareT, typename KeySerial, typename ValueSerial>
int BPlusTree<KeyT, ValueT, KeyCompareT, KeySerial, ValueSerial>::insert_leaf_node(pgno_t pageno, void *cpage,
	const KeyT &key, const ValueT &value, KeyT **newkey, pgno_t *leftpg,
	pgno_t *rightpg, collect_array_t *col_ar)
{
	int ret = 0;
	ssize_t insert_size, key_size, value_size;

	bpt_node_t *bpt = (bpt_node_t *)cpage;

	if (!validate_leftbytes(cpage)) {
		U_LOG_FATAL("leftbytes err![leftbytes:%u][knum:%u]", bpt->leftbytes, bpt->knum);
		return -1;
	}

	key_size = keyserial_.get_size(key);
	if (key_size < 0) {
		U_LOG_WARNING("Invalid size of key!");
		ret = RET_INNER_ERR;
		goto err;
	}

	value_size = valueserial_.get_size(value);
	if (value_size < 0) {
		U_LOG_WARNING("Invalid size of value!");
		ret = RET_ARG_ERR;
		goto err;
	}

	if (key_size <= 0 || value_size <= 0) {
		U_LOG_FATAL("Invalid size of key/value![key_size:%zd][value_size:%zd]",
			key_size, value_size);
		ret = RET_INNER_ERR;
		goto err;
	}

	insert_size = key_size + value_size;

	if (insert_size + sizeof(offset_t) > bpt->leftbytes) {

		// need to split current leaf page
		ret = split_leaf_node(pageno, cpage, key, value, newkey, leftpg, rightpg, col_ar);
		if (ret < 0) {
			U_LOG_FATAL("Split leaf node err![%s]", perror(ret));
			ret = RET_INNER_ERR;
			goto err;
		}

		// The key is already in B+ Tree
		if (RET_DUPDATA == ret) {
			goto err;
		}

	} else {

		*newkey = NULL;
		ret = insert_nonfull_leaf_node(pageno, cpage, key, value);
		if (ret < 0) {
			U_LOG_FATAL("Insert key/value pair into non-full leaf node err![%s]", perror(ret));
			ret = RET_INNER_ERR;
			goto err;
		}

		// The key is already in B+ Tree
		if (RET_DUPDATA == ret) {
			goto err;
		}
	}

	return RET_SUCCESS;
err:
	return ret;
}

template <typename KeyT, typename ValueT, typename KeyCompareT, typename KeySerial, typename ValueSerial>
int BPlusTree<KeyT, ValueT, KeyCompareT, KeySerial, ValueSerial>::split_leaf_node(pgno_t pageno, void *cpage,
	const KeyT &key, const ValueT &value, KeyT **newkey, pgno_t *leftpg, pgno_t *rightpg,
	collect_array_t *col_ar)
{
	int ret;
	ssize_t load_size;
	ssize_t move_size, insert_size, key_size, value_size;

	char left_page[pagesize_];
	char right_page[pagesize_];
	//char link_page[pagesize_];

	offset_t i, j;
	offset_t *hit, *off, *left_off, *right_off;
	offset_t left_knum, right_knum;
	pgno_t curpos;

	bpt_node_t *left_bpt, *right_bpt, *bpt;

	KeyT key_obj;

	key_size = keyserial_.get_size(key);
	if (key_size < 0) {
		U_LOG_WARNING("Invalid size of key!");
		ret = RET_INNER_ERR;
		goto err;
	}

	value_size = valueserial_.get_size(value);
	if (value_size < 0) {
		U_LOG_WARNING("Invalid size of value!");
		ret = RET_INNER_ERR;
		goto err;
	}

	if (key_size <= 0 || value_size <= 0) {
		U_LOG_FATAL("Invalid size of key/value![key_size:%zd][value_size:%zd]",
			key_size, value_size);
		ret = RET_INNER_ERR;
		goto err;
	}

	insert_size = key_size + value_size;

	bpt = (bpt_node_t *)cpage;
	off = (offset_t *)bpt->data;

	if (!validate_leftbytes(cpage)) {
		U_LOG_FATAL("leftbytes err![leftbytes:%u][knum:%u]", bpt->leftbytes, bpt->knum);
		return -1;
	}

	// check whether the key is already in B+ Tree
	hit = bsearch_equal(key, off, off + bpt->knum, cpage);

	if (NULL != hit) {
		U_LOG_WARNING("The key is already in B+ Tree!");
		ret = RET_DUPDATA;
		goto err;
	}

	ret = alloc_new_page(leftpg, left_page);
	if (ret < 0) {
		U_LOG_FATAL("alloc new left page when split leaf node err!");
		ret = RET_INNER_ERR;
		goto err;
	}

	ret = alloc_new_page(rightpg, right_page);
	if (ret < 0) {
		U_LOG_FATAL("alloc new left page when split leaf node err!");
		ret = RET_INNER_ERR;
		goto err;
	}

	// copy current page content into left page
	memcpy(left_page, cpage, pagesize_);


	left_bpt = (bpt_node_t *)left_page;
	left_off = (offset_t *)left_bpt->data;

	right_bpt = (bpt_node_t *)right_page;
	right_off = (offset_t *)right_bpt->data;

	right_knum = bpt->knum / 2;
	left_knum = bpt->knum - right_knum;

	if (0 == left_knum) {
		U_LOG_FATAL("num of key in left node is 0 when split leaf node!");
		ret = RET_INNER_ERR;
		goto err;
	}

	curpos = pagesize_;

	for (i = left_knum, j = 0; i < bpt->knum; i++, j++) {

		move_size = off[i-1] - off[i];
		right_off[j] = curpos - move_size;

		memcpy((char *)right_page + right_off[j], (char *)left_page + off[i], move_size);

		//curpos -= dump_size;
		curpos -= move_size;
	}

	// modify new leaf bpt node
	right_bpt->knum = right_knum;
	right_bpt->type = TREE_LEAF_NODE;
	if (0 == right_knum) {
		right_bpt->leftbytes = pagesize_ - sizeof(bpt_node_t);
	} else {
		right_bpt->leftbytes = pagesize_ - sizeof(bpt_node_t) - right_knum * sizeof(offset_t) - (pagesize_ - right_off[right_knum-1]);
	}


	// modify old leaf bpt node
	left_bpt->knum = left_knum;
	left_bpt->leftbytes = pagesize_ - sizeof(bpt_node_t) - left_knum * sizeof(offset_t) - (pagesize_ - off[left_knum-1]);


	// rewrite both pages into disk
	ret = disk_->write(*leftpg, left_page);
	if (Disk::RET_SUCCESS != ret) {
		U_LOG_FATAL("Write page into disk when split leaf node err![pgn:%u][%s]",
				*leftpg, Disk::perror(ret));
		ret = RET_INNER_ERR;
		goto err;
	}

	ret = disk_->write(*rightpg, right_page);
	if (Disk::RET_SUCCESS != ret) {
		U_LOG_FATAL("Write page into disk when split leaf node err![pgn:%u][%s]",
				*rightpg, Disk::perror(ret));
		ret = RET_INNER_ERR;
		goto err;
	}

	// insert a key/value into nonfull leaf
	//kt = (KeyT *)(left_page + off[left_knum-1]);

	load_size = keyserial_.load(key_obj, ((char *)left_page + off[left_knum-1]), pagesize_);
	if (load_size < 0) {
		U_LOG_FATAL("Load key object err![load_size:%zd]", load_size);
		return RET_INNER_ERR;
	}

	if (comp_(key_obj, key)) {
		ret = insert_nonfull_leaf_node(*rightpg, right_page, key, value);
		if (ret < 0) {
			U_LOG_FATAL("Insert key/value pair into non-full leaf node err![%s]", perror(ret));
			goto err;
		}
	} else {
		ret = insert_nonfull_leaf_node(*leftpg, left_page, key, value);
		if (ret < 0) {
			U_LOG_FATAL("Insert key/value pair into non-full leaf node err![%s]", perror(ret));
			goto err;
		}
	}

	*newkey = new KeyT();
	if (NULL == *newkey) {
		U_LOG_FATAL("alloc space for one new key err!");
		ret = RET_NOSPACE_ERR;
		goto err;
	}

	if (0 == right_bpt->knum) {
		U_LOG_FATAL("There no data in new leaf node!");
		ret = RET_INNER_ERR;
		goto err;
	}


  // This is a BUG, newkey must be loaded not memcpy
  //	memcpy(*newkey, (char *)right_page + right_off[0], key_size);

	load_size = keyserial_.load(**newkey, (char *)right_page + off[0], pagesize_);
	if (load_size < 0) {
		U_LOG_FATAL("Load new key object err![load_size:%zd]", load_size);
		ret = RET_INNER_ERR;
		goto err;
	}

	// push current pageno. into collect array
	if (col_ar->cur_ >= BPT_MAX_HEIGHT) {
		U_LOG_FATAL("Collect array is full![cur:%u]", col_ar->cur_);
		ret = RET_INNER_ERR;
		goto err;
	}

	col_ar->col_pg_[col_ar->cur_++] = pageno;

	return RET_SUCCESS;

err:
	return ret;
}

template <typename KeyT, typename ValueT, typename KeyCompareT, typename KeySerial, typename ValueSerial>
int BPlusTree<KeyT, ValueT, KeyCompareT, KeySerial, ValueSerial>::insert_nonfull_leaf_node(pgno_t pageno, void *cpage,
	const KeyT &key, const ValueT &value)
{
	int ret = 0;
	ssize_t insert_size, key_size, value_size;
	ssize_t dump_size;

	offset_t *off, *hit, *itr;
	bpt_node_t *bpt;
	char *st, *end;
//	KeyT *kt;

	bpt = (bpt_node_t *)cpage;
	off = (offset_t *)bpt->data;

	if (!validate_leftbytes(cpage)) {
		U_LOG_FATAL("leftbytes err![leftbytes:%u][knum:%u]", bpt->leftbytes, bpt->knum);
		ret = RET_INNER_ERR;
		goto err;
	}

	key_size = keyserial_.get_size(key);
	if (key_size < 0) {
		U_LOG_WARNING("Invalid size of key!");
		ret = RET_INNER_ERR;
		goto err;
	}

	value_size = valueserial_.get_size(value);
	if (key_size <= 0 || value_size <= 0) {
		U_LOG_FATAL("Invalid size of key/value![key_size:%zd][value_size:%zd]",
			key_size, value_size);
		ret = RET_INNER_ERR;
		goto err;
	}

	insert_size = key_size + value_size;

	if (0 == bpt->knum) {

		// need space: KeyT + ValueT + offset_t
		if (insert_size + sizeof(offset_t) > bpt->leftbytes) {
			U_LOG_FATAL("The size of key/value is too large to insert into an empty leaf node."
				"[size:%zd]", insert_size);
			ret = RET_NOSPACE_ERR;
			goto err;
		}

		// key num increase by 1
		bpt->knum = 1;

		*off = pagesize_ - insert_size;
/*
		kt = (KeyT *)((char *)cpage + *off);
		dump_size = dump_kv((void *)kt, key, value);
		if (dump_size < 0 || dump_size != insert_size) {
			U_LOG_FATAL("Dumping key/value data into leaf failed![dump_size:%zd][key_size:%zd]"
				"[value_size:%zd]", dump_size, key_size, value_size);
			ret = RET_INNER_ERR;
			goto err;
		}
*/

		dump_size = keyserial_.dump(key, (char *)cpage + *off, key_size);
		if (dump_size < 0 || dump_size != key_size) {
			U_LOG_FATAL("Dump object err![dump_size:%zd][size:%zd]", 
				dump_size, key_size);
			ret = RET_INNER_ERR;
			goto err;
		}

		dump_size = valueserial_.dump(value, (char *)cpage + *off + key_size, value_size);
		if (dump_size < 0) {
			U_LOG_FATAL("dumping value into page buffer failed![dump_size:%zd][value_size:%zd]",
					dump_size, value_size);
			ret = RET_INNER_ERR;
			goto err;
		}

		// page leftbytes decrease by 'insert_size + offset_t'
		bpt->leftbytes -= (insert_size + sizeof(offset_t));

		goto out;
	}

	// check whether the key is already in B+ Tree
	hit = bsearch_equal(key, off, off + bpt->knum, cpage);
	if (NULL != hit) {
		U_LOG_WARNING("The key is already in B+ Tree!");
		ret = RET_DUPDATA;
		goto err;
	}

	// bsearch insert position
	hit = bsearch_upper(key, off, off + bpt->knum, cpage);

	if (hit == off + bpt->knum) {

		// the key is larger than anyone in current leaf node
		*hit = *(hit-1) - insert_size;
		dump_size = keyserial_.dump(key, (char *)cpage + *hit, key_size);
		if (dump_size < 0 || dump_size != key_size) {
			U_LOG_FATAL("Dump object err![dump_size:%zd][size:%zd]", 
				dump_size, key_size);
			ret = RET_INNER_ERR;
			goto err;
		}

		dump_size = valueserial_.dump(value, (char *)cpage + *hit + key_size, value_size);
		if (dump_size < 0) {
			U_LOG_FATAL("dumping value into page buffer failed![dump_size:%zd][value_size:%zd]",
					dump_size, value_size);
			ret = RET_INNER_ERR;
			goto err;
		}

		// key num increase by 1
		++(bpt->knum);

		// page leftbytes decrease by 'insert_size + offset_t'
		bpt->leftbytes -= (insert_size + sizeof(offset_t));

	} else {

		st = (char *)cpage + off[bpt->knum-1];
		if (hit == off) {
			end = (char *)cpage + pagesize_;
		} else {
			end = (char *)cpage + *(hit-1);
		}

		// move memory ('st' to 'end') 
		memmove(st - insert_size, st, end - st);


		dump_size = keyserial_.dump(key, (char *)end - insert_size, key_size);
		if (dump_size < 0 || dump_size != key_size) {
			U_LOG_FATAL("Dump object err![dump_size:%zd][size:%zd]", 
				dump_size, key_size);
			ret = RET_INNER_ERR;
			goto err;
		}

		dump_size = valueserial_.dump(value, (char *)end - insert_size + key_size, value_size);
		if (dump_size < 0) {
			U_LOG_FATAL("dumping value into page buffer failed![dump_size:%zd][value_size:%zd]",
					dump_size, value_size);
			ret = RET_INNER_ERR;
			goto err;
		}

		// modify offset
		for (itr = off + bpt->knum ; itr != hit; itr--) {
			*itr = *(itr-1) - insert_size;
		}

		*hit = end - insert_size - (char *)cpage;

		// key num increase by 1
		++(bpt->knum);

		// page leftbytes decrease by 'insert_size + offset_t'
		bpt->leftbytes -= (insert_size + sizeof(offset_t));

	}

out:

	ret = disk_->write(pageno, cpage);
	if (Disk::RET_SUCCESS != ret) {
		U_LOG_FATAL("Write page into disk err![pgn:%u][%s]", pageno, Disk::perror(ret));
		ret = RET_INNER_ERR;
		goto err;
	}

	return RET_SUCCESS;
err:
	return ret;
}

template <typename KeyT, typename ValueT, typename KeyCompareT, typename KeySerial, typename ValueSerial>
int BPlusTree<KeyT, ValueT, KeyCompareT, KeySerial, ValueSerial>::remove_leaf_node(pgno_t pageno, void *cpage,
	const KeyT &key, bool &empty)
{

	int ret = 0;
	ssize_t remove_size, load_size;
	char *st, *end;

	offset_t *off, *hit;
	offset_t i;
	bpt_node_t *bpt;
	KeyT key_obj;
	ValueT value_obj;

	bpt = (bpt_node_t *)cpage;
	off = (offset_t *)bpt->data;

	if (!validate_leftbytes(cpage)) {
		U_LOG_FATAL("leftbytes err![leftbytes:%u][knum:%u]", bpt->leftbytes, bpt->knum);
		return -1;
	}

	if (0 == bpt->knum) {
		ret = RET_NOTFOUND;
		goto err;
	}

	hit = bsearch_equal(key, off, off + bpt->knum, cpage);
	if (NULL == hit) {
		U_LOG_WARNING("The key to be deleted is not found in B+ Tree!");
		ret = RET_NOTFOUND;
		goto err;
	}

	if (1 == bpt->knum) {
		// there is only one key/value pair
		empty = true;
		ret = collect_old_page(pageno, cpage);
		if (RET_SUCCESS != ret) {
			U_LOG_FATAL("Collect old page err![pgn:%u][%s]", pageno, perror(ret));
			ret = RET_INNER_ERR;
			goto err;
		}
		goto suc;
	}

	load_size = keyserial_.load(key_obj, ((char *)cpage + *hit), pagesize_);
	if (load_size < 0) {
		U_LOG_FATAL("Load key object err![load_size:%zd]", load_size);
		return RET_INNER_ERR;
	}

	load_size = valueserial_.load(value_obj, ((char *)cpage + *hit + keyserial_.get_size(key_obj)), pagesize_);
	if (load_size < 0) {
		U_LOG_FATAL("Load value object err![load_size:%zd]", load_size);
		return RET_INNER_ERR;
	}

	remove_size = keyserial_.get_size(key_obj) + valueserial_.get_size(value_obj);

	if (hit == off + bpt->knum - 1) {
		--(bpt->knum);
		bpt->leftbytes += (sizeof(offset_t) + remove_size);
		goto out;
	}

	st = (char *)cpage + off[bpt->knum-1];
	end = (char *)cpage + *hit;

	memmove(st + remove_size, st, end - st);

	--(bpt->knum);
	for (i = hit-off; i < bpt->knum; i++) {
		off[i] = off[i+1] + remove_size;
	}
	bpt->leftbytes += (sizeof(offset_t) + remove_size);

out:
	// write current page into disk & cache
	ret = disk_->write(pageno, cpage);
	if (Disk::RET_SUCCESS != ret) {
		U_LOG_FATAL("Write page into disk when collect old page err![pgn:%u][%s]",
				pageno, Disk::perror(ret));
		ret = RET_INNER_ERR;
		goto err;
	}

suc:
	return RET_SUCCESS;
err:
	return ret;
}

/*
template <typename KeyT, typename ValueT, typename KeyCompareT, typename KeySerial, typename ValueSerial>
ssize_t BPlusTree<KeyT, ValueT, KeyCompareT, KeySerial, ValueSerial>::dump_kv(void *buf, const KeyT &key, const ValueT &value)
{
	ssize_t key_size, value_size;
	ssize_t dump_size, ret_size;

	key_size = get_size(key);
	value_size = get_size(value);

	if (key_size <= 0 || value_size <= 0) {
		U_LOG_FATAL("Invalid size of key/value![key_size:%zd][value_size:%zd]",
			key_size, value_size);
		goto err;
	}

	ret_size = 0;

	dump_size = dump(key, buf, key_size);
	if (dump_size < 0 || dump_size != key_size) {
		U_LOG_FATAL("Dumping key data into leaf failed![dump_size:%zd][key_size:%zd]",
				dump_size, key_size);
		goto err;
	}
	
	ret_size += dump_size;
	buf = (void *)((char *)buf + key_size);

	dump_size = dump(value, buf, value_size);
	if (dump_size < 0 || dump_size != value_size) {
		U_LOG_FATAL("Dumping value data into leaf failed![dump_size:%zd][value_size:%zd]",
				dump_size, value_size);
		goto err;
	}

	ret_size += dump_size;

	return ret_size;

err:
	return -1;
}

template <typename KeyT, typename ValueT, typename KeyCompareT, typename KeySerial, typename ValueSerial>
template <class T>
ssize_t BPlusTree<KeyT, ValueT, KeyCompareT, KeySerial, ValueSerial>::dump(const T &obj, void *buf, size_t buf_size) {
	
	size_t size = sizeof(T);

	if (NULL == buf) {
		U_LOG_FATAL("Null pointer to buffer when dumping data.");
		return -1;
	}

	if (size > buf_size) {
		U_LOG_FATAL("No space when dumping data.[size:%zu][bufsize:%zu]", size, buf_size);
		size = buf_size;	
	}
	
	memcpy(buf, &obj, size);
	
	return size;
}

template <typename KeyT, typename ValueT, typename KeyCompareT, typename KeySerial, typename ValueSerial>
template <class T>
ssize_t BPlusTree<KeyT, ValueT, KeyCompareT, KeySerial, ValueSerial>::load(const T &obj, void *buf, size_t buf_size) {

	size_t size = sizeof(T);
	
	if (NULL == buf) {
		U_LOG_FATAL("Null pointer to buffer when dumping data.");
		return -1;
	}

	if (buf_size > size) {
		U_LOG_FATAL("No space when loading data.[size:%zu][bufsize:%zu]", size, buf_size);
		buf_size = size;
	}

	memcpy(&obj, buf, buf_size);

	return buf_size;
}

template <typename KeyT, typename ValueT, typename KeyCompareT, typename KeySerial, typename ValueSerial>
template <class T>
ssize_t BPlusTree<KeyT, ValueT, KeyCompareT, KeySerial, ValueSerial>::get_size(const T &obj) {
	return sizeof(T);
}

*/

template <typename KeyT, typename ValueT, typename KeyCompareT, typename KeySerial, typename ValueSerial>
offset_t* BPlusTree<KeyT, ValueT, KeyCompareT, KeySerial, ValueSerial>::bsearch_equal(const KeyT &key, offset_t *left, offset_t *right, void *base) {

	int len = right - left;
	int half;
	ssize_t load_size;
	offset_t *middle;
	KeyT key_obj;

	while (len > 0) {
		half = len >> 1;
		middle = left + half;
		load_size = keyserial_.load(key_obj, ((char *)base + *middle), pagesize_);
		if (load_size < 0) {
			U_LOG_FATAL("Load object err when bsearch equal![size:%zd]", load_size);
			return NULL;
		}
		if (comp_(key_obj, key)) {
			left = middle + 1;
			len = len - half - 1;
		} else {
			len = half;
		}
	}

	if (left == right) {
		return NULL;
	}

	load_size = keyserial_.load(key_obj, ((char *)base + *left), pagesize_);
	if (load_size < 0) {
		U_LOG_FATAL("Load object err when bsearch equal![size:%zd]", load_size);
		return NULL;
	}

	if (comp_(key, key_obj)) {
		return NULL;
	} else {
		return left;
	}
}

template <typename KeyT, typename ValueT, typename KeyCompareT, typename KeySerial, typename ValueSerial>
offset_t* BPlusTree<KeyT, ValueT, KeyCompareT, KeySerial, ValueSerial>::bsearch_lower(const KeyT &key, offset_t *left, offset_t *right, void *base) {

	int len = right - left;
	int half;
	ssize_t load_size;
	offset_t *middle;
	KeyT key_obj;

	while (len > 0) {
		half = len >> 1;
		middle = left + half;
		load_size = keyserial_.load(key_obj, ((char *)base + *middle), pagesize_);
		if (load_size < 0) {
			U_LOG_FATAL("Load object err when bsearch lower![size:%zd]", load_size);
			return NULL;
		}
		if (comp_(key_obj, key)) {
			left = middle + 1;
			len = len - half - 1;
		} else {
			len = half;
		}
	}

	return left;
}

template <typename KeyT, typename ValueT, typename KeyCompareT, typename KeySerial, typename ValueSerial>
offset_t* BPlusTree<KeyT, ValueT, KeyCompareT, KeySerial, ValueSerial>::bsearch_upper(const KeyT &key, offset_t *left, offset_t *right, void *base) {

	int len = right - left;
	int half;
	ssize_t load_size;
	offset_t *middle;
	KeyT key_obj;

	while (len > 0) {
		half = len >> 1;
		middle = left + half;
		load_size = keyserial_.load(key_obj, ((char *)base + *middle), pagesize_);
		if (load_size < 0) {
			U_LOG_FATAL("Load object err when bsearch upper![size:%zd]", load_size);
			return NULL;
		}
		if (comp_(key, key_obj)) {
			len = half;
		} else {
			left = middle + 1;
			len = len - half - 1;
		}
	}
	return left;
}

template <typename KeyT, typename ValueT, typename KeyCompareT, typename KeySerial, typename ValueSerial>
int BPlusTree<KeyT, ValueT, KeyCompareT, KeySerial, ValueSerial>::alloc_new_page(pgno_t *pgn, void *page) {

	int ret = 0;
	bpt_node_t *bpt;
	pgno_t next;
	
	ret = disk_->alloc(bpt_meta_->unused_pgno, page);
	if (Disk::RET_SUCCESS != ret) {
		U_LOG_FATAL("alloc new page from disk err![ret:%d][%s]",
			ret, Disk::perror(ret));
		ret = RET_INNER_ERR;
		goto err;
	}

	*pgn = bpt_meta_->unused_pgno;

	// modify cur_pgno_
	bpt = (bpt_node_t *)page;
	next = bpt->next;

	if (0 == next) {
		++(bpt_meta_->unused_pgno);
	}
	else {
		bpt_meta_->unused_pgno = next;
	}

	// reset page
	bzero(page, pagesize_);

	return RET_SUCCESS;
err:
	return ret;
}

template <typename KeyT, typename ValueT, typename KeyCompareT, typename KeySerial, typename ValueSerial>
int BPlusTree<KeyT, ValueT, KeyCompareT, KeySerial, ValueSerial>::collect_old_page(const pgno_t pgn, void *cpage) {

	// do not clear page, write it into disk

	int ret = 0;
	bpt_node_t *bpt;

	bpt = (bpt_node_t *)cpage;

	bpt->next = bpt_meta_->unused_pgno;

	// write it into disk & cache
	ret = disk_->write(pgn, cpage);
	if (Disk::RET_SUCCESS != ret) {
		U_LOG_FATAL("Write page into disk when collect old page err![pgn:%u][%s]",
				pgn, Disk::perror(ret));
		ret = RET_INNER_ERR;
		goto err;
	}

	// modify unused page link pointer
	bpt_meta_->unused_pgno = pgn;

	return RET_SUCCESS;
err:
	return ret;
}

template <typename KeyT, typename ValueT, typename KeyCompareT, typename KeySerial, typename ValueSerial>
int BPlusTree<KeyT, ValueT, KeyCompareT, KeySerial, ValueSerial>::collect_old_page(const pgno_t pgn) {

	int ret = 0;
	bpt_node_t *bpt;
	char cpage[pagesize_];

	ret = disk_->read(pgn, cpage);
	if (Disk::RET_SUCCESS != ret) {
		U_LOG_FATAL("Reading page from disk err![ret:%d][%s]", ret, Disk::perror(ret));
		ret = RET_INNER_ERR;
		goto err;
	}

	bpt = (bpt_node_t *)cpage;
	bpt->next = bpt_meta_->unused_pgno;

	// write it into disk & cache
	ret = disk_->write(pgn, cpage);
	if (Disk::RET_SUCCESS != ret) {
		U_LOG_FATAL("Write page into disk when collect old page err![pgn:%u][%s]",
				pgn, Disk::perror(ret));
		ret = RET_INNER_ERR;
		goto err;
	}

	// modify unused page link pointer
	bpt_meta_->unused_pgno = pgn;

	return RET_SUCCESS;
err:
	return ret;
}

template <typename KeyT, typename ValueT, typename KeyCompareT, typename KeySerial, typename ValueSerial>
template <class T>
void BPlusTree<KeyT, ValueT, KeyCompareT, KeySerial, ValueSerial>::ptrfree(T **p) {
	if (NULL != *p) {
		free(*p);
		*p = NULL;
	}
}

template <typename KeyT, typename ValueT, typename KeyCompareT, typename KeySerial, typename ValueSerial>
template <class T>
void BPlusTree<KeyT, ValueT, KeyCompareT, KeySerial, ValueSerial>::ptrdel(T **p) {
	if (NULL != *p) {
		delete (*p);
		*p = NULL;
	}
}

template <typename KeyT, typename ValueT, typename KeyCompareT, typename KeySerial, typename ValueSerial>
void BPlusTree<KeyT, ValueT, KeyCompareT, KeySerial, ValueSerial>::fdclose(int *fd) {
	if (-1 != *fd) {
		close(*fd);
		*fd = -1;
	}
}

// debug

#ifdef _BPLUSTREE_DEBUG_
template <typename KeyT, typename ValueT, typename KeyCompareT, typename KeySerial, typename ValueSerial>
int BPlusTree<KeyT, ValueT, KeyCompareT, KeySerial, ValueSerial>::print_node_info(const pgno_t pageno) {

	int ret;
	char cpage[pagesize_];

	offset_t i, *off;
	pgno_t *pgn;
	bpt_node_t *bpt;
	KeyT key_obj;
	ValueT value_obj;
	uint64_t tmp;

	printf("[PAGE:%u]", pageno);
	if (META_PAGE == pageno) {

		printf("[ROOT_PAGE_NO->%u] [UNUSED_PAGE_NO->%u] [PAGESIZE->%u]\n",
			bpt_meta_->root_pgno, bpt_meta_->unused_pgno, bpt_meta_->pagesize);

		return RET_SUCCESS;
	}

	ret = disk_->read(pageno, cpage);
	if (Disk::RET_SUCCESS != ret) {
		U_LOG_FATAL("Reading page from disk err![ret:%d][%s]", ret, Disk::perror(ret));
		return RET_INNER_ERR;
	}

	bpt = (bpt_node_t *)cpage;
	off = (offset_t *)bpt->data;

	if (TREE_LEAF_NODE == bpt->type) {

		printf("[LEAF: %u key/value(s)][leftbytes:%u][prev:%u][next:%u] ->", bpt->knum, bpt->leftbytes, bpt->prev, bpt->next);
		for (i = 0; i < bpt->knum; i++) {

			//kt = (KeyT *)((char *)cpage + off[i]);
			//vt = (ValueT *)((char *)kt + get_size(*kt));
			keyserial_.load(key_obj, (char *)cpage + off[i], pagesize_);
			valueserial_.load(value_obj, (char *)cpage + off[i] + keyserial_.get_size(key_obj), pagesize_);

			key_obj.get_uint64(&tmp);
			printf(" (%lu, %u)", tmp, value_obj);

		}
		puts("");
	} else {

		printf("[INNER: %u key(s)][leftbytes:%u][prev:%u][next:%u] ->", bpt->knum, bpt->leftbytes, bpt->prev, bpt->next);

		pgn = (pgno_t *)((char *)cpage + pagesize_ - sizeof(pgno_t));
		printf(" %u", *pgn);
		for (i = 0; i < bpt->knum; i++) {

//			kt = (KeyT *)((char *)cpage + off[i]);
//			pgn = (pgno_t *)((char *)kt - sizeof(pgno_t));

			keyserial_.load(key_obj, (char *)cpage + off[i], pagesize_);
			pgn = (pgno_t *)((char *)cpage + off[i] - sizeof(pgno_t));

			key_obj.get_uint64(&tmp);
			printf(" <%lu> %u", tmp, *pgn);
		}
		puts("");
	}

	return RET_SUCCESS;
}

template <typename KeyT, typename ValueT, typename KeyCompareT, typename KeySerial, typename ValueSerial>
int BPlusTree<KeyT, ValueT, KeyCompareT, KeySerial, ValueSerial>:: check_key(pgno_t pageno, uint64_t key1, uint64_t key2) {

	int ret = 0;
	char cpage[pagesize_];

	offset_t *off;
	bpt_node_t *bpt;

	uint64_t k1, k2, k3;
	k1 = *(uint64_t *)&key1;
	k2 = *(uint64_t *)&key2;

	ret = disk_->read(pageno, cpage);
	if (Disk::RET_SUCCESS != ret) {
		U_LOG_FATAL("[validate]Reading page from disk err![ret:%d][%s]", ret, Disk::perror(ret));
		return RET_INNER_ERR;
	}

	bpt= (bpt_node_t *)cpage;
	off = (offset_t *)bpt->data;

	for (uint32_t i = 0; i < bpt->knum; i++) {
		k3 = *(uint64_t *)((char *)cpage + off[i]);
		/*
		if (key < key1 || key >= key2) {
			U_LOG_FATAL("[validate]key check err![key1:%lu][key2:%lu][key:%lu]",
				key1, key2, key);
			goto err;
		}
		*/
		if (k3 < k1 || k3 >= k2) {
			U_LOG_FATAL("[validate]key check err![key1:%lu][key2:%lu][key:%lu]",
				k1, k2, k3);
			goto err;
		}
	}
	return 0;
err:
	print_node_info(pageno);
	return -1;
}

template <typename KeyT, typename ValueT, typename KeyCompareT, typename KeySerial, typename ValueSerial>
int BPlusTree<KeyT, ValueT, KeyCompareT, KeySerial, ValueSerial>::validate_node(pgno_t pageno) {
	int ret = 0;
	char cpage[pagesize_];

	offset_t *off;
	pgno_t *pgno;
	bpt_node_t *bpt;

	uint64_t key1, key2;

	ret = disk_->read(pageno, cpage);
	if (Disk::RET_SUCCESS != ret) {
		U_LOG_FATAL("[validate]Reading page from disk err![ret:%d][%s]", ret, Disk::perror(ret));
		return RET_INNER_ERR;
	}

	bpt= (bpt_node_t *)cpage;
	off = (offset_t *)bpt->data;

	if (!validate_leftbytes(cpage)) {
		U_LOG_FATAL("[validate]leftbytes err![leftbytes:%u][knum:%u]", bpt->leftbytes, bpt->knum);
		return -1;
	}

	for (uint32_t i = 1; i < bpt->knum; i++) {
		key1 = *(uint64_t *)((char *)cpage + off[i-1]);
		key2 = *(uint64_t *)((char *)cpage + off[i]);
		if (key1 >= key2) {
			ret = -1;
			goto err;
		}
	}

	if (TREE_LEAF_NODE == bpt->type) {
		return 0;
	} else if (TREE_INNER_NODE == bpt->type) {

		pgno = (pgno_t *)((char *)cpage + pagesize_ - sizeof(pgno_t));

		if (bpt->knum == 0) {
			// if no key
			ret = validate_node(*pgno);
			if (ret < 0) {
				goto err;
			}
			return 0;
		}

		key1 = *(uint64_t *)((char *)pgno - sizeof(uint64_t));

		ret = check_key(*pgno, 0, key1);
		if (ret < 0) {
			U_LOG_FATAL("[validate]check key err![pageno:%u]", *pgno);
			goto err;
		}

		for (uint32_t i = 0; i + 1 < bpt->knum; i++) {
			key1 = *(uint64_t *)((char *)cpage + off[i]);
			key2 = *(uint64_t *)((char *)cpage + off[i+1]);
			pgno = (pgno_t *)((char *)cpage + off[i] - sizeof(pgno_t));

			ret = check_key(*pgno, key1, key2);
			if (ret < 0) {
				U_LOG_FATAL("[validate]check key err![pageno:%u]", *pgno);
				goto err;
			}
		}

		pgno = (pgno_t *)((char *)cpage + off[bpt->knum-1]- sizeof(pgno_t));
		key1 = *(uint64_t *)((char *)cpage + off[bpt->knum-1]);
		ret = check_key(*pgno, key1, INF);
		if (ret < 0) {
			U_LOG_FATAL("[validate]check key err![pageno:%u]", *pgno);
			goto err;
		}

	}
	return ret;
err:
	print_node_info(pageno);
	return ret;
}

template <typename KeyT, typename ValueT, typename KeyCompareT, typename KeySerial, typename ValueSerial>
int BPlusTree<KeyT, ValueT, KeyCompareT, KeySerial, ValueSerial>::validate_whole_tree() {
	int ret = 0;
	ret = validate_node(bpt_meta_->root_pgno);
	if (ret < 0) {
		U_LOG_WARNING("validate err![pageno:%u]", bpt_meta_->root_pgno);
		return -1;
	}
	return ret;
}

#endif  // _BPLUSTREE_DEBUG_

#endif	// _BPLUSTREE_H_
