#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <time.h>
#include <vector>
#include <algorithm>
#include <stdlib.h>
#include <deque>
#include <map>

#define _LRUCACHE_DEBUG_
#define _LRUCACHE_STATISTICS_
#define _BPLUSTREE_DEBUG_

#include "lrucache.h"
#include "disk.h"
#include "bplustree.h"

class TestLRUCache {

public:
	TestLRUCache(pgno_t mcp, pgno_t pgs, uint32_t mhs)
		: maxcachepages_(mcp), pagesize_(pgs), maxhashsize_(mhs), 
			cacheleft_(mcp), lru_(NULL), fake_lru_(NULL), fp_(NULL), fname_(NULL) {}
	~TestLRUCache() {

		if (fp_) fclose(fp_);
		if (lru_) delete lru_;
		if (fake_lru_) free(fake_lru_);

	}

	void set_file(const char *fn) { fname_ = fn; }
	void set_max_value(const uint32_t maxv) { max_value = maxv; }
	void set_optimes(const uint32_t opt) { optimes = opt; }

	int init() {

		lru_ = new LRUCache(maxcachepages_, pagesize_, maxhashsize_);
		if (NULL == lru_) {
			U_LOG_FATAL("instantiate LRUCache class err!");
			return -1;
		}

		lru_->init();

		fake_lru_ = (FAKE *)calloc(maxcachepages_, sizeof(FAKE));
		if (NULL == fake_lru_) {
			U_LOG_FATAL("alloc fake lru space err!");
			return -1;
		}

		ptr = 0;
		if (NULL == fname_) {
			fp_ = stdout;
		} else {
			fp_ = fopen(fname_, "w");
			if (NULL == fp_) {
				U_LOG_FATAL("fopen %s err!", fname_);
				return -1;
			}
		}

		return 0;
	}

	int runtest() {

		int ret = 0, ret2 = 0;
		pgno_t pgno;
		uint32_t value, v_lru, v_fake;

		const char *opcmd[]= {"get", "push"};
		const int optn = sizeof(opcmd) / sizeof(opcmd[0]);
		srand(time(NULL));

		for (uint32_t opt = 0; opt < optimes; opt++) {

			int opflag = rand() % optn;
			switch (opflag) {

				case 0:
					pgno = rand() % maxhashsize_;

					fprintf(fp_, "get value of key[%u]!\t", pgno);

					ret = lru_->get(pgno, &v_lru);
					if (ret < 0) {
						U_LOG_WARNING("lrucache get value err![%s]", LRUCache::perror(ret));
						return -1;
					}
					ret2 = fake_get(pgno, &v_fake);
					if (ret < 0) {
						U_LOG_WARNING("fake lru get value err!");
						return -1;
					}
					
					if (LRUCache::RET_FOUND == ret) {
						if (ret2 == 2) {
							fprintf(fp_, "FAILED! [lru FOUND:%u][fake NOT FOUND]\n", v_lru);
							return -1;
						}
						if (v_lru == v_fake) {
							fprintf(fp_, "BINGO! [value:%u]\n", v_lru);
						} else {
							fprintf(fp_, "FAILED! [lru:%u][fake:%u]\n", v_lru, v_fake);
							return -1;
						}
					} else if (LRUCache::RET_NOTFOUND == ret) {
						if (ret2 == 1) {
							fprintf(fp_, "FAILED! [lru NOT FOUND][fake FOUND:%u]\n", v_fake);
							return -1;
						} else {
							fprintf(fp_, "BINGO! [NOT FOUND]\n");
						}
					}
					break;

				case 1:

					pgno = rand() % maxhashsize_;
					value = rand() % max_value;

					fprintf(fp_, "push key/value[%u/%u] pair into cache!\n", pgno, value);

					ret = lru_->push(pgno, &value);
					if (LRUCache::RET_SUCCESS != ret) {
						U_LOG_WARNING("lrucache push key/value err![%s]", LRUCache::perror(ret));
						return -1;
					}

					ret = fake_push(pgno, &value);
					if (0 != ret) {
						U_LOG_WARNING("fake lru push key/value err!");
						return -1;
					}
					break;

			}
		}

		return 0;
	}

	int unittest(int casenum) {
		int ret = 0;
		for (int ca = 1; ca <=casenum; ca++) {
			fprintf(fp_, "********* Run test case %d ********************\n", ca);
			ret = runtest();
			if (ret < 0) {
				break;
			}
			fprintf(fp_, "********* End test case %d ********************\n\n", ca);
		}
		return 0;
	}

private:

	typedef struct _FAKE {
		pgno_t key;
		uint32_t value;
	}FAKE;

	pgno_t maxcachepages_;
	pgno_t pagesize_;
	uint32_t maxhashsize_;
	uint32_t cacheleft_;
	LRUCache *lru_;
	FAKE *fake_lru_;
	FILE *fp_;
	uint32_t ptr;
	uint32_t max_value;
	uint32_t optimes;
	const char *fname_;

	int fake_get(pgno_t pageno, void *data) {
		
		uint32_t i, j;
		bool found = false;
		for (i = 0; i < ptr; i++) {
			if (fake_lru_[i].key == pageno) {
				memcpy(data, &fake_lru_[i].value, sizeof(uint32_t));
				found = true;
				j = i;
				break;
			}
		}
		
		if (found) {
			FAKE tmp = fake_lru_[j];
			for (i = j; i > 0; i--) {
				fake_lru_[i] = fake_lru_[i-1];
			}
			fake_lru_[0] = tmp;
			return 1;
		} 

		return 2;
	}

	int fake_push(pgno_t pageno, void *data) {

		uint32_t i, j;
		bool found = false;
		for (i = 0; i < ptr; i++) {
			if (fake_lru_[i].key == pageno) {
				found = true;
				j = i;
				break;
			}
		}

		if (found) {
			FAKE tmp = fake_lru_[j];
			for (i = j; i > 0; i--) {
				fake_lru_[i] = fake_lru_[i-1];
			}
			memcpy(&tmp.value, data, sizeof(uint32_t));
			fake_lru_[0] = tmp;
			return 0;
		}

		if (ptr >= maxcachepages_) {
			ptr--;
		}
		
		for (i = ptr; i > 0; i--) {
			fake_lru_[i] = fake_lru_[i-1];
		}
		memcpy(&fake_lru_[0].key, &pageno, sizeof(pgno_t));
		memcpy(&fake_lru_[0].value, data, sizeof(uint32_t));
		++ptr;

		return 0;
	}
};

class TestBPlusTree {
public:

	typedef uint64_t KeyT;
	typedef uint32_t ValueT;

	TestBPlusTree(pgno_t pgs, uint32_t ln, pgno_t mcp, uint32_t mhs) :
		pagesize_(pgs), level_num_(ln), maxhashsize_(mhs), maxcachepages_(mcp), 
			bpt_fname_(NULL), log_fname_(NULL), bpt_(NULL), fp_(NULL)  {}
	~TestBPlusTree() {

		if (bpt_) { delete bpt_; }
	}

	void set_log_file(const char *file) { log_fname_ = file; }
	void set_bpt_file(const char *file) { bpt_fname_ = file; }
	void set_max_key(const KeyT max_key) { max_key_ = max_key; };
	void set_max_value(const ValueT max_value) { max_value_ = max_value; };
	void set_optimes(const uint32_t opt) { optimes_ = opt; }

	int init() {

		int ret = 0;
		bpt_ = new BPlusTree<KeyT, ValueT, comp_>();

		if (NULL == bpt_) {
			U_LOG_FATAL("instantiate BPlusTree class err!");
			return -1;
		}

		if (NULL == bpt_fname_) {
			U_LOG_FATAL("Invalid file name! You need set file name first.");
			return -1;
		}

		ret = bpt_->open(bpt_fname_, pagesize_, level_num_, 
			maxcachepages_, maxhashsize_);

		if (BPT::RET_SUCCESS != ret) {
			U_LOG_FATAL("BPlusTree open failed![fname:%s][%s]", 
				bpt_fname_, BPT::perror(ret));
			return -1;
		}

		if (NULL == log_fname_) {
			fp_ = stdout;
		} else {
			fp_ = fopen(log_fname_, "w");
			if (NULL == fp_) {
				U_LOG_FATAL("open log file err!");
				return -1;
			}
		}

		fake_bpt_.clear();

		return 0;
	}

	int runtest() {
		int ret = 0, ret2 = 0;
		KeyT key;
		ValueT value, v_bpt, v_fake;

//		const char *opcmd[] = {"get", "put", "delet", "modify"};
		//const int optn = sizeof(opcmd) / sizeof(opcmd[0]);
		const int optn = 4;

		srand(time(NULL));
		uint64_t off = 0;
		for (int i = 0; i < (int)max_key_; i++) {

			key = rand() % max_key_ + off;
			fprintf(fp_, "[%d]pre put key[%llu].\n", i, key);
			ret = bpt_->put(key, key);
			if (ret < 0) {
				U_LOG_WARNING("bpt pre put key/value err![%s]", BPT::perror(ret));
				return -1;
			}

			ret = fake_put(key, key);
			if (ret < 0) {
				U_LOG_WARNING("fake bpt pre put key/value err![%s]", BPT::perror(ret));
				return -1;
			}
		}

		for (uint32_t opt = 0; opt < optimes_; opt++) {

			int opflag = rand() % optn;
			if (opflag == 0) opflag = 2;
			if (opflag == 3) opflag = 2;
			/*
			int opflag = rand() % 10;
			if (opflag < 2) opflag = 0;
			else if (opflag < 6) opflag = 1;
			else if (opflag < 8) opflag = 2;
			else opflag = 3;
			*/
			fprintf(fp_, "[%u]\t", opt);
			key = rand() % max_key_ + off;
			switch (opflag) {
				case 0:

					fprintf(fp_, "get value![key:%llu]\t\t\t\t\t", key);

					ret = bpt_->get(key, v_bpt);
					if (ret < 0) {
						U_LOG_WARNING("bpt get value err![%s]", BPT::perror(ret));
						goto err;;
					}

					ret2 = fake_get(key, &v_fake);
					if (ret2 < 0) {
						U_LOG_WARNING("fake bpt get value err!");
						goto err;;
					}

					if (BPT::RET_FOUND == ret) {
						if (ret2 == 2) {
							fprintf(fp_, "FAILED! [bpt FOUND:%u][fake NOT FOUND]\n", v_bpt);
							goto err;;
						}
						if (v_bpt == v_fake) {
							fprintf(fp_, "BINGO! [value:%u]\n", v_bpt);
						} else {
							fprintf(fp_, "FAILED! [bpt:%u][fake:%u]\n", v_bpt, v_fake);
							goto err;;
						}
					} else if (BPT::RET_NOTFOUND == ret) {
						if (ret2 == 1) {
							fprintf(fp_, "FAILED! [bpt NOT FOUND][fake FOUND:%u]\n", v_fake);
							goto err;;
						} else {
							fprintf(fp_, "BINGO! [NOT FOUND]\n");
						}
					}
					break;

				case 1:
					value = rand() % max_value_;

					fprintf(fp_, "put key/value![key:%llu][value:%u]\t\t\t\t\t", key, value);

					ret = bpt_->put(key, value);
					if (ret < 0) {
						U_LOG_WARNING("bpt put key/value err![key/value:%lu/%u][%s]",
							key, value, BPT::perror(ret));
						goto err;;
					}

					ret2 = fake_put(key, value);
					if (ret2 < 0) {
						U_LOG_WARNING("fake bpt put key/value err!");
						goto err;;
					}

					if (BPT::RET_DUPDATA == ret) {
						if (ret2 == 0) {
							fprintf(fp_, "FAILED! [bpt:DUPDATA][fake:SUCC]\n");
							goto err;;
						} else if (ret2 == 1){
							fprintf(fp_, "BINGO! [DUPDATA]\n");
						}
					} else {
						if (ret2 == 0) {
							fprintf(fp_, "BINGO! [SUCC]\n");
						} else if (ret2 == 1) {
							fprintf(fp_, "FAILED! [bpt:SUCC][fake:DUPDATA]\n");
							goto err;;
						}
					}
					break;
				case 2:

					fprintf(fp_, "delet key![key:%llu]\t\t\t\t\t", key);

					ret = bpt_->delet(key);
					if (ret < 0) {
						U_LOG_WARNING("bpt delet key err![key:%lu]", key);
						goto err;;
					}

					ret2 = fake_delet(key);
					if (ret2 < 0) {
						U_LOG_WARNING("fake bpt delet key err![key:%lu]", key);
						goto err;;
					}

					if (BPT::RET_NOTFOUND == ret) {
						if (ret2 == 1) {
							fprintf(fp_, "BINGO! [NOT FOUND]\n");
						} else {
							fprintf(fp_, "FAILED! [bpt:NOT FOUND][fake:SUCC]\n");
							goto err;;
						}
					} else {
						if (ret2 == 0) {
							fprintf(fp_, "BINGO! [SUCC]\n");
						} else {
							fprintf(fp_, "FAILED! [bpt:SUCC][fake:NOT FOUND]\n");
							goto err;;
						}
					}
					break;
				case 3:
					value = rand() % max_value_;

					fprintf(fp_, "update key/value![key:%llu][value:%u]\t", key, value);

					ret = bpt_->update(key, value);
					if (ret < 0) {
						U_LOG_WARNING("bpt update key/value err![key:%lu][value:%u]", key, value);
						goto err;;
					}

					ret2 = fake_upate(key, value);
					if (ret2 < 0) {
						U_LOG_WARNING("fake bpt update key/value err![key:%lu][value:%u]", key, value);
						goto err;;
					}

					if (BPT::RET_SUCCESS == ret) {
						if (ret2 == 1) {
							fprintf(fp_, "FAILED! [bpt:SUCC][fake:NOT FOUND]\n");
							goto err;;
						} else {
							fprintf(fp_, "BINGO! [SUCC]\n");
						}
					} else {
						if (ret2 == 1) {
							fprintf(fp_, "BINGO! [NOT FOUND]\n");
						} else {
							fprintf(fp_, "FAILED! [bpt:NOT FOUND][fake:SUCC]\n");
							goto err;;
						}
					}

			}

	//		fprintf(fp_, "[map_size:%d]\n", fake_bpt_.size());
			ret = bpt_->validate_whole_tree();
			if (ret < 0) {
				U_LOG_FATAL("[validate]validate whole tree err!");
				return -1;
			}
/*
			for (int i = 0; i < 30; i++) {
				bpt_->print_node_info(i);
			}
			puts("\n\n");
*/
		}

		return 0;
err:

		return -1;
	}

	int unittest(int casenum) {
		int ret = 0;
		for (int ca = 1; ca <=casenum; ca++) {
			fprintf(fp_, "********* Run test case %d ********************\n", ca);
			ret = runtest();
			if (ret < 0) {
				fprintf(fp_, "************** test case %d failed ***********\n", ca);
				break;
			}
			fprintf(fp_, "********* End test case %d ********************\n\n", ca);
		}

		if (0 == ret) {
			fprintf(fp_, "************** Unittest Success!! *****************\n");
		} else {
			fprintf(fp_, "************** Unittest Failed !! *****************\n");
		}

		return 0;
	}

private:

	struct comp_ {
		bool operator() (const KeyT &a, const KeyT &b) {
			return a < b;
		}
	};

	typedef BPlusTree<KeyT, ValueT, comp_> BPT;

	pgno_t pagesize_;
	uint32_t level_num_;
	uint32_t maxhashsize_;
	pgno_t maxcachepages_;
	const char *bpt_fname_;
	const char *log_fname_;
	BPT *bpt_;
	FILE *fp_;

	KeyT max_key_;
	ValueT max_value_;
	uint32_t optimes_;

	std::map<KeyT, ValueT> fake_bpt_;

	int fake_get(const KeyT key, void *data) {
		if (fake_bpt_.find(key) == fake_bpt_.end()) {
			return 2;
		}

		ValueT value = fake_bpt_[key];
		memcpy(data, &value, sizeof(ValueT));
		return 1;
	}

	int fake_put(const KeyT key, const ValueT value) {

		if (fake_bpt_.find(key) != fake_bpt_.end()) {
			return 1;
		} else {
			fake_bpt_[key] = value;
		}
		return 0;
	}

	int fake_delet(const KeyT key) {

		std::map<KeyT, ValueT>::iterator itr = fake_bpt_.find(key);
		if (itr == fake_bpt_.end()) {
			// not found
			return 1;
		} else {
			// delet it
			fake_bpt_.erase(itr);
		}

		return 0;
	}

	int fake_upate(const KeyT key, const ValueT value) {

		if (fake_bpt_.find(key) == fake_bpt_.end()) {
			return 1;
		}

		fake_bpt_[key] = value;
		return 0;
	}
};

void test_LRU() {

	int ret = 0;
	pgno_t maxcachepages = 100;
	pgno_t pagesize = sizeof(uint32_t);
	uint32_t maxhashsize = 1000;

	TestLRUCache *test = new TestLRUCache(maxcachepages, pagesize, maxhashsize);

//	test->set_file("unit_output/lru");
	test->set_max_value(1000);
	test->set_optimes(1000000);

	ret = test->init();
	if (ret < 0) {
		fprintf(stderr, "test lru init failed!\n");
		return;
	}

	ret = test->unittest(100);
	if (ret < 0) {
		fprintf(stderr, "lru unittest err!\n");
		return;
	}

	if (0 == ret) {
		printf("************** Unittest Success!! *****************\n");
	}

	delete test;

}

void test_BPT() {

	int ret = 0;
	pgno_t maxcachepages = 1024;
	pgno_t pagesize = 4096;
	uint32_t maxhashsize = 1024 * 1024;
	uint32_t level_num = 1024;

	TestBPlusTree *test = NULL;
	test = new TestBPlusTree(pagesize, level_num, maxcachepages, maxhashsize);
	if (NULL == test) {
		U_LOG_FATAL("Instantiate BPlusTree class err!");
		return ;
	}

	test->set_bpt_file("unit_output/bpt.dat");
	//test->set_log_file("unit_output/bpt_log");

	test->set_max_key(3000000);
	test->set_max_value(1000000);
	test->set_optimes(600000000);

	ret = test->init();
	if (ret < 0) {
		U_LOG_FATAL("init bpt err!");
		return ;
	}

	ret = test->unittest(1);
	if (ret == 0) {
		printf("Pass Unittest!!!\n");
	}

	delete test;
}

struct comp {
	bool operator() (const uint32_t &a, const uint32_t &b) {
		return a < b;
	}
};

void debug_BPT() {

	int ret = 0;
	pgno_t maxcachepages = 100;
	pgno_t pagesize = 64;
	uint32_t maxhashsize = 1000;
	uint32_t level_num = 10;


	typedef BPlusTree<uint64_t, uint32_t, comp> BPT;

//	const uint32_t v_key[] = {14, 10, 12, 14, 2, 5};
//	const uint32_t v_value[] = {5, 1, 2, 2, 2, 9};

	BPT *bpt = NULL;
	bpt = new BPT();
	if (NULL == bpt) {
		fprintf(stderr, "new bpt err!");
		return;
	}

	ret = bpt->open("unit_output/debug", pagesize, level_num, maxcachepages, maxhashsize);
	if (BPT::RET_SUCCESS != ret) {
		fprintf(stderr, "BPlusTree open failed![%s]", BPT::perror(ret));
		return;
	}

	int optimes;
	uint64_t key;
	uint32_t value;
	char op[100];
	scanf("%d", &optimes);
	while (optimes--) {
		scanf("%s", op);
		if (op[0] == 'p') {
			scanf("%lu %u", &key, &value);
			ret = bpt->put(key, value);
			if (ret < 0) {
				fprintf(stderr, "put err\n");
				break;
			}
		} else if (op[0] == 'd') {
			scanf("%lu", &key);
			ret = bpt->delet(key);
			if (ret < 0) {
				fprintf(stderr, "delet err\n");
				break;
			}
		}
		for (int i = 0; i < 10; i++) {
			bpt->print_node_info(i);
		}
	}

/*
	for (int key = 0; key < 6; key++) {
		ret = bpt->put(v_key[key], v_value[key]);
		for (int i = 0; i < 10; i++) {
			//bpt->print_node_info(i);
		}
		puts("\n\n");
	}
*/
/*
	ret = bpt->delet(0);
	for (int i = 0; i < 10; i++) {
		bpt->print_node_info(i);
	}
	puts("\n\n");
	
	ret = bpt->update(1, 1000);
	for (int i = 0; i < 10; i++) {
		bpt->print_node_info(i);
	}
	puts("\n\n");

	ret = bpt->put(1, 1);
	for (int i = 0; i < 10; i++) {
		bpt->print_node_info(i);
	}
	puts("\n\n");

	ret = bpt->put(0, 10);
	for (int i = 0; i < 10; i++) {
		bpt->print_node_info(i);
	}
	puts("\n\n");
*/
}

int main() {

	//test_LRU();
	test_BPT();
//	debug_BPT();
	return 0;
}
