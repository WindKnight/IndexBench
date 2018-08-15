/*
 * selector_btree_mt.h
 *
 *  Created on: Jul 5, 2018
 *      Author: wyd
 */

#ifndef SELECTOR_BTREE_MT_H_
#define SELECTOR_BTREE_MT_H_


#include "index_btree/selector_btree.h"
#include <gpp.h>


class IndexReadTask : public GPP::Active<int>::Message {
public:
	IndexReadTask(SharedPtr<Bitmap> bit_map, int64_t begin, int64_t end, std::vector<int64_t> *result,
			tbb::concurrent_queue<IndexBTreeReader*> *reader_queue, GPP::Semaphore *sem);
	virtual ~IndexReadTask();

	virtual int execute();

private:
	SharedPtr<Bitmap> bit_map_;
	int64_t begin_, end_;
	std::vector<int64_t> *result_;
	tbb::concurrent_queue<IndexBTreeReader*> *reader_queue_;
	GPP::Semaphore *sem_;
};


class SelectorBTreeMT : public SelectorBTree {
public:
	SelectorBTreeMT(HeadInfo *head_info);
	virtual ~SelectorBTreeMT();

private:
	virtual bool GetAllTracesByOrder(const IndexAttr &index_attr, SharedPtr<Bitmap> bit_map, std::vector<int64_t> *p_valid_trace_arr);
	virtual IndexBTreeReader *OpenBTreeReader(const IndexAttr &index_attr, HeadInfo *head_info);

	GPP::Active<int> *active_;
	int thread_num_;
};


#endif /* SELECTOR_BTREE_MT_H_ */
