/*
 * selector_btree_mt.cpp
 *
 *  Created on: Jul 5, 2018
 *      Author: wyd
 */


#include "index_btree_mt/selector_btree_mt.h"
#include "index_btree_mt/index_btree_reader_mt.h"
#include "index_btree/index_btree.h"
#include "util/util.h"
#include "util/util_bitmap.h"
#include "util/util_log.h"
#include <sys/sysinfo.h>

#define SIZE_PER_TASK	64 //MB

SelectorBTreeMT::SelectorBTreeMT(HeadInfo *head_info) :
	SelectorBTree(head_info){

	thread_num_ = get_nprocs();
	active_ = new GPP::Active<int>(thread_num_);

}
SelectorBTreeMT::~SelectorBTreeMT() {
	if(NULL != active_) {
		delete active_;
		active_ = NULL;
	}
}

IndexBTreeReader *SelectorBTreeMT::OpenBTreeReader(const IndexAttr &index_attr, HeadInfo *head_info) {
	IndexBTreeReader *btree_reader = new IndexBTreeReaderMT(index_attr, head_info);
	btree_reader->Open();
	return btree_reader;
}


bool SelectorBTreeMT::GetAllTracesByOrder(const IndexAttr &index_attr, SharedPtr<Bitmap> bit_map, std::vector<int64_t> *p_valid_trace_arr) {

	struct timeval t_begin, t_end;
	gettimeofday(&t_begin, NULL);

	tbb::concurrent_queue<IndexBTreeReader*> reader_queue;
	for(int i = 0; i < thread_num_; ++i) {
		IndexBTreeReader *reader = OpenBTreeReader(index_attr, head_info_);
		reader_queue.push(reader);
	}

	int size_per_task = SIZE_PER_TASK << 20;
	int trace_num_per_task = size_per_task / sizeof(int64_t);
	int task_num = (total_trace_num_ + trace_num_per_task - 1) / trace_num_per_task;

	std::vector<std::vector<int64_t> > task_result_arr;
	GPP::Semaphore *sem = new GPP::Semaphore();
	int64_t begin = 0, end = 0;
	for(int i = 0; i < task_num; ++i) {
		std::vector<int64_t> new_arr;
		task_result_arr.push_back(new_arr);
	}

	for(int i = 0; i < task_num; ++i) {
		end = begin + trace_num_per_task;
		end = end > total_trace_num_ ? total_trace_num_ : end;
		IndexReadTask *task = new IndexReadTask(bit_map, begin, end, &(task_result_arr[i]), &reader_queue, sem);
		active_->send(task);

		begin = end;
	}

	sem->acquire(task_num);
	delete sem;

	p_valid_trace_arr->clear();
	for(int i = 0; i < task_num; ++i) {
		int task_trace_num = task_result_arr[i].size();
		for(int trace_i = 0; trace_i < task_trace_num; ++ trace_i) {
			p_valid_trace_arr->push_back(task_result_arr[i][trace_i]);
		}
	}

	IndexBTreeReader *reader;
	while(reader_queue.try_pop(reader)) {
		reader->Close();
		delete reader;
	}

	gettimeofday(&t_end, NULL);
	double read_index_as_order_time = CALTIME(t_begin, t_end);
	printf("Read index as order time : %f s\n", read_index_as_order_time);

	return true;
}


IndexReadTask::IndexReadTask(SharedPtr<Bitmap> bit_map, int64_t begin, int64_t end, std::vector<int64_t> *result,
		tbb::concurrent_queue<IndexBTreeReader*> *reader_queue, GPP::Semaphore *sem) :
	bit_map_(bit_map), begin_(begin), end_(end), result_(result), reader_queue_(reader_queue), sem_(sem){

}
IndexReadTask::~IndexReadTask() {

}

int IndexReadTask::execute() {
	IndexBTreeReader *reader;
	int retry_times = 3;
	while(!reader_queue_->try_pop(reader)) {
		usleep(1);
		sched_yield();
		retry_times --;
		if(retry_times <= 0) {
			Err("Get BTree reader error\n");
			return -1;
		}
	}

//	printf("Get one task, begin = %lld, end = %lld\n", begin_, end_);
	reader->Seek(begin_);

	if(NULL != bit_map_) {
		for(int64_t i = begin_; i < end_; ++i) {
			int64_t trace_id = reader->Next() & 0x7fffffffffffffff;
			if(bit_map_->IsSet(trace_id)) {
				result_->push_back(trace_id);
			}
		}
	} else {
		for(int64_t i = begin_; i < end_; ++i) {
			result_->push_back(reader->Next() & 0x7fffffffffffffff);
		}
	}

	reader_queue_->push(reader);
	sem_->release(1);
	return 0;
}
