/*
 * index_btree_reader_mt.h
 *
 *  Created on: Jul 5, 2018
 *      Author: wyd
 */

#ifndef INDEX_BTREE_READER_MT_H_
#define INDEX_BTREE_READER_MT_H_

#include "index_btree/index_btree_reader.h"
#include <gpp.h>
#include <tbb/concurrent_queue.h>


struct SearchTask {
	std::vector<int> task_id;
	int index_level;
	int64_t search_offset;
};

typedef tbb::concurrent_queue<SearchTask> TaskQueue;
typedef tbb::concurrent_queue<int> TaskNumQueue;
//typedef tbb::concurrent_vector<std::vector<int64_t> > ResultArray;


class ResultNode {
public:
	ResultNode();
	~ResultNode();

	bool AddChild(int children_num);
	ResultNode *GetChild(int child_id);
	inline int GetChildNum() {
		return children_num_;
	}
	void RecordResult(const std::vector<int64_t> &result);
	void CopyResult(std::vector<int64_t> *p_result_arr);

	void PrintNodeId();

private:
	ResultNode *children_;
	int children_num_;
	std::vector<int64_t> result_;
	std::vector<int> node_id_;
};


class ResultPool {
public:
	ResultPool();
	~ResultPool();
	bool Init();

	bool AddChild(const std::vector<int> &task_id, int children_num);
	bool AddRoot();
	ResultNode *FindNode(const std::vector<int> &task_id);
	ResultNode *FindRoot();
	void CopyAllResult(std::vector<int64_t> *p_result_arr);

private:
	ResultNode *root_;
};


class IndexBTreeReaderMT : public IndexBTreeReader {
public:
	IndexBTreeReaderMT(const IndexAttr &index_attr, HeadInfo *head_info);
	virtual ~IndexBTreeReaderMT();

	virtual bool Open();
	virtual void Close();
	virtual bool GetValidTraces(const RowFilterByKey &row_filter, std::vector<int64_t> *valid_trace_arr);
	bool SearchBTree(int index_level, int64_t begin_off);

	void SetPara(TaskQueue *task_queue, TaskNumQueue *task_num_queue, GPP::Semaphore *task_num_sem,
			std::vector<RuleArr> *p_rule_table, ResultPool *result_pool);
	bool MallocBlockBuf();

	bool GetOneTask(SearchTask &task);
	void RecordUnfinishedTask(int task_num);
	void FinishOneTask(int index_level);

private:

	virtual char *GetBlockBufP(int tier_i);
	virtual void RecordSearchResult(int index_level, const std::vector<int64_t> &result_arr);

	void PushOneTask(const SearchTask &task);
	bool IsLeafLevel(int index_level);

	int index_level_num_;
	TaskQueue *task_queue_;
	TaskNumQueue *task_num_queue_;
	GPP::Semaphore *task_num_sem_;
	std::vector<RuleArr> *p_rule_table_;

	ResultPool *result_pool_;
	char *block_buf_;

	std::vector<int> cur_task_id_;
};

class SearchTreeThread : public GPP::Thread {
public:
	SearchTreeThread(IndexBTreeReaderMT *index_reader);
	virtual ~SearchTreeThread();

	virtual void execute();

private:

	IndexBTreeReaderMT *index_reader_;

};

#endif /* INDEX_BTREE_READER_MT_H_ */
