/*
 * index_btree_reader_sort_mt.h
 *
 *  Created on: Aug 12, 2018
 *      Author: wangyida
 */

#ifndef INDEX_BTREE_READER_SORT_MT_H_
#define INDEX_BTREE_READER_SORT_MT_H_

#include "index_btree_sort/index_btree_reader_sort.h"
#include <gpp.h>
#include <tbb/concurrent_queue.h>


struct SearchTaskSort {
	std::vector<int> task_id;
	int index_level;
//	int64_t search_offset;
	IndexElement index_ele;
};

typedef tbb::concurrent_queue<SearchTaskSort> TaskQueueSort;
typedef tbb::concurrent_queue<int> TaskNumQueueSort;


class ResultNodeSort {
public:
	ResultNodeSort();
	~ResultNodeSort();

	bool AddChild(int children_num);
	ResultNodeSort *GetChild(int child_id);
	inline int GetChildNum() {
		return children_num_;
	}
	void RecordResult(const std::vector<IndexElement> &result);
	void CopyResult(std::vector<IndexElement> *p_result_arr);

	void PrintNodeId();

private:
	ResultNodeSort *children_;
	int children_num_;
	std::vector<IndexElement> result_;
	std::vector<int> node_id_;
};


class ResultPoolSort {
public:
	ResultPoolSort();
	~ResultPoolSort();
	bool Init();

	bool AddChild(const std::vector<int> &task_id, int children_num);
	bool AddRoot();
	ResultNodeSort *FindNode(const std::vector<int> &task_id);
	ResultNodeSort *FindRoot();
	void CopyAllResult(std::vector<IndexElement> *p_result_arr);

private:
	ResultNodeSort *root_;
};



class IndexBTreeReaderSortMT : public IndexBTreeReaderSort {
public:
	IndexBTreeReaderSortMT(const IndexAttr &index_attr, HeadInfo *head_info);
	virtual ~IndexBTreeReaderSortMT();

	virtual bool Open();
	virtual void Close();

	virtual bool GetValidIndex(const RowFilterByKey &row_filter, std::vector<IndexElement> *index_ele_arr);
	bool SearchBTree(int index_level, const IndexElement &tree_ele);

	void SetPara(TaskQueueSort *task_queue, TaskNumQueueSort *task_num_queue, GPP::Semaphore *task_num_sem,
			std::vector<RuleArr> *p_rule_table, ResultPoolSort *result_pool);
	bool MallocBlockBuf();

	bool GetOneTask(SearchTaskSort &task);
	void PushOneTask(const SearchTaskSort &task);
	void RecordUnfinishedTask(int task_num);
	void FinishOneTask(int index_level);

private:

	virtual char *GetBlockBufP(int tier_i);
	virtual void RecordSearchResult(int index_level, const std::vector<IndexElement> &ele_head_arr, const std::vector<int64_t> &result_arr);

	bool IsLeafLevel(int index_level);

	int index_level_num_;
	TaskQueueSort *task_queue_;
	TaskNumQueueSort *task_num_queue_;
	GPP::Semaphore *task_num_sem_;
	std::vector<RuleArr> *p_rule_table_;

	ResultPoolSort *result_pool_;
	char *block_buf_;

	std::vector<int> cur_task_id_;

};


class SearchTreeThreadSort : public GPP::Thread {
public:
	SearchTreeThreadSort(IndexBTreeReaderSortMT *index_reader_sort);
	virtual ~SearchTreeThreadSort();

	virtual void execute();

private:

	IndexBTreeReaderSortMT *index_reader_sort_;

};



#endif /* INDEX_BTREE_READER_SORT_MT_H_ */
