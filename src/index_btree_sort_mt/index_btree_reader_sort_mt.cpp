/*
 * index_btree_reader_sort_mt.cpp
 *
 *  Created on: Aug 12, 2018
 *      Author: wangyida
 */

#include "index_btree_sort_mt/index_btree_reader_sort_mt.h"

#include <sys/sysinfo.h>
#include <sched.h>
#include <time.h>
#include "util/util_log.h"

ResultNodeSort::ResultNodeSort() {
	children_ = NULL;
	children_num_= 0;
}
ResultNodeSort::~ResultNodeSort() {
	if(NULL != children_) {
		delete [] children_;
		children_ = NULL;
	}
}

bool ResultNodeSort::AddChild(int children_num) {
	children_num_ = children_num;
	children_ = new ResultNodeSort[children_num_];
	if(NULL == children_) {
		printf("Result node add child error\n");
		return false;
	}
	if(node_id_.empty()) {
		for(int i = 0; i < children_num_; ++i) {
			children_[i].node_id_.push_back(i);
		}
	} else {
		for(int i = 0; i < children_num_; ++i) {
			children_[i].node_id_ = node_id_;
			children_[i].node_id_.push_back(i);
		}
	}

	return true;
}
ResultNodeSort *ResultNodeSort::GetChild(int child_id) {
	if(child_id >= children_num_) {
		this->PrintNodeId();
		Err("Invalid child id : %d, the children_num of node is : %d\n", child_id ,children_num_);
		return NULL;
	}
	return &(children_[child_id]);
}
void ResultNodeSort::RecordResult(const std::vector<IndexElement> &result) {
	result_ = result;
}
void ResultNodeSort::CopyResult(std::vector<IndexElement> *p_result_arr) {
	for(int64_t i = 0; i < result_.size(); ++i) {
		p_result_arr->push_back(result_[i]);
	}
}

void ResultNodeSort::PrintNodeId() {

}

ResultPoolSort::ResultPoolSort() {
	root_ = NULL;
}
ResultPoolSort::~ResultPoolSort() {
	if(NULL != root_) {
		delete root_;
		root_ = NULL;
	}
}
bool ResultPoolSort::Init() {
	if(NULL != root_) {
		delete root_;
		root_ = NULL;
	}
	root_ = new ResultNodeSort();
	return true;
}

bool ResultPoolSort::AddChild(const std::vector<int> &task_id, int children_num) {
	ResultNodeSort *parent = FindNode(task_id);
	parent->AddChild(children_num);
	return true;
}
bool ResultPoolSort::AddRoot() {
	ResultNodeSort *parent = FindRoot();
	if(!parent->AddChild(1)) {
		Err("Add root child error\n");
		return false;
	}
	return true;
}
ResultNodeSort *ResultPoolSort::FindNode(const std::vector<int> &task_id) {
	ResultNodeSort *parent = root_;
	for(int index_level = 0; index_level < task_id.size(); ++index_level) {
		ResultNodeSort *node = parent->GetChild(task_id[index_level]);
		parent = node;
	}
	return parent;
}
ResultNodeSort *ResultPoolSort::FindRoot() {
	return root_;
}
void ResultPoolSort::CopyAllResult(std::vector<IndexElement> *p_result_arr) {
	std::queue<ResultNodeSort*> search_queue;
	search_queue.push(root_);
	while(!search_queue.empty()) {
		ResultNodeSort *node = search_queue.front();
		search_queue.pop();

		int children_num = node->GetChildNum();
		if(children_num > 0) {
			for(int i = 0; i < children_num; ++i) {
				ResultNodeSort *node_child = node->GetChild(i);
				search_queue.push(node_child);
			}
		} else {
			node->CopyResult(p_result_arr);
		}
	}
}


IndexBTreeReaderSortMT::IndexBTreeReaderSortMT(const IndexAttr &index_attr, HeadInfo *head_info) :
		IndexBTreeReaderSort(index_attr, head_info){
	block_buf_ = NULL;
	result_pool_ = NULL;
}
IndexBTreeReaderSortMT::~IndexBTreeReaderSortMT() {

}

bool IndexBTreeReaderSortMT::Open() {
	if(!OpenFiles()) {
		printf("OpenFiles ERROR\n");
		return false;
	}
	if(!MallocTraceIDBuf()) {
		printf("Malloc trace id buf error\n");
		return false;
	}
	SetBlockSize();

	index_level_num_ = index_attr_.keypos_arr.size();

	 return true;
}
void IndexBTreeReaderSortMT::Close() {
	CloseFiles();

	if(NULL != trace_id_buf_) {
		delete trace_id_buf_;
		trace_id_buf_ = NULL;
	}
	if(NULL != block_buf_) {
		delete block_buf_;
		block_buf_ = NULL;
	}
}
bool IndexBTreeReaderSortMT::GetValidIndex(const RowFilterByKey &row_filter, std::vector<IndexElement> *index_ele_arr) {
	 printf("Using SortMT READER!!!\n");

	 std::vector<RuleArr> rule_table = GetRuleTable(row_filter);
	 TaskQueueSort task_queue;
	 TaskNumQueueSort task_num_queue;
	 GPP::Semaphore task_num_sem;
	 ResultPoolSort *result_pool_sort = new ResultPoolSort();
	 if(NULL == result_pool_sort) {
		 printf("New result pool error\n");
		 return false;
	 }
	 if(!result_pool_sort->Init()) {
		 printf("result_pool init error\n");
		 return false;
	 }

	 int cpu_cores = get_nprocs();
	 SearchTreeThreadSort **search_thread_arr = new SearchTreeThreadSort*[cpu_cores];
	 for(int i = 0; i < cpu_cores; ++i) {
		 IndexBTreeReaderSortMT *index_reader_sort = new IndexBTreeReaderSortMT(index_attr_, head_info_);
		 index_reader_sort->Open();
		 index_reader_sort->SetPara(&task_queue, &task_num_queue, &task_num_sem, &rule_table, result_pool_sort);
		 if(!index_reader_sort->MallocBlockBuf()) {
			 Err("Btree index reader malloc block buf error\n");
			 return false;
		 }

		 search_thread_arr[i] = new SearchTreeThreadSort(index_reader_sort);
		 search_thread_arr[i]->start();
	 }

	 srand(time(NULL));

	 SearchTaskSort new_task;
	 new_task.index_level = 0;
//	 new_task.search_offset = 0;
	 new_task.index_ele.trace_id = 0;
	 AddBlockSize(new_task.index_ele.trace_id, lv0_block_size_);
	 new_task.task_id.push_back(0);

	 result_pool_sort->AddRoot();

	 task_num_queue.push(1);
	 task_queue.push(new_task);


	 while(1) {
		 int task_num;
		 if(task_num_queue.try_pop(task_num)) {
			 task_num_sem.acquire(task_num);
		 } else {
			 break;
		 }
	 }

	 for(int i = 0; i < cpu_cores; ++i) {
		 SearchTaskSort new_task;
		 new_task.index_level = -1;
		 task_queue.push(new_task);
	 }

	 for(int i = 0; i <cpu_cores; ++i) {
		 search_thread_arr[i]->wait();
		 delete search_thread_arr[i];
	 }

	 result_pool_sort->CopyAllResult(index_ele_arr);
//	 for(int i = 0; i < result_arr.size(); ++i) {
//		 for(int trace_i  = 0; trace_i < result_arr[i].size(); ++trace_i) {
//			 valid_trace_arr->push_back(result_arr[i][trace_i]);
//		 }
//	 }
	 delete result_pool_sort;
	 printf("Get Valid Traces Over!\n");

	 return true;
}
bool IndexBTreeReaderSortMT::SearchBTree(int index_level, const IndexElement &tree_ele) {
	return IndexBTreeReaderSort::SearchBTree(index_level, tree_ele, (*p_rule_table_)[index_level]);
}

void IndexBTreeReaderSortMT::SetPara(TaskQueueSort *task_queue, TaskNumQueueSort *task_num_queue, GPP::Semaphore *task_num_sem,
		std::vector<RuleArr> *p_rule_table, ResultPoolSort *result_pool) {
	task_queue_ = task_queue;
	task_num_queue_ = task_num_queue;
	task_num_sem_ = task_num_sem;
	p_rule_table_ = p_rule_table;
	result_pool_ = result_pool;
}
bool IndexBTreeReaderSortMT::MallocBlockBuf() {
	block_buf_ = new char[tree_block_size_];
	if(NULL == block_buf_) {
		return false;
	}
	return true;
}

bool IndexBTreeReaderSortMT::GetOneTask(SearchTaskSort &task) {
	bool ret = task_queue_->try_pop(task);
	if(ret) {
		cur_task_id_ = task.task_id;
	}
	return ret;
}
void IndexBTreeReaderSortMT::PushOneTask(const SearchTaskSort &task) {
	task_queue_->push(task);
}
void IndexBTreeReaderSortMT::RecordUnfinishedTask(int task_num) {
	task_num_queue_->push(task_num);
}
void IndexBTreeReaderSortMT::FinishOneTask(int index_level) {
	task_num_sem_->release(1);
}

char *IndexBTreeReaderSortMT::GetBlockBufP(int tier_i) {
	return block_buf_;
}
void IndexBTreeReaderSortMT::RecordSearchResult(int index_level, const std::vector<IndexElement> &ele_head_arr, const std::vector<int64_t> &result_arr) {

	int ele_head_i = 0;
	int ele_head_num = ele_head_arr.size();

	int64_t result_num = result_arr.size();
	std::vector<IndexElement> index_ele_arr;
	for(int64_t i = 0; i < result_num; ++i) {
		IndexElement tmp_ele;
		tmp_ele.Malloc(index_key_size_arr_[index_level]);
		tmp_ele.CopyFrom(ele_head_arr[ele_head_i]);
		tmp_ele.trace_id = EraseTailFlag(result_arr[i]);
		if(result_arr[i] < 0) {
			ele_head_i ++;
			if(ele_head_i >= ele_head_num && i < result_num - 1) {
				Err("Logical ERROR!!!\n");
				exit(-1);
			}
		}

		index_ele_arr.push_back(tmp_ele);
	}

	ResultNodeSort *node_sort = result_pool_->FindNode(cur_task_id_);

	if(IsLeafLevel(index_level)) {
		node_sort->RecordResult(index_ele_arr);
//		printf("This is a leaf task, do not generate new task\n");
	} else {
		int64_t num = index_ele_arr.size();
		node_sort->AddChild(num);

		RecordUnfinishedTask(num);

		for(int64_t i = 0; i < num; ++i) {
			SearchTaskSort new_task;
			new_task.index_level = index_level + 1;
			new_task.index_ele = index_ele_arr[i];
			new_task.task_id = cur_task_id_;
			new_task.task_id.push_back(i);
			PushOneTask(new_task);
		}
	}
}

bool IndexBTreeReaderSortMT::IsLeafLevel(int index_level) {
	return index_level == index_level_num_ - 1;
}


SearchTreeThreadSort::SearchTreeThreadSort(IndexBTreeReaderSortMT *index_reader_sort) :
		index_reader_sort_(index_reader_sort){

}
SearchTreeThreadSort::~SearchTreeThreadSort() {
	if(NULL != index_reader_sort_) {
		delete index_reader_sort_;
		index_reader_sort_ = NULL;
	}
}

void SearchTreeThreadSort::execute() {
	while(1) {
		SearchTaskSort task;
		if(index_reader_sort_->GetOneTask(task)) {
			if(task.index_level >= 0) {
//				printf("Get one task, index_level = %d, offset = %lld\n", task.index_level, task.search_offset);

				if(!index_reader_sort_->SearchBTree(task.index_level, task.index_ele)) {
					printf("Search BTree of level : %d, offset : %lld error\n", task.index_level, task.index_ele.trace_id);
					exit(-1);
				}
				index_reader_sort_->FinishOneTask(task.index_level);
			} else {
				index_reader_sort_->Close();
				printf("Get end task, exit\n");
				break;
			}
		} else {
			usleep(10);
			sched_yield();
		}
	}
}
