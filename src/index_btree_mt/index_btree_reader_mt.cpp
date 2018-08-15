/*
 * index_btree_reader_mt.cpp
 *
 *  Created on: Jul 5, 2018
 *      Author: wyd
 */

#include "index_btree_mt/index_btree_reader_mt.h"
#include <sys/sysinfo.h>
#include <sched.h>
#include <time.h>
#include "util/util_log.h"


ResultNode::ResultNode() {
	children_ = NULL;
	children_num_= 0;
}
ResultNode::~ResultNode() {
	if(NULL != children_) {
		delete [] children_;
		children_ = NULL;
	}
}

bool ResultNode::AddChild(int children_num) {

	children_num_ = children_num;
	children_ = new ResultNode[children_num_];
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

ResultNode *ResultNode::GetChild(int child_id) {
	if(child_id >= children_num_) {
		this->PrintNodeId();
		Err("Invalid child id : %d, the children_num of node is : %d\n", child_id ,children_num_);
		return NULL;
	}
	return &(children_[child_id]);
}

void ResultNode::RecordResult(const std::vector<int64_t> &result) {
	result_ = result;
}

void ResultNode::CopyResult(std::vector<int64_t> *p_result_arr) {

	for(int64_t i = 0; i < result_.size(); ++i) {
		p_result_arr->push_back(result_[i]);
	}
}

void ResultNode::PrintNodeId() {
	printf("Node id : ");
	for(int i = 0; i < node_id_.size(); ++i) {
		printf("%d, ", node_id_[i]);
	}
	printf("\n");
}

ResultPool::ResultPool() {
	root_ = NULL;
}
ResultPool::~ResultPool() {
	if(NULL != root_) {
		delete root_;
		root_ = NULL;
	}
}
bool ResultPool::Init() {
	if(NULL != root_) {
		delete root_;
		root_ = NULL;
	}
	root_ = new ResultNode();
	return true;
}

bool ResultPool::AddChild(const std::vector<int> &task_id, int children_num) {
	ResultNode *parent = FindNode(task_id);
	parent->AddChild(children_num);
	return true;
}

bool ResultPool::AddRoot() {
	ResultNode *parent = FindRoot();
	if(!parent->AddChild(1)) {
		Err("Add root child error\n");
		return false;
	}
	return true;
}

ResultNode *ResultPool::FindRoot() {
	return root_;
}

ResultNode *ResultPool::FindNode(const std::vector<int> &task_id) {
	ResultNode *parent = root_;
	for(int index_level = 0; index_level < task_id.size(); ++index_level) {
		ResultNode *node = parent->GetChild(task_id[index_level]);
		parent = node;
	}
	return parent;
}

void ResultPool::CopyAllResult(std::vector<int64_t> *p_result_arr) {
	std::queue<ResultNode*> search_queue;
	search_queue.push(root_);
	while(!search_queue.empty()) {
		ResultNode *node = search_queue.front();
		search_queue.pop();

		int children_num = node->GetChildNum();
		if(children_num > 0) {
			for(int i = 0; i < children_num; ++i) {
				ResultNode *node_child = node->GetChild(i);
				search_queue.push(node_child);
			}
		} else {
			node->CopyResult(p_result_arr);
		}
	}
}

IndexBTreeReaderMT::IndexBTreeReaderMT(const IndexAttr &index_attr, HeadInfo *head_info) :
	IndexBTreeReader(index_attr, head_info){
	block_buf_ = NULL;
	result_pool_ = NULL;
}
IndexBTreeReaderMT::~IndexBTreeReaderMT() {
}

bool IndexBTreeReaderMT::Open() {

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
void IndexBTreeReaderMT::Close() {
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
bool IndexBTreeReaderMT::GetValidTraces(const RowFilterByKey &row_filter, std::vector<int64_t> *valid_trace_arr) {
	 printf("Using MT READER!!!\n");

	 std::vector<RuleArr> rule_table = GetRuleTable(row_filter);
	 TaskQueue task_queue;
	 TaskNumQueue task_num_queue;
	 GPP::Semaphore task_num_sem;
	 ResultPool *result_pool = new ResultPool();
	 if(NULL == result_pool) {
		 printf("New result pool error\n");
		 return false;
	 }
	 if(!result_pool->Init()) {
		 printf("result_pool init error\n");
		 return false;
	 }

	 int cpu_cores = get_nprocs();
	 SearchTreeThread **search_thread_arr = new SearchTreeThread*[cpu_cores];
	 for(int i = 0; i < cpu_cores; ++i) {
		 IndexBTreeReaderMT *index_reader = new IndexBTreeReaderMT(index_attr_, head_info_);
		 index_reader->Open();
		 index_reader->SetPara(&task_queue, &task_num_queue, &task_num_sem, &rule_table, result_pool);
		 if(!index_reader->MallocBlockBuf()) {
			 Err("Btree index reader malloc block buf error\n");
			 return false;
		 }

		 search_thread_arr[i] = new SearchTreeThread(index_reader);
		 search_thread_arr[i]->start();
	 }

	 srand(time(NULL));

	 SearchTask new_task;
	 new_task.index_level = 0;
	 new_task.search_offset = 0;
	 AddBlockSize(new_task.search_offset, lv0_block_size_);
	 new_task.task_id.push_back(0);

	 result_pool->AddRoot();

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
		 SearchTask new_task;
		 new_task.index_level = -1;
		 task_queue.push(new_task);
	 }

	 for(int i = 0; i <cpu_cores; ++i) {
		 search_thread_arr[i]->wait();
		 delete search_thread_arr[i];
	 }

	 result_pool->CopyAllResult(valid_trace_arr);
//	 for(int i = 0; i < result_arr.size(); ++i) {
//		 for(int trace_i  = 0; trace_i < result_arr[i].size(); ++trace_i) {
//			 valid_trace_arr->push_back(result_arr[i][trace_i]);
//		 }
//	 }
	 delete result_pool;
	 printf("Get Valid Traces Over!\n");

	 return true;
}

bool IndexBTreeReaderMT::SearchBTree(int index_level, int64_t begin_off) {
	return IndexBTreeReader::SearchBTree(index_level, begin_off, (*p_rule_table_)[index_level]);
}

void IndexBTreeReaderMT::RecordSearchResult(int index_level, const std::vector<int64_t> &result_arr) {
//	printf("Record Search Result, index_level = %d, result = %lld\n", index_level, result);
	ResultNode *node = result_pool_->FindNode(cur_task_id_);

	if(IsLeafLevel(index_level)) {
		node->RecordResult(result_arr);
//		printf("This is a leaf task, do not generate new task\n");
	} else {
		int64_t num = result_arr.size();
		node->AddChild(num);

		RecordUnfinishedTask(num);

		for(int64_t i = 0; i < num; ++i) {
			SearchTask new_task;
			new_task.index_level = index_level + 1;
			new_task.search_offset = result_arr[i];
			new_task.task_id = cur_task_id_;
			new_task.task_id.push_back(i);
			PushOneTask(new_task);
		}
	}
}

void IndexBTreeReaderMT::SetPara(TaskQueue *task_queue, TaskNumQueue *task_num_queue, GPP::Semaphore *task_num_sem,
		std::vector<RuleArr> *p_rule_table, ResultPool *result_pool) {
	task_queue_ = task_queue;
	task_num_queue_ = task_num_queue;
	task_num_sem_ = task_num_sem;
	p_rule_table_ = p_rule_table;
	result_pool_ = result_pool;
}
bool IndexBTreeReaderMT::MallocBlockBuf() {
	block_buf_ = new char[tree_block_size_];
	if(NULL == block_buf_) {
		return false;
	}
	return true;
}

bool IndexBTreeReaderMT::GetOneTask(SearchTask &task) {
	bool ret = task_queue_->try_pop(task);
	if(ret) {
		cur_task_id_ = task.task_id;
	}
	return ret;
}

void IndexBTreeReaderMT::PushOneTask(const SearchTask &task) {
	task_queue_->push(task);
}

void IndexBTreeReaderMT::RecordUnfinishedTask(int task_num) {
	task_num_queue_->push(task_num);
}

void IndexBTreeReaderMT::FinishOneTask(int index_level) {
	task_num_sem_->release(1);
}

bool IndexBTreeReaderMT::IsLeafLevel(int index_level) {
	return index_level == index_level_num_ - 1;
}


char *IndexBTreeReaderMT::GetBlockBufP(int tier_i) {
	return block_buf_;
}


SearchTreeThread::SearchTreeThread(IndexBTreeReaderMT *index_reader) :
		index_reader_(index_reader){

}
SearchTreeThread::~SearchTreeThread() {
	if(NULL != index_reader_) {
		delete index_reader_;
		index_reader_ = NULL;
	}
}

void SearchTreeThread::execute() {
	while(1) {
		SearchTask task;
		if(index_reader_->GetOneTask(task)) {
			if(task.index_level >= 0) {
//				printf("Get one task, index_level = %d, offset = %lld\n", task.index_level, task.search_offset);

				if(!index_reader_->SearchBTree(task.index_level, task.search_offset)) {
					printf("Search BTree of level : %d, offset : %lld error\n", task.index_level, task.search_offset);
					exit(-1);
				}
				index_reader_->FinishOneTask(task.index_level);
			} else {
				index_reader_->Close();
				printf("Get end task, exit\n");
				break;
			}
		} else {
			usleep(10);
			sched_yield();
		}
	}
}


