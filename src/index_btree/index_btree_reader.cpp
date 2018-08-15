/*
 * index_btree_reader.cpp
 *
 *  Created on: May 23, 2018
 *      Author: wyd
 */

#include "index_btree/index_btree_reader.h"
#include "head_data.h"
#include "index_btree/index_btree_data_struct.h"
#include "index.h"
#include "util/util_string.h"
#include "util/util.h"
#include "util/util_log.h"
#include "job_para.h"
#include <sys/time.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>

#define CHECK		if(NULL == trace_id_buf_) {\
		printf("Trace ID BUF ERROR HERE : file : %s, line : %d\n", __FILE__, __LINE__);fflush(stdout);\
		exit(-3);\
	}


IndexBTreeReader::IndexBTreeReader(const IndexAttr &index_attr, HeadInfo *head_info) :
	index_attr_(index_attr), head_info_(head_info){

	index_type_ = INDEX_TYPE_BTREE;
	trace_id_buf_ = NULL;

}
IndexBTreeReader::~IndexBTreeReader() {
	if(NULL != trace_id_buf_) {
		delete trace_id_buf_;
		trace_id_buf_ = NULL;
	}

	for(int i = 0; i < block_buf_arr_.size(); ++i) {
		if(block_buf_arr_[i] != NULL) {
			delete block_buf_arr_[i];
			block_buf_arr_[i] = NULL;
		}
	}
}

void IndexBTreeReader::InitStatistic() {
	call_read_trace_id_num_ = 0;
	call_load_block_num_ = 0;

	load_block_size_ = 0;

	read_trace_id_time_ = 0.0;
	load_block_time_ = 0.0;
	read_trace_time1_ = 0.0;
	read_trace_time2_ = 0.0;
	total_read_trace_num_ = 0;
	binary_search_call_num_ = 0;
	binary_search_time_ = 0.0;
	record_result_time_ = 0.0;
}

float IndexBTreeReader::GetTotalReadSize() {
	float total_trace_id_size = 1.0 * total_read_trace_num_ * sizeof(int64_t) / 1024 / 1024;
	float total_block_size = 1.0 * load_block_size_ / 1024 / 1024;
	float total_read_size = total_trace_id_size + total_block_size;

	return total_read_size;
}

float IndexBTreeReader::GetTotalReadTime() {
	return (read_trace_id_time_ + load_block_time_);
}

float IndexBTreeReader::GetLoadBlockSize() {
	float total_block_size = 1.0 * load_block_size_ / 1024 / 1024;
	return total_block_size;
}
float IndexBTreeReader::GetLoadBlockTime() {
	return load_block_time_;
}
float IndexBTreeReader::GetReadTraceSize() {
	float total_trace_id_size = 1.0 * total_read_trace_num_ * sizeof(int64_t) / 1024 / 1024;
	return total_trace_id_size;
}
float IndexBTreeReader::GetReadTraceTime() {
	return read_trace_id_time_;
}

void IndexBTreeReader::PrintStatistic() {
	printf("Call read trace id num : %d\n", call_read_trace_id_num_);
	printf("Read trace id time : %f s\n", read_trace_id_time_);
//	printf("time1 : %f s, time2 : %f s\n", read_trace_time1_, read_trace_time2_);
	printf("Total read trace id num : %lld\n", total_read_trace_num_);
	float total_trace_id_size = 1.0 * total_read_trace_num_ * sizeof(int64_t) / 1024 / 1024;
	printf("Total read trace id size : %.5f MB\n", total_trace_id_size);
	float speed = 1.0 * total_trace_id_size / read_trace_id_time_;
	printf("Total read trace speed : %.5f MB/s\n", speed);

	printf("Call load block num : %d\n", call_load_block_num_);
	printf("Load block time : %f s\n", load_block_time_);
	printf("Total load block size : %.5f MB\n", 1.0 * load_block_size_ / 1024 / 1024);
	speed = 1.0 * load_block_size_ / 1024 / 1024 / load_block_time_;
	printf("Total load block speed : %.5f MB/s\n", speed);
}

bool IndexBTreeReader::OpenFiles() {

    index_dir_ = Index::GetIndexFullDir(index_attr_, INDEX_TYPE_BTREE);

    std::string fname_base = index_dir_ + "/index";
    std::string fname_meta = fname_base + ".meta";

    fd_meta_ = open(fname_meta.c_str(), O_RDONLY);
    if(fd_meta_ < 0) {
    	printf("Open fd_meta_ error : %s\n", strerror(errno));
    	return false;
    }
    if(sizeof(total_trace_num_) != read(fd_meta_, &total_trace_num_, sizeof(total_trace_num_))) {
    	close(fd_meta_);
    	return false;
    }

    if(sizeof(lv0_block_size_) != read(fd_meta_, &lv0_block_size_, sizeof(lv0_block_size_))) {
    	close(fd_meta_);
    	return false;
    }

	std::string trace_id_fname = fname_base + "_traceid_lv" + ToString(index_attr_.keypos_arr.size() - 1);
	fd_sequencial_read_ = open(trace_id_fname.c_str(), O_RDONLY);
    if(fd_sequencial_read_ < 0) {
    	printf("Open fd_seq error : %s\n", strerror(errno));
        return false;
    }

	for(int i = 0; i < index_attr_.keypos_arr.size(); ++i) {
		std::string btree_fname = fname_base + "_lv" + ToString(i);
		int fd_btree = open(btree_fname.c_str(), O_RDONLY);
	    if(fd_btree < 0) {
	    	printf("Open %s error : %s\n", btree_fname.c_str(), strerror(errno));
	        return false;
	    }
	    lv_btree_fd_arr_.push_back(fd_btree);


		std::string trace_id_fname = fname_base + "_traceid_lv" + ToString(i);
		int fd_trace_id = open(trace_id_fname.c_str(), O_RDONLY);
	    if(fd_trace_id < 0) {
	    	printf("Open %s error : %s\n", trace_id_fname.c_str(), strerror(errno));
	        return false;
	    }
	    lv_trace_id_fd_arr_.push_back(fd_trace_id);
	}

    return true;
}

bool IndexBTreeReader::MallocTraceIDBuf() {
	buffered_trace_id_num_ = BUFFER_TRACE_ID_NUM;
	trace_id_buf_ = new int64_t[buffered_trace_id_num_];

	buf_begin_id_ = -1;
	buf_end_id_ = -1;
	cur_read_id_ = 0;

	return true;
}

void IndexBTreeReader::SetBlockSize() {
	tree_block_size_ = ToInt(JobPara::GetPara("BlockSize"));
}



bool IndexBTreeReader::Open() {

	if(!OpenFiles()) {
		printf("OpenFiles ERROR\n");
		return false;
	}

	if(!MallocTraceIDBuf()) {
		printf("Malloc trace id buf error\n");
		return false;
	}
	SetBlockSize();
	for(int i = 0; i < index_attr_.keypos_arr.size(); ++i) {
		ValidOffArr valid_off_arr;
		valid_off_table_.push_back(valid_off_arr);
	}
	return true;
}

void IndexBTreeReader::CloseFiles() {
	close(fd_meta_);
	close(fd_sequencial_read_);

	for(int i = 0; i < index_attr_.keypos_arr.size(); ++i) {
		close(lv_btree_fd_arr_[i]);
		close(lv_trace_id_fd_arr_[i]);
	}

}

void IndexBTreeReader::Close() {
	CloseFiles();

	if(NULL != trace_id_buf_) {
		delete trace_id_buf_;
		trace_id_buf_ = NULL;
	}

	for(int i = 0; i < block_buf_arr_.size(); ++i) {
		if(block_buf_arr_[i] != NULL) {
			delete block_buf_arr_[i];
			block_buf_arr_[i] = NULL;
		}
	}
}


int64_t IndexBTreeReader::Next() {
	if(cur_read_id_ >= buf_end_id_ || cur_read_id_ < buf_begin_id_) {

		struct timeval t_begin, t_end;
		gettimeofday(&t_begin, NULL);

		int64_t left_trace_num = total_trace_num_ - cur_read_id_;
		int64_t read_len = buffered_trace_id_num_ < left_trace_num ? buffered_trace_id_num_ : left_trace_num;
		lseek(fd_sequencial_read_, cur_read_id_ * sizeof(int64_t), SEEK_SET);
		int read_size = read_len * sizeof(int64_t);
		int ret = read(fd_sequencial_read_, trace_id_buf_, read_size);
		if(ret != read_size) {
			Err("Read next trace id error\n");
			return -1;
		}

		buf_begin_id_ = cur_read_id_;
		buf_end_id_ = buf_begin_id_ + read_len;

		gettimeofday(&t_end, NULL);
		double time = CALTIME(t_begin, t_end);
		read_trace_id_time_ += time;
		total_read_trace_num_ += read_len;
	}
	int64_t ret = trace_id_buf_[cur_read_id_ - buf_begin_id_];
	cur_read_id_ ++;
	return ret;
}
bool IndexBTreeReader::Seek(int64_t trace_id) {
	cur_read_id_ = trace_id;
	return true;
}


int64_t IndexBTreeReader::GetTotalTraceNum() {
	return total_trace_num_;
}


char *IndexBTreeReader::GetBlockBufP(int tier_i) {

	while(tier_i >= block_buf_arr_.size()) {
		char *p = new char[tree_block_size_];
		block_buf_arr_.push_back(p);
	}
	return block_buf_arr_[tier_i];
}

bool IndexBTreeReader::LoadBlock(int index_level, int64_t offset, char *buf) {

	struct timeval t_begin, t_end;
	gettimeofday(&t_begin, NULL);
	/*
	 * TODO cache previous loaded block.
	 */
	lseek(lv_btree_fd_arr_[index_level], offset, SEEK_SET);
	int ret = read(lv_btree_fd_arr_[index_level], buf, cur_tree_block_size_);
	if(ret < cur_tree_block_size_) {
		Err("Load Block error! ret = %d, real_block_size = %d, Error is : %s, fd = %d, buf = %x\n",
				ret, cur_tree_block_size_, strerror(errno), lv_btree_fd_arr_[index_level], buf);
		return false;
	}

	gettimeofday(&t_end, NULL);
	double time = CALTIME(t_begin, t_end);
	call_load_block_num_ ++;
	load_block_size_ += ret;
	load_block_time_ += time;

	return true;
}


/*
 * find the last element that <= start value
 */
int IndexBTreeReader::BinarySearchStartValue(char *buf, int ele_num_in_buf,
		int ele_size, KeyType key_type, char *start_p) {

#if 0

	char *p = buf + (ele_num_in_buf - 1) * ele_size;
	int i = ele_num_in_buf - 1;

	for(; i >= 0; --i) {
		if(!KeyLessThan(start_p, p, key_type))
			break;
		p -= ele_size;
	}

	if(i < 0)
		return 0;
	return i;
#else

	struct timeval t_begin, t_end;
	gettimeofday(&t_begin, NULL);

	int left_i = 0, right_i = ele_num_in_buf - 1;
	while(left_i <= right_i) {
		int mid_i = (left_i + right_i) >> 1;
		char *mid_p = buf + mid_i * ele_size;
		if(!KeyLessThan(start_p, mid_p, key_type)) {
			left_i = mid_i + 1;
		} else {
			right_i = mid_i - 1;
		}
	}

	int ret;

	if(left_i >= ele_num_in_buf) {  //TODO
		ret = left_i - 1;
//		return left_i - 1;
	} else {
		char *left_p = buf + left_i * ele_size;
		if(KeyLessThan(start_p, left_p, key_type)) {
			ret = left_i - 1 < 0 ? 0 : left_i - 1;
//			return left_i - 1 < 0 ? 0 : left_i - 1;
		} else {
			ret = left_i;
//			return left_i;
		}
	}

	gettimeofday(&t_end, NULL);
	float time = CALTIME(t_begin, t_end);

	binary_search_time_ += time;
	binary_search_call_num_ ++;

	return ret;
#endif
}


bool IndexBTreeReader::SearchBTree(int index_level, int64_t tree_begin_off, const RuleArr &rule_arr) { //TODO deal with inc and group/tolerance


	struct timeval t_begin, t_end;
	gettimeofday(&t_begin, NULL);

//	uint16_t cur_tree_block_size = tree_begin_off >> 47;
//	tree_begin_off &= 0x
	cur_tree_block_size_ = GetBlockSize(tree_begin_off);
//	printf("cur_tree_block_size_ = %d\n", cur_tree_block_size_);
	tree_begin_off = EraseBlockSize(tree_begin_off);

//	printf("cur_tree_block_Size = %d, tree_begin_off = %lld\n", cur_tree_block_size_, tree_begin_off);


	TreeHead tree_head;

	int key_id = index_attr_.keypos_arr[index_level];
	int key_size = head_info_->GetKeySize(key_id);
	KeyType key_type = head_info_->GetKeyType(key_id);
	int offset_size = sizeof(int64_t);

	tmp_result_of_one_tree_.clear();

	if(rule_arr.empty()) {
		char *block_p = GetBlockBufP(0);

		if(!LoadBlock(index_level, tree_begin_off, block_p)) {
			return false;
		}
		tree_head.LoadFrom(block_p);

		ReadValidTraceID(index_level, tree_head.trace_id_begin_off, tree_head.trace_id_num, 0, tree_head.trace_id_num - 1);
	} else {
		for(int rule_i = 0; rule_i < rule_arr.size(); ++rule_i) { //TODO redundent read block
				int64_t block_begin_off = 0;
				int block_id = 0;
				int higher_tier_block_num = 0;
				int id_in_block = 0;
				int full_ele_num_in_block = 0;
				int ele_size = 0;
				char *block_p = NULL;
				int ele_block_num_this_tier;
				int64_t tree_root_begin_off = tree_begin_off;

				char *start_p = rule_arr[rule_i].GetStartValueP(), *end_p = rule_arr[rule_i].GetEndValueP();

//				printf("Search BTree of level : %d, tree_begin_off = %lld, key_id = %d, rule[%d].key_id = %d, start = %d, end = %d\n",
//						index_level, tree_begin_off, key_id, rule_i, rule_arr[rule_i].GetKeyID(), *(int*)start_p, *(int*)end_p);

				block_p = GetBlockBufP(0);

				if(!LoadBlock(index_level, tree_begin_off, block_p)) {
					return false;
				}
				tree_head.LoadFrom(block_p);
//				printf("index_level : %d, extra_root = %d, tier_num = %d, begin_id_off = %d, trace_id_num = %d\n",
//						index_level, cur_tree_head_.extra_root, cur_tree_head_.tier_block_num_arr.size(), cur_tree_head_.trace_id_begin_off
//						,cur_tree_head_.trace_id_num);
				BlockHead block_head;
				int valid_block_size = cur_tree_block_size_ - block_head.GetHeadSize();
				for(int tier_i = 0; tier_i < tree_head.tier_block_num_arr.size(); ++tier_i) {
					if(tier_i == 0) {
						if(!tree_head.extra_root) {

//							printf("tier_i = %d, block_id = %d, id_in_block = %d, tree_head_size = %d, block_begin_off = %lld\n",
//									tier_i, block_id, id_in_block, cur_tree_head_.GetHeadSize(), cur_tree_begin_off_ + cur_tree_head_.GetHeadSize());

							block_p = block_p + tree_head.GetHeadSize();

						} else {
							tree_root_begin_off = tree_begin_off + cur_tree_block_size_;
							if(!LoadBlock(index_level, tree_root_begin_off, block_p)) {
								Err("Load Block error!\n");
								return false;
							}
						}
					} else {
						block_id = block_id * full_ele_num_in_block + id_in_block;
						block_begin_off = tree_root_begin_off + higher_tier_block_num * cur_tree_block_size_ + block_id * cur_tree_block_size_;

//						printf("tier_i = %d, block_id = %d, id_in_block = %d, block_begin_off = %lld\n", tier_i, block_id, id_in_block, block_begin_off);

						block_p = GetBlockBufP(tier_i);
						if(!LoadBlock(index_level, block_begin_off, block_p)) {
							Err("Load Block error!\n");
							return false;
						}
					}

					ele_block_num_this_tier = tree_head.tier_block_num_arr[tier_i];

					block_head.LoadFrom(block_p);

					if(tier_i == tree_head.tier_block_num_arr.size() - 1) {
						ele_size = key_size + offset_size;
					} else {
						ele_size = key_size;
					}
					full_ele_num_in_block = valid_block_size / ele_size;
					char *search_p = block_p + block_head.GetHeadSize();

//					printf("tier_i = %d, ele_num = %d, ele_size = %d, search_p = %d, start_p = %d\n",
//							tier_i, block_head.ele_num, ele_size, *(int*)search_p, *(int*)start_p);
					id_in_block = BinarySearchStartValue(search_p, block_head.ele_num, ele_size, key_type, start_p);
//					printf("After search, id_in_block = %d\n", id_in_block);


					higher_tier_block_num += ele_block_num_this_tier;

					if(id_in_block < 0)
						break;
				}
				if(id_in_block < 0) {
					continue; //continue for next rule
				}

				int total_block_num = higher_tier_block_num;

				/*
				 * the front search always find the element <= start value. so if the leaf element < start value,
				 * the next element which is bigger than start value is exactly what we want.
				 * when we find the next element, it may exceed the block range, so we may have to load the next block.
				 * if the next block exceeds the last block, it means that there is no valid element in this tree. So we continue
				 * to the next rule.
				 */
				char *ele_p = block_p + block_head.GetHeadSize() + ele_size * id_in_block;
				if(KeyLessThan(ele_p, start_p, key_type)) {
//					printf("start_p = %d, ele_p = %d, ELE_P ++\n", *(int*)ele_p, *(int*)start_p);
					id_in_block ++;
					ele_p += ele_size;

					if(id_in_block >= block_head.ele_num) {
						block_id ++;
						block_begin_off += cur_tree_block_size_;
						if(block_id >= ele_block_num_this_tier) { // the start value is bigger than any element in this tree
//							printf("block_id = %d, block_num_this_tier = %d, EXCEEDED! continue to next rule\n", block_id, ele_block_num_this_tier);
							continue;//continue to do next rule_Arr
						} else {
							if(!LoadBlock(index_level, block_begin_off, block_p)) {
								Err("LoadBlock error\n");
								return false;
							}
							block_head.LoadFrom(block_p);
							id_in_block = 0;
							ele_p = block_p + block_head.GetHeadSize();
						}
					}
				}
//				printf("start_value = %d, ele_find = %d, ele_before = %d, ele_after = %d\n",
//						*(int*)start_p, *(int*)ele_p, *(int*)(ele_p - ele_size), *(int*)(ele_p + ele_size));
				int64_t begin_trace_id_idx, end_trace_id_idx; //the relative id in the trace id buf
				memcpy(&begin_trace_id_idx, ele_p + key_size, sizeof(int64_t));
				end_trace_id_idx = -1;
				while(!KeyLessThan(end_p, ele_p, key_type)) {
					memcpy(&end_trace_id_idx, ele_p + key_size, sizeof(int64_t));

//					printf("value_p = %d, end_p = %d, end_idx = %lld, block_id = %d, id_in_block = %d, ele_num = %d\n",
//							*(int*)ele_p, *(int*)end_p, end_trace_id_idx, block_id, id_in_block, block_head.ele_num);

					id_in_block ++;
					ele_p += ele_size;

					if(id_in_block >= block_head.ele_num) {
						block_id ++;
						block_begin_off += cur_tree_block_size_;
						if(block_id >= ele_block_num_this_tier) { // get to the end
//							printf("block_id = %d, block_num_this_tier = %d, Exceeded!\n", block_id, ele_block_num_this_tier);
							break; //break from search end id;
						} else {
							if(!LoadBlock(index_level, block_begin_off, block_p)) {
								Err("LoadBlock error\n");
								return false;
							}
							block_head.LoadFrom(block_p);

							id_in_block = 0;
							ele_p = block_p + block_head.GetHeadSize();
						}
					}
				}
				if(end_trace_id_idx < begin_trace_id_idx) {
//					printf("the end trace idx is smaller than the begin trace idx\n");
					continue; //continue to do next rule_Arr
				}
				ReadValidTraceID(index_level, tree_head.trace_id_begin_off, tree_head.trace_id_num, begin_trace_id_idx, end_trace_id_idx);
			}
	}

//	printf("record search result, result num is : %lld\n", tmp_result_of_one_tree_.size());fflush(stdout);
	RecordSearchResult(index_level, tmp_result_of_one_tree_);

	gettimeofday(&t_end, NULL);
	float time = CALTIME(t_begin, t_end);
	search_time_ += time;

	return true;
}

void IndexBTreeReader::RecordSearchResult(int index_level, const std::vector<int64_t> &result_arr) {
//	valid_off_table_[index_level].push_back(result);

	struct timeval t_begin, t_end;
	gettimeofday(&t_begin, NULL);

	for(int64_t i = 0; i < result_arr.size(); ++i) {
		valid_off_table_[index_level].push_back(result_arr[i]);
	}


	gettimeofday(&t_end, NULL);
	float time = CALTIME(t_begin, t_end);
	record_result_time_ += time;
}

bool IndexBTreeReader::ReadValidTraceID(int index_level, int64_t trace_id_begin_off, int64_t trace_num_this_tree,
		int64_t begin_trace_id_idx, int64_t end_trace_id_idx) {

//	printf("ReadValidTraceID, trace_num_in_tree = %d, begin_idx = %d, end_idx = %d\n",
//			trace_num_this_tree, begin_trace_id_idx, end_trace_id_idx);

	struct timeval t_begin, t_end;
	struct timeval t_b, t_e;
	gettimeofday(&t_begin, NULL);
	double time1 = 0.0, time2 = 0.0;

	int64_t read_trace_num = 0;

	int64_t left_num = end_trace_id_idx - begin_trace_id_idx + 1;
	int read_num = 0;
	int64_t begin_offset = trace_id_begin_off + begin_trace_id_idx * sizeof(int64_t);
	lseek(lv_trace_id_fd_arr_[index_level], begin_offset, SEEK_SET);
	while(left_num > 0) {
		read_num = buffered_trace_id_num_ < left_num ? buffered_trace_id_num_ : left_num;
		int read_size = read_num * sizeof(int64_t);
//		printf("cur_level : %d, begin off of this tree: %lld, trace_num of this tree : %lld,"
//				"begin trace id : %lld, end trace id : %lld, read num : %d\n",
//				index_level, trace_id_begin_off, trace_num_this_tree, begin_trace_id_idx, end_trace_id_idx, read_num);

		gettimeofday(&t_b, NULL);

		int read_ret = read(lv_trace_id_fd_arr_[index_level], trace_id_buf_, read_size);
		if(read_ret != read_size) {
			Err("Read error : %s, errno = %d, read_ret = %d, read_size = %d, trace_id_buf_ = %x\n",
					strerror(errno), errno, read_ret, read_size, trace_id_buf_);
			exit(-1);
		}
		gettimeofday(&t_e, NULL);
		double time_tmp = CALTIME(t_b,t_e);
		time1 += time_tmp;
		left_num -= read_num;

		for(int i = 0; i < read_num; ++i) {
//			valid_off_table_[index_level].push_back(trace_id_buf_[i] & 0x7fffffffffffffff);
//			RecordSearchResult(index_level, trace_id_buf_[i] & 0x7fffffffffffffff);

			tmp_result_of_one_tree_.push_back(EraseTailFlag(trace_id_buf_[i]));
		}
		read_trace_num += read_num;
	}

	if(trace_id_buf_[read_num - 1] >= 0) {
		left_num = trace_num_this_tree - 1 - end_trace_id_idx;

		while(left_num > 0) {
			read_num = buffered_trace_id_num_ < left_num ? buffered_trace_id_num_ : left_num;
			int read_size = read_num * sizeof(int64_t);

//			printf("read num = %d, left_num = %d\n", read_num, left_num);
			gettimeofday(&t_b, NULL);

			int read_ret = read(lv_trace_id_fd_arr_[index_level], trace_id_buf_, read_size);
			if(read_ret != read_size) {
				Err("Read error : %s, errno = %d, read_ret = %d, read_size = %d, trace_id_buf_ = %x\n",
						strerror(errno), errno, read_ret, read_size, trace_id_buf_);
				exit(-1);
			}

			gettimeofday(&t_e, NULL);
			double time_tmp = CALTIME(t_b,t_e);
			time2 += time_tmp;

			left_num -= read_num;

			int i = 0;
			for(; i < read_num; ++i) {
//				printf("push back : %lld\n", trace_id_buf_[i] & 0x7fffffffffffffff);
//				valid_off_table_[index_level].push_back(trace_id_buf_[i] & 0x7fffffffffffffff);
//				RecordSearchResult(index_level, trace_id_buf_[i] & 0x7fffffffffffffff);
				tmp_result_of_one_tree_.push_back(EraseTailFlag(trace_id_buf_[i]));
				if(trace_id_buf_[i] < 0) {
//					printf("i = %d, END OF VALID TRACE, trace id = %lld\n", i, trace_id_buf_[i] & 0x7fffffffffffffff);
					break;
				}
			}

			read_trace_num += read_num;
			if(i < read_num && trace_id_buf_[i] < 0)
				break;
		}
	}

	gettimeofday(&t_end, NULL);
	double time = CALTIME(t_begin, t_end);
	call_read_trace_id_num_ ++;
	read_trace_id_time_ += time;
	total_read_trace_num_ += read_trace_num;
	read_trace_time1_ += time1;
	read_trace_time2_ += time2;

	return true;
}


bool IndexBTreeReader::LoadValidTraces(int index_level, const RuleArr &rule_arr) {

	float time = 0;

	if(index_level == 0) {

		int64_t tree_begin_offset = 0;
		AddBlockSize(tree_begin_offset, lv0_block_size_);

		if(!SearchBTree(index_level, tree_begin_offset, rule_arr)) {
			Err("Search BTree error!\n");
			return false;
		}
	} else {

		struct timeval t_begin, t_end;
		gettimeofday(&t_begin,NULL);

		int64_t size = valid_off_table_[index_level- 1].size();
		ValidOffArr &tmp_arr = valid_off_table_[index_level- 1];
		for(int64_t i = 0; i < size; ++i) {
			if(!SearchBTree(index_level, tmp_arr[i], rule_arr)) {
				Err("Search BTree error!\n");
				return false;
			}
		}

		gettimeofday(&t_end,NULL);
		time += CALTIME(t_begin, t_end);
	}

	printf("time load = %f\n", time);
	return true;
}

std::vector<RuleArr> IndexBTreeReader::GetRuleTable(const RowFilterByKey &row_filter) {

	struct timeval t_begin, t_end;
	gettimeofday(&t_begin,NULL);

	std::vector<int> rule_key_id_arr = row_filter.GetKeyIDArr();
	std::vector<RuleArr> rule_table;
	int rule_key_num = rule_key_id_arr.size();
	for(int i = 0; i < index_attr_.keypos_arr.size(); ++i) {
		if(i < rule_key_num) {
			rule_table.push_back(row_filter.GetFilterRuleOf(rule_key_id_arr[i]));
		} else {
			RuleArr empty_rule_arr;
			rule_table.push_back(empty_rule_arr);
		}
	}

	gettimeofday(&t_end,NULL);
	float time = CALTIME(t_begin, t_end);
	printf("time get rule table = %f\n", time);

	return rule_table;
}

bool IndexBTreeReader::GetValidTraces(const RowFilterByKey &row_filter, std::vector<int64_t> *valid_trace_arr) {

	struct timeval t_begin, t_end;
	gettimeofday(&t_begin,NULL);

	std::vector<RuleArr> rule_table = GetRuleTable(row_filter);
	for(int i = 0; i < index_attr_.keypos_arr.size(); ++i) {
		LoadValidTraces(i, rule_table[i]);
	}

	gettimeofday(&t_end,NULL);
	float time = CALTIME(t_begin, t_end);
	printf("time1 = %f\n", time);

	gettimeofday(&t_begin,NULL);

	int level = index_attr_.keypos_arr.size() - 1;

	*valid_trace_arr = valid_off_table_[level];

	gettimeofday(&t_end,NULL);
	time = CALTIME(t_begin, t_end);
	printf("time2 = %f\n", time);

	printf("SearchTime = %f\n", search_time_);

	return true;
}


int IndexBTreeReader::call_read_trace_id_num_ = 0, IndexBTreeReader::call_load_block_num_ = 0;

int64_t IndexBTreeReader::load_block_size_ = 0;

float IndexBTreeReader::read_trace_id_time_ = 0.0, IndexBTreeReader::load_block_time_ = 0.0;
float IndexBTreeReader::read_trace_time1_ = 0.0, IndexBTreeReader::read_trace_time2_ = 0.0;
int64_t IndexBTreeReader::total_read_trace_num_ = 0;
int64_t IndexBTreeReader::binary_search_call_num_ = 0;
float IndexBTreeReader::binary_search_time_ = 0, IndexBTreeReader::record_result_time_ = 0;
float IndexBTreeReader::search_time_ = 0;






