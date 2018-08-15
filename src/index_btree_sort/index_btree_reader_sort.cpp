/*
 * index_btree_reader_sort.cpp
 *
 *  Created on: Aug 1, 2018
 *      Author: wyd
 */

#include "index_btree_sort/index_btree_reader_sort.h"
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

IndexBTreeReaderSort::IndexBTreeReaderSort(const IndexAttr &index_attr, HeadInfo *head_info) :
	IndexBTreeReader(index_attr, head_info){

	int index_key_size = 0;
	for(int i = 0; i < index_attr.keypos_arr.size(); ++i) {
		int key_id = index_attr.keypos_arr[i];
		int key_size = head_info_->GetKeySize(key_id);
		index_key_size += key_size;
		index_key_size_arr_.push_back(index_key_size);

		ValidEleArr ele_arr;
		valid_ele_table_.push_back(ele_arr);
	}

}
IndexBTreeReaderSort::~IndexBTreeReaderSort() {

}

bool IndexBTreeReaderSort::SearchBTree(int index_level, const IndexElement &tree_ele, const RuleArr &rule_arr) {

	int64_t tree_begin_off = tree_ele.trace_id;
	cur_tree_block_size_ = GetBlockSize(tree_begin_off);
	tree_begin_off = EraseBlockSize(tree_begin_off);

//	printf("index level = %d, cur_tree_block_Size = %d, tree_begin_off = %lld\n", index_level, cur_tree_block_size_, tree_begin_off);

	TreeHead tree_head;

	int key_id = index_attr_.keypos_arr[index_level];
	int key_size = head_info_->GetKeySize(key_id);
	KeyType key_type = head_info_->GetKeyType(key_id);
	int offset_size = sizeof(int64_t);

	tmp_result_of_one_tree_.clear();

	std::vector<IndexElement> ele_head_arr;

	if(rule_arr.empty()) {

		char *block_p = GetBlockBufP(0);

		if(!LoadBlock(index_level, tree_begin_off, block_p)) {
			return false;
		}
		tree_head.LoadFrom(block_p);

		int64_t tree_root_begin_off = tree_begin_off;
		if(tree_head.extra_root) {
			tree_root_begin_off += cur_tree_block_size_;
		}
		int64_t block_begin_off = 0;
		int tier_num = tree_head.tier_block_num_arr.size();

		if(tier_num <= 1) {

			if(tree_head.extra_root) {

				block_begin_off = tree_root_begin_off;

				if(!LoadBlock(index_level, block_begin_off, block_p)) {
					Err("Load Block error!\n");
					return false;
				}
			} else {
				block_p = block_p + tree_head.GetHeadSize();
			}
		} else {

			int higher_tier_block_num = 0;
			for(int tier_i = 0; tier_i < tier_num - 1; ++tier_i) {
				higher_tier_block_num += tree_head.tier_block_num_arr[tier_i];
			}

			block_begin_off = tree_root_begin_off + higher_tier_block_num * cur_tree_block_size_;

			if(!LoadBlock(index_level, block_begin_off, block_p)) {
				Err("Load Block error!\n");
				return false;
			}
		}

		BlockHead block_head;
		block_head.LoadFrom(block_p);
		char *ele_p = block_p + block_head.GetHeadSize();
		int64_t begin_trace_id_idx, end_trace_id_idx; //the relative id in the trace id buf
		memcpy(&begin_trace_id_idx, ele_p + key_size, sizeof(int64_t));
		end_trace_id_idx = -1;

		int ele_size = key_size + offset_size;
		int id_in_block = 0, block_id = 0;

		while(1) {
			memcpy(&end_trace_id_idx, ele_p + key_size, sizeof(int64_t));
			IndexElement tmp_ele;
			tmp_ele.Malloc(index_key_size_arr_[index_level]);

			if(tree_ele.size_ > 0) {
				tmp_ele.CopyFrom(tree_ele);
			}
			memcpy(tmp_ele.key_buf + tree_ele.size_, ele_p, key_size);
			ele_head_arr.push_back(tmp_ele);

			id_in_block ++;
			ele_p += ele_size;

			if(id_in_block >= block_head.ele_num) {
				block_id ++;
				block_begin_off += cur_tree_block_size_;
				if(block_id >= tree_head.tier_block_num_arr[tier_num - 1]) { // get to the end
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

		ReadValidTraceID(index_level, tree_head.trace_id_begin_off, tree_head.trace_id_num, begin_trace_id_idx, end_trace_id_idx);

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
					IndexElement tmp_ele;
					tmp_ele.Malloc(index_key_size_arr_[index_level]);

					if(tree_ele.size_ > 0) {
						tmp_ele.CopyFrom(tree_ele);
					}
					memcpy(tmp_ele.key_buf + tree_ele.size_, ele_p, key_size);
					ele_head_arr.push_back(tmp_ele);
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

//	printf("After search of level : %d, ele size is : %d, ele arr is : \n", index_level, ele_head_arr[0].size_);
//	for(int i = 0; i < ele_head_arr.size(); ++i) {
//		int *p = (int*)ele_head_arr[i].key_buf;
//		printf("ele[%d] = %d\n", i, p[0]);
//	}

//	printf("record search result, result num is : %lld\n", tmp_result_of_one_tree_.size());fflush(stdout);
	RecordSearchResult(index_level, ele_head_arr, tmp_result_of_one_tree_);
	for(int i = 0; i < ele_head_arr.size(); ++i) {
		delete ele_head_arr[i].key_buf;
		ele_head_arr[i].key_buf = NULL;
	}
	return true;
}

bool IndexBTreeReaderSort::ReadValidTraceID(int index_level, int64_t trace_id_begin_off, int64_t trace_num_this_tree,
		int64_t begin_trace_id_idx, int64_t end_trace_id_idx) {

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

			tmp_result_of_one_tree_.push_back(trace_id_buf_[i]);
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
				tmp_result_of_one_tree_.push_back(trace_id_buf_[i]);
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

void IndexBTreeReaderSort::RecordSearchResult(int index_level, const std::vector<IndexElement> &ele_head_arr, const std::vector<int64_t> &result_arr) {
	int ele_head_i = 0;
	int ele_head_num = ele_head_arr.size();

	int64_t result_num = result_arr.size();
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

		valid_ele_table_[index_level].push_back(tmp_ele);
	}
}


bool IndexBTreeReaderSort::LoadValidTraces(int index_level, const RuleArr &rule_arr) {
	if(index_level == 0) {

		IndexElement root_ele;
		root_ele.trace_id = 0;
		AddBlockSize(root_ele.trace_id, lv0_block_size_);

		if(!SearchBTree(index_level, root_ele, rule_arr)) {
			Err("Search BTree error!\n");
			return false;
		}
	} else {
		for(int i = 0; i < valid_ele_table_[index_level- 1].size(); ++i) {
			if(!SearchBTree(index_level, valid_ele_table_[index_level- 1][i], rule_arr)) {
				Err("Search BTree error!\n");
				return false;
			}
		}
	}
	return true;
}



bool IndexBTreeReaderSort::GetValidIndex(const RowFilterByKey &row_filter, std::vector<IndexElement> *index_ele_arr) {
	std::vector<RuleArr> rule_table = GetRuleTable(row_filter);
	for(int i = 0; i < index_attr_.keypos_arr.size(); ++i) {
		LoadValidTraces(i, rule_table[i]);
	}

	int level = index_attr_.keypos_arr.size() - 1;
	*index_ele_arr = valid_ele_table_[level];

	return true;
}
