/*
 * index_btree_writer.cpp
 *
 *  Created on: May 20, 2018
 *      Author: wyd
 */


#include "index_btree/index_btree_writer.h"

#include <stdio.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include "index.h"
#include "index_btree/index_btree_data_struct.h"
#include "util/util_string.h"
#include "util/util_log.h"
#include "job_para.h"


IndexBTreeWriter::IndexBTreeWriter(const IndexAttr &index_attr, HeadInfo *head_info) :
	index_attr_(index_attr), head_info_(head_info){

	index_type_ = INDEX_TYPE_BTREE;

	int key_offset = 0;
	for(int i = 0; i < index_attr_.keypos_arr.size(); ++i) {
		int key_id = index_attr_.keypos_arr[i];
		int key_size = head_info_->GetKeySize(key_id);
		key_size_arr_.push_back(key_size);
		int index_size = key_size + sizeof(int64_t);
		leaf_index_size_arr_.push_back(index_size);
		key_offset_arr_.push_back(key_offset);
		key_offset += key_size;

		key_type_arr_.push_back(head_info_->GetKeyType(key_id));
	}



}
IndexBTreeWriter::~IndexBTreeWriter() {

}


bool IndexBTreeWriter::OpenWrite() {

	index_dir_ = Index::GetIndexFullDir(index_attr_, index_type_);

    std::string fname_base = index_dir_ + "/index";

    std::string fname_meta = fname_base + ".meta";

    fd_meta_ = open(fname_meta.c_str(), O_WRONLY | O_CREAT | O_TRUNC, ACCESSPERMS);
    if(fd_meta_ < 0) {
    	return false;
    }


	for(int i = 0; i < index_attr_.keypos_arr.size(); ++i) {
		lv_btree_file_off_arr_.push_back(0);
		std::string btree_fname = fname_base + "_lv" + ToString(i);
		int fd_btree = open(btree_fname.c_str(), O_WRONLY | O_CREAT | O_TRUNC, ACCESSPERMS);
	    if(fd_btree < 0) {
	        return false;
	    }

	    lv_btree_fd_arr_.push_back(fd_btree);

	    lv_trace_id_file_off_arr_.push_back(0);
	    std::string trace_id_fname = fname_base + "_traceid_lv" + ToString(i);
	    int fd_traceid = open(trace_id_fname.c_str(), O_WRONLY | O_CREAT | O_TRUNC, ACCESSPERMS);
	    if(fd_traceid < 0) {
	        return false;
	    }
	    lv_trace_id_fd_arr_.push_back(fd_traceid);

	    EleBuf ele_buf;
	    ele_buf_arr_.push_back(ele_buf);
		cur_key_flag_arr_.push_back(NULL);
		trace_num_in_btree_lv_.push_back(0);
		begin_trace_id_in_btree_lv_.push_back(0);
	}

	total_trace_num_ = 0;

	total_trace_id_num_in_buf_ = BUFFER_TRACE_ID_NUM;
	trace_id_buf_ = new int64_t[total_trace_id_num_in_buf_];

	tree_block_size_ = ToInt(JobPara::GetPara("BlockSize"));

//	sizeof_ele_num_ = sizeof(int);
//	sizeof_block_num_ = sizeof(int);
	return true;
}

bool IndexBTreeWriter::WriteOneIndex(const IndexElement &index) {
	int index_level = index_attr_.keypos_arr.size() - 1;
	total_trace_num_++;
	return WriteOneIndex(index, index_level);
}


bool IndexBTreeWriter::HighLevelKeyEqual(char *index_ele_buf, int cur_level) {
	char *p1 = cur_key_flag_arr_[cur_level - 1], *p2 = index_ele_buf;
	for(int i = 0; i < cur_level; ++i) {
		int key_id = index_attr_.keypos_arr[i];
		if(!KeyEqual(p1, p2, head_info_->GetKeyType(key_id))) {
			return false;
		}
		int key_size = head_info_->GetKeySize(key_id);
		p1 += key_size;
		p2 += key_size;
	}
	return true;
}

bool IndexBTreeWriter::WriteOneIndex(const IndexElement &index_ele, int index_level) {
	if(index_level > 0) {
		if(cur_key_flag_arr_[index_level - 1] == NULL) {

			int tmp_size = key_offset_arr_[index_level];
			cur_key_flag_arr_[index_level - 1] = new char[tmp_size];
			memcpy(cur_key_flag_arr_[index_level - 1], index_ele.key_buf, tmp_size);


			/*
			 *a new higher key arrive, write it out
			 */
			IndexElement key_ele_higher_lv;
//			key_ele_higher_lv.key_buf = new char[tmp_size];
			key_ele_higher_lv.Malloc(tmp_size);
			memcpy(key_ele_higher_lv.key_buf, index_ele.key_buf, tmp_size);
			key_ele_higher_lv.trace_id = lv_btree_file_off_arr_[index_level];

			if(!WriteOneIndex(key_ele_higher_lv, index_level - 1)) {
				Err("write index to level : %d eror\n", index_level - 1);
				return false;
			}

		} else {
			if(!HighLevelKeyEqual(index_ele.key_buf, index_level)) {

				/*
				 * build btree in cur index level. update the lv_file_offset_arr_.
				 */
				if(!BuildBTree(index_level)) {
					Err("Build btree of level %d error\n", index_level);
					return false;
				}

				int64_t size = ele_buf_arr_[index_level - 1].size();
//				int64_t mask = cur_tree_block_size_;
//				mask <<= 47;
//				ele_buf_arr_[index_level - 1][size - 1].trace_id_arr[0] |= mask;
				AddBlockSize(ele_buf_arr_[index_level - 1][size - 1].trace_id_arr[0],  cur_tree_block_size_);



				/*
				 * after build this btree, the last element in higher level should store the block size of the just built tree.
				 * because the last element point to this tree.
				 */

				/*
				 *a new higher key arrive, write it out
				 */
				IndexElement key_ele_higher_lv;
				int tmp_size = key_offset_arr_[index_level];
//				key_ele_higher_lv.key_buf = new char[tmp_size];
				key_ele_higher_lv.Malloc(tmp_size);
				memcpy(key_ele_higher_lv.key_buf, index_ele.key_buf, tmp_size);
				key_ele_higher_lv.trace_id = lv_btree_file_off_arr_[index_level];
				if(!WriteOneIndex(key_ele_higher_lv, index_level - 1)) {
					Err("write index to level : %d error\n", index_level - 1);
					return false;
				}

				/*
				 * update higher cur key flag
				 */
				memcpy(cur_key_flag_arr_[index_level - 1], index_ele.key_buf, tmp_size);
				/*
				 * write key to this level.
				 */
			}
		}
	}

	int size = ele_buf_arr_[index_level].size();

	if(!ele_buf_arr_[index_level].empty() &&
			KeyEqual(ele_buf_arr_[index_level][size - 1].key_buf,
					index_ele.key_buf + key_offset_arr_[index_level],
					key_type_arr_[index_level])) {

		ele_buf_arr_[index_level][size - 1].trace_id_arr.push_back(index_ele.trace_id);
	} else {
		IndexBTreeElement key_ele_this_lv;
		key_ele_this_lv.key_buf = new char[key_size_arr_[index_level]];
		memcpy(key_ele_this_lv.key_buf, index_ele.key_buf + key_offset_arr_[index_level], key_size_arr_[index_level]);
		key_ele_this_lv.trace_id_arr.push_back(index_ele.trace_id);

		ele_buf_arr_[index_level].push_back(key_ele_this_lv);
	}

	trace_num_in_btree_lv_[index_level] ++;

	return true;
}
bool IndexBTreeWriter::CloseWrite() {

	uint16_t lv0_block_size;

	for(int level = index_attr_.keypos_arr.size() - 1; level >= 0; --level) { //TODO order is wrong

		if(!ele_buf_arr_[level].empty()) {
			if(!BuildBTree(level)) {
				Err("Build BTree of level : %d error\n", level);
				return false;
			}

			if(level > 0) {
				int64_t size = ele_buf_arr_[level - 1].size();
				int64_t mask = cur_tree_block_size_;
				mask <<= 47;
				ele_buf_arr_[level - 1][size - 1].trace_id_arr[0] |= mask;
			} else {
				lv0_block_size = cur_tree_block_size_;
			}

		}

		if(NULL != cur_key_flag_arr_[level]) {
			delete cur_key_flag_arr_[level];
			cur_key_flag_arr_[level] = NULL;
		}
		close(lv_btree_fd_arr_[level]);
		close(lv_trace_id_fd_arr_[level]);
	}

#if 0
	for(int i = 0; i < index_attr_.keypos_arr.size(); ++i) { //TODO order is wrong
		if(!BuildBTree(i)) {
			Err("Build BTree of level : %d error\n", i);
			return false;
		}

		if(NULL != cur_key_flag_arr_[i]) {
			delete cur_key_flag_arr_[i];
			cur_key_flag_arr_[i] = NULL;
		}
		close(lv_btree_fd_arr_[i]);
		close(lv_trace_id_fd_arr_[i]);
	}

#endif

	int ret = write(fd_meta_, &total_trace_num_, sizeof(total_trace_num_));
	if(ret != sizeof(total_trace_num_)) {
		Err("write meta error\n");
		close(fd_meta_);
	    std::string fname_base = index_dir_ + "/index";
	    std::string fname_meta = fname_base + ".meta";
	    remove(fname_meta.c_str());
	    return false;
	}

	ret = write(fd_meta_, &lv0_block_size, sizeof(lv0_block_size));
	if(ret != sizeof(lv0_block_size)) {
		Err("write meta error\n");
		close(fd_meta_);
	    std::string fname_base = index_dir_ + "/index";
	    std::string fname_meta = fname_base + ".meta";
	    remove(fname_meta.c_str());
	    return false;
	}

	close(fd_meta_);

	if(NULL != trace_id_buf_) {
		delete trace_id_buf_;
		trace_id_buf_ = NULL;
	}
	for(int i = 0; i < block_buf_arr_.size(); ++i) {
		delete block_buf_arr_[i];
		block_buf_arr_[i] = NULL;
	}
	block_buf_arr_.clear();
	block_buf_write_p_arr_.clear();
	tier_write_offset_arr_.clear();

	return true;

}

bool IndexBTreeWriter::WriteOutTraceIDBuf() {

	int write_size = cur_trace_id_in_buf_ < total_trace_id_num_in_buf_ ? cur_trace_id_in_buf_ : total_trace_id_num_in_buf_;
	write_size *= sizeof(trace_id_buf_[0]);

//	printf("cur_trace_id_in_buf_ = %d, index_write_id_ = %lld\n", cur_trace_id_in_buf_, index_write_id_);
//
//	printf("write size = %d, trace_id[0] = %lld, trace_id[0] after or = %lld\n",
//			write_size, trace_id_buf_[1] & 0x7fffffffffffffff, trace_id_buf_[1]);
	int iRet = write(lv_trace_id_fd_arr_[cur_building_index_level_], trace_id_buf_, write_size);
	if(iRet != write_size) {
		Err("index level : %d, write out trace id buffer error\n", cur_building_index_level_);
		return false;
	}
//	trace_id_write_offset_ += write_size;
//	cur_write_offset_ = trace_id_write_offset_;

	return true;
}

bool IndexBTreeWriter::WriteToTraceIDBuf(const std::vector<int64_t> &trace_id_arr) {

	for(int i = 0; i < trace_id_arr.size(); ++i) {
		trace_id_buf_write_p_[cur_trace_id_in_buf_] = trace_id_arr[i];
		if(i == trace_id_arr.size() - 1) {
			AddTailFlag(trace_id_buf_write_p_[cur_trace_id_in_buf_]);
//			trace_id_buf_write_p_[cur_trace_id_in_buf_] |= 0x8000000000000000;
//			printf("index level : %d, before or, trace id is : %llu(%lld), after or, trace id is : %llu(%lld)\n",
//					cur_building_index_level_, trace_id_arr[i], trace_id_arr[i], trace_id_buf_write_p_[cur_trace_id_in_buf_], trace_id_buf_write_p_[cur_trace_id_in_buf_]);
		}
		cur_trace_id_in_buf_++;
		index_write_id_ ++;
//		printf("index_write_id = %d\n", index_write_id_);
		if(cur_trace_id_in_buf_ >= total_trace_id_num_in_buf_ || index_write_id_ >= cur_tree_head_.trace_id_num) {
			WriteOutTraceIDBuf();
			/*
			 * reset trace id buf pointer
			 */
			trace_id_buf_write_p_ = trace_id_buf_;
			cur_trace_id_in_buf_ = 0;
		}
	}
	return true;
}

bool IndexBTreeWriter::WriteOutTierBlock(int tier) {

	if(cur_btree_write_offset_ != tier_write_offset_arr_[tier]) {
		int64_t seek_ret = lseek(lv_btree_fd_arr_[cur_building_index_level_], tier_write_offset_arr_[tier], SEEK_SET);
		if(seek_ret != tier_write_offset_arr_[tier]) {
			printf("LSEEK ERROR, ret = %lld, offset = %lld, error is : %s\n",
					seek_ret, tier_write_offset_arr_[tier], strerror(errno));
		}
	}
	int iRet = write(lv_btree_fd_arr_[cur_building_index_level_], block_buf_arr_[tier], cur_tree_block_size_);
	if(iRet != cur_tree_block_size_) {
		Err("index level : %d, tier : %d, write out tier block error\n", cur_building_index_level_, tier);
		return false;
	}

	tier_write_offset_arr_[tier] += cur_tree_block_size_;
	cur_btree_write_offset_ = tier_write_offset_arr_[tier];

	return true;
}


bool IndexBTreeWriter::WriteToBlock(const IndexBTreeElement &btree_ele, int tier) {

	if(tier == tree_hight_ - 1) {
		memcpy(block_buf_write_p_arr_[tier], btree_ele.key_buf, key_size_arr_[cur_building_index_level_]);
		block_buf_write_p_arr_[tier] += key_size_arr_[cur_building_index_level_];
		memcpy(block_buf_write_p_arr_[tier], &index_write_id_, sizeof(index_write_id_));
		block_buf_write_p_arr_[tier] += sizeof(index_write_id_);

/*		if(cur_building_index_level_ == 1) {
			int tmp = *(int*)btree_ele.key_buf;
			if(tmp >= 1500 && tmp <=3500) {
				printf("key = %d, trace_id = %lld, trace_id_begin_off in write = %lld\n",
						tmp, btree_ele.trace_id_arr[0], trace_id_write_offset_);
			}
		}*/

		if(!WriteToTraceIDBuf(btree_ele.trace_id_arr)) {
			Err("index level : %d, tier : %d, Write to Trace ID Buf ERROR\n", cur_building_index_level_, tier);
			return false;
		}

	} else {
		memcpy(block_buf_write_p_arr_[tier], btree_ele.key_buf, key_size_arr_[cur_building_index_level_]);
		block_buf_write_p_arr_[tier] += key_size_arr_[cur_building_index_level_];
	}

	if(block_write_ele_num_arr_[tier] == 0) {
		guard_ele_arr_[tier] = btree_ele;
		// gurd element is the first element in a block of lower level. It will be write to the
		//buffer of higher level block and used to find lower level block when search trees.

	}
	block_write_ele_num_arr_[tier] ++;

//	printf("tier is : %d, ele is : %d, block ele num = %d, total_ele_num = %d\n",
//			tier, *((int*)btree_ele.key_buf), block_write_ele_num_arr_[tier], block_total_ele_num_arr_[tier]);

	if(block_write_ele_num_arr_[tier] >= block_total_ele_num_arr_[tier] || index_write_id_ >= cur_tree_head_.trace_id_num) {

		BlockHead block_head;
		block_head.ele_num = block_write_ele_num_arr_[tier];

		if(tier == 0 && !cur_tree_head_.extra_root) {

			block_head.SaveTo(block_buf_arr_[tier] + cur_tree_head_.GetHeadSize());

		} else {

			block_head.SaveTo(block_buf_arr_[tier]);
		}

//		printf("write our tier : %d, ele num is %d\n", tier, block_head.ele_num);

		if(!WriteOutTierBlock(tier)) {
			Err("index level : %d, tier : %d, Write out tier block ERROR\n", cur_building_index_level_, tier);
			return false;
		}

		block_write_ele_num_arr_[tier] = 0;
		//reset block buf p;
		block_buf_write_p_arr_[tier] = block_buf_arr_[tier] + block_head.GetHeadSize();
		if(tier > 0) {
//			printf("tier is %d, write to higher tier block, guard_ele is %d\n", tier, *((int*)guard_ele_arr_[tier].key_buf));

			//gurd element is the first element in a block of lower level. It will be write to the
			//buffer of higher level block and used to find lower level block when search trees.
			if(!WriteToBlock(guard_ele_arr_[tier], tier - 1)) {
				Err("index level : %d, tier : %d, write to higher tier block error\n", cur_building_index_level_, tier);
				return false;
			}
		}
	}

	return true;
}

template <class T>
void ReverseArr(std::vector<T> *arr_p) {
	int head = 0, tail = arr_p->size() - 1;

	while(head < tail) {
		T tmp = (*arr_p)[head];
		(*arr_p)[head] = (*arr_p)[tail];
		(*arr_p)[tail] = tmp;

		head ++;
		tail --;
	}

}


bool IndexBTreeWriter::BuildBTree(int index_level) {

//	printf("################Building BTree of Level %d###################\n", index_level);

	cur_building_index_level_ = index_level;
//	trace_num_this_tree_ = trace_num_in_btree_lv_[index_level];

	bool bRet = true;

	BlockHead block_head;
	int block_head_size = block_head.GetHeadSize();

	int valid_block_size = tree_block_size_ - block_head_size;

	EleBuf &ele_buf = ele_buf_arr_[index_level];

	block_total_ele_num_arr_.clear();
	block_write_ele_num_arr_.clear();
	block_num_arr_.clear();

	tree_hight_ = 0;
	int block_num_this_tier = 0, total_block_num = 0, block_num_last_tier = ele_buf.size();
	int root_ele_size = 0;

	do {
		int index_size, index_num_in_block;
		if(tree_hight_ == 0) {
			index_size = leaf_index_size_arr_[index_level];
		}else {
			index_size = key_size_arr_[index_level];
		}

		index_num_in_block = valid_block_size / index_size;
		block_num_this_tier = (block_num_last_tier + index_num_in_block - 1) / index_num_in_block;
		if(block_num_this_tier <= 1) {
			root_ele_size = block_num_last_tier * index_size;
		}
		block_num_last_tier = block_num_this_tier;

		block_total_ele_num_arr_.push_back(index_num_in_block);
		block_num_arr_.push_back(block_num_this_tier);
		total_block_num += block_num_this_tier;
		tree_hight_ ++;
//		printf("index_leve = %d, tree_hight = %d, block_num = %d\n", index_level, tree_hight_, block_num_this_tier);

	} while(block_num_this_tier > 1);

	ReverseArr(&block_total_ele_num_arr_);
	ReverseArr(&block_num_arr_);

	/*
	 * determine whether need a extra block for the root head. if true, the block num of tier 0 and total block num should ++;
	 */

	/*
	 * init tree related parameters
	 */

	int64_t trace_id_size = trace_num_in_btree_lv_[index_level] * sizeof(trace_id_buf_[0]);

	cur_tree_head_.tier_block_num_arr = block_num_arr_;
	int tree_head_size = cur_tree_head_.GetHeadSize();

//	cur_tree_head_.real_block_size = tree_block_size_;
	cur_tree_block_size_ = tree_block_size_;
	int root_real_block_size = root_ele_size + block_head_size + tree_head_size;

	if(root_real_block_size > tree_block_size_) {
		cur_tree_head_.extra_root = 1;

	} else {
		cur_tree_head_.extra_root = 0;
		if(total_block_num <= 1) {
			cur_tree_block_size_ = root_real_block_size;
		}
	}

	int64_t total_block_size = total_block_num * cur_tree_block_size_;

//	printf("Index leve : %d, total_block_size = %lld\n", index_level, total_block_size);

	cur_tree_head_.trace_id_begin_off = lv_trace_id_file_off_arr_[index_level];
	cur_tree_head_.trace_id_num = trace_num_in_btree_lv_[index_level];


//	printf("BUILDING!!! exta_root = %d, begin_offset = %d, trace_num = %d, tree_head size = %d\n",
//			cur_tree_head_.extra_root, cur_tree_head_.trace_id_begin_off, cur_tree_head_.trace_id_num, cur_tree_head_.GetHeadSize());


	/*
	 * New one block buf for each tier
	 */

	if(tree_hight_ > block_buf_arr_.size()) {
		for(int i = block_buf_arr_.size(); i < tree_hight_; ++i) {
			char *p = new char[tree_block_size_];
			block_buf_arr_.push_back(p);

			block_buf_write_p_arr_.push_back(NULL);
			tier_write_offset_arr_.push_back(0);
		}
	}

	/*
	 * if extra root block , write the tree head out first.
	 */

	cur_tree_head_.SaveTo(block_buf_arr_[0]);

	if(cur_tree_head_.extra_root) {

		lseek(lv_btree_fd_arr_[cur_building_index_level_], lv_btree_file_off_arr_[index_level], SEEK_SET);

		int iRet = write(lv_btree_fd_arr_[cur_building_index_level_], block_buf_arr_[0], tree_block_size_); //TODO resize the extra root block
		if(iRet != tree_block_size_) {
			Err("index level : %d, write out tree head error\n", cur_building_index_level_);
			return false;
		}

		lv_btree_file_off_arr_[index_level] += tree_block_size_;
	}


	for(int i = 0; i < tree_hight_; ++i) {
		block_buf_write_p_arr_[i] = block_buf_arr_[i] + block_head_size;
		if(i == 0 && !cur_tree_head_.extra_root) {
			block_buf_write_p_arr_[i] += cur_tree_head_.GetHeadSize();
		}

		int front_block_num = 0;
		for(int j = i - 1; j >= 0; --j) {
			front_block_num += block_num_arr_[j];
		}
		tier_write_offset_arr_[i] = lv_btree_file_off_arr_[index_level] + (int64_t)front_block_num * tree_block_size_;
		block_write_ele_num_arr_.push_back(0);
		IndexBTreeElement btree_ele;
		guard_ele_arr_.push_back(btree_ele);

//		printf("front_block_num of tier : %d is %d\n", i, front_block_num);
	}


//	trace_id_write_offset_ = cur_tree_head_.trace_id_begin_off;
	/*
	 * init trace_id buf parameters
	 */
	trace_id_buf_write_p_ = trace_id_buf_;
	cur_trace_id_in_buf_ = 0;
	index_write_id_ = 0;

	/*
	 * init file write parameters
	 */
	cur_btree_write_offset_ = -1;
//	cur_trace_id_write_offset_ = lv_trace_id_file_off_arr_[index_level];

	for(int64_t i = 0; i < ele_buf.size(); ++i) {
		if(!WriteToBlock(ele_buf[i], tree_hight_ - 1)) {
			Err("index level : %d, Write index element to block error\n", index_level);
			bRet = false;
			goto EXIT;
		}
	}

	lv_btree_file_off_arr_[index_level] += total_block_size;
	lv_trace_id_file_off_arr_[index_level] += trace_id_size;
//	printf("index_level = %d, btree_file_offset = %lld\n",
//			index_level, lv_btree_file_off_arr_[index_level]);
//
//	printf("index_level = %d, ele_buf_arr_ size = %d, trace_id size = %lld, total_block_num = %d, total_block_size = %lld\n",
//			index_level, ele_buf.size(), trace_id_size, total_block_num, total_block_size);

	trace_num_in_btree_lv_[index_level] = 0;

//	printf("################Level %d Building Over###################\n", index_level);

EXIT:

	for(int i = 0; i < ele_buf.size(); ++i) {
		delete ele_buf[i].key_buf;
	}
	ele_buf.clear();
	return bRet;
}


