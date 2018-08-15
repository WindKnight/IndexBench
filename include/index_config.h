/*
 * index_config.h
 *
 *  Created on: May 1, 2018
 *      Author: wyd
 */

#ifndef INDEX_CONFIG_H_
#define INDEX_CONFIG_H_

#include "head_data.h"
#include <string.h>

#define MAX_INDEX_KEY_NUM   5
//#define INDEX_DIR		"/data/wyd/IndexBench/index_data"

struct IndexAttr{
	std::vector<int> keypos_arr;
	std::vector<bool> asc_arr_;
};

struct IndexElement{
	IndexElement() {
		key_buf = NULL;
		size_ = 0;
		trace_id = 0;
	}
	~IndexElement() {

	}
	void Malloc(int size) {
		key_buf = new char[size];
		size_ = size;
	}

	void CopyFrom(const IndexElement &ele) {
		memcpy(key_buf, ele.key_buf, ele.size_);
	}

	char *key_buf;
	int size_;
    int64_t trace_id;
};

#define INDEX_TYPE_ORDER		"index_order"
#define INDEX_TYPE_BTREE		"index_btree"


struct IndexEleCompare {

	IndexEleCompare(HeadInfo *head_info, const IndexAttr &index_attr) :
		head_info_(head_info), index_attr_(index_attr) {
		key_num_ = index_attr_.keypos_arr.size();
	}


	bool operator() (const IndexElement &index1, const IndexElement &index2) const {

		char *p1 = index1.key_buf, *p2 = index2.key_buf;

	    for(int i = 0; i < key_num_; ++i) {

	        int key_id = index_attr_.keypos_arr[i];
	        bool is_asc = index_attr_.asc_arr_[i];
	        KeyType key_type = head_info_->GetKeyType(key_id);

	        if(!KeyEqual(p1, p2, key_type)) {
	        	return (KeyLessThan(p1, p2, key_type)^is_asc) ? false : true;
	        }

	        p1 += head_info_->GetKeySize(key_id);
	        p2 += head_info_->GetKeySize(key_id);
	    }

	    return false;
	}

	HeadInfo *head_info_;
	IndexAttr index_attr_;
	int key_num_;
};


#endif /* INDEX_CONFIG_H_ */
