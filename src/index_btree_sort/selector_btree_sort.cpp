/*
 * selector_btree_sort.cpp
 *
 *  Created on: Jul 30, 2018
 *      Author: wyd
 */




#include "index_btree_sort/selector_btree_sort.h"

#include "index_btree_sort/index_btree_reader_sort.h"
#include "row_filter_by_key.h"
#include "index_config.h"
#include "order.h"
#include "index.h"
#include "util/util_log.h"
#include "util/util.h"
#include "util/util_bitmap.h"
#include "util/util_scoped_pointer.h"

#include "tbb/parallel_sort.h"


struct Comparator {

	Comparator(HeadInfo *head_info, const IndexAttr &index_attr, Order *p_order) :
		head_info_(head_info), index_attr_(index_attr), p_order_(p_order) {
		key_num_ = index_attr_.keypos_arr.size();

		std::vector<int> key_offset_arr;
		int key_offset = 0;
		for(int i = 0; i < key_num_; ++i) {
			int key_id = index_attr_.keypos_arr[i];
			int key_size = head_info_->GetKeySize(key_id);
			key_offset_arr.push_back(key_offset);
			key_offset += key_size;
		}

		order_key_id_arr_ = p_order_->GetKeyPosArr();
		order_is_asc_arr_ = p_order_->GetAscArr();
		for(int i = 0; i < order_key_id_arr_.size(); ++i) {
			int order_key_id = order_key_id_arr_[i];
			for(int index_key_i = 0; index_key_i < key_num_; ++index_key_i) {
				if(index_attr_.keypos_arr[index_key_i] == order_key_id) {
					order_key_off_arr_.push_back(key_offset_arr[index_key_i]);
				}
			}
		}
	}


	bool operator() (const IndexElement &index1, const IndexElement &index2) const {


		for(int i = 0; i < order_key_id_arr_.size(); ++i) {
			char *p1 = index1.key_buf + order_key_off_arr_[i];
			char *p2 = index2.key_buf + order_key_off_arr_[i];
			int key_id = order_key_id_arr_[i];
			bool is_asc = order_is_asc_arr_[i];
			KeyType key_type = head_info_->GetKeyType(key_id);

	        if(!KeyEqual(p1, p2, key_type)) {
	        	return (KeyLessThan(p1, p2, key_type)^is_asc) ? false : true;
	        }
		}

		return false;

//		char *p1 = index1.key_buf, *p2 = index2.key_buf;
//
//	    for(int i = 0; i < key_num_; ++i) {
//
//	        int key_id = index_attr_.keypos_arr[i];
//	        bool is_asc = index_attr_.asc_arr_[i];
//	        KeyType key_type = head_info_->GetKeyType(key_id);
//
//	        if(!KeyEqual(p1, p2, key_type)) {
//	        	return (KeyLessThan(p1, p2, key_type)^is_asc) ? false : true;
//	        }
//
//	        p1 += head_info_->GetKeySize(key_id);
//	        p2 += head_info_->GetKeySize(key_id);
//	    }
//
//	    return false;
	}

	HeadInfo *head_info_;
	IndexAttr index_attr_;
	int key_num_;
	Order *p_order_;
	std::vector<int> order_key_off_arr_;
	std::vector<int> order_key_id_arr_;
	std::vector<bool> order_is_asc_arr_;
};


SelectorBTreeSort::SelectorBTreeSort(HeadInfo *head_info) :
	SelectorBTree(head_info){

}
SelectorBTreeSort::~SelectorBTreeSort() {

}

std::vector<int64_t>  SelectorBTreeSort::GetTraces() {
	InitStatistics();
	sort_time_ = 0.0;

	std::vector<int64_t> valid_trace_arr, null_ret;
	struct timeval t_fetch_begin, t_fetch_end;
	gettimeofday(&t_fetch_begin, NULL);


	bool valid_order, valid_filter;
	CheckOrderAndFilter(valid_order, valid_filter);

	IndexAttr order_attr;
	order_attr.keypos_arr = order_.GetKeyPosArr();
	order_attr.asc_arr_ = order_.GetAscArr();

	if(valid_order) {

		if(!valid_filter) {

			printf("Filter is empty, load all index directly...\n");
			if(!GetAllTracesByOrder(order_attr	, NULL, &valid_trace_arr)) {
				return null_ret;
			}
		} else {

			row_filter_.AddOrder(order_attr.keypos_arr);

			if(!GetValidTracesWithFilter(row_filter_, &valid_trace_arr)) {
				Err("Get valid trace with filter error\n");
				return null_ret;
			}
		}
	} else {
		if(!valid_filter) {
			FetchAll(&valid_trace_arr);
		} else {
			if(!SelectorBTree::GetValidTracesWithFilter(false, row_filter_, &valid_trace_arr)) {
				Err("Get valid trace with filter error\n");
				return null_ret;
			}
		}
	}

	gettimeofday(&t_fetch_end, NULL);
	total_time_ = CALTIME(t_fetch_begin, t_fetch_end);
	fetch_time_ = total_time_ - extra_time_;

	fetched_trace_num_ = valid_trace_arr.size();
	read_size_ = IndexBTreeReader::GetTotalReadSize();
	read_time_ = IndexBTreeReader::GetTotalReadTime();

	return valid_trace_arr;
}

IndexBTreeReaderSort *SelectorBTreeSort::OpenBTreeReaderSort(const IndexAttr &index_attr, HeadInfo *head_info) {
	IndexAttr tmp_attr = Index::FindIndex(index_attr, INDEX_TYPE_BTREE);
	if(tmp_attr.keypos_arr.empty()) {
		if(!BuildIndex(index_attr)) {
			Err("Build btree error\n");
			return NULL;
		}
	}

	IndexBTreeReaderSort *index_btree_reader_sort = new IndexBTreeReaderSort(index_attr, head_info);
	if(NULL == index_btree_reader_sort) {
		Err("new btree reader error\n");
		return NULL;
	}
	if(!index_btree_reader_sort->Open()) {
		Err("open btree reader error\n");
		return NULL;
	}
	return index_btree_reader_sort;
}

bool SelectorBTreeSort::GetValidTracesWithFilter(const RowFilterByKey &row_filter, std::vector<int64_t> *p_valid_trace_arr) {

	IndexAttr filter_index_attr;
	filter_index_attr.keypos_arr = row_filter.GetKeyIDArr();
	for(int i = 0; i < filter_index_attr.keypos_arr.size(); ++i) {
		filter_index_attr.asc_arr_.push_back(true); //TODO any order is OK
	}

	int strict_order_key_num = row_filter_.GetValidKeyNum();

	IndexAttr real_filter_index_attr;
	real_filter_index_attr = Index::FindIndexIgnoreOrder(filter_index_attr, INDEX_TYPE_BTREE, strict_order_key_num);

	if(real_filter_index_attr.keypos_arr.empty()) {
		real_filter_index_attr = filter_index_attr;
	}

	ScopedPointer<IndexBTreeReaderSort> index_filter_reader(OpenBTreeReaderSort(real_filter_index_attr, head_info_));
	if(NULL == index_filter_reader.data()) {
		Err("Open btree reader error\n");
		return false;
	}
	struct timeval t_begin, t_end;
	gettimeofday(&t_begin, NULL);

	std::vector<IndexElement> index_ele_arr;

	index_filter_reader->GetValidIndex(row_filter, &index_ele_arr);
	index_filter_reader->Close();

	struct timeval t_b, t_e;
	gettimeofday(&t_b, NULL);

	tbb::parallel_sort(index_ele_arr.begin(), index_ele_arr.end(), Comparator(head_info_, real_filter_index_attr, &order_));

	for(int64_t i = 0;i < index_ele_arr.size(); ++i) {
		p_valid_trace_arr->push_back(index_ele_arr[i].trace_id);
	}
	gettimeofday(&t_e, NULL);
	sort_time_ += CALTIME(t_b, t_e);

	gettimeofday(&t_end, NULL);
	double time_s = CALTIME(t_begin, t_end);
	printf("Search time : %f s\n", time_s);

	return true;
}

void SelectorBTreeSort::RecordPrivate(MetricManager *metric_manager) {
	SelectorBTree::RecordPrivate(metric_manager);
	metric_manager->SetFloat("SortTime(s)", sort_time_, PRI);
}

float SelectorBTreeSort::sort_time_ = 0.0;
