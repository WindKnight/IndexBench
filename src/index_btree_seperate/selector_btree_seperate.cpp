/*
 * selector_btree_seperate.cpp
 *
 *  Created on: Jul 19, 2018
 *      Author: wyd
 */


#include "index_btree_seperate/selector_btree_seperate.h"
#include "util/util.h"
#include "util/util_bitmap.h"
#include "order.h"
#include "index.h"
#include "row_filter_by_key.h"
#include "util/util_scoped_pointer.h"
#include "index_btree/index_btree_reader.h"



SelectorBTreeSeperate::SelectorBTreeSeperate(HeadInfo *head_info) :
	SelectorBTree(head_info){

}
SelectorBTreeSeperate::~SelectorBTreeSeperate() {

}

std::vector<int64_t> SelectorBTreeSeperate::GetTraces() {

	InitStatistics();

	std::vector<int64_t> valid_trace_arr, null_ret;
	valid_trace_arr.clear();
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
			if(!GetAllTracesByOrder(order_attr, NULL, &valid_trace_arr)) {
				return null_ret;
			}
		} else {

			struct timeval t_begin, t_end;
			gettimeofday(&t_begin, NULL);

			SharedPtr<Bitmap> bitmap = BuildBitmapWithFilter();

			gettimeofday(&t_end, NULL);
			double time_s = CALTIME(t_begin, t_end);
			printf("Search time : %f s\n", time_s);

			if(!GetAllTracesByOrder(order_attr, bitmap, &valid_trace_arr)) {
				return null_ret;
			}
		}
	} else {
		if(!valid_filter) {
			FetchAll(&valid_trace_arr);
		} else {
			struct timeval t_begin, t_end;
			gettimeofday(&t_begin, NULL);

			SharedPtr<Bitmap> bitmap = BuildBitmapWithFilter();

			gettimeofday(&t_end, NULL);
			double time_s = CALTIME(t_begin, t_end);
			printf("Search time : %f s\n", time_s);

			for(int64_t i = 0; i < total_trace_num_; ++i) {
				if(bitmap->IsSet(i)) {
					valid_trace_arr.push_back(i);
				}
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

SharedPtr<Bitmap> SelectorBTreeSeperate::BuildBitmapWithFilter() {
	std::vector<int> filter_key_id_arr = row_filter_.GetKeyIDArr();
	int filter_key_num = filter_key_id_arr.size();
	std::vector<SharedPtr<Bitmap> > bit_map_arr;
	for(int i = 0; i < filter_key_num; ++i) {

		RowFilterByKey tmp_row_filter;
		std::vector<Key> rule_arr = row_filter_.GetFilterRuleOf(filter_key_id_arr[i]);
		for(int key_i = 0; key_i < rule_arr.size(); ++key_i) {
			tmp_row_filter.FilterBy(rule_arr[key_i]);
		}

		std::vector<int64_t> tmp_result;
		GetValidTracesWithFilter(true, tmp_row_filter, &tmp_result);

		SharedPtr<Bitmap> bit_map = BuildBitmap(tmp_result);
		bit_map_arr.push_back(bit_map);
	}

	for(int i = 1; i < filter_key_num; ++i) {
		bit_map_arr[0]->And(bit_map_arr[i]);
	}

	return bit_map_arr[0];
}


