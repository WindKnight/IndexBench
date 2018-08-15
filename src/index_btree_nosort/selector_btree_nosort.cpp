/*
 * selector_btree_nosort.cpp
 *
 *  Created on: Jul 30, 2018
 *      Author: wyd
 */


#include "index_btree_nosort/selector_btree_nosort.h"

#include "index_btree/index_btree_reader.h"
#include "row_filter_by_key.h"
#include "index_config.h"
#include "order.h"
#include "util/util_log.h"
#include "util/util.h"
#include "util/util_bitmap.h"
#include "util/util_scoped_pointer.h"




SelectorBTreeNoSort::SelectorBTreeNoSort(HeadInfo *head_info) :
	SelectorBTree(head_info){

}
SelectorBTreeNoSort::~SelectorBTreeNoSort() {

}


void SelectorBTreeNoSort::RecordPrivate(MetricManager *metric_manager) {

	SelectorBTree::RecordPrivate(metric_manager);
	metric_manager->SetFloat("BSearchTime", IndexBTreeReader::GetBinarySearchTime(), PRI);

}

std::vector<int64_t>  SelectorBTreeNoSort::GetTraces() {
	InitStatistics();

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
			if(!GetAllTracesByOrder(order_attr, NULL, &valid_trace_arr)) {
				return null_ret;
			}
		} else {

			row_filter_.AddOrderAndSort(order_attr.keypos_arr);
			if(!GetValidTracesWithFilter(true, row_filter_, &valid_trace_arr)) {
				Err("Get valid trace with filter error\n");
				return null_ret;
			}
		}
	} else {
		if(!valid_filter) {
			FetchAll(&valid_trace_arr);
		} else {
			if(!GetValidTracesWithFilter(false, row_filter_, &valid_trace_arr)) {
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
