/*
 * selector_btree.cpp
 *
 *  Created on: May 23, 2018
 *      Author: wyd
 */

#include "index_btree/selector_btree.h"

#include "index_btree/index_btree.h"
#include "index_btree/index_btree_reader.h"
#include "head_data.h"
#include "index.h"
#include "order.h"
#include "row_filter_by_key.h"
#include "util/util_log.h"
#include "util/util.h"
#include "util/util_bitmap.h"
#include "util/util_scoped_pointer.h"
#include <stdlib.h>



SelectorBTree::SelectorBTree(HeadInfo *head_info) :
	Selector(head_info) {

}
SelectorBTree::~SelectorBTree() {

}


std::vector<int64_t>  SelectorBTree::GetTraces() {

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
			std::vector<int> filter_key_id_arr = row_filter_.GetKeyIDArr();
			int arr_contained = IsArrContained(filter_key_id_arr, order_attr.keypos_arr);
			if(0 < arr_contained) {
				printf("Keys in filter is contained in order, use the index of order to fetch trace ...\n");

				ScopedPointer<IndexBTreeReader> index_btree_reader(OpenBTreeReader(order_attr, head_info_));
				if(NULL == index_btree_reader.data()) {
					Err("Open btree reader error\n");
					return null_ret;
				}

				struct timeval t_begin, t_end;
				gettimeofday(&t_begin, NULL);

				row_filter_.SortFilterKeys(order_attr.keypos_arr);
				index_btree_reader->GetValidTraces(row_filter_, &valid_trace_arr);
				index_btree_reader->Close();

				gettimeofday(&t_end, NULL);
				double time_s = CALTIME(t_begin, t_end);
				printf("Search time : %f s\n", time_s);

			} else {

				bool strict_key_order = false;;
				if(0 > arr_contained) {
					printf("Keys in order is contained in filter, use the index of filter to fetch trace ...\n");
					row_filter_.SortFilterKeys(order_attr.keypos_arr);
					strict_key_order = true;
				}

				if(!GetValidTracesWithFilter(strict_key_order, row_filter_, &valid_trace_arr)) {
					Err("Get valid trace with filter error\n");
					return null_ret;
				}

				if(0 == arr_contained) {
					printf("Keys in filter is not contained in order, use index of filter to fetch trace first\n"
							"then record these in a bitmap, and get trace id as the order...\n");

					SharedPtr<Bitmap> bit_map = BuildBitmap(valid_trace_arr);
					if(!GetAllTracesByOrder(order_attr, bit_map, &valid_trace_arr)) {
						return null_ret;
					}
				}
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

bool SelectorBTree::GetValidTracesWithFilter(bool strict_key_order, const RowFilterByKey &row_filter, std::vector<int64_t> *p_valid_trace_arr) {

	RowFilterByKey tmp_filter = row_filter;

	IndexAttr filter_index_attr;
	filter_index_attr.keypos_arr = tmp_filter.GetKeyIDArr();
	for(int i = 0; i < filter_index_attr.keypos_arr.size(); ++i) {
		filter_index_attr.asc_arr_.push_back(true); //TODO any order is OK
	}

	IndexAttr real_filter_index_attr;
	if(strict_key_order) {
		real_filter_index_attr = Index::FindIndex(filter_index_attr, INDEX_TYPE_BTREE);
	} else { // == 0
		real_filter_index_attr = Index::FindIndexIgnoreOrder(filter_index_attr, INDEX_TYPE_BTREE);
		tmp_filter.SortFilterKeys(real_filter_index_attr.keypos_arr);
	}

	if(real_filter_index_attr.keypos_arr.empty()) {
		real_filter_index_attr = filter_index_attr;
	}

	ScopedPointer<IndexBTreeReader> index_filter_reader(OpenBTreeReader(real_filter_index_attr, head_info_));
	if(NULL == index_filter_reader.data()) {
		Err("Open btree reader error\n");
		return false;
	}
	struct timeval t_begin, t_end;
	gettimeofday(&t_begin, NULL);
	index_filter_reader->GetValidTraces(tmp_filter, p_valid_trace_arr);
	index_filter_reader->Close();

	gettimeofday(&t_end, NULL);
	double time_s = CALTIME(t_begin, t_end);
	printf("Search time : %f s\n", time_s);

	return true;
}

Bitmap *SelectorBTree::BuildBitmap(const std::vector<int64_t> &valid_trace_arr) {
	struct timeval t_begin, t_end;
	gettimeofday(&t_begin, NULL);

	Bitmap *bit_map = new Bitmap(total_trace_num_);
	bit_map->ClearAll();
	for(std::vector<int64_t>::const_iterator it = valid_trace_arr.begin(); it != valid_trace_arr.end(); ++it) {
		bit_map->SetBit(*it);
	}

	gettimeofday(&t_end, NULL);
	double build_bitmap_time = CALTIME(t_begin, t_end);
//	printf("Build bitmap time : %f s\n", build_bitmap_time);

	return bit_map;
}

bool SelectorBTree::GetAllTracesByOrder(const IndexAttr &index_attr, SharedPtr<Bitmap> bit_map, std::vector<int64_t> *p_valid_trace_arr) {

	ScopedPointer<IndexBTreeReader> index_btree_reader(OpenBTreeReader(index_attr, head_info_));
	if(NULL == index_btree_reader.data()) {
		Err("Open btree reader error\n");
		return false;
	}

	struct timeval t_begin, t_end;
	gettimeofday(&t_begin, NULL);

	p_valid_trace_arr->clear();

	if(NULL != bit_map) {
		for(int64_t i = 0; i < total_trace_num_; ++i) {
			int64_t trace_id = index_btree_reader->Next() & 0x7fffffffffffffff;
			if(bit_map->IsSet(trace_id)) {
				p_valid_trace_arr->push_back(trace_id);
			}
		}
	} else {
		for(int64_t i = 0; i < total_trace_num_; ++i) {
			p_valid_trace_arr->push_back(index_btree_reader->Next() & 0x7fffffffffffffff);
		}
	}

	index_btree_reader->Close();

	gettimeofday(&t_end, NULL);
	double read_index_as_order_time = CALTIME(t_begin, t_end);
	printf("Read index as order time : %f s\n", read_index_as_order_time);

	return true;
}

Index *SelectorBTree::GetIndexInstance(const IndexAttr &attr, HeadInfo *head_info) {
	Index *ret = new IndexBTree(attr, head_info);
	return ret;
}

IndexBTreeReader *SelectorBTree::OpenBTreeReader(const IndexAttr &index_attr, HeadInfo *head_info) {

	IndexAttr tmp_attr = Index::FindIndex(index_attr, INDEX_TYPE_BTREE);
	if(tmp_attr.keypos_arr.empty()) {
		if(!BuildIndex(index_attr)) {
			Err("Build btree error\n");
			return NULL;
		}
	}

	IndexBTreeReader *index_btree_reader = new IndexBTreeReader(index_attr, head_info);
	if(NULL == index_btree_reader) {
		Err("new btree reader error\n");
		return NULL;
	}
	if(!index_btree_reader->Open()) {
		Err("open btree reader error\n");
		return NULL;
	}
	return index_btree_reader;
}


void SelectorBTree::InitStatistics() {
	InitPublicStat();
	IndexBTreeReader::InitStatistic();
}

void SelectorBTree::RecordPrivate(MetricManager *metric_manager) {

	metric_manager->SetFloat("LDBlkSize(MB)", IndexBTreeReader::GetLoadBlockSize(), PRI);
	metric_manager->SetFloat("LDBlkTime(s)", IndexBTreeReader::GetLoadBlockTime(), PRI);
	metric_manager->SetFloat("RDTrcSize(MB)", IndexBTreeReader::GetReadTraceSize(), PRI);
	metric_manager->SetFloat("RDTrcTime(s)", IndexBTreeReader::GetReadTraceTime(), PRI);

}

int64_t SelectorBTree::GetGatherNum() {

}
Gather *SelectorBTree::GetGather(int gather_id) {

}



