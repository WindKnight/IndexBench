/*
 * index_order_reader.cpp
 *
 *  Created on: May 7, 2018
 *      Author: wyd
 */

#include "index_order/selector_order_hhj.h"
#include "order.h"
#include "index_order/index_order.h"
#include "row_filter_by_key.h"
#include "index_order/index_order_reader.h"
#include "head_data.h"
#include "util/util.h"
#include "util/util_scoped_pointer.h"


SelectorOrderHHJ::SelectorOrderHHJ(HeadInfo *head_info) :
	Selector(head_info){
	gather_flag_.key_buf = NULL;
}
SelectorOrderHHJ::~SelectorOrderHHJ() {
	if(NULL != gather_flag_.key_buf) {
		delete gather_flag_.key_buf;
		gather_flag_.key_buf = NULL;
	}
}

void SelectorOrderHHJ::InitStatistics() {
	InitPublicStat();
	read_head_size_ = 0;
	read_head_time_ = 0;
	IndexOrderReader::InitStatistic();
}

std::vector<int64_t> SelectorOrderHHJ::GetTraces() {
	InitStatistics();
	std::vector<int64_t> valid_trace_arr, null_ret;//TODO (save all valid trace in memory will cost a lot of memory space
	struct timeval t_fetch_begin, t_fetch_end;
	gettimeofday(&t_fetch_begin, NULL);

#ifdef USE_GATHER
	gather_begin_id_.clear();
#endif
	bool valid_order, valid_filter;
	CheckOrderAndFilter(valid_order, valid_filter);

	IndexAttr order_attr;
	order_attr.keypos_arr = order_.GetKeyPosArr();
	order_attr.asc_arr_ = order_.GetAscArr();

	if(valid_order) {
//		IndexAttr real_order_attr = GetRealOrderAttr(INDEX_TYPE_ORDER);

#if 0
		IndexAttr attr;
		attr.keypos_arr = order_->GetKeyPosArr();
		attr.asc_arr_ = order_->GetAscArr();

		real_attr_ = Index::FindIndex(attr, INDEX_TYPE_ORDER);
		if(real_attr_.keypos_arr.empty()) {

			gettimeofday(&t_begin, NULL);

			IndexOrder *index_order = new IndexOrder(attr, head_info_);
			if(!index_order->BuildIndex()) {
				Err("Build Index ERROR\n");
				delete index_order;
				return null_ret;
			}
			real_attr_ = attr;
			delete index_order;

			ClearCache();

			gettimeofday(&t_end, NULL);
			double time_tmp = CALTIME(t_begin, t_end);
			extra_time_ += time_tmp;
		}
#endif
		int index_key_size = 0;
		for(std::vector<int>::iterator it = order_attr.keypos_arr.begin(); it != order_attr.keypos_arr.end(); ++it) {
			index_key_size += head_info_->GetKeySize(*it);
		}

		IndexElement index_ele;
//		index_ele.key_buf = new char[index_key_size];
		index_ele.Malloc(index_key_size);
		IndexOrderReader *index_order_reader = OpenIndexReader(order_attr, head_info_);

		if(!valid_filter) {

			for(int64_t i = 0; i < index_order_reader->GetTotalTraceNum(); ++i) {
				 index_order_reader->Next(&index_ele);

	#ifdef USE_GATHER
				if(IsNewGather(index_ele)) {
					gather_begin_id_.push_back(valid_trace_arr.size());
				}
	#endif
				valid_trace_arr.push_back(index_ele.trace_id);
			}
		} else {

			std::vector<int> filter_key_id_arr = row_filter_.GetKeyIDArr();

			if(0 < IsArrContained(filter_key_id_arr, order_attr.keypos_arr)) {
				row_filter_.SortFilterKeys(order_attr.keypos_arr);
				int key_first_id = order_attr.keypos_arr[0];
				int key_first_size = head_info_->GetKeySize(key_first_id);
				std::vector<Key> KeyFirstFilterRules = row_filter_.GetFilterRuleOf(key_first_id);
				for(int rule_i = 0; rule_i < KeyFirstFilterRules.size(); ++rule_i) {

					Key key_tmp = KeyFirstFilterRules[rule_i];
					std::vector<KeyFirst> key_first_arr = index_order_reader->GetAllValidKeyFirst(key_tmp); //TODO (deal with inc and group/tolerance)
					for(int key_first_i = 0; key_first_i < key_first_arr.size(); ++key_first_i) {
		//				printf("valid_key_first = %d, key_num = %d, offset = %d\n",
		//						*((int*)key_first_arr[key_first_i].key_buf), key_first_arr[key_first_i].trace_num, key_first_arr[key_first_i].offset);
						index_order_reader->Seek(key_first_arr[key_first_i].offset); //TODO (fast search)
						for(int i = 0; i < key_first_arr[key_first_i].trace_num; ++i) {
							index_order_reader->Next(&index_ele);

							if(row_filter_.IsKeyOtherValid(index_ele.key_buf + key_first_size)) {
	#ifdef USE_GATHER
								if(IsNewGather(index_ele)) {
		//							printf("New Gather!\n");
									gather_begin_id_.push_back(valid_trace_arr_.size());
								}
	#endif
								valid_trace_arr.push_back(index_ele.trace_id);
							}
						}
					}
				}
			} else {
				TraceSelect(index_order_reader, &index_ele, &valid_trace_arr);
			}
		}

		delete index_ele.key_buf;
		index_order_reader->Close();
		delete index_order_reader;
	} else {
		if(!valid_filter) {
			FetchAll(&valid_trace_arr);
		} else {
			TraceSelect(NULL, NULL, &valid_trace_arr);
		}
	}

	gettimeofday(&t_fetch_end, NULL);
	total_time_ = CALTIME(t_fetch_begin, t_fetch_end);
	fetch_time_ = total_time_ - extra_time_;

	fetched_trace_num_ = valid_trace_arr.size();
	read_size_ += IndexOrderReader::GetTotalReadSize();
	read_size_ += read_head_size_;
	read_time_ += IndexOrderReader::GetTotalReadTime();
	read_time_ += read_head_time_;

	return valid_trace_arr;
}

void SelectorOrderHHJ::TraceSelect(IndexOrderReader *index_reader, IndexElement *p_ele, std::vector<int64_t> *p_result_arr) {
	ScopedPointer<HeadDataReader> head_reader(new HeadDataReader(head_info_));
	head_reader->Open();

	int head_size = head_info_->GetHeadSize();
	ScopedPointer<char> head_buf(new char[head_size]);

	for(int64_t i = 0; i < total_trace_num_; ++i) {

		int64_t valid_trace_id;

		if(NULL != index_reader) {
			index_reader->Next(p_ele);
			head_reader->Seek(p_ele->trace_id);
			valid_trace_id = p_ele->trace_id;
		} else {
			valid_trace_id = i;
		}


		struct timeval t_begin, t_end;
		gettimeofday(&t_begin, NULL);

		head_reader->Next(head_buf.data());

		gettimeofday(&t_end, NULL);
		double time = CALTIME(t_begin, t_end);
		read_head_time_ += time;

		if(row_filter_.IsHeadValid(head_buf.data())) {
#ifdef USE_GATHER
			if(IsNewGather(index_ele)) {
				gather_begin_id_.push_back(valid_trace_arr_.size());
			}
#endif

			p_result_arr->push_back(valid_trace_id);
		}
	}

	read_head_size_ = 1.0 * total_trace_num_ * head_reader->GetHeadSize() / 1024 / 1024;

	head_reader->Close();
}


bool SelectorOrderHHJ::IsNewGather(const IndexElement &my_index) {

#ifdef USE_GATHER
	if(gather_flag_.key_buf == NULL) {
		index_size_ = 0;
		for(int i = 0; i < real_attr_.keypos_arr.size(); ++i) {
			index_size_ += head_info_->GetKeySize(real_attr_.keypos_arr[i]);
		}
		gather_flag_.key_buf = new char[index_size_];
		memcpy(gather_flag_.key_buf, my_index.key_buf, index_size_);
		gather_flag_.trace_id = my_index.trace_id;
		return true;
	} else {
		int flag_pos = order_->GetGatherPos();
		char *p1 = gather_flag_.key_buf;
		char *p2 = my_index.key_buf;

		for(int i = 0; i <= flag_pos; ++i) {
			KeyType key_type = head_info_->GetKeyType(real_attr_.keypos_arr[i]);

			if(!KeyEqual(p1, p2, key_type)) {
				memcpy(gather_flag_.key_buf, my_index.key_buf, index_size_);
				gather_flag_.trace_id = my_index.trace_id;
				return true;
			}

			int key_size = head_info_->GetKeySize(real_attr_.keypos_arr[i]);
			p1 += key_size;
			p2 += key_size;
		}

		return false;
	}
#endif
}

Index *SelectorOrderHHJ::GetIndexInstance(const IndexAttr &attr, HeadInfo *head_info) {
	Index *ret = new IndexOrder(attr, head_info);
	return ret;
}

void SelectorOrderHHJ::RecordPrivate(MetricManager *metric_manager) {
	metric_manager->SetFloat("RDHeadSize(MB)", read_head_size_, PRI);
	metric_manager->SetFloat("RDHeadTime(s)", read_head_time_, PRI);
	metric_manager->SetFloat("TotalRDSize(MB)", IndexOrderReader::GetTotalReadSize(), PRI);
	metric_manager->SetFloat("TotalRDTime(s)", IndexOrderReader::GetTotalReadTime(), PRI);
}

int64_t SelectorOrderHHJ::GetGatherNum() {
	return gather_begin_id_.size();
}
Gather *SelectorOrderHHJ::GetGather(int gather_id) {
#ifdef USE_GATHER
	if(gather_id < 0 || gather_id >= gather_begin_id_.size()) {
		return NULL;
	}

	int gather_begin_id, gather_end_id;
	if(gather_id == gather_begin_id_.size() - 1) {
		gather_end_id = valid_trace_arr_.size() - 1;
	} else {
		gather_end_id = gather_begin_id_[gather_id + 1] - 1;
	}
	gather_begin_id = gather_begin_id_[gather_id];

	std::vector<int64_t> gather_trace_id;
	std::vector<int64_t>::iterator it = valid_trace_arr_.begin();
	gather_trace_id.assign(it + gather_begin_id, it + gather_end_id + 1);
//	gather_trace_id = valid_trace_arr_.

	Gather *gather = new Gather(gather_trace_id);
	return gather;
#endif
}


IndexOrderReader *SelectorOrderHHJ::OpenIndexReader(const IndexAttr &index_attr, HeadInfo *head_info) {
	IndexAttr tmp_attr = Index::FindIndex(index_attr, INDEX_TYPE_ORDER);
	if(tmp_attr.keypos_arr.empty()) {
		if(!BuildIndex(index_attr)) {
			Err("Build index order error\n");
			return NULL;
		}
	}
	IndexOrderReader *index_order_reader = new IndexOrderReader(index_attr, head_info);
	if(NULL == index_order_reader) {
		Err("new index order reader error\n");
		return NULL;
	}
	if(!index_order_reader->Open()) {
		Err("open index order reader error\n");
		return NULL;
	}
	return index_order_reader;
}



