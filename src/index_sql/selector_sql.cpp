/*
 * selector_sql.cpp
 *
 *  Created on: Aug 11, 2018
 *      Author: wangyida
 */


#include "index_sql/selector_sql.h"
#include "index_sql/index_sql.h"
#include "index_sql/index_sql_reader.h"
#include "util/util.h"
#include "util/util_scoped_pointer.h"



SelectorSql::SelectorSql(HeadInfo *head_info) :
	Selector(head_info){

}
SelectorSql::~SelectorSql() {

}

void SelectorSql::InitStatistics() {
	InitPublicStat();
}

std::vector<int64_t>  SelectorSql::GetTraces() {
	InitStatistics();

	std::vector<int64_t> valid_trace_arr, null_ret;
	struct timeval t_fetch_begin, t_fetch_end;
	gettimeofday(&t_fetch_begin, NULL);


	bool valid_order, valid_filter;
	CheckOrderAndFilter(valid_order, valid_filter);

	if(valid_order) {

		if(!valid_filter) {

			printf("Filter is empty, load all index directly...\n");
			if(!GetAllTracesByOrder(&valid_trace_arr)) {
				return null_ret;
			}
		} else {

			if(!GetValidTracesWithFilter(&valid_trace_arr)) {
				Err("Get valid trace with filter error\n");
				return null_ret;
			}
		}
	} else {
		if(!valid_filter) {
			FetchAll(&valid_trace_arr);
		} else {
			if(!GetValidTracesWithFilter(&valid_trace_arr)) {
				Err("Get valid trace with filter error\n");
				return null_ret;
			}
		}
	}

	gettimeofday(&t_fetch_end, NULL);
	total_time_ = CALTIME(t_fetch_begin, t_fetch_end);
	fetch_time_ = total_time_ - extra_time_;

	fetched_trace_num_ = valid_trace_arr.size();
//	read_size_ = IndexBTreeReader::GetTotalReadSize();
//	read_time_ = IndexBTreeReader::GetTotalReadTime();

	return valid_trace_arr;
}
int64_t SelectorSql::GetGatherNum() {
	return 0;
}
Gather *SelectorSql::GetGather(int gather_id) {
	return NULL;
}
Index *SelectorSql::GetIndexInstance(const IndexAttr &attr, HeadInfo *head_info) {
	Index *ret = new IndexSql(attr, head_info);
	return ret;
}


IndexSqlReader *SelectorSql::OpenSqlReader(HeadInfo *head_info) {
//	if(!BuildIndex(index_attr)) {
//		Err("Build btree error\n");
//		return NULL;
//	}

	IndexSqlReader *index_sql_reader = new IndexSqlReader(head_info);
	if(NULL == index_sql_reader) {
		Err("new sql reader error\n");
		return NULL;
	}
	if(!index_sql_reader->Open()) {
		Err("open sql reader error\n");
		return NULL;
	}
	return index_sql_reader;
}

bool SelectorSql::GetAllTracesByOrder(std::vector<int64_t> *p_valid_trace_arr) {


	IndexAttr index_attr;
	index_attr.keypos_arr = order_.GetKeyPosArr();
	index_attr.asc_arr_ = order_.GetAscArr();

	if(!BuildIndex(index_attr)) {
		Err("Build sql error\n");
		return false;
	}

	ScopedPointer<IndexSqlReader> index_sql_reader(OpenSqlReader(head_info_));
	if(NULL == index_sql_reader.data()) {
		Err("Open sql reader error\n");
		return false;
	}

//	ScopedPointer<IndexSql> index_sql(new IndexSql(index_attr, head_info_));
//	if(NULL == index_sql.data()) {
//		Err("New index sql error\n");
//		return false;
//	}
//	index_sql->BuildIndex();

	struct timeval t_begin, t_end;
	gettimeofday(&t_begin, NULL);

	p_valid_trace_arr->clear();

	index_sql_reader->GetValidTraces(row_filter_, order_, p_valid_trace_arr);
	index_sql_reader->Close();

	gettimeofday(&t_end, NULL);
	double read_index_as_order_time = CALTIME(t_begin, t_end);
	printf("Read index as order time : %f s\n", read_index_as_order_time);

	return true;
}

bool SelectorSql::GetValidTracesWithFilter(std::vector<int64_t> *p_valid_trace_arr) {

	IndexAttr order_attr;
	order_attr.keypos_arr = order_.GetKeyPosArr();
	order_attr.asc_arr_ = order_.GetAscArr();
	row_filter_.AddOrder(order_attr.keypos_arr);


	IndexAttr index_attr;
	index_attr.keypos_arr = row_filter_.GetKeyIDArr();
	for(int i = 0; i < index_attr.keypos_arr.size(); ++i) {
		index_attr.asc_arr_.push_back(true); //TODO any order is OK
	}

	if(!BuildIndex(index_attr)) {
		Err("Build sql error\n");
		return false;
	}

	ScopedPointer<IndexSqlReader> index_sql_reader(OpenSqlReader(head_info_));
	if(NULL == index_sql_reader.data()) {
		Err("Open sql reader error\n");
		return false;
	}

	struct timeval t_begin, t_end;
	gettimeofday(&t_begin, NULL);

	p_valid_trace_arr->clear();

	index_sql_reader->GetValidTraces(row_filter_, order_, p_valid_trace_arr);
	index_sql_reader->Close();

	gettimeofday(&t_end, NULL);
	double read_index_as_order_time = CALTIME(t_begin, t_end);
	printf("Read index as order time : %f s\n", read_index_as_order_time);

	return true;
}



void SelectorSql::RecordPrivate(MetricManager *metric_manager) {

}
