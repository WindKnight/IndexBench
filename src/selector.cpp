/*
 * index_reader.cpp
 *
 *  Created on: May 7, 2018
 *      Author: wyd
 */



#include "selector.h"
#include "head_data.h"
#include "index_order/selector_order_hhj.h"
#include "index_btree/selector_btree.h"
#include "index_btree_mt/selector_btree_mt.h"
#include "index_btree_sort/selector_btree_sort.h"
#include "index_btree_nosort/selector_btree_nosort.h"
#include "index_btree_seperate/selector_btree_seperate.h"
#include "index_btree_sort_mt/selector_btree_sort_mt.h"
#include "index_sql/selector_sql.h"
#include "row_filter_by_key.h"
#include "order.h"
#include "job_para.h"
#include "index.h"
#include "util/util_scoped_pointer.h"
#include "util/util.h"
#include "util/util_string.h"


Gather::Gather(const std::vector<int64_t> trace_id_arr) :
	trace_id_arr_(trace_id_arr){

}
Gather::~Gather() {

}

int64_t Gather::GetTraceNum() {
	return trace_id_arr_.size();
}
std::vector<int64_t> Gather::GetAll() {
	return trace_id_arr_;
}

MetricManager::MetricManager(const std::string &selector_type) :
	selector_type_(selector_type){

}
MetricManager::~MetricManager() {

}

void MetricManager::SetDouble(const std::string &name, double value, char type) {
	MetricTable *p_tab = NULL;
	if(type == PUB) {
		p_tab = &metric_tab_public_;
	} else if(type == PRI) {
		p_tab = &metric_tab_private_;
	} else {
		printf("Logical error, wrong metric type\n");
		exit(-1);
	}
	if(p_tab->end() == p_tab->find(name)) {
		MetricArr metric_arr;
		metric_arr.push_back(Double2Str(value));
		p_tab->insert(std::make_pair(name, metric_arr));
	} else {
		(*p_tab)[name].push_back(Double2Str(value));
	}
}
void MetricManager::SetFloat(const std::string &name, float value, char type) {
	MetricTable *p_tab = NULL;
	if(type == PUB) {
		p_tab = &metric_tab_public_;
	} else if(type == PRI) {
		p_tab = &metric_tab_private_;
	} else {
		printf("Logical error, wrong metric type\n");
		exit(-1);
	}
	if(p_tab->end() == p_tab->find(name)) {
		MetricArr metric_arr;
		metric_arr.push_back(Float2Str(value));
		p_tab->insert(std::make_pair(name, metric_arr));
	} else {
		(*p_tab)[name].push_back(Float2Str(value));
	}
}
void MetricManager::SetLong(const std::string &name, int64_t value, char type) {
	MetricTable *p_tab = NULL;
	if(type == PUB) {
		p_tab = &metric_tab_public_;
	} else if(type == PRI) {
		p_tab = &metric_tab_private_;
	} else {
		printf("Logical error, wrong metric type\n");
		exit(-1);
	}
	if(p_tab->end() == p_tab->find(name)) {
		MetricArr metric_arr;
		metric_arr.push_back(Int642Str(value));
		p_tab->insert(std::make_pair(name, metric_arr));
	} else {
		(*p_tab)[name].push_back(Int642Str(value));
	}
}
void MetricManager::SetInt(const std::string &name, int value, char type) {
	MetricTable *p_tab = NULL;
	if(type == PUB) {
		p_tab = &metric_tab_public_;
	} else if(type == PRI) {
		p_tab = &metric_tab_private_;
	} else {
		printf("Logical error, wrong metric type\n");
		exit(-1);
	}
	if(p_tab->end() == p_tab->find(name)) {
		MetricArr metric_arr;
		metric_arr.push_back(ToString(value));
		p_tab->insert(std::make_pair(name, metric_arr));
	} else {
		(*p_tab)[name].push_back(ToString(value));
	}
}

void MetricManager::PrintAll() {
	PrintPublicHead();
	PrintPrivateHead();
	printf("\n");

	for(int i = 0; i < metric_tab_public_.begin()->second.size(); ++i) {
		PrintPublic(i);
		PrintPrivate(i);
		printf("\n");
	}
}

void MetricManager::PrintPublicHead() {
	printf("TestRount");
	for(MetricTable::iterator it = metric_tab_public_.begin(); it != metric_tab_public_.end(); ++it) {
		printf("\t%s", it->first.c_str());
	}
}
void MetricManager::PrintPublic(int test_round) {
	printf("%-9d", test_round);
	for(MetricTable::iterator it = metric_tab_public_.begin(); it != metric_tab_public_.end(); ++it) {
		std::string name = it->first;
		MetricArr &arr = it->second;
		std::string format = "\t%-" + ToString(name.size()) + "s";
		printf(format.c_str(), arr[test_round].c_str());
	}

}

void MetricManager::PrintPrivateHead() {
	for(MetricTable::iterator it = metric_tab_private_.begin(); it != metric_tab_private_.end(); ++it) {
		printf("\t%s", it->first.c_str());
	}
}
void MetricManager::PrintPrivate(int test_round) {
	for(MetricTable::iterator it = metric_tab_private_.begin(); it != metric_tab_private_.end(); ++it) {
		std::string name = it->first;
		MetricArr &arr = it->second;
		std::string format = "\t%-" + ToString(name.size()) + "s";
		printf(format.c_str(), arr[test_round].c_str());
	}
}



Selector *Selector::GetInstance(const std::string &index_type, HeadInfo *head_info) {
	Selector *ret;
	if(index_type == "OrderHHJ") {
		ret = new SelectorOrderHHJ(head_info);
	} else if(index_type == "BTree") {
		ret = new SelectorBTree(head_info);
	} else if(index_type == "BTreeMT") {
		ret = new SelectorBTreeMT(head_info);
	} else if(index_type == "BTreeSeperate") {
		ret = new SelectorBTreeSeperate(head_info);
	} else if(index_type == "BTreeSort") {
		ret = new SelectorBTreeSort(head_info);
	} else if(index_type == "BTreeNoSort") {
		ret = new SelectorBTreeNoSort(head_info);
	} else if(index_type == "Sql") {
		ret = new SelectorSql(head_info);
	} else if(index_type == "BTreeSortMT") {
		ret = new SelectorBTreeSortMT(head_info);
	}
	else {
		return NULL;
	}

	return ret;
}


Selector::Selector(HeadInfo *head_info) :
	head_info_(head_info){

	total_trace_num_ = JobPara::GetTraceNum();
}
Selector::~Selector() {

}

void Selector::SetRowFilter(const RowFilterByKey &row_filter) {
	row_filter_ = row_filter;
	row_filter_.SetHeadInfo(head_info_);
	row_filter_.Normalize();
}
void Selector::SetOrder(const Order &order) {
	order_ = order;
}

MetricManager *Selector::GetMetricManager(const std::string &index_type) {
	MetricManager *ret = new MetricManager(index_type);
	return ret;
}

void Selector::InitPublicStat() {
	total_time_ = 0.0;
	fetch_time_ = 0.0;
	extra_time_ = 0.0;
	build_index_num_ = 0;
	build_index_time_ = 0.0;
	read_size_ = 0.0;
	read_time_ = 0.0;
}

void Selector::FetchAll(std::vector<int64_t> *p_valid_trace_arr) {
	for(int64_t i = 0; i < total_trace_num_; ++i) {
		p_valid_trace_arr->push_back(i);
	}
}

void Selector::CheckOrderAndFilter(bool &valid_order, bool &valid_filter) {
	if(order_.GetKeyPosArr().empty()) {
		valid_order = false;
	} else {
		valid_order = true;
	}

	if(row_filter_.GetKeyIDArr().empty()) {
		valid_filter = false;
	} else {
		valid_filter = true;
	}
}

void Selector::RecordMetric(MetricManager *metric_manager) {
	RecordPublic(metric_manager);
	RecordPrivate(metric_manager);
}

IndexAttr Selector::GetRealOrderAttr(const std::string &index_type) {
	IndexAttr attr;
	attr.keypos_arr = order_.GetKeyPosArr();
	attr.asc_arr_ = order_.GetAscArr();

	IndexAttr real_attr = Index::FindIndex(attr, index_type);
	if(real_attr.keypos_arr.empty()) {
		real_attr = attr;
	}

	return real_attr;
}

bool Selector::BuildIndex(const IndexAttr &attr) {
	struct timeval t_begin, t_end;
	gettimeofday(&t_begin, NULL);

	ScopedPointer<Index> index(GetIndexInstance(attr, head_info_));
	if(!index->BuildIndex()) {
		Err("Build Index ERROR\n");
		return false;
	}

	gettimeofday(&t_end, NULL);
	double time_tmp = CALTIME(t_begin, t_end);
	build_index_time_ += time_tmp;
	extra_time_ += time_tmp;

	gettimeofday(&t_begin, NULL);

	ClearCache();

	gettimeofday(&t_end, NULL);
	time_tmp = CALTIME(t_begin, t_end);
	extra_time_ += time_tmp;

	build_index_num_ ++;
	return true;
}

void Selector::RecordPublic(MetricManager *metric_manager) {
	metric_manager->SetLong("FetNum", fetched_trace_num_, PUB);
	float fetch_ratio = 1.0 * 100 * fetched_trace_num_ / total_trace_num_;
	metric_manager->SetFloat("FetRatio", fetch_ratio, PUB);
	metric_manager->SetDouble("FetTime(s)", fetch_time_, PUB);
	metric_manager->SetDouble("TotalTime(s)", total_time_, PUB);

	metric_manager->SetFloat("RDSize(MB)", read_size_, PUB);
	metric_manager->SetFloat("RDTime(s)", read_time_, PUB);

	float read_speed = read_time_ == 0 ? 0 : read_size_ / read_time_;
	metric_manager->SetFloat("RDSpd(MB/s)", read_speed, PUB);
	metric_manager->SetInt("BldNum", build_index_num_, PUB);
	metric_manager->SetFloat("BldTime(s)", build_index_time_, PUB);
}
