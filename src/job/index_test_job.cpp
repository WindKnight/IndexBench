/*
 * index_test_job.cpp
 *
 *  Created on: Jul 2, 2018
 *      Author: wyd
 */

#include "job/index_test_job.h"
#include "job_para.h"
#include "util/util.h"
#include "util/util_string.h"
#include <sys/time.h>
#include <stdlib.h>
#include "index_btree/index_btree_reader.h";
#include "util/util_scoped_pointer.h"


IndexTestJob::IndexTestJob(HeadInfo *head_info) :
	head_info_(head_info) {
	total_trace_num_ = JobPara::GetTraceNum();
}
IndexTestJob::~IndexTestJob() {

}

void IndexTestJob::AddIndexType(const std::string &index_type) {
	index_type_arr_.push_back(index_type);
	Selector *slct = Selector::GetInstance(index_type, head_info_);
	selector_arr_.push_back(slct);
	MetricManager *metric_man = slct->GetMetricManager(index_type);
	metric_man_arr_.push_back(metric_man);
	std::vector<int64_t> result;
	result_arr_.push_back(result);
}

void IndexTestJob::AddTestUnit(const TestUnit &test_unit) {
	test_unit_arr_.push_back(test_unit);
}



void IndexTestJob::GenerateWorkload(int test_num) {

	for(int i = 0; i < test_num; ++i) {
		TestUnit test_unit;
		GenerateOrder(test_unit.order);
		GenerateFilter(&test_unit);

		this->AddTestUnit(test_unit);
	}
}

void IndexTestJob::GenerateOrder(SharedPtr<Order> order) {

	std::string order_str = JobPara::GetPara("Order");
	if("" != order_str) {
		std::vector<std::string> order_str_vec = SplitString(order_str,",");
		for(int i = 0; i < order_str_vec.size(); ++i) {
			order->OrderBy(ToInt(order_str_vec[i]));
		}
	} else {
#if 1
		struct timeval t;
		gettimeofday(&t, NULL);
		int64_t seed = t.tv_sec * 1000000 + t.tv_usec;
		srand(seed);

		int order_key_num = rand() % 3 + 1;

		int key_id_arr[4];
		for(int i = 0; i < 4; ++i) {
			key_id_arr[i] = i;
		}
		Shuffle(key_id_arr, 4);

		for(int i = 0; i < order_key_num; ++i) {
			int key_id = key_id_arr[i];
			order->OrderBy(key_id);
		}
#else
		order->OrderBy(2).OrderBy(3).OrderBy(1);
#endif
	}

}

void IndexTestJob::GenerateFilter(TestUnit *p_test_unit) {

	struct timeval t;
	gettimeofday(&t, NULL);
	int64_t seed = t.tv_sec * 1000000 + t.tv_usec;
	srand(seed);

	int filter_num = rand() % 5;
//	printf("filter_num = %d\n", filter_num);
	for(int i = 0; i < filter_num; ++i) {
		int key_id = rand() % 4;
		float ratio = 1.0 * (rand() % 81 + 20) / 100;
//		printf("Set filter : key %d, ratio %f\n", key_id ,ratio);
		p_test_unit->SetFilter(key_id, ratio);
	}
}

bool IndexTestJob::Start() {

	std::string check_result = JobPara::GetPara("CheckResult");
	bool b_check = check_result == "true" ? true : false;

	for(int test_i = 0; test_i < test_unit_arr_.size(); ++test_i) {

		printf("##############  Test %d  ###############\n", test_i);

		test_unit_arr_[test_i].order->Print();
		test_unit_arr_[test_i].row_filter->Print();

		for(int selector_i = 0; selector_i < selector_arr_.size(); ++selector_i) {

			printf("\n***********************************\n");
			printf("Index Type : %s\n", index_type_arr_[selector_i].c_str());
			ClearCache();
			printf("Fetching traces...\n");

			Selector *slct = selector_arr_[selector_i];
			slct->SetOrder(*test_unit_arr_[test_i].order);
			slct->SetRowFilter(*test_unit_arr_[test_i].row_filter);

			struct timeval t_begin, t_end;
			gettimeofday(&t_begin, NULL);

			std::vector<int64_t> tmp_result;
			int64_t fetch_trace_num;

			if(b_check) {
				result_arr_[selector_i] = slct->GetTraces();
				fetch_trace_num = result_arr_[selector_i].size();
			} else {
				tmp_result = slct->GetTraces();
				fetch_trace_num = tmp_result.size();
			}


			gettimeofday(&t_end, NULL);
			double time = CALTIME(t_begin, t_end);

			slct->RecordMetric(metric_man_arr_[selector_i]);

			printf(">>>>>>Fetching Over<<<<<\n");
			printf("Fetch Trace Number : %lld\n", fetch_trace_num);
			printf("Total Trace Number : %lld\n", total_trace_num_);
			printf("Fetch ratio : %.2f\%\n", 1.0 * fetch_trace_num / total_trace_num_ * 100);
			printf("Time : %.5f (%.5f) s\n", slct->GetFetchTime(), time);

			IndexBTreeReader::PrintStatistic();
		}

		if(b_check) {
			if(!CheckResult()) {
				break;
			}
		}
	}

	printf("Test Configuration \n");

	for(int test_i = 0; test_i < test_unit_arr_.size(); ++test_i) {
		printf("----------------------------------------------------------------\n");
		printf("Test Round : %d\n", test_i);
		test_unit_arr_[test_i].order->Print();
		test_unit_arr_[test_i].row_filter->Print();
	}
	printf("----------------------------------------------------------------\n");


	for(int selector_i = 0; selector_i < selector_arr_.size(); ++selector_i) {
		printf("****************  Print Metric of %s ****************\n", index_type_arr_[selector_i].c_str());
		metric_man_arr_[selector_i]->PrintAll();
	}


	return true;
}

bool IndexTestJob::CheckResult() {

	ScopedPointer<HeadDataReader> head_read(new HeadDataReader(head_info_));
	head_read->Open();

	int64_t trace_num = result_arr_[0].size();
	for(int slct_i = 1; slct_i < result_arr_.size(); ++slct_i) {
		if(trace_num != result_arr_[slct_i].size()) {
			printf("Selector %s and %s has different result size !\n", index_type_arr_[0].c_str(), index_type_arr_[slct_i].c_str());

			for(int64_t i = 0; i < result_arr_[0].size(); ++i) {
				int64_t trace_id = result_arr_[0][i];
				printf("Selector : %s, index entry[%lld] is %lld, head is : ",
						index_type_arr_[0].c_str(), i, trace_id);
				head_read->PrintHead(trace_id);

			}

			for(int64_t i = 0; i < result_arr_[slct_i].size(); ++i) {

				int64_t trace_id = result_arr_[slct_i][i];
				printf("Selector : %s, index entry[%lld] is %lld, head is : ",
						index_type_arr_[slct_i].c_str(), i, trace_id);
				head_read->PrintHead(trace_id);

			}

			return false;
		}
	}

#if 1
	for(int64_t trace_i = 0; trace_i < trace_num; ++trace_i) {
		for(int selector_i = 1; selector_i < result_arr_.size(); ++selector_i) {
			if(result_arr_[selector_i][trace_i] != result_arr_[selector_i - 1][trace_i]) {
				printf("Selector %s and %s has different result at %lld\n",
						index_type_arr_[selector_i].c_str(), index_type_arr_[selector_i - 1].c_str(), trace_i);


				int64_t begin = trace_i - 100  >0 ? trace_i - 100 : 0;
				int64_t end = trace_i + 100 < trace_num ? trace_i + 100 : trace_num - 1;


				for(int64_t i = begin; i <= end; ++i) {
					int64_t trace_id = result_arr_[selector_i][i];
					printf("Selector : %s, index entry[%lld] is %lld, head is : ",
							index_type_arr_[selector_i].c_str(), i, trace_id);
					head_read->PrintHead(trace_id);

				}

				for(int64_t i = begin; i <= end; ++i) {

					int64_t trace_id = result_arr_[selector_i - 1][i];
					printf("Selector : %s, index entry[%lld] is %lld, head is : ",
							index_type_arr_[selector_i - 1].c_str(), i, trace_id);
					head_read->PrintHead(trace_id);

				}

				return false;
			}
		}
	}
#endif
	head_read->Close();

	return true;
}


void TestUnit::GenerateRowFilter(int key_id, float ratio, SharedPtr<RowFilterByKey> row_filter) {
	struct timeval t;
	gettimeofday(&t, NULL);
	int64_t seed = t.tv_sec * 1000000 + t.tv_usec;
	srand(seed);

	int key_value_num = JobPara::GetValueNumber(key_id);
	int filter_trace_num = ratio * key_value_num;

	int group_num = (rand() % 5) + 1;
	while(group_num > filter_trace_num) {
		group_num --;
	}
	int trace_num_per_group = filter_trace_num / group_num;

	for(int i = 0; i < group_num; ++i) {
		int start = rand() % key_value_num;
		int end = start + trace_num_per_group - 1;
		row_filter->FilterBy(Key(key_id, start, end, 1, 1));
//		printf("Filter by key : %d, start : %d, end : %d\n",key_id, start, end);
	}
}



