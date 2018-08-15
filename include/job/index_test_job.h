/*
 * index_test_job.h
 *
 *  Created on: Jun 10, 2018
 *      Author: wyd
 */

#ifndef INDEX_TEST_JOB_H_
#define INDEX_TEST_JOB_H_

#include "util/util_shared_pointer.h"
#include "order.h"
#include "row_filter_by_key.h"
#include <vector>
#include <stdint.h>
#include "selector.h"



class HeadInfo;


struct TestUnit {
public:
	TestUnit() {
		order = new Order();
		row_filter = new RowFilterByKey();
	}
	SharedPtr<Order> order;
	SharedPtr<RowFilterByKey> row_filter;
public:
	void SetFilter(int key_id, float ratio) {
		GenerateRowFilter(key_id, ratio, row_filter);
	}

	void GenerateRowFilter(int key_id, float ratio, SharedPtr<RowFilterByKey> row_filter);
};



class IndexTestJob {
public:

	IndexTestJob(HeadInfo *head_info);
	~IndexTestJob();

	void AddIndexType(const std::string &index_type);
	void AddTestUnit(const TestUnit &test_unit);

	void GenerateWorkload(int test_num);
	bool Start();

private:
	void GenerateOrder(SharedPtr<Order> order);
	void GenerateFilter(TestUnit *p_test_unit);
	bool CheckResult();

	HeadInfo *head_info_;
	int64_t total_trace_num_;

	std::vector<std::string> index_type_arr_;
	std::vector<Selector *> selector_arr_;
	std::vector<MetricManager *> metric_man_arr_;
	std::vector<TestUnit> test_unit_arr_;
	std::vector<std::vector<int64_t> > result_arr_;

};



#endif /* INDEX_TEST_JOB_H_ */
