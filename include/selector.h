/*
 * index_reader.h
 *
 *  Created on: Apr 29, 2018
 *      Author: wyd
 */

#ifndef SELECTOR_H_
#define SELECTOR_H_

#include <stdint.h>
#include <vector>
#include "util/util_shared_pointer.h"
#include "row_filter_by_key.h"
#include "order.h"


#define PUB		0
#define PRI		1

class HeadInfo;
class Index;
struct IndexAttr;

typedef std::vector<std::string> MetricArr;
typedef std::map<std::string, MetricArr> MetricTable;


class MetricManager {
public:
	MetricManager(const std::string &selector_type);
	~MetricManager();

#if 0
	void SetFetchedTraceNum(int64_t trace_num);
	void SetFetchRatio(float fetch_ratio);
	void SetFetchTime(float fetch_time);
	void SetTotalTime(float total_time);
	void SetTotalReadSize(float read_size);
	void SetTotalReadTime(float read_time);
	void SetReadSpeed(float speed);
	void SetBuildIndexNum(int build_num);
	void SetBuildIndexTime(float build_time);
#endif

	void SetDouble(const std::string &name, double value, char type);
	void SetFloat(const std::string &name, float value, char type);
	void SetLong(const std::string &name, int64_t value, char type);
	void SetInt(const std::string &name, int value, char type);


	void PrintAll();

protected:

	void PrintPublicHead();
	void PrintPublic(int test_round);

	void PrintPrivateHead();
	void PrintPrivate(int test_round);


	std::string selector_type_;

	MetricTable metric_tab_public_;
	MetricTable metric_tab_private_;


#if 0
	std::vector<int64_t> fetched_trace_num_arr_;
	std::vector<float> fetch_ratio_arr_;
	std::vector<float> fetch_time_arr_;
	std::vector<float> total_time_arr_;
	std::vector<float> total_read_size_arr_; //MB
	std::vector<float> total_read_time_arr_;
	std::vector<float> read_speed_arr_;  //MB/s
	std::vector<int> build_index_num_arr_;
	std::vector<float> build_index_time_arr_;
#endif

private:

};


class Gather {
public:
	Gather(const std::vector<int64_t> trace_id_arr);
	virtual ~Gather();

	virtual int64_t GetTraceNum();
	virtual std::vector<int64_t> GetAll();
private:
	std::vector<int64_t> trace_id_arr_;
};


class Selector {
public:
	static Selector *GetInstance(const std::string &index_type, HeadInfo *head_info);
	Selector(HeadInfo *head_info);
	~Selector();

	void SetRowFilter(const RowFilterByKey &row_filter);
	void SetOrder(const Order &order);

	double GetFetchTime() {
		return fetch_time_;
	}

	MetricManager *GetMetricManager(const std::string &index_type);

	virtual std::vector<int64_t>  GetTraces() = 0;

	virtual int64_t GetGatherNum() = 0;
	virtual Gather *GetGather(int gather_id) = 0;
	virtual Index *GetIndexInstance(const IndexAttr &attr, HeadInfo *head_info) = 0;

	void RecordMetric(MetricManager *metric_manager);

protected:
	IndexAttr GetRealOrderAttr(const std::string &index_type);
	bool BuildIndex(const IndexAttr &attr);
	void CheckOrderAndFilter(bool &valid_order, bool &valid_filter);
	void InitPublicStat();

	void FetchAll(std::vector<int64_t> *p_valid_trace_arr);

	virtual void RecordPrivate(MetricManager *metric_manager) = 0;

    HeadInfo *head_info_;
    RowFilterByKey row_filter_;
    Order order_;

	int64_t total_trace_num_;

	/*
	 * statistic
	 */
    double total_time_, fetch_time_, extra_time_;
	float read_size_, read_time_;
    int build_index_num_;
    float build_index_time_;
	int64_t fetched_trace_num_;

private:
	void RecordPublic(MetricManager *metric_manager);

};


#endif /* SELECTOR_H_ */
