/*
 * index_order_reader.h
 *
 *  Created on: May 7, 2018
 *      Author: wyd
 */

#ifndef SELECTOR_ORDER_HHJ_H_
#define SELECTOR_ORDER_HHJ_H_


#include "selector.h"
#include "index_config.h"
#include "index_order/index_order_config.h"

class HeadInfo;
class IndexOrderReader;


class SelectorOrderHHJ : public Selector{
public:
	SelectorOrderHHJ(HeadInfo *head_info);
	virtual ~SelectorOrderHHJ();

	virtual std::vector<int64_t> GetTraces();

	virtual int64_t GetGatherNum();
	virtual Gather *GetGather(int gather_id);

	virtual void RecordPrivate(MetricManager *metric_manager);

private:
	IndexOrderReader *OpenIndexReader(const IndexAttr &index_attr, HeadInfo *head_info);
	void InitStatistics();
	bool IsNewGather(const IndexElement &my_index);
	virtual Index *GetIndexInstance(const IndexAttr &attr, HeadInfo *head_info);
	void TraceSelect(IndexOrderReader *index_reader, IndexElement *p_ele, std::vector<int64_t> *p_result_arr);

	std::vector<int64_t> gather_begin_id_;
	IndexElement gather_flag_;
//	IndexAttr real_attr_;
	int index_size_;
	float read_head_size_, read_head_time_;
};



#endif /* SELECTOR_ORDER_HHJ_H_ */
