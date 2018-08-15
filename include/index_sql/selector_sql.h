/*
 * selector_sql.h
 *
 *  Created on: Aug 11, 2018
 *      Author: wangyida
 */

#ifndef SELECTOR_SQL_H_
#define SELECTOR_SQL_H_

#include "selector.h"

class IndexSqlReader;

class SelectorSql : public Selector {
public:
	SelectorSql(HeadInfo *head_info);
	virtual ~SelectorSql();

	virtual std::vector<int64_t>  GetTraces();
	virtual int64_t GetGatherNum();
	virtual Gather *GetGather(int gather_id);
	virtual Index *GetIndexInstance(const IndexAttr &attr, HeadInfo *head_info);

protected:
	virtual void RecordPrivate(MetricManager *metric_manager);

	void InitStatistics();
	bool GetAllTracesByOrder(std::vector<int64_t> *p_valid_trace_arr);
	bool GetValidTracesWithFilter(std::vector<int64_t> *p_valid_trace_arr);

	IndexSqlReader *OpenSqlReader(HeadInfo *head_info);
};


#endif /* SELECTOR_SQL_H_ */
