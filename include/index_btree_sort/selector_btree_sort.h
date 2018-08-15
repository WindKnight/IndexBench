/*
 * selector_btree_sort.h
 *
 *  Created on: Jul 30, 2018
 *      Author: wyd
 */

#ifndef SELECTOR_BTREE_SORT_H_
#define SELECTOR_BTREE_SORT_H_

#include "index_btree/selector_btree.h"

class IndexBTreeReaderSort;



class SelectorBTreeSort : public SelectorBTree {
public:
	SelectorBTreeSort(HeadInfo *head_info);
	virtual ~SelectorBTreeSort();

	virtual std::vector<int64_t>  GetTraces();
private:
	virtual void RecordPrivate(MetricManager *metric_manager);
	virtual IndexBTreeReaderSort *OpenBTreeReaderSort(const IndexAttr &index_attr, HeadInfo *head_info);
	virtual bool GetValidTracesWithFilter(const RowFilterByKey &row_filter, std::vector<int64_t> *p_valid_trace_arr);

	static float sort_time_;
};





#endif /* SELECTOR_BTREE_SORT_H_ */
