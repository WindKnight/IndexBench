/*
 * selector_btree_nosort.h
 *
 *  Created on: Jul 30, 2018
 *      Author: wyd
 */

#ifndef SELECTOR_BTREE_NOSORT_H_
#define SELECTOR_BTREE_NOSORT_H_


#include "index_btree/selector_btree.h"


class SelectorBTreeNoSort : public SelectorBTree {
public:
	SelectorBTreeNoSort(HeadInfo *head_info);
	virtual ~SelectorBTreeNoSort();

	virtual std::vector<int64_t>  GetTraces();

private:
	virtual void RecordPrivate(MetricManager *metric_manager);


};


#endif /* SELECTOR_BTREE_NOSORT_H_ */
