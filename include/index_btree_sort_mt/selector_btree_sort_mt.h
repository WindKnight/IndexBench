/*
 * selector_btree_sort_mt.h
 *
 *  Created on: Aug 12, 2018
 *      Author: wangyida
 */

#ifndef SELECTOR_BTREE_SORT_MT_H_
#define SELECTOR_BTREE_SORT_MT_H_


#include "index_btree_sort/selector_btree_sort.h"

class SelectorBTreeSortMT : public SelectorBTreeSort {
public:
	SelectorBTreeSortMT(HeadInfo *head_info);
	virtual ~SelectorBTreeSortMT();

private:
	virtual IndexBTreeReaderSort *OpenBTreeReaderSort(const IndexAttr &index_attr, HeadInfo *head_info);
};

#endif /* SELECTOR_BTREE_SORT_MT_H_ */
