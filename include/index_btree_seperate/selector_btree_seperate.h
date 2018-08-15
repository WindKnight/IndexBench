/*
 * selector_btree_seperate.h
 *
 *  Created on: Jul 19, 2018
 *      Author: wyd
 */

#ifndef SELECTOR_BTREE_SEPERATE_H_
#define SELECTOR_BTREE_SEPERATE_H_


#include "index_btree/selector_btree.h"

class SelectorBTreeSeperate : public SelectorBTree {
public:
	SelectorBTreeSeperate(HeadInfo *head_info);
	virtual ~SelectorBTreeSeperate();

	virtual std::vector<int64_t>  GetTraces();
private:
	SharedPtr<Bitmap> BuildBitmapWithFilter();

};

#endif /* SELECTOR_BTREE_SEPERATE_H_ */
