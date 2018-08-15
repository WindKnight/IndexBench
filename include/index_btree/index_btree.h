/*
 * index_btree.h
 *
 *  Created on: May 17, 2018
 *      Author: wyd
 */

#ifndef INDEX_BTREE_H_
#define INDEX_BTREE_H_

#include "index.h"

class IndexBTreeWriter;

class IndexBTree : public Index {
public:
	IndexBTree(const IndexAttr &index_attr, HeadInfo *head_info);
	virtual ~IndexBTree();

	virtual bool BuildIndex();
	virtual bool Remove();

private:
	virtual IndexBTreeWriter *OpenWriter(const IndexAttr &index_attr, HeadInfo *head_info);
};



#endif /* INDEX_BTREE_H_ */
