/*
 * selector_btree_sort_mt.cpp
 *
 *  Created on: Aug 12, 2018
 *      Author: wangyida
 */


#include "index_btree_sort_mt/selector_btree_sort_mt.h"
#include "index.h"
//#include "index_btree_sort/index_btree_reader_sort.h"
#include "index_btree_sort_mt/index_btree_reader_sort_mt.h"


SelectorBTreeSortMT::SelectorBTreeSortMT(HeadInfo *head_info) :
	SelectorBTreeSort(head_info){

}
SelectorBTreeSortMT::~SelectorBTreeSortMT() {

}

IndexBTreeReaderSort *SelectorBTreeSortMT::OpenBTreeReaderSort(const IndexAttr &index_attr, HeadInfo *head_info) {
	IndexAttr tmp_attr = Index::FindIndex(index_attr, INDEX_TYPE_BTREE);
	if(tmp_attr.keypos_arr.empty()) {
		if(!BuildIndex(index_attr)) {
			Err("Build btree error\n");
			return NULL;
		}
	}
	IndexBTreeReaderSort *index_btree_reader_sort_mt = new IndexBTreeReaderSortMT(index_attr, head_info);
	if(NULL == index_btree_reader_sort_mt) {
		Err("new btree reader error\n");
		return NULL;
	}
	if(!index_btree_reader_sort_mt->Open()) {
		Err("open btree reader error\n");
		return NULL;
	}
	return index_btree_reader_sort_mt;
}
