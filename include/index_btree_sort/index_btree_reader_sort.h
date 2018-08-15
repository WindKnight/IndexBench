/*
 * index_btree_reader_sort.h
 *
 *  Created on: Jul 30, 2018
 *      Author: wyd
 */

#ifndef INDEX_BTREE_READER_SORT_H_
#define INDEX_BTREE_READER_SORT_H_

#include "index_btree/index_btree_reader.h"

class IndexBTreeReaderSort : public IndexBTreeReader {
public:
	IndexBTreeReaderSort(const IndexAttr &index_attr, HeadInfo *head_info);
	virtual ~IndexBTreeReaderSort();

	virtual bool GetValidIndex(const RowFilterByKey &row_filter, std::vector<IndexElement> *index_ele_arr);
protected:

	bool SearchBTree(int index_level, const IndexElement &ele, const RuleArr &rule_arr);
	bool LoadValidTraces(int index_level, const RuleArr &rule_arr);
	bool ReadValidTraceID(int index_level, int64_t trace_id_begin_off, int64_t trace_num_this_tree,
			int64_t begin_trace_id_idx, int64_t end_trace_id_idx);
	virtual void RecordSearchResult(int index_level, const std::vector<IndexElement> &ele_head_arr, const std::vector<int64_t> &result_arr);

	typedef std::vector<IndexElement> ValidEleArr;
	std::vector<ValidEleArr> valid_ele_table_;
	std::vector<int> index_key_size_arr_;
};


#endif /* INDEX_BTREE_READER_SORT_H_ */
