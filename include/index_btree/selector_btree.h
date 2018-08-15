/*
 * selector_btree.h
 *
 *  Created on: May 23, 2018
 *      Author: wyd
 */

#ifndef SELECTOR_BTREE_H_
#define SELECTOR_BTREE_H_


#include "selector.h"
#include "index_config.h"
#include <stdint.h>

class HeadInfo;
class IndexBTreeReader;
class Bitmap;

class SelectorBTree : public Selector {
public:
	SelectorBTree(HeadInfo *head_info);
	virtual ~SelectorBTree();

	virtual std::vector<int64_t>  GetTraces();

	virtual int64_t GetGatherNum();
	virtual Gather *GetGather(int gather_id);

private:

	virtual Index *GetIndexInstance(const IndexAttr &attr, HeadInfo *head_info);

protected:
	void InitStatistics();

	virtual void RecordPrivate(MetricManager *metric_manager);
	virtual bool GetAllTracesByOrder(const IndexAttr &index_attr, SharedPtr<Bitmap> bit_map, std::vector<int64_t> *p_valid_trace_arr);
	virtual IndexBTreeReader *OpenBTreeReader(const IndexAttr &index_attr, HeadInfo *head_info);

	virtual bool GetValidTracesWithFilter(bool strict_key_order, const RowFilterByKey &row_filter, std::vector<int64_t> *p_valid_trace_arr);

	Bitmap *BuildBitmap(const std::vector<int64_t> &valid_trace_arr);
};


#endif /* SELECTOR_BTREE_H_ */
