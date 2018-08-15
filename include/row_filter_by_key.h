/*
 * seiscache_row_filter_by_key.h
 *
 *  Created on: Apr 12, 2018
 *      Author: wyd
 */

#ifndef ROW_FILTER_BY_KEY_H_
#define ROW_FILTER_BY_KEY_H_

#include "key.h"
#include <map>

class HeadInfo;
typedef std::vector<Key> RuleArr;


class RowFilterByKey {
public:
	RowFilterByKey() {}
	~RowFilterByKey() {}

	void operator=(const RowFilterByKey &row_filter);
	RowFilterByKey& FilterBy(const Key& key);
	void Print();

	friend class SelectorOrderHHJ;
	friend class SelectorBTree;
	friend class SelectorBTreeSeperate;
	friend class Selector;
	friend class SelectorBTreeSort;
	friend class SelectorBTreeNoSort;
	friend class IndexBTreeReader;
	friend class SelectorSql;
	friend class IndexSqlReader;

private:
	struct KeyRuleStruct {
		int key_id;
		RuleArr rule_arr;
	};

	std::vector<KeyRuleStruct> rule_table_;
	HeadInfo *head_info_;
	int valid_key_num_;


private:
	std::vector<int> GetKeyIDArr() const;
	std::vector<Key> GetFilterRuleOf(int key_id) const;
	int GetValidKeyNum() const ;
	int GetFetchNum(const RuleArr &rule_arr);
	bool IsHeadValid(char *head_buf);

	void SetHeadInfo(HeadInfo *head_info);
	void SortFilterKeys(const std::vector<int> &arr_order);
	void AddOrderAndSort(const std::vector<int> &arr_order);
	void AddOrder(const std::vector<int> &arr_order);
	void SortFilterByFetchNum();
	void Normalize();
	int FindKey(const std::vector<KeyRuleStruct> &rule_table, int key_id);
	int FindKeyId(int key_id) const;

	/*
	 * only used by order HHJ;
	 */
	bool IsKeyOtherValid(char *key_buf);

};


int IsArrContained(const std::vector<int> &arr_filter, const std::vector<int> &arr_order);



#endif /* ROW_FILTER_BY_KEY_H_ */
