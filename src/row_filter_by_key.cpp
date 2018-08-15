/*
 * seiscache_row_filter_by_key.cpp
 *
 *  Created on: Apr 14, 2018
 *      Author: wyd
 */

#include "row_filter_by_key.h"

#include <algorithm>
#include <set>
#include "data_type.h"
#include "head_data.h"
#include "util/util_log.h"

static int FindKey(const std::vector<int> &arr_filter, int key_id) {
	for(int filter_i = 0; filter_i < arr_filter.size(); ++filter_i) {
		if(arr_filter[filter_i] == key_id) {
			return filter_i;
		}
	}
	return -1;
}

int RowFilterByKey::FindKey(const std::vector<KeyRuleStruct> &rule_table, int key_id) {
	for(int filter_i = 0; filter_i < rule_table.size(); ++filter_i) {
		if(rule_table[filter_i].key_id == key_id) {
			return filter_i;
		}
	}
	return -1;
}

struct FilterRuleComp {
	FilterRuleComp(KeyType key_type) :
		key_type_(key_type) {

	}
	bool operator() (const Key &key1, const Key &key2) const  {
		return KeyLessThan(key1.GetStartValueP(), key2.GetStartValueP(), key_type_);
	}

	KeyType key_type_;
};

int RowFilterByKey::FindKeyId(int key_id) const {
	int iret = 0;
	for(; iret < rule_table_.size(); ++iret) {
		if(rule_table_[iret].key_id == key_id)
			break;
	}
	return iret;
}

void RowFilterByKey::operator=(const RowFilterByKey &row_filter) {
	rule_table_ = row_filter.rule_table_;
	head_info_ = row_filter.head_info_;
	valid_key_num_ = row_filter.valid_key_num_;
}

RowFilterByKey& RowFilterByKey::FilterBy(const Key& key) {
	int key_id = key.GetKeyID();

	int pos = FindKeyId(key_id);
	if(pos == rule_table_.size()) {
		KeyRuleStruct rule_struct;
		rule_struct.key_id = key_id;
		rule_struct.rule_arr.push_back(key);
		rule_table_.push_back(rule_struct);
	} else {
		rule_table_[pos].rule_arr.push_back(key);
	}

	return *this;
}

void RowFilterByKey::Print() {
	printf("Filter by : \n");
	for(int key_i = 0; key_i < rule_table_.size(); ++key_i) {
		printf("key_id : %d, ", rule_table_[key_i].key_id);
		for(int rule_i = 0; rule_i < rule_table_[key_i].rule_arr.size(); ++rule_i) {
			int start = *(int*)(rule_table_[key_i].rule_arr[rule_i].GetStartValueP());
			int end = *(int*)(rule_table_[key_i].rule_arr[rule_i].GetEndValueP());
			printf("| %d ~ %d | ", start, end);
		}
		printf("\n");
	}
}


std::vector<int> RowFilterByKey::GetKeyIDArr() const {
	std::vector<int> ret_arr;
	for(std::vector<KeyRuleStruct>::const_iterator it = rule_table_.begin(); it != rule_table_.end(); ++it) {
		ret_arr.push_back(it->key_id);
	}

	return ret_arr;
}

std::vector<Key> RowFilterByKey::GetFilterRuleOf(int key_id) const {
	int pos = FindKeyId(key_id);
	return rule_table_[pos].rule_arr;
}


void RowFilterByKey::SetHeadInfo(HeadInfo *head_info) {
	head_info_ = head_info;
}


void RowFilterByKey::SortFilterKeys(const std::vector<int> &arr_order) {  	//TODO how to order the row filter
//	std::vector<KeyRuleStruct> sort_result;
//	std::vector<int> arr_filter = GetKeyIDArr();

	for(int order_i = 0, sort_filter_i = 0;
			order_i < arr_order.size() && sort_filter_i < rule_table_.size();
			++order_i, ++sort_filter_i) {

//		printf("arr_filter[%d] = %d, arr_order[%d] = %d\n",
//				sort_filter_i, rule_table_[sort_filter_i].key_id,
//				order_i, arr_order[order_i]);
		if(rule_table_[sort_filter_i].key_id != arr_order[order_i]) {
			int find_filter_i = FindKey(rule_table_, arr_order[order_i]);
//			printf("find_filter_i = %d\n", find_filter_i);
			KeyRuleStruct tmp = rule_table_[sort_filter_i];
			rule_table_[sort_filter_i] = rule_table_[find_filter_i];
			rule_table_[find_filter_i] = tmp;
		}
//		int find_filter_i = FindKey(arr_filter, arr_order[order_i]);
//		sort_result.push_back(rule_table_[find_filter_i]);
	}

	std::vector<int> tmp_arr = GetKeyIDArr();
	printf("after sort : \n");
	for(int i = 0; i < tmp_arr.size(); ++i) {
		printf("%d, ", tmp_arr[i]);
	}
	printf("\n");

//	rule_table_ = sort_result;

}


void RowFilterByKey::AddOrderAndSort(const std::vector<int> &arr_order) {
	SortFilterByFetchNum();

	std::vector<KeyRuleStruct> tmp_rule_table;
	for(int order_i = 0; order_i < arr_order.size(); ++order_i) {
		int order_key_id = arr_order[order_i];
		int find_filter_i = FindKey(rule_table_, order_key_id);
		if(0 > find_filter_i) {
			KeyRuleStruct rule_tmp;
			rule_tmp.key_id = order_key_id;
			tmp_rule_table.push_back(rule_tmp);
		} else {
			tmp_rule_table.push_back(rule_table_[find_filter_i]);
		}
	}

	for(int filter_i = 0; filter_i < rule_table_.size(); ++filter_i) {
		int filter_key_id = rule_table_[filter_i].key_id;
		int find_filter_i = FindKey(tmp_rule_table, filter_key_id);
		if(0 > find_filter_i) {
			tmp_rule_table.push_back(rule_table_[filter_i]);
		}
	}
	rule_table_ = tmp_rule_table;


	std::vector<int> tmp_arr = GetKeyIDArr();
	printf("after sort : \n");
	for(int i = 0; i < tmp_arr.size(); ++i) {
		printf("%d, ", tmp_arr[i]);
	}
	printf("\n");
}

void RowFilterByKey::SortFilterByFetchNum() {
	bool flag = false;
	while(1) {
		int swap_time = 0;
		for(int i = 0; i < rule_table_.size() - 1; ++i) {
			int key1_fetch_num = GetFetchNum(rule_table_[i].rule_arr);
			int key2_fetch_num = GetFetchNum(rule_table_[i + 1].rule_arr);
//			printf("key1 : %d, fetch_num = %d, key2 : %d, fetch_num = %d\n",
//					rule_table_[i].key_id, key1_fetch_num, rule_table_[i + 1].key_id, key2_fetch_num);
			if(key1_fetch_num > key2_fetch_num) {
				KeyRuleStruct tmp = rule_table_[i];
				rule_table_[i] = rule_table_[i + 1];
				rule_table_[i + 1] = tmp;
				swap_time ++;
			}
		}
		if(swap_time == 0)
			break;
	}
}
int RowFilterByKey::GetFetchNum(const RuleArr &rule_arr) {
	int fetch_num = 0;
	for(int i = 0; i < rule_arr.size(); ++i) {
		fetch_num += rule_arr[i].GetFetchNum();
	}
	return fetch_num;
}

int RowFilterByKey::GetValidKeyNum() const {
	int ret = 0;
	for(int i = 0; i < rule_table_.size(); ++i) {
		if(!rule_table_[i].rule_arr.empty()) {
			ret ++;
		}
	}
	return ret;
}

void RowFilterByKey::AddOrder(const std::vector<int> &arr_order) {
	SortFilterByFetchNum();
	for(int i = 0; i < arr_order.size(); ++i) {
		int order_key_id = arr_order[i];
		int find_i = FindKey(rule_table_, order_key_id);
		if(0 > find_i) {
			KeyRuleStruct rule_tmp;
			rule_tmp.key_id = order_key_id;
			rule_table_.push_back(rule_tmp);
		}
	}

	std::vector<int> tmp_arr = GetKeyIDArr();
	printf("after Add : \n");
	for(int i = 0; i < tmp_arr.size(); ++i) {
		printf("%d, ", tmp_arr[i]);
	}
	printf("\n");


}


void RowFilterByKey::Normalize() {
	for(std::vector<KeyRuleStruct>::iterator it_of_keys = rule_table_.begin();
			it_of_keys != rule_table_.end(); ++it_of_keys) {
		int key_id = it_of_keys->key_id;
		KeyType key_type = head_info_->GetKeyType(key_id);
		RuleArr rule_arr_of_key = it_of_keys->rule_arr;
		std::sort(rule_arr_of_key.begin(), rule_arr_of_key.end(), FilterRuleComp(key_type));

		std::set<int> erase_eles;
		for(int i = 1; i < rule_arr_of_key.size(); ++i) {
			for(int j = 0; j < i; ++j) {
				if(!KeyLessThan(rule_arr_of_key[j].GetEndValueP(), rule_arr_of_key[i].GetStartValueP(), key_type)) {
					if(KeyLessThan(rule_arr_of_key[j].GetEndValueP(), rule_arr_of_key[i].GetEndValueP(), key_type)) {
						memcpy(rule_arr_of_key[j].GetEndValueP(), rule_arr_of_key[i].GetEndValueP(), head_info_->GetKeySize(key_id));
					}
					erase_eles.insert(i);

				}
			}
		}

		it_of_keys->rule_arr.clear();
		for(int i = 0; i < rule_arr_of_key.size(); ++i) {
			if(erase_eles.find(i) == erase_eles.end()) {
				it_of_keys->rule_arr.push_back(rule_arr_of_key[i]);
			}
		}

		printf("key id = %d, After Normalize : \n", key_id);
		for(int i = 0; i < it_of_keys->rule_arr.size(); ++i) {
			printf("| start : %d, end : %d | ", *(int*)(it_of_keys->rule_arr[i].GetStartValueP()),
					*(int*)(it_of_keys->rule_arr[i].GetEndValueP()));
		}
		printf("\n");
	}
}


bool RowFilterByKey::IsKeyOtherValid(char *key_buf) { //TODO deal with empty rules? deal with inc and group/tolerance
	char *p = key_buf;
	for(int i = 1; i < rule_table_.size(); ++i) {
		int key_id = rule_table_[i].key_id;
		KeyType key_type = head_info_->GetKeyType(key_id);
		int key_size = head_info_->GetKeySize(key_id);

		bool valid_this_key = false;
		for(std::vector<Key>::iterator it = rule_table_[i].rule_arr.begin(); it != rule_table_[i].rule_arr.end(); ++it) {
			char *start_value_p = it->GetStartValueP();
			char *end_value_p = it->GetEndValueP();

			if(!(KeyLessThan(p, start_value_p, key_type) || KeyLessThan(end_value_p, p, key_type))) {
				valid_this_key = true;
				break;
			}
		}

		if(!valid_this_key)
			return false;

		p += key_size;
	}

	return true;
}


bool RowFilterByKey::IsHeadValid(char *head_buf) {
	for(int i = 0; i < rule_table_.size(); ++i) {
		int key_id = rule_table_[i].key_id;
		KeyType key_type = head_info_->GetKeyType(key_id);
		int key_size = head_info_->GetKeySize(key_id);
		int key_offset = head_info_->GetKeyOffset(key_id);

		bool valid_this_key = false;
		char *p = head_buf + key_offset;
		for(std::vector<Key>::iterator it = rule_table_[i].rule_arr.begin(); it != rule_table_[i].rule_arr.end(); ++it) {
			char *start_value_p = it->GetStartValueP();
			char *end_value_p = it->GetEndValueP();

			if(!(KeyLessThan(p, start_value_p, key_type) || KeyLessThan(end_value_p, p, key_type))) {
				valid_this_key = true;
				break;
			}
		}

		if(!valid_this_key)
			return false;
	}
	return true;
}


/*
 * if arr_filter is contained in arr_order, return 1;
 * if arr_order is contained in arr_filter, return -1;
 * else return 0;
 */

int IsArrContained(const std::vector<int> &arr_filter, const std::vector<int> &arr_order) {

	int size_filter = arr_filter.size();
	int size_order = arr_order.size();

	if(size_filter <= size_order) {
//		int filter_num = size_filter;
		for(int order_i = 0; order_i < size_order; ++order_i) {
			if(0 <= FindKey(arr_filter, arr_order[order_i])) {
				size_filter --;
				if(size_filter <= 0) {
					return 1;
				}
			} else {
				return 0;
			}
		}
	} else {
		for(int order_i = 0; order_i < size_order; ++order_i) {
			if(0 > FindKey(arr_filter, arr_order[order_i])) {
				return 0;
			}
		}
		return -1;
	}
}


