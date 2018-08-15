/*
 * order.h
 *
 *  Created on: Apr 13, 2018
 *      Author: wyd
 */

#ifndef ORDER_H_
#define ORDER_H_


#include <vector>

class IndexOrder;

class Order {
public:

	Order();
	Order (const Order &order);
	~Order();

	Order& operator=(const Order &order);

	Order& OrderBy(int key_id, bool ascending = true, bool gather_flag = false);
	void Print();

	inline std::vector<int> GetKeyPosArr() const {
		return key_pos_arr_;
	}
	inline std::vector<bool> GetAscArr() {
		return asc_arr_;
	}
	inline bool IsAsc(int order_id) {
		return asc_arr_[order_id];
	}
	inline int GetGatherPos() {
		return gather_pos_;
	}

private:

	friend class Selector;
	friend class SelectorOrderHHJ;
	friend class SelectorBTree;
	friend class SelectorBTreeSeperate;
	friend class SelectorBTreeSort;
	friend class SelectorBTreeNoSort;

private:
	std::vector<int> key_pos_arr_;
	std::vector<bool> asc_arr_;
	int gather_pos_;
};



#endif /* ORDER_H_ */
