/*
 * seiscache_order.cpp
 *
 *  Created on: Apr 13, 2018
 *      Author: wyd
 */

#include "order.h"

#include "util/util_log.h"
#include <stdio.h>



Order::Order() {
	gather_pos_ = -1;
}

Order::Order (const Order &order) {
	*this = order;
}

Order::~Order() {

}


Order& Order::operator=(const Order &order) {
	key_pos_arr_ = order.key_pos_arr_;
	asc_arr_ = order.asc_arr_;
	gather_pos_ = order.gather_pos_;
	return *this;
}


Order& Order::OrderBy(int key_id, bool ascending, bool gather_flag) {
	Order null_ret;
	key_pos_arr_.push_back(key_id);
	asc_arr_.push_back(ascending);
	if(gather_flag == true) {
		if(gather_pos_ > 0) {
			Err("gather_flag has been set already\n");
			return null_ret;
		} else {
			gather_pos_ = key_pos_arr_.size() - 1;
		}
	}

	return *this;
}

void Order::Print() {
	printf("Order by : ");
	for(int i = 0; i < key_pos_arr_.size(); ++i) {
		printf("%s%d", i == 0 ? "" : ",", key_pos_arr_[i]);
	}
	printf("\n");
}



