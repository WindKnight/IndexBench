/*
 * seiscache_key.cpp
 *
 *  Created on: Apr 13, 2018
 *      Author: wyd
 */

#include "key.h"

#include "head_data.h"
#include <string.h>
#include <stdio.h>


Key::Key() {
	group_ = 0;
	tolerance_ = 0.0;
	values_ = NULL;
	fetch_num_ = 0;
}


Key::~Key() {
	if(NULL != values_) {
		delete values_;
		values_ = NULL;
	}
}


Key::Key(const Key& key) {
	values_ = NULL;
	group_ = 0;
	tolerance_ = 0.0;
	*this = key;
}



Key& Key::operator=(const Key& key) {
	key_operator_ = key.key_operator_;
	key_id_ = key.key_id_;
	if(NULL != values_) {
		delete values_;
		values_ = NULL;
	}
	key_size_ = key.key_size_;

	int total_size;
	switch(key.key_operator_) {
	case EQUAL :
		total_size = key_size_;
		break;
	case RANGE_GROUP :
		total_size = key_size_ * 3;
		group_ = key.group_;
		break;
	case RANGE_TOLERANCE:
		total_size = key_size_ * 3;
		tolerance_ = key.tolerance_;
		break;
	default:
		return *this;
		break;
	}

	values_ = new char[total_size];
	memcpy(values_, key.values_, total_size);

	fetch_num_ = key.fetch_num_;

	return *this;
}


Key::KeyOperator Key::GetOperator() const {
	return key_operator_;
}


int Key::GetKeyID() const {
	return key_id_;
}


