/*
 * seiscache_key.h
 *
 *  Created on: Apr 12, 2018
 *      Author: wyd
 */

#ifndef KEY_H_
#define KEY_H_

#include "data_type.h"
#include <stdio.h>
#include <string.h>


class Key {
public:
	enum KeyOperator {
		NONE			= -1,
		EQUAL 			= 0,
		RANGE_GROUP 	= 1,
		RANGE_TOLERANCE = 2,
	};

	Key();

	~Key();

	Key(const Key& key);

	template <class KTYPE>
	Key(int key_id, KTYPE value) {
		values_ = NULL;
		group_ = 0;
		tolerance_ = 0.0;
//		SetKey(key_id, value);
		SetKey(key_id, value, value, 0, 0);
	}

	template <class KINT>
	Key(int key_id, KINT start_value, KINT end_value, KINT inc, int group = 1) {
		values_ = NULL;
		group_ = 0;
		tolerance_ = 0.0;
		SetKey(key_id, start_value, end_value, inc, group);
	}

	template <class KFLOAT>
	Key(int key_id, KFLOAT start_value, KFLOAT end_value, KFLOAT inc, float tolerance = 0.0f) {
		values_ = NULL;
		group_ = 0;
		tolerance_ = 0.0;
		SetKey(key_id, start_value, end_value, inc, tolerance);
	}

	Key& operator=(const Key& key);


	template <typename KTYPE>
	void SetKey(int key_id, KTYPE value) {
		key_operator_ = EQUAL;
		key_id_ = key_id;

		if(NULL != values_) {
			delete values_;
			values_ = NULL;
		}

		key_size_ = sizeof(value);
		values_ = new char[key_size_];
//		((KTYPE*)values_)[0] = value;
		memcpy(values_, &value, key_size_);

		fetch_num_ = 1;
	}


	/*
	 * int key filter
	 */


	template <class KINT>
	void SetKey(int key_id, KINT start_value, KINT end_value, KINT inc, int group = 1) {
		key_operator_ = RANGE_GROUP;
		key_id_ = key_id;

		if(NULL != values_) {
			delete values_;
			values_ = NULL;
		}

		key_size_ = sizeof(start_value);

		values_ = new char[3 * key_size_];
		char *p = values_;
		memcpy(p, &start_value, key_size_);
		p += key_size_;
		memcpy(p, &end_value, key_size_);
		p += key_size_;
		memcpy(p, &inc, key_size_);
		group_ = group;

		fetch_num_ = (end_value - start_value) / inc + 1;
//		printf("start = %d, end = %d, inc = %d, fetch_num = %d\n", start_value, end_value, inc, fetch_num_);
	}



	/*
	 * float key filter
	 */

	template <class KFLOAT>
	void SetKey(int key_id, KFLOAT start_value, KFLOAT end_value, KFLOAT inc, float tolerance = 0.0f) {
		key_operator_ = RANGE_TOLERANCE;
		key_id_ = key_id;

		if(NULL != values_) {
			delete values_;
			values_ = NULL;
		}

		key_size_ = sizeof(start_value);

		values_ = new char[3 * key_size_];
		char *p = values_;
		memcpy(p, &start_value, key_size_);
		p += key_size_;
		memcpy(p, &end_value, key_size_);
		p += key_size_;
		memcpy(p, &inc, key_size_);
		tolerance_ = tolerance;

		fetch_num_ = (int)((end_value - start_value) / inc) + 1;
//		printf("start = %f, end = %f, inc = %f, fetch_num = %d\n", start_value, end_value, inc, fetch_num_);

	}

	friend class RowFilterByKey;
	friend struct FilterRuleComp;
	friend class IndexOrderReader;
	friend class IndexBTreeReader;
	friend class IndexBTreeReaderSort;
	friend class IndexSqlReader;

private:

	inline int GetFetchNum() const {
//		printf("fetch_num of key : %d is : %d\n", key_id_, fetch_num_);
		return fetch_num_;
	}

	KeyOperator GetOperator() const;

	int GetKeyID() const;

	char *GetStartValueP() const {
		return values_;
	}

	char *GetEndValueP() const {
		return values_ + key_size_;
	}

	char *GetIncP() const {
		return values_ + (key_size_ << 1);
	}

#if 0
	template <class KTYPE>
	void GetKeyValue(KTYPE& value) const {
		value = ((KTYPE*)values_)[0];
	}
#endif

	template <class KINT>
	void GetKeyValue(KINT& start_value, KINT& end_value, KINT& inc, int& group) const {
//		start_value = ((KINT*)values_)[0];
//		end_value = ((KINT*)values_)[1];
//		inc = ((KINT*)values_)[2];
		char *p = values_;
		memcpy(&start_value, p, key_size_);
		p += key_size_;
		memcpy(&end_value, p, key_size_);
		p += key_size_;
		memcpy(&inc, p, key_size_);

		group = group_;
	}


	template <class KFLOAT>
	void GetKeyValue(KFLOAT& start_value, KFLOAT& end_value, KFLOAT& inc, float& tolerance) const {
		char *p = values_;
		memcpy(&start_value, p, key_size_);
		p += key_size_;
		memcpy(&end_value, p, key_size_);
		p += key_size_;
		memcpy(&inc, p, key_size_);

		tolerance = tolerance_;
	}


	KeyOperator key_operator_;
	int key_id_;

	char *values_;
	int key_size_;
	int group_;
	float tolerance_;
	int fetch_num_;

};


#endif /* KEY_H_ */
