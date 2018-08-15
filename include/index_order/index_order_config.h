/*
 * index_order_datatype.h
 *
 *  Created on: Apr 29, 2018
 *      Author: wyd
 */

#ifndef INDEX_ORDER_DATATYPE_H_
#define INDEX_ORDER_DATATYPE_H_

#include <vector>
#include "stdio.h"
#include "util/util_log.h"


struct KeyFirst{
	KeyFirst() {
		key_buf = NULL;
	}
	~KeyFirst() {

	}

	char *key_buf;
	uint64_t trace_num;
	uint64_t offset;
};

struct KeyFirstPos{
	KeyFirst key_first;
	int reducer_id;
	int offset;
};


#define MAX_KEY_FIRST_BUF_SIZE	4	//MB
#define MAX_BUF_SIZE    		32	//MB
#define MERGE_BUF_SIZE  		32	//MB

#define BUFFER_INDEX_OTH_NUM		64




#endif /* INDEX_ORDER_DATATYPE_H_ */
