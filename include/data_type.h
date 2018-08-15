/*
 * data_type.h
 *
 *  Created on: May 7, 2018
 *      Author: wyd
 */

#ifndef DATA_TYPE_H_
#define DATA_TYPE_H_


#include <vector>



#define HT_INT		0x0100
#define HT_UINT		0x0200
#define HT_FL		0x0300
#define HT_STR		0x0400

enum KeyType {
	TINYINT = 0x0101,
	SMALLINT = 0x1002,
	INT = 0x0104,  //int32_t
	BIGINT = 0x0108,  //int64_t

	UTINYINT = 0x0201,  //uint8_t
	USMALLINT = 0x0202,  //uint16_t
	UINT = 0x0204,  //uint32_t
	UBIGINT = 0x0208,  //uint64_t

	FLOAT = 0x0304,  //float
	DOUBLE = 0x0308  //double
};


struct HeadType {
	std::vector<KeyType> head_type;
};



bool KeyLessThan(void *p1, void *p2, KeyType key_type);

bool KeyEqual(void *p1, void *p2, KeyType key_type);


#endif /* DATA_TYPE_H_ */
