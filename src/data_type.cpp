/*
 * data_type.cpp
 *
 *  Created on: May 9, 2018
 *      Author: wyd
 */

#include "data_type.h"

#include <stdint.h>
#include <string.h>
#include "util/util_string.h"


#define VALUE_COPY(v1,p1,v2,p2)	\
	memcpy(&v1, p1, sizeof(v1));	\
	memcpy(&v2, p2, sizeof(v2));


bool KeyLessThan(void *p1, void *p2, KeyType key_type) {
	switch (key_type) {
	case TINYINT :
	{
		int8_t a, b;
		VALUE_COPY(a,p1,b,p2)
		return a < b;
	}
		break;
	case SMALLINT :
	{
		int16_t a,b;
		VALUE_COPY(a,p1,b,p2)
		return a < b;
	}
		break;
	case INT :
	{
		int32_t a,b;
		VALUE_COPY(a,p1,b,p2)
		return a < b;
	}
		break;
	case BIGINT :
	{
		int64_t a,b;
		VALUE_COPY(a,p1,b,p2)
		return a < b;
	}
		break;

	case UTINYINT :
	{
		uint8_t a, b;
		VALUE_COPY(a,p1,b,p2)
		return a < b;
	}
		break;
	case USMALLINT :
	{
		uint16_t a,b;
		VALUE_COPY(a,p1,b,p2)
		return a < b;
	}
		break;
	case UINT :
	{
		uint32_t a,b;
		VALUE_COPY(a,p1,b,p2)
		return a < b;
	}
		break;
	case UBIGINT :
	{
		uint64_t a,b;
		VALUE_COPY(a,p1,b,p2)
		return a < b;
	}
		break;

	case FLOAT :
	{
		float a,b;
		VALUE_COPY(a,p1,b,p2)
		return a < b;
	}
		break;
	case DOUBLE :
	{
		double a,b;
		VALUE_COPY(a,p1,b,p2)
		return a < b;
	}
		break;
	default:
		return false;
		break;
	}
	return false;
}

#if 0
bool KeyEqual(void *p1, void *p2, KeyType key_type) {
	if(HT_FL == (key_type & 0xff00)) {
		switch(key_type) {
		case FLOAT :
		{
			float a,b;
			VALUE_COPY(a,p1,b,p2)
			return FloatEqual(a,b);
		}
			break;
		case DOUBLE :
		{
			double a,b;
			VALUE_COPY(a,p1,b,p2)
			return DoubleEqual(a,b);
		}
			break;
		default:
			return false;
			break;
		}
	} else {
		return (0 == strncmp((char*)p1, (char*)p2, key_type & 0x00ff));
	}
}
#endif


bool KeyEqual(void *p1, void *p2, KeyType key_type) {
	switch (key_type) {
	case TINYINT :
	{
		int8_t a, b;
		VALUE_COPY(a,p1,b,p2)
		return a == b;
	}
		break;
	case SMALLINT :
	{
		int16_t a,b;
		VALUE_COPY(a,p1,b,p2)
		return a == b;
	}
		break;
	case INT :
	{
		int32_t a,b;
		VALUE_COPY(a,p1,b,p2)
		return a == b;
	}
		break;
	case BIGINT :
	{
		int64_t a,b;
		VALUE_COPY(a,p1,b,p2)
		return a == b;
	}
		break;

	case UTINYINT :
	{
		uint8_t a, b;
		VALUE_COPY(a,p1,b,p2)
		return a == b;
	}
		break;
	case USMALLINT :
	{
		uint16_t a,b;
		VALUE_COPY(a,p1,b,p2)
		return a == b;
	}
		break;
	case UINT :
	{
		uint32_t a,b;
		VALUE_COPY(a,p1,b,p2)
		return a == b;
	}
		break;
	case UBIGINT :
	{
		uint64_t a,b;
		VALUE_COPY(a,p1,b,p2)
		return a == b;
	}
		break;

	case FLOAT :
	{
		float a,b;
		VALUE_COPY(a,p1,b,p2)
		return FloatEqual(a,b);
	}
		break;
	case DOUBLE :
	{
		double a,b;
		VALUE_COPY(a,p1,b,p2)
		return DoubleEqual(a,b);
	}
		break;
	default:
		return false;
		break;
	}
	return false;
}



