/*
 * index_btree_data_struct.cpp
 *
 *  Created on: Jul 26, 2018
 *      Author: wyd
 */


#include "index_btree/index_btree_data_struct.h"

void AddTailFlag(int64_t &trace_id) {
	trace_id |= 0x8000000000000000;
}
int64_t EraseTailFlag(int64_t trace_id) {
	return trace_id & 0x7fffffffffffffff;
}
void AddBlockSize(int64_t &trace_id, uint16_t block_size) {
	int64_t mask = block_size;
	mask <<= 47;
	trace_id |= mask;
}
int64_t EraseBlockSize(int64_t trace_id) {
	return trace_id & 0x00007fffffffffff;
}
uint16_t GetBlockSize(int64_t trace_id) {
	uint16_t cur_tree_block_size = trace_id >> 47;
	return cur_tree_block_size;
}
