/*
 * index_btree_data_struct.h
 *
 *  Created on: Jul 26, 2018
 *      Author: wyd
 */

#ifndef INDEX_BTREE_DATA_STRUCT_H_
#define INDEX_BTREE_DATA_STRUCT_H_


#include <vector>
#include <stdint.h>
#include <string.h>
#include <stdio.h>

//#define	BLOCK_SIZE 				4096 //Bytes
#define BUFFER_TRACE_ID_NUM		64 //trace id num in buffer

struct IndexBTreeElement{
	IndexBTreeElement() {
		key_buf = NULL;
	}
	~IndexBTreeElement() {

	}
	char *key_buf;
    std::vector<int64_t> trace_id_arr;
};


struct TreeHead {
	char extra_root;
	int64_t trace_id_begin_off;
	int64_t trace_id_num;
	std::vector<int> tier_block_num_arr;

	inline int GetHeadSize() {
		int size = sizeof(extra_root) + sizeof(trace_id_begin_off) + sizeof(trace_id_num) + sizeof(int8_t) + sizeof(int) * tier_block_num_arr.size();
		return size;
	}
	inline void SaveTo(char *buf) {
		char *p = buf;

		memcpy(p, &extra_root, sizeof(extra_root));
		p += sizeof(extra_root);

//		memcpy(p, &real_block_size, sizeof(real_block_size));
//		p += sizeof(real_block_size);

		memcpy(p, &trace_id_begin_off, sizeof(trace_id_begin_off));
		p += sizeof(trace_id_begin_off);

		memcpy(p, &trace_id_num, sizeof(trace_id_num));
		p += sizeof(trace_id_num);

		int8_t tier_num = tier_block_num_arr.size();
		memcpy(p, &tier_num, sizeof(tier_num));
		p += sizeof(tier_num);

		for(int i = 0; i < tier_block_num_arr.size(); ++i) {
			memcpy(p, &(tier_block_num_arr[i]), sizeof(int));
			p += sizeof(int);
		}
	}
	inline void LoadFrom(char *buf) {
		char *p = buf;

		memcpy(&extra_root, p, sizeof(extra_root));
		p += sizeof(extra_root);

//		memcpy(&real_block_size, p, sizeof(real_block_size));
//		p += sizeof(real_block_size);

		memcpy(&trace_id_begin_off, p, sizeof(trace_id_begin_off));
		p += sizeof(trace_id_begin_off);

		memcpy(&trace_id_num, p, sizeof(trace_id_num));
		p += sizeof(trace_id_num);

		int8_t tier_num;
		memcpy(&tier_num, p, sizeof(tier_num));
		p += sizeof(tier_num);

		tier_block_num_arr.clear();

		for(int8_t i = 0; i < tier_num; ++i) {
			int block_num;
			memcpy(&block_num, p, sizeof(block_num));
			tier_block_num_arr.push_back(block_num);
			p += sizeof(block_num);
		}

	}
};

struct BlockHead {
	int ele_num;

	inline int GetHeadSize() {
		return sizeof(ele_num);
	}
	inline void SaveTo(char *buf) {
		memcpy(buf, &ele_num, sizeof(ele_num));
	}
	inline void LoadFrom(char *buf) {
		memcpy(&ele_num, buf, sizeof(ele_num));
	}

};

void AddTailFlag(int64_t &trace_id);
int64_t EraseTailFlag(int64_t trace_id);
void AddBlockSize(int64_t &trace_id, uint16_t block_size);
int64_t EraseBlockSize(int64_t trace_id);
uint16_t GetBlockSize(int64_t trace_id);

#endif /* INDEX_BTREE_DATA_STRUCT_H_ */
