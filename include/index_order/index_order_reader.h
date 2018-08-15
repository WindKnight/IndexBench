/*
 * index_order_reader.h
 *
 *  Created on: May 15, 2018
 *      Author: wyd
 */

#ifndef INDEX_ORDER_READER_H_
#define INDEX_ORDER_READER_H_

#include <stdint.h>
#include <vector>
#include "index_config.h"
#include "index_order_config.h"
#include "data_type.h"


class HeadInfo;
class Key;

class IndexOrderReader {
public:
	IndexOrderReader(const IndexAttr &index_attr, HeadInfo *head_info);
	~IndexOrderReader();

	static void InitStatistic();
	static float GetTotalReadSize();
	static float GetTotalReadTime();

	bool Open();
	bool Seek(int64_t trace_id);
	bool Next(IndexElement * index_ele);
	void Close();
	int64_t GetTotalTraceNum();

	std::vector<KeyFirst> GetAllValidKeyFirst(const Key &key);

private:
	std::string index_type_;
	IndexAttr index_attr_;
	HeadInfo *head_info_;

	int64_t total_trace_num_;
	int cur_key_first_id_;
	KeyFirst cur_key_first_;
	std::vector<KeyFirst> key_first_arr_;

	int64_t cur_key_first_begin_trace_id_,cur_key_first_end_trace_id_;

	std::string index_dir_;
	int fd_key_first_, fd_key_oth_;
	int fd_meta_;

	KeyType key_first_type_;
	int key_first_size_;


	int real_key_num_;
	int key_other_size_, one_index_size_;
	int key_first_record_size_;

	int64_t key_first_num_;
	int64_t cur_trace_id_;

	int64_t buf_begin_id_, buf_end_id_;

	char *key_other_buf_;
	int buffered_index_oth_num_;

private:
	bool LoadAllKeyFirst();
	int Search(char *start_value_p);

	static int64_t read_key_first_size_, read_key_oth_size_;
	static float read_key_first_time_, read_key_oth_time_;
};


#endif /* INDEX_ORDER_READER_H_ */
