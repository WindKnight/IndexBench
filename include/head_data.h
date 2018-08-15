/*
 * head_data.h
 *
 *  Created on: Apr 19, 2018
 *      Author: wyd
 */

#ifndef HEAD_DATA_H_
#define HEAD_DATA_H_

#include <stdint.h>
#include <string>

#include "data_type.h"
/*
 * key_id	type	value			meaning
 *
 * 0		uint	0-99			shot
 * 1		uint	0-99999			trace
 * 2		uint	0-4999			cmp_line
 * 3		uint	0-4999			cmp
 * 4		float	-1000.0-1000.0	offset
 * 5		float	-180.0-180.0	angle
 */

#define BUF_TRACE_NUM	1048576 //1M
//#define HEAD_DATA_DIR	"/data/wyd/IndexBench/head_data"
//#define TOTAL_KEY_NUM	6
//#define TRACE_NUM_IN_ONE_SHOT	100000
#define TRACE_NUM_IN_CMP_LINE	1000

const char * const key_name[] = {
		"shot",
		"trace",
		"cmp_line",
		"cmp",
		"offset",
		"angel"
};


class HeadInfo {
public:
	HeadInfo(const HeadType &head_type);
	~HeadInfo();

	KeyType GetKeyType(int key_id) const;
	int GetKeySize(int key_id) const; //in bytes
	int GetHeadSize() const;  //in bytes
	int GetKeyOffset(int key_id) const;
	int GetKeyNum();
private:
	HeadType head_type_;
	int head_size_;
	std::vector<int> key_size_arr_;
	std::vector<int> key_offset_arr_;
};

class SqlManager;

class HeadData {
public :

	static std::string GetHeadName(HeadInfo *head_info);
	static std::string GetSqlTableName(HeadInfo *head_info);

	HeadData(HeadInfo *head_info);
	~HeadData();

	bool MakeData(bool make_sql);
private:

	void InsertToSql(SqlManager *sql_man, uint32_t *p, int64_t total_trace_id);

	HeadInfo *head_info_;
	int head_size_;
	int total_key_num_;
};

class HeadDataReader {
public:
	HeadDataReader(HeadInfo *head_info);
	~HeadDataReader();

	bool Open();
	bool Seek(int64_t trace_id);
	bool Next(void *head);
	bool Get(int64_t trace_id, void *head);
	bool HasNext();
	void Close();

	inline int GetHeadSize() {
		return one_head_size_;
	}

	void PrintHead(int64_t trace_id);

private:

	HeadInfo *head_info_;

	int fd_;
	std::string filename_;
	int one_head_size_;
	int64_t total_trace_num_, trace_id_;

#if 0
	int64_t left_trace_num_;
	char *buf_;
	int64_t buf_start_id_, buf_end_id_;
	int buf_trace_num_;
	int buf_len_; //how many int is there in the buf
	int offset_;
#endif

};



#endif /* HEAD_DATA_H_ */
