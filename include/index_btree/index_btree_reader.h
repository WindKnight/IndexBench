/*
 * index_btree_reader.h
 *
 *  Created on: May 23, 2018
 *      Author: wyd
 */

#ifndef INDEX_BTREE_READER_H_
#define INDEX_BTREE_READER_H_

#include "index_config.h"
#include "util/util_shared_pointer.h"
#include "row_filter_by_key.h"
#include "index_btree_data_struct.h"

class HeadInfo;

class IndexBTreeReader {
public:

	static void PrintStatistic();
	static void InitStatistic();
	static float GetTotalReadSize();
	static float GetTotalReadTime();
	static float GetLoadBlockSize();
	static float GetLoadBlockTime();
	static float GetValidLoadSize();
	static float GetReadTraceSize();
	static float GetReadTraceTime();
	static inline int64_t GetBinarySearchNum() {
		return binary_search_call_num_;
	}
	static inline float GetBinarySearchTime() {
		return binary_search_time_;
	}
	static inline float GetRecordResultTime() {
		return record_result_time_;
	}

	IndexBTreeReader(const IndexAttr &index_attr, HeadInfo *head_info);
	virtual ~IndexBTreeReader();

	virtual bool Open();
	virtual void Close();
	virtual int64_t Next();
	virtual bool Seek(int64_t trace_id);
	virtual int64_t GetTotalTraceNum();
	virtual bool GetValidTraces(const RowFilterByKey &row_filter, std::vector<int64_t> *valid_trace_arr);

protected:

	typedef std::vector<int64_t> ValidOffArr;

	bool OpenFiles();
	void CloseFiles();

	bool MallocTraceIDBuf();
	void SetBlockSize();

	std::vector<RuleArr> GetRuleTable(const RowFilterByKey &row_filter);
	int BinarySearchStartValue(char *buf, int ele_num_in_buf, int ele_size, KeyType key_type, char *value_p); // search the first value that >= start value
	bool SearchBTree(int index_level, int64_t begin_off, const RuleArr &rule_arr);
	bool LoadBlock(int index_level, int64_t offset, char *buf);

	IndexAttr index_attr_;
	HeadInfo *head_info_;
	std::string index_dir_;

	int64_t total_trace_num_;

	int tree_block_size_;
	uint16_t cur_tree_block_size_, lv0_block_size_;

	int64_t *trace_id_buf_;
	int buffered_trace_id_num_;

    std::vector<int> lv_btree_fd_arr_;
    std::vector<int> lv_trace_id_fd_arr_;
    int fd_sequencial_read_;
    int fd_meta_;

	std::vector<int64_t> tmp_result_of_one_tree_;

protected:
	virtual char *GetBlockBufP(int tier_i);
	virtual void RecordSearchResult(int index_level, const std::vector<int64_t> &result_arr);

	bool ReadValidTraceID(int index_level, int64_t trace_id_begin_off, int64_t trace_num_this_tree,
			int64_t begin_trace_id_idx, int64_t end_trace_id_idx);
	bool LoadValidTraces(int index_level, const RuleArr &rule_arr);

	std::string index_type_;

    int64_t buf_begin_id_, buf_end_id_;
    int64_t cur_read_id_;

	std::vector<ValidOffArr> valid_off_table_;
	std::vector<char *> block_buf_arr_; //block buf arr. one buf each tier


protected: //for statistics
	static int call_read_trace_id_num_, call_load_block_num_;
	static int64_t load_block_size_;
	static float read_trace_id_time_, load_block_time_;
	static float read_trace_time1_, read_trace_time2_;
	static int64_t total_read_trace_num_;

	static int64_t binary_search_call_num_;
	static float binary_search_time_, record_result_time_;
	static float search_time_;

};

#endif /* INDEX_BTREE_READER_H_ */
