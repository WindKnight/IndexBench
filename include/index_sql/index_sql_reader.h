/*
 * index_sql_reader.h
 *
 *  Created on: Aug 12, 2018
 *      Author: wangyida
 */

#ifndef INDEX_SQL_READER_H_
#define INDEX_SQL_READER_H_


#include <vector>
#include <stdint.h>

class HeadInfo;
class SqlManager;
class RowFilterByKey;
class Order;

class IndexSqlReader {
public:
	IndexSqlReader(HeadInfo *head_info);
	~IndexSqlReader();

	bool Open();
	void Close();
	bool GetValidTraces(const RowFilterByKey &row_filter, const Order &order, std::vector<int64_t> *valid_trace_arr);

private:
	HeadInfo *head_info_;
	SqlManager *sql_man_;
};

#endif /* INDEX_SQL_READER_H_ */
