/*
 * index_sql_reader.cpp
 *
 *  Created on: Aug 12, 2018
 *      Author: wangyida
 */

#include "index_sql/index_sql_reader.h"
#include "sql_manager.h"
#include "head_data.h"
#include "row_filter_by_key.h"
#include "order.h"
#include "util/util_string.h"


IndexSqlReader::IndexSqlReader(HeadInfo *head_info) :
	head_info_(head_info){
	sql_man_ = NULL;
}
IndexSqlReader::~IndexSqlReader() {
	if(NULL != sql_man_) {
		delete sql_man_;
		sql_man_ = NULL;
	}
}

bool IndexSqlReader::Open() {
	sql_man_ = new SqlManager();
	if(NULL == sql_man_) {
		return false;
	}
	if(!sql_man_->Init()) {
		return false;
	}
	return true;
}
void IndexSqlReader::Close() {
	sql_man_->Close();
}
bool IndexSqlReader::GetValidTraces(const RowFilterByKey &row_filter, const Order &order, std::vector<int64_t> *valid_trace_arr) {
	std::string table_name = HeadData::GetSqlTableName(head_info_);
	std::string query = "select trace_id from ";
	query += table_name;

	std::string where_statement;
	if(0 < row_filter.GetValidKeyNum()) {
		where_statement = " where ";

		std::vector<int> key_id_arr = row_filter.GetKeyIDArr();
		for(int i = 0; i < key_id_arr.size(); ++i) {

			int key_id = key_id_arr[i];
			KeyType key_type = head_info_->GetKeyType(key_id);
			std::string key_name_tmp = key_name[key_id];

			std::vector<Key> rule_arr = row_filter.GetFilterRuleOf(key_id_arr[i]);

			if(!rule_arr.empty()) {
				if(i > 0) {
					where_statement += " and ";
				}

				where_statement += " ( ";

				for(int rule_i = 0; rule_i < rule_arr.size(); ++rule_i) {
					std::string begin, end;
					if((key_type & HT_INT) || (key_type & HT_UINT)) {
						begin = ToString(*(int*)rule_arr[rule_i].GetStartValueP());
						end = ToString(*(int*)rule_arr[rule_i].GetEndValueP());
					} else if(key_type & HT_FL){
						begin = Float2Str(*(float*)rule_arr[rule_i].GetStartValueP());
						end = Float2Str(*(float*)rule_arr[rule_i].GetEndValueP());
					}

					if(rule_i > 0) {
						where_statement += " or ";
					}
					where_statement += " ( " + key_name_tmp + " >= " + begin + " and " + key_name_tmp + " <= " + end + " ) ";
				}

				where_statement += " ) ";
			}
		}
	}

	std::string order_statement;
	std::vector<int> key_order_arr = order.GetKeyPosArr();
	if(!key_order_arr.empty()) {
		order_statement = " order by ";
		for(int key_i = 0; key_i < key_order_arr.size(); ++key_i) {
			int key_id = key_order_arr[key_i];
			std::string key_name_tmp = key_name[key_id];
			if(key_i > 0) {
				order_statement += ",";
			}
			order_statement += key_name_tmp;
		}
	}

	query += where_statement + order_statement;

	printf("query = %s\n", query.c_str());fflush(stdout);

	sql_man_->Query(query);
	MYSQL_RES *result = sql_man_->GetResult();

	MYSQL_ROW row;
	while(sql_man_->NextRow(result, row)) {
		int64_t trace_id = ToInt64(row[0]);
		valid_trace_arr->push_back(trace_id - 1);
	}

	return true;
}


