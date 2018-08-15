/*
 * index_sql.cpp
 *
 *  Created on: Aug 11, 2018
 *      Author: wangyida
 */

#include "index_sql/index_sql.h"
#include "sql_manager.h"
#include "util/util_string.h"
#include "util/util_log.h"

#include <stdio.h>

IndexSql::IndexSql(const IndexAttr &index_attr, HeadInfo *head_info) :
	Index(index_attr, head_info){

}

std::string IndexSql::GetIdxName() {
	std::string idx_name = "idx_";

	int key_num = index_attr_.keypos_arr.size();
	for(int i = 0; i < key_num; ++i) {
		int key_id = index_attr_.keypos_arr[i];
		idx_name += ToString(key_id) + "_";
	}
	idx_name += "trace_id";

	return idx_name;
}

bool IndexSql::BuildIndex() {
	std::string sql_table_name = HeadData::GetSqlTableName(head_info_);
	SqlManager *sql_man = new SqlManager();
	if(!sql_man->Init()) {
		Err("Build sql index error\n");
		return false;
	}
	std::string idx_name = GetIdxName();
	std::string idx_list = "";

	int key_num = index_attr_.keypos_arr.size();
	for(int i = 0; i < key_num; ++i) {
		int key_id = index_attr_.keypos_arr[i];
		idx_list += key_name[key_id];
		idx_list += ",";
	}
	idx_list += "trace_id";

	std::string build_idx_query = "create index " + idx_name + " on " + sql_table_name + " (" + idx_list + ")";

	printf("build_idx_query = %s\n", build_idx_query.c_str());
	sql_man->Query(build_idx_query);

	sql_man->Close();
	delete sql_man;
	return true;
}
bool IndexSql::Remove() {
	std::string sql_table_name = HeadData::GetSqlTableName(head_info_);
	SqlManager *sql_man = new SqlManager();
	if(!sql_man->Init()) {
		Err("Build sql index error\n");
		return false;
	}
	std::string idx_name = GetIdxName();

	std::string drop_idx_query = "drop index " + idx_name + " on " + sql_table_name;

	sql_man->Query(drop_idx_query);
	return true;
}


