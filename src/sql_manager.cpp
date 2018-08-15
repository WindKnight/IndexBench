/*
 * sql_manager.cpp
 *
 *  Created on: Aug 10, 2018
 *      Author: wangyida
 */

#include "sql_manager.h"
#include <stdio.h>

SqlManager::SqlManager() {
	conn_ = NULL;
}
SqlManager::~SqlManager() {
	if(NULL != conn_) {
		mysql_close(conn_);
		conn_ = NULL;
	}
}

bool SqlManager::Init() {
	char *server = "localhost";
	char *user = "root";
	char *password = "112106gw";
	char *database = "test";

	int  port=3306;

	conn_ = mysql_init(NULL);

	if (!mysql_real_connect(conn_, server, user, password, database, port, NULL, 0)) {
		fprintf(stderr, "connect error: %s\n", mysql_error(conn_));
		return false;
	}
	return true;
}


bool SqlManager::Query(const std::string &query) {
	if (mysql_query(conn_, query.c_str())) {
		fprintf(stderr, "%s\n", mysql_error(conn_));
		return false;
	}
	return true;
}
MYSQL_RES *SqlManager::GetResult() {
	MYSQL_RES *ret = mysql_store_result(conn_);//将查询的全部结果读取到客户端
	return ret;
}
bool SqlManager::NextRow(MYSQL_RES *result, MYSQL_ROW &row) {
	row = mysql_fetch_row(result);
	if(NULL != row) {
		return true;
	} else {
		return false;
	}
}
void SqlManager::FreeResult(MYSQL_RES *result) {
	mysql_free_result(result);
}

void SqlManager::Close() {
	mysql_close(conn_);
	conn_ = NULL;
}
