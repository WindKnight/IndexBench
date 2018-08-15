/*
 * sql_manager.h
 *
 *  Created on: Aug 10, 2018
 *      Author: wangyida
 */

#ifndef SQL_MANAGER_H_
#define SQL_MANAGER_H_

#include "mysql/mysql.h"
#include <string>

class SqlManager {
public:
	SqlManager();
	~SqlManager();

	bool Init();

	bool Query(const std::string &query);
	MYSQL_RES *GetResult();
	bool NextRow(MYSQL_RES *result, MYSQL_ROW &row);
	void FreeResult(MYSQL_RES *result);

	void Close();

private:
	   MYSQL *conn_;
};


#endif /* SQL_MANAGER_H_ */
