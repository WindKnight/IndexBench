/*
 * index_sql.h
 *
 *  Created on: Aug 11, 2018
 *      Author: wangyida
 */

#ifndef INDEX_SQL_H_
#define INDEX_SQL_H_


#include "index.h"

class IndexSql : public Index {
public:
	IndexSql(const IndexAttr &index_attr, HeadInfo *head_info);
	virtual ~IndexSql() {}

	virtual bool BuildIndex();
	virtual bool Remove();

private:
	std::string GetIdxName();

};

#endif /* INDEX_SQL_H_ */
