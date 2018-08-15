/*
 * seis_index.h
 *
 *  Created on: Apr 22, 2018
 *      Author: wyd
 */

#ifndef SEIS_INDEX_H_
#define SEIS_INDEX_H_

#include <stdint.h>
#include <vector>
#include <string>
#include "index_config.h"

class HeadInfo;
class Selector;

class Index {
public:
	Index(const IndexAttr &index_attr, HeadInfo *head_info);
	~Index() {}

	virtual bool BuildIndex() = 0;
	virtual bool Remove() = 0;

    static std::string GetIndexRootDir(std::string index_type_name);
    static std::string GetIndexFullDir(const IndexAttr &index_attr, std::string index_type_name);

    static IndexAttr FindIndex(const IndexAttr &index_attr, std::string index_type_name);
    static IndexAttr FindIndexIgnoreOrder(const IndexAttr &index_attr, std::string index_type_name, int strict_sort_key_num = 0);

    static bool IsValid(const IndexAttr &index_attr, std::string index_type_name);
protected:

    std::string index_type_;
	IndexAttr index_attr_;
	HeadInfo *head_info_;
};


#endif /* SEIS_INDEX_H_ */
