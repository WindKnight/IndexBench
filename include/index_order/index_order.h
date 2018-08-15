/*
 * index_order.h
 *
 *  Created on: Apr 22, 2018
 *      Author: wyd
 */

#ifndef INDEX_ORDER_H_
#define INDEX_ORDER_H_

#include "index.h"

#include <string>

#include "index_order/index_order_config.h"
class HeadInfo;
class Selector;


class IndexOrder : public Index {
public:

#if 0
    static std::string GetIndexRootDir();
    static std::string GetIndexFullDir(const IndexAttr &index_attr);

    static IndexAttr FindIndex(const IndexAttr &index_attr);
    static bool IsValid(const IndexAttr &index_attr);
#endif

#if 0
    static void Remove(const IndexAttr &index_attr);

    static bool ValidOrRemove(const IndexAttr &index_attr);
    static bool InvalidateIndex(const SeisDataAttr &data_attr, int keycode);
#endif

public:
	IndexOrder(const IndexAttr &index_attr, HeadInfo *head_info);
	virtual ~IndexOrder();


	virtual bool BuildIndex();
	virtual bool Remove();

private:

    int real_key_num_;

    int fd_key_first_, fd_key_oth_;

    char *index_buf_, *record_p_;
    char *key_first_buf_, *record_p_key_first_;

    int one_index_size_;
    int key_first_record_size_;

};


#endif /* INDEX_ORDER_H_ */
