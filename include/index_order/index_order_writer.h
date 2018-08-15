/*
 * IndexWriter.h
 *
 *  Created on: May 18, 2017
 *      Author: wzb
 */

#ifndef INDEX_ORDER_WRITER_H_
#define INDEX_ORDER_WRITER_H_


#include <vector>
#include <string>
#include <map>
#include <stdint.h>

#include <index_config.h>
#include "index_order/index_order_config.h"
#include "data_type.h"

class HeadInfo;

class IndexOrderWriter {
public:
	IndexOrderWriter(const IndexAttr &index_attr, HeadInfo *head_info);
	~IndexOrderWriter();

    bool OpenWrite(uint16_t reducer_id);
    bool WriteOneIndex(const IndexElement &index);
    void CloseWrite();

    bool Merge(int reduce_num);

private:

    bool InitWriteBuf();
    bool WriteKeyFirst(int fd, const KeyFirst &key_first);
    bool WriteKeyOther(const IndexElement &index);
    bool ReadKeyFirst(int fd, KeyFirst *key_first);
    bool Flush();
    bool FlushKeyFirst(int fd);
    bool WriteMeta();
    bool MergeMeta(const std::string &fname_base, int reduce_num);


    std::string index_type_;
    IndexAttr index_attr_;
    HeadInfo *head_info_;

    int64_t total_trace_num_;

    int max_buf_index_num_;
    int cur_buf_index_num_;

    bool init_key_first_;
    KeyFirst cur_key_first_;

    int max_key_first_num_;
    int cur_key_first_num_;

    int fd_meta_;

    char *index_buf_, *record_p_;
    char *key_first_buf_, *record_p_key_first_;

    int real_key_num_;
    int one_index_size_;
    int key_first_size_, key_other_size_;
    KeyType key_first_type_;
    int key_first_record_size_;
    std::string index_dir_;
    int fd_key_first_, fd_key_oth_;


#if 0
    struct KeyFirstCompare {

    	bool operator() (const KWValue &key1, const KWValue &key2) const {
            int pos = IndexOrderKeyWord::GetKeyWordPos(0);
            if(!IndexOrderKeyWord::IsFloatKey(pos)) {
            	return key1.int_value < key2.int_value ? true : false;
            } else {
                return key1.float_value < key2.float_value ? true : false;
            }
    	}

    };

//    typedef	std::map<KWValue, KeyFirstPos, KeyFirstCompare> KeyFirstRegion;
    typedef	std::map<int, KeyFirstPos> KeyFirstRegion;
    KeyFirstRegion key_first_region_;
#endif

};


#endif /* INDEX_ORDER_WRITER_H_ */
