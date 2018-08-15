/*
 * index_btree_writer.h
 *
 *  Created on: May 20, 2018
 *      Author: wyd
 */

#ifndef INDEX_BTREE_WRITER_H_
#define INDEX_BTREE_WRITER_H_

#include "index_btree/index_btree_data_struct.h"
#include "index_config.h"


class IndexBTreeWriter {
public:
	IndexBTreeWriter(const IndexAttr &index_attr, HeadInfo *head_info);
	~IndexBTreeWriter();

    bool OpenWrite();
    bool WriteOneIndex(const IndexElement &index);
    bool WriteOneIndex(const IndexElement &index, int index_level);
    bool CloseWrite();
private:

    bool BuildBTree(int index_level);
    bool HighLevelKeyEqual(char *index_ele_buf, int cur_level);
    bool WriteToBlock(const IndexBTreeElement &btree_ele, int tier);
    bool WriteOutTierBlock(int tier);
    bool WriteToTraceIDBuf(const std::vector<int64_t> &trace_id_arr);
    bool WriteOutTraceIDBuf();

    std::string index_type_;
    IndexAttr index_attr_;
    HeadInfo *head_info_;
    std::string index_dir_;
    int64_t total_trace_num_;

    std::vector<char*> cur_key_flag_arr_; //size() == index_level_num
    std::vector<int> lv_btree_fd_arr_;
    std::vector<int> lv_trace_id_fd_arr_;
    int fd_meta_;
    std::vector<int64_t> lv_btree_file_off_arr_; //size() == index_level_num  record the current btree writing offset of each index level.
    std::vector<int64_t> lv_trace_id_file_off_arr_;
    std::vector<int> leaf_index_size_arr_;
    std::vector<int> key_size_arr_;
    std::vector<int> key_offset_arr_;
    std::vector<KeyType> key_type_arr_;
    std::vector<int64_t> trace_num_in_btree_lv_;
    std::vector<int64_t> begin_trace_id_in_btree_lv_;

    typedef std::vector<IndexBTreeElement> EleBuf;
    std::vector<EleBuf> ele_buf_arr_;

private:

    std::vector<int> block_write_ele_num_arr_; //size() == btree hight
    std::vector<int> block_total_ele_num_arr_; //size() == btree hight
    std::vector<IndexBTreeElement> guard_ele_arr_;

    /*
     * parameters related to tree
     */
    int cur_building_index_level_;
    int tree_hight_;
    int64_t index_write_id_;

    int64_t cur_btree_write_offset_;
    int tree_block_size_;

    std::vector<int> block_num_arr_;

    TreeHead cur_tree_head_;
    uint16_t cur_tree_block_size_;

    /*
     * parameters related to element buffer
     */
    std::vector<char*> block_buf_arr_;
    std::vector<char*> block_buf_write_p_arr_;
    std::vector<int64_t> tier_write_offset_arr_;

    /*
     * parameter related to trace_id_buf
     */
    int cur_trace_id_in_buf_, total_trace_id_num_in_buf_;
    int64_t *trace_id_buf_, *trace_id_buf_write_p_;



};


#endif /* INDEX_BTREE_WRITER_H_ */
