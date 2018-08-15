/*
 * index_btree.cpp
 *
 *  Created on: May 17, 2018
 *      Author: wyd
 */



#include "index_btree/index_btree.h"

#include <sys/time.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>

#include "tbb/parallel_sort.h"

#include "index_config.h"
#include "index_btree/index_btree_writer.h"
#include "index_btree/selector_btree.h"
#include "job_para.h"
#include "util/util.h"
#include "util/util_log.h"
#include "util/util_io.h"
#include "util/util_string.h"

IndexBTree::IndexBTree(const IndexAttr &index_attr, HeadInfo *head_info) :
	Index(index_attr, head_info){
	index_type_ = INDEX_TYPE_BTREE;
}
IndexBTree::~IndexBTree() {

}

IndexBTreeWriter *IndexBTree::OpenWriter(const IndexAttr &index_attr, HeadInfo *head_info) {
    IndexBTreeWriter *ret = new IndexBTreeWriter(index_attr_, head_info_);
    if(NULL == ret) {
    	Err("New btree writer error\n");
    	return NULL;
    }
    if(!ret->OpenWrite()) {
    	Err("Open writer error\n");
    	return NULL;
    }

    return ret;
}

bool IndexBTree::BuildIndex() {

    uint64_t trace_num = JobPara::GetTraceNum();

	printf("Begin to build BTREE index of key : \n");
    for(int i = 0; i < index_attr_.keypos_arr.size(); ++i) {
    	int key_id = index_attr_.keypos_arr[i];
    	bool is_asc = index_attr_.asc_arr_[i];
    	printf("%d%s", key_id, is_asc ? "" : "_d");
    	if(i == index_attr_.keypos_arr.size() - 1) {
    		printf("\n");
    	} else {
    		printf(",");
    	}
    }

	struct timeval t_all_begin, t_all_end;
	gettimeofday(&t_all_begin, NULL);

//    std::vector<int> key_pos_arr = IndexOrderKeyWord::GetPosArr();
    int key_num = index_attr_.keypos_arr.size();
    if(key_num > MAX_INDEX_KEY_NUM) { //TODO
        Err("The number of index keys exceeds the ceiling of 5\n");
//        exit(-1);
        return false;
    }

	std::string index_dir = GetIndexFullDir(index_attr_, index_type_);

	if(0 > access(index_dir.c_str(), F_OK | R_OK | W_OK)) {
		RecursiveRemove(index_dir);
		bool bRet = RecursiveMakeDir(index_dir);
		if(!bRet) {
			Err("Make index dir error\n");
		}
	}

	int head_size = head_info_->GetHeadSize();
	char *head_buf = new char[head_size];
    uint64_t read_trace_num = 0;
    std::vector<IndexElement> index_buf;

    printf("Begin to read data...\n");fflush(stdout);
    struct timeval t_begin, t_end;
    gettimeofday(&t_begin, NULL);

    HeadDataReader *reader = new HeadDataReader(head_info_);
    reader->Open();

    index_buf.reserve(trace_num);


    int total_key_buf_size = 0;
    for(int i = 0; i < index_attr_.keypos_arr.size(); ++i) {
    	int key_id = index_attr_.keypos_arr[i];
    	total_key_buf_size += head_info_->GetKeySize(key_id);
    }

    for(uint64_t i = 0; i < trace_num; ++i) {
    	bool ret = reader->Next(head_buf);
        if(!ret) {
        	Err("read head error\n");
        	return false;
        }
        IndexElement index;
        index.trace_id = i;
//        index.key_buf = new char[total_key_buf_size];
        index.Malloc(total_key_buf_size);
        char *p = index.key_buf;
        for(std::vector<int>::iterator it = index_attr_.keypos_arr.begin(); it != index_attr_.keypos_arr.end(); ++it) {
        	int key_id = *it;
        	int key_size = head_info_->GetKeySize(key_id);
        	int key_offset = head_info_->GetKeyOffset(key_id);
        	memcpy(p, head_buf + key_offset, key_size);
        	p += key_size;
        }

        index_buf.push_back(index);
    }

    reader->Close();
    delete reader;

    gettimeofday(&t_end, NULL);
    float time_read = CALTIME(t_begin, t_end);
    printf("Read over, read time is : %f s, begin to sort...\n", time_read);fflush(stdout);

    gettimeofday(&t_begin, NULL);

    tbb::parallel_sort(index_buf.begin(), index_buf.end(), IndexEleCompare(head_info_, index_attr_));

    gettimeofday(&t_end, NULL);
    float time_sort = CALTIME(t_begin, t_end);
    printf("Sort over, sort time is : %f s, begin to write out...\n", time_sort);

    gettimeofday(&t_begin, NULL);

#if 0
    KeyType key_type = head_info_->GetKeyType(index_attr_.keypos_arr[0]);
    for(int64_t i = 1; i < index_buf.size(); ++i) {
    	if(KeyLessThan(index_buf[i].key_buf, index_buf[i - 1].key_buf, key_type)) {
    		printf("Sort Error, key_type = %d, index_ele[%lld] = %lld, index_ele[%lld] = %lld\n",
    				key_type, i, *(int*)index_buf[i].key_buf, i - 1, *(int*)index_buf[i - 1].key_buf);

        	if(KeyEqual(index_buf[i].key_buf, index_buf[i - 1].key_buf, key_type)) {
        		printf("Logical Error, %lld is equal to %lld\n", *(int*)index_buf[i].key_buf, i - 1, *(int*)index_buf[i - 1].key_buf);
        	}
    	}

    }
#endif


    IndexBTreeWriter *index_writer = OpenWriter(index_attr_, head_info_);
//    index_writer->OpenWrite();
    for(std::vector<IndexElement>::iterator it = index_buf.begin(); it != index_buf.end(); ++it) {

    	index_writer->WriteOneIndex(*it);
    	if(it->key_buf != NULL) {
    		delete it->key_buf;
    		it->key_buf = NULL;
    	}
    }
    index_writer->CloseWrite();

    gettimeofday(&t_end, NULL);
    float time_write = CALTIME(t_begin, t_end);
    printf("Write out over, write time is : %f s\n", time_write);fflush(stdout);


    gettimeofday(&t_all_end, NULL);
    float time_all = CALTIME(t_all_begin, t_all_end);
    printf("Build BTREE index over, Time Elapsed is : %f s\n", time_all);fflush(stdout);

    delete index_writer;
	return true;
}

bool IndexBTree::Remove() {
	std::string index_dir = GetIndexFullDir(index_attr_, index_type_);
	return RecursiveRemove(index_dir);
}




