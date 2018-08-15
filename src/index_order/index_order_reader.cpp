/*
 * index_order_reader.cpp
 *
 *  Created on: May 15, 2018
 *      Author: wyd
 */

#include "index_order/index_order_reader.h"

#include "index_order/index_order.h"
#include "head_data.h"
#include "util/util_scoped_pointer.h"
#include "key.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <string.h>
#include "util/util.h"



IndexOrderReader::IndexOrderReader(const IndexAttr &index_attr, HeadInfo *head_info) :
	index_attr_(index_attr), head_info_(head_info){

	index_type_ = INDEX_TYPE_ORDER;

    real_key_num_ = index_attr_.keypos_arr.size();

    key_other_size_ = 0;
    for(int i = 1; i < real_key_num_; ++i) {
    	key_other_size_ += head_info_->GetKeySize(index_attr_.keypos_arr[i]);
    }
    one_index_size_ = key_other_size_ + sizeof(uint64_t);

    key_first_size_ = head_info_->GetKeySize(index_attr_.keypos_arr[0]);
    key_first_type_ = head_info_->GetKeyType(index_attr_.keypos_arr[0]);
    cur_key_first_.key_buf = NULL;
    key_first_record_size_ = key_first_size_ + sizeof(cur_key_first_.trace_num) + sizeof(cur_key_first_.offset);
    key_other_buf_ = NULL;
}
IndexOrderReader::~IndexOrderReader() {
	for(int i = 0; i < key_first_arr_.size(); ++i) {
		if(key_first_arr_[i].key_buf != NULL) {
			delete key_first_arr_[i].key_buf;
			key_first_arr_[i].key_buf = NULL;
		}
	}
	if(NULL != key_other_buf_) {
		delete key_other_buf_;
		key_other_buf_ = NULL;
	}
}

void IndexOrderReader::InitStatistic() {
	read_key_first_size_ = 0;
	read_key_oth_size_ = 0;
	read_key_first_time_ = 0.0;
	read_key_oth_time_ = 0.0;
}

float IndexOrderReader::GetTotalReadSize() {
	return 1.0 * (read_key_first_size_ + read_key_oth_size_) / 1024 / 1024;
}
float IndexOrderReader::GetTotalReadTime() {
	return read_key_first_time_ + read_key_oth_time_;
}

bool IndexOrderReader::Open() {
	index_dir_ = IndexOrder::GetIndexFullDir(index_attr_, index_type_);

	if(!IndexOrder::IsValid(index_attr_, INDEX_TYPE_ORDER))
		return false;

	std::string fname_base = index_dir_ + "/index";

//    ReadIndexMeta(fname_base);

    std::string fname_key_first = fname_base + "_key_first";
    std::string fname_key_oth = fname_base + "_key_oth";
    std::string fname_meta = fname_base + ".meta";

    fd_key_first_ = open(fname_key_first.c_str(), O_RDONLY);
    if(fd_key_first_ < 0) {
        return false;
    }

    fd_key_oth_ = open(fname_key_oth.c_str(), O_RDONLY);
    if(fd_key_oth_ < 0) {
        return false;
    }

    fd_meta_ = open(fname_meta.c_str(), O_RDONLY);
	if(fd_meta_ < 0) {
		return false;
	}

    if(sizeof(total_trace_num_) != read(fd_meta_, &total_trace_num_, sizeof(total_trace_num_))) {
    	close(fd_meta_);
    	return false;
    }

    LoadAllKeyFirst();

    cur_key_first_id_ = 0;
    cur_key_first_ = key_first_arr_[cur_key_first_id_];

    cur_key_first_begin_trace_id_ = cur_key_first_.offset;
    cur_key_first_end_trace_id_ = cur_key_first_begin_trace_id_ + cur_key_first_.trace_num - 1;

    cur_trace_id_ = 0;

    buffered_index_oth_num_ = BUFFER_INDEX_OTH_NUM;
    buf_begin_id_ = -1;
    buf_end_id_ = -1;

    int buf_size = buffered_index_oth_num_ * one_index_size_;
    key_other_buf_ = new char[buf_size];

    return true;
}
bool IndexOrderReader::Seek(int64_t trace_id) {
	if(trace_id < 0 || trace_id > total_trace_num_)
		return false;

	cur_trace_id_ = trace_id;

	return false;
}
bool IndexOrderReader::Next(IndexElement * index_ele) {

	if(cur_trace_id_ >= total_trace_num_) {
		Err("the trace id has exceeded the end\n");
		return false;
	}

	if(cur_trace_id_ >= buf_end_id_ || cur_trace_id_ < buf_begin_id_) {

		struct timeval time_begin, time_end;
		gettimeofday(&time_begin, NULL);

		int64_t left_trace_num = total_trace_num_ - cur_trace_id_;
		int64_t read_len = buffered_index_oth_num_ < left_trace_num ? buffered_index_oth_num_ : left_trace_num;
		int read_size = read_len * one_index_size_;
		int64_t offset = cur_trace_id_ * one_index_size_;
		lseek(fd_key_oth_, offset, SEEK_SET);

		int ret = read(fd_key_oth_, key_other_buf_, read_size);
		if(ret != read_size) {
			Err("Read next trace id error\n");
			return -1;
		}

	    gettimeofday(&time_end, NULL);
	    float time = CALTIME(time_begin,time_end);
	    read_key_oth_size_ += ret;
	    read_key_oth_time_ += time;

		buf_begin_id_ = cur_trace_id_;
		buf_end_id_ = buf_begin_id_ + read_len;

		for(int i = 0; i < key_first_arr_.size(); ++i) { //TODO(FAST SEARCH)
			if((cur_trace_id_ >= key_first_arr_[i].offset) && (cur_trace_id_ < key_first_arr_[i].offset + key_first_arr_[i].trace_num)) {
				cur_key_first_id_ = i;
				cur_key_first_ = key_first_arr_[cur_key_first_id_];

			    cur_key_first_begin_trace_id_ = cur_key_first_.offset;
			    cur_key_first_end_trace_id_ = cur_key_first_begin_trace_id_ + cur_key_first_.trace_num - 1;
				break;
			}
		}
	}

	char *p_index = key_other_buf_ + one_index_size_ * (cur_trace_id_ - buf_begin_id_);

	char *p_dest = index_ele->key_buf;
	memcpy(p_dest, cur_key_first_.key_buf, key_first_size_);
	p_dest += key_first_size_;
	memcpy(p_dest, p_index, key_other_size_);
	memcpy(&(index_ele->trace_id), p_index + key_other_size_, sizeof(index_ele->trace_id));

	cur_trace_id_ ++;
	if(cur_trace_id_ > cur_key_first_end_trace_id_) {
		cur_key_first_id_ ++;
		cur_key_first_ = key_first_arr_[cur_key_first_id_];

	    cur_key_first_begin_trace_id_ = cur_key_first_.offset;
	    cur_key_first_end_trace_id_ = cur_key_first_begin_trace_id_ + cur_key_first_.trace_num - 1;
	}

	return true;
}

int64_t IndexOrderReader::GetTotalTraceNum() {
	return total_trace_num_;
}

int IndexOrderReader::Search(char *start_value_p) {   //TODO  search the first key whose value >= start_value
	int num = key_first_arr_.size();

	int left_i = 0, right_i = num - 1;

	while(left_i <= right_i) {
		int mid_i = (left_i + right_i) >> 1;

		if(KeyLessThan(key_first_arr_[mid_i].key_buf, start_value_p, key_first_type_)) {
			left_i = mid_i + 1;
		} else {
			right_i = mid_i - 1;
		}
	}

	if(KeyLessThan(key_first_arr_[left_i].key_buf, start_value_p, key_first_type_)) {
		return left_i + 1 >= num ? -1 : left_i + 1;
	} else {
		return left_i;
	}

#if 0
	for(int i = 0; i < num; ++i) {
		if(!KeyLessThan(key_first_arr_[i].key_buf, start_value_p, key_first_type_)) {
			return i;
		}
	}
#endif

}

std::vector<KeyFirst> IndexOrderReader::GetAllValidKeyFirst(const Key &key) {
	char *start_value_p = key.GetStartValueP();
	char *end_value_p = key.GetEndValueP();

	std::vector<KeyFirst> ret_vec;

	int i = Search(start_value_p);
	if(0 > i) {
		return ret_vec;
	} else {
		for(;i < key_first_arr_.size(); ++i) {
			if(!KeyLessThan(end_value_p, key_first_arr_[i].key_buf, key_first_type_)) {
				ret_vec.push_back(key_first_arr_[i]);
			}
		}
	}

	return ret_vec;
}

bool IndexOrderReader::LoadAllKeyFirst() {

	struct timeval time_begin, time_end;
	gettimeofday(&time_begin, NULL);

    struct stat key_first_stat;
    fstat(fd_key_first_, &key_first_stat);
    if(0 != (key_first_stat.st_size % key_first_record_size_)) {
        Err("key_first file is faulty\n");
        return false;
    }
    ScopedPointer<char> key_first_buf(new char[key_first_stat.st_size]);
    key_first_num_ = key_first_stat.st_size / key_first_record_size_;
    int64_t ret = read(fd_key_first_, key_first_buf.data(), key_first_stat.st_size);
    if(ret != key_first_stat.st_size) {
    	Err("Read key first file error\n");
    	return false;
    }

    gettimeofday(&time_end, NULL);
    float time = CALTIME(time_begin,time_end);
    read_key_first_size_ += ret;
    read_key_first_time_ += time;

    char *p = key_first_buf.data();

    for(int i = 0; i < key_first_num_; ++i) {

    	KeyFirst key_first;
    	key_first.key_buf = new char[key_first_size_];

    	memcpy(key_first.key_buf, p, key_first_size_);
        p += key_first_size_;

        memcpy(&(key_first.trace_num), p, sizeof(key_first.trace_num));
        p += sizeof(key_first.trace_num);

        memcpy(&(key_first.offset), p, sizeof(key_first.offset));
        p += sizeof(key_first.offset);

#if 0
        printf("key_first_value = %u, key_first_t_num = %lld, key_first_offset = %lld\n",
        		*((uint*)(key_first.key_buf)), key_first.trace_num, key_first.offset);fflush(stdout);
#endif

        key_first_arr_.push_back(key_first);
    }

    return true;
}


void IndexOrderReader::Close() {

	close(fd_key_first_);
	close(fd_key_oth_);
	close(fd_meta_);

	for(int i = 0; i < key_first_arr_.size(); ++i) {
		if(key_first_arr_[i].key_buf != NULL) {
			delete key_first_arr_[i].key_buf;
			key_first_arr_[i].key_buf = NULL;
		}
	}
	if(NULL != key_other_buf_) {
		delete key_other_buf_;
		key_other_buf_ = NULL;
	}
}

int64_t IndexOrderReader::read_key_first_size_ = 0, IndexOrderReader::read_key_oth_size_ = 0;
float IndexOrderReader::read_key_first_time_ = 0.0, IndexOrderReader::read_key_oth_time_ = 0.0;

