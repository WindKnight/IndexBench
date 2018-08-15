/*
 * IndexOrderWriter.cpp
 *
 *  Created on: May 18, 2017
 *      Author: wzb
 */

#include "index_order/index_order_writer.h"
#include "index_order/index_order.h"

#include <stdio.h>
#include <fcntl.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/time.h>
#include <algorithm>

#include "util/util.h"
#include "util/util_log.h"
#include "util/util_string.h"
#include "util/util_scoped_pointer.h"

#include "head_data.h"


struct KeyPosLess {
	KeyPosLess(KeyType key_first_type, bool is_asc) :
		key_first_type_(key_first_type), is_asc_(is_asc) {
//		is_asc_ = IndexOrderKeyWord::IsKeyWordAsc(0);
	}

	bool operator()(const KeyFirstPos &pos1, const KeyFirstPos &pos2) const {
        if(!KeyEqual(pos1.key_first.key_buf, pos2.key_first.key_buf, key_first_type_)) {
        	return (KeyLessThan(pos1.key_first.key_buf, pos2.key_first.key_buf, key_first_type_)^is_asc_) ? false : true;
        }
        return false;
	}

	KeyType key_first_type_;
	bool is_asc_;
};


IndexOrderWriter::IndexOrderWriter(const IndexAttr &index_attr, HeadInfo *head_info) {

	index_type_ = INDEX_TYPE_ORDER;

	index_attr_ = index_attr;
	head_info_ = head_info;
    index_buf_ = NULL;
    key_first_buf_ = NULL;

    real_key_num_ = index_attr.keypos_arr.size();

    key_other_size_ = 0;
    for(int i = 1; i < real_key_num_; ++i) {
    	key_other_size_ += head_info_->GetKeySize(index_attr_.keypos_arr[i]);
    }
    one_index_size_ = key_other_size_ + sizeof(uint64_t);

    key_first_size_ = head_info_->GetKeySize(index_attr_.keypos_arr[0]);
    key_first_type_ = head_info_->GetKeyType(index_attr_.keypos_arr[0]);
//    one_index_size_ = (real_key_num_ - 1) * sizeof(KWValue) + ; //TODO
    cur_key_first_.key_buf = NULL;
    key_first_record_size_ = key_first_size_ + sizeof(cur_key_first_.trace_num) + sizeof(cur_key_first_.offset);

}

IndexOrderWriter::~IndexOrderWriter() {
    if (NULL != index_buf_) {
        delete index_buf_;
        index_buf_ = NULL;
    }
    if (NULL != key_first_buf_) {
        delete key_first_buf_;
        key_first_buf_ = NULL;
    }
    if (NULL != cur_key_first_.key_buf) {
    	delete cur_key_first_.key_buf;
    	cur_key_first_.key_buf = NULL;
    }
}

bool IndexOrderWriter::OpenWrite(uint16_t reducer_id) {

    index_dir_ = Index::GetIndexFullDir(index_attr_, index_type_);

    std::string fname_base = index_dir_ + "/index";
    std::string suffix = "_reduce" + ToString(reducer_id);
    std::string fname_key_first = fname_base + "_key_first" + suffix;
    std::string fname_key_oth = fname_base + "_key_oth" + suffix;

    std::string fname_meta = fname_base + ".meta." + ToString(reducer_id);

    fd_key_first_ = open(fname_key_first.c_str(), O_WRONLY | O_CREAT | O_TRUNC, ACCESSPERMS);
    if(fd_key_first_ < 0) {
        return false;
    }

    fd_key_oth_ = open(fname_key_oth.c_str(), O_WRONLY | O_CREAT | O_TRUNC, ACCESSPERMS);
    if(fd_key_oth_ < 0) {
        return false;
    }


    fd_meta_ = open(fname_meta.c_str(), O_WRONLY | O_CREAT | O_TRUNC, ACCESSPERMS);
    if(fd_meta_ < 0) {
    	return false;
    }

    if(!InitWriteBuf()) {
    	return false;
    }

    total_trace_num_ = 0;

    return true;

}

bool IndexOrderWriter::WriteOneIndex(const IndexElement &index) {
	if(index.key_buf == NULL) {
		Err("Write Index ERROR, there is no key in the index\n");
		return false;
	}

    if(init_key_first_) {
//    	printf("key_first = %d\n", index.kv_arr[0].int_value);fflush(stdout);
//    	cur_key_first_.value = index.kv_arr[0];
    	memcpy(cur_key_first_.key_buf, index.key_buf, key_first_size_);
    	cur_key_first_.trace_num = 0;
    	cur_key_first_.offset = 0;
        init_key_first_ = false;
    }

//    if(cur_key_first_.value.int_value != index.kv_arr[0].int_value) { // TODO how to deal with float
    if(!KeyEqual(cur_key_first_.key_buf, index.key_buf, key_first_type_)) {
//    	printf("key_first = %d\n", index.kv_arr[0].int_value);fflush(stdout);
        if(!WriteKeyFirst(fd_key_first_, cur_key_first_)) {
        	Err("Write Index Error, Record key_first error\n");
        	return false;
        }
//        cur_key_first_.value = index.kv_arr[0];
        memcpy(cur_key_first_.key_buf, index.key_buf, key_first_size_);
        cur_key_first_.offset += cur_key_first_.trace_num;
        cur_key_first_.trace_num = 0;
    }

    if(real_key_num_ >= 1) {
    	if(!WriteKeyOther(index)) {
        	Err("Write Index Error, Record key other error\n");
        	return false;
    	}
    }

    total_trace_num_ ++;

    return true;

}
void IndexOrderWriter::CloseWrite() {
    if(!WriteKeyFirst(fd_key_first_, cur_key_first_)) {
    	Err("Close Write ERROR, Record key_first error\n");
    	exit(-1);
    }
    if(!FlushKeyFirst(fd_key_first_)) {
        Err("Close Write ERROR, Flush Key First ERROR\n");
        exit(-1);
    }

    if(!WriteMeta()) {
        Err("Close Write ERROR, Write meta ERROR\n");
        exit(-1);
    }

    if(!Flush()) {
        Err("Close Write ERROR, Flush Index ERROR\n");
        exit(-1);
    }

    close(fd_key_first_);
    close(fd_key_oth_);
    close(fd_meta_);

    if (NULL != index_buf_) {
        delete index_buf_;
        index_buf_ = NULL;
    }
    if (NULL != key_first_buf_) {
        delete key_first_buf_;
        key_first_buf_ = NULL;
    }
    if (NULL != cur_key_first_.key_buf) {
    	delete cur_key_first_.key_buf;
    	cur_key_first_.key_buf = NULL;
    }
}

bool IndexOrderWriter::Merge(int reducer_num) {

#if 1

    struct timeval time_begin, time_end;

    gettimeofday(&time_begin, NULL);

    std::string fname_base = index_dir_ + "/index";

    std::string fname_key_first = fname_base + "_key_first";
    std::string fname_key_oth = fname_base + "_key_oth";

    if(reducer_num <= 1) {

        std::string fname_key_first_r0 = fname_key_first + "_reduce0";
        std::string fname_key_oth_r0 = fname_key_oth + "_reduce0";

        int iRet = rename(fname_key_first_r0.c_str(), fname_key_first.c_str());
        if(0 > iRet) {
        	Err("Merge first key of reducer 0 fail\n");
        	return false;
        }

        iRet = rename(fname_key_oth_r0.c_str(), fname_key_oth.c_str());
        if(0 > iRet) {
        	Err("Merge other key of reducer 0 fail\n");
        	return false;
        }


        if(! MergeMeta(fname_base, reducer_num)) {
            Err("Write Index Meta ERROR\n");
            return false;
        }
        return true;
    } else {


    	if(!InitWriteBuf()) {
    		return false;
    	}
        uint32_t merge_buf_len = MERGE_BUF_SIZE << 20;
        ScopedPointer<char> merge_buf(new char[merge_buf_len]);

        int fd_key_first = open(fname_key_first.c_str(), O_WRONLY | O_CREAT | O_TRUNC, ACCESSPERMS);
        if(fd_key_first < 0) {
            return false;
        }
        int fd_key_oth = open(fname_key_oth.c_str(), O_WRONLY | O_CREAT | O_TRUNC, ACCESSPERMS);
        if(fd_key_oth < 0) {
            return false;
        }

        gettimeofday(&time_end, NULL);
        float time_stage_0 = CALTIME(time_begin, time_end);
        printf("time_state_0 = %f\n", time_stage_0);fflush(stdout);
        /*
         * read all key_first and get the reducers they belong to
         */

        std::vector<KeyFirstPos> key_first_pos_arr;

        gettimeofday(&time_begin, NULL);

        for(int reducer_id = 0; reducer_id < reducer_num; ++ reducer_id) {
        	KeyFirstPos pos;
            pos.reducer_id = reducer_id;
            std::string suffix = "_reduce" + ToString(reducer_id);
            std::string fname_key_first_r = fname_base + "_key_first" + suffix;

            int fd_key_first_r = open(fname_key_first_r.c_str(), O_RDONLY );
            if(fd_key_first_r < 0)
    	    {
            	printf("reduce file : %s does not exist\n", fname_key_first_r.c_str());
                return false;
    	    }

            struct stat key_first_stat;
            fstat(fd_key_first_r, &key_first_stat);
            uint64_t key_first_size = key_first_stat.st_size;
            if(0 != (key_first_size % key_first_record_size_)) {
                Err("key_first file of reducer : %d ERROR\n", reducer_id);
                return false;
            }
            int key_first_num = key_first_size / key_first_record_size_;

            for(int i = 0; i < key_first_num; ++i) {
            	pos.key_first.key_buf = new char[key_first_size_];
            	ReadKeyFirst(fd_key_first_r, &(pos.key_first));

                pos.offset = i;
                key_first_pos_arr.push_back(pos);
            }
            close(fd_key_first_r);
        }

        gettimeofday(&time_end, NULL);
        float time_stage_1 = CALTIME(time_begin, time_end);
        printf("time_state_1 = %f\n", time_stage_1);fflush(stdout);


        gettimeofday(&time_begin, NULL);
        /*
         * open all key_oth_reducer file to read index data
         */
        int *fd_key_oth_arr = new int[reducer_num];
        for(int reducer_id = 0; reducer_id < reducer_num; ++ reducer_id) {

            std::string suffix = "_reduce" + ToString(reducer_id);
            std::string fname_key_oth_r = fname_base + "_key_oth" + suffix;

            fd_key_oth_arr[reducer_id] = open(fname_key_oth_r.c_str(), O_RDONLY );
            if(fd_key_oth_arr[reducer_id] < 0)
                return false;

            struct stat key_oth_stat;
            fstat(fd_key_oth_arr[reducer_id], &key_oth_stat);
            uint64_t key_oth_size = key_oth_stat.st_size;
            if(0 != (key_oth_size % one_index_size_)) {
                Err("Key other file of reducer : %d ERROR\n", reducer_id);
                return false;
            }
        }

        gettimeofday(&time_end, NULL);
        float time_stage_2 = CALTIME(time_begin, time_end);
        printf("time_state_2 = %f\n", time_stage_2);fflush(stdout);



        gettimeofday(&time_begin, NULL);
        /*
         * iterate on key_first and merge all index together
         */
        uint64_t base_offset = 0;


        //TODO sort
        bool is_asc = index_attr_.asc_arr_[0];
        std::sort(key_first_pos_arr.begin(), key_first_pos_arr.end(), KeyPosLess(key_first_type_, is_asc));

        for(std::vector<KeyFirstPos>::iterator it = key_first_pos_arr.begin(); it != key_first_pos_arr.end(); ++it) {
        	int reducer_id = it->reducer_id;

        	uint64_t file_offset =  it->key_first.offset * one_index_size_;

        	lseek(fd_key_oth_arr[reducer_id], file_offset, SEEK_SET);
        	uint64_t left_size = it->key_first.trace_num * one_index_size_;
        	while(left_size > 0) {
        		int read_size = left_size > merge_buf_len ? merge_buf_len : left_size;
        		int iRet = read(fd_key_oth_arr[reducer_id], merge_buf.data(), read_size);
        		if(iRet != read_size) {
        			Err("Merge ERROR, Read key_oth of reducer : %d ERROR\n", reducer_id);
        			return false;
        		}
        		iRet = write(fd_key_oth, merge_buf.data(), read_size);
        		if(iRet != read_size) {
        			Err("Merge ERROR, Write key_oth to final result file ERROR\n");
        			return false;
        		}
        		left_size -= read_size;
        	}

        	it->key_first.offset = base_offset;
        	WriteKeyFirst(fd_key_first, it->key_first);

        	base_offset += it->key_first.trace_num;

        	if(NULL != (it->key_first).key_buf) {
            	delete (it->key_first).key_buf;
            	(it->key_first).key_buf = NULL;
        	}
        }


        gettimeofday(&time_end, NULL);
        float time_stage_3 = CALTIME(time_begin, time_end);
        printf("time_state_3 = %f\n", time_stage_3);fflush(stdout);


        gettimeofday(&time_begin, NULL);


        if(!FlushKeyFirst(fd_key_first)) {
        	Err("Merge failed, flush key first error\n");
        	return false;
        }

        gettimeofday(&time_end, NULL);
        float time_stage_31 = CALTIME(time_begin, time_end);
        printf("time_state_31 = %f\n", time_stage_31);fflush(stdout);


        /*
         * finalize
         */
        close(fd_key_first);
        close(fd_key_oth);

        delete key_first_buf_;
        key_first_buf_ = NULL;



        /*
         * delete all temporary index files generated by reducers
         */

        gettimeofday(&time_begin, NULL);
        for(int reducer_id = 0; reducer_id < reducer_num; reducer_id ++) {

            std::string suffix = "_reduce" + ToString(reducer_id);
            std::string fname_key_first_r = fname_base + "_key_first" + suffix;
            std::string fname_key_oth_r = fname_base + "_key_oth" + suffix;

        	close(fd_key_oth_arr[reducer_id]);

        	int iRet = unlink(fname_key_first_r.c_str());
        	if(0 > iRet) {
        		Warn("Delete key_first file of reduce : %d ERROR\n", reducer_id);
        	}

        	iRet = unlink(fname_key_oth_r.c_str());
        	if(0 > iRet) {
        		Warn("Delete key_other file of reduce : %d ERROR\n", reducer_id);
        	}

        }
        gettimeofday(&time_end, NULL);
        float time_stage_4 = CALTIME(time_begin, time_end);
        printf("time_state_4 = %f\n", time_stage_4);fflush(stdout);


        delete fd_key_oth_arr;
        fd_key_oth_arr = NULL;


        gettimeofday(&time_begin, NULL);

        if(! MergeMeta(fname_base, reducer_num)) {
            Err("Write Index Meta ERROR\n");
            return false;
        }

        gettimeofday(&time_end, NULL);
        float time_stage_5 = CALTIME(time_begin, time_end);
        printf("time_state_5 = %f\n", time_stage_5);fflush(stdout);

        return true;
    }
#endif

}


bool IndexOrderWriter::WriteMeta() {
    if(sizeof(total_trace_num_) != write(fd_meta_, &total_trace_num_, sizeof(total_trace_num_))) {
    	return false;
    }

    return true;
}


bool IndexOrderWriter::MergeMeta(const std::string &fname_base, int reduce_num) {
	std::string fname_meta = fname_base + ".meta";

	int64_t total_trace_num = 0;

	for(int i = 0 ; i < reduce_num; ++i) {
		std::string fname_meta_r = fname_meta + "." + ToString(i);

		int fd_meta_r = open(fname_meta_r.c_str(), O_RDONLY);
		if(fd_meta_r < 0) {
			return false;
		}

		int64_t trace_num_r;
	    if(sizeof(trace_num_r) != read(fd_meta_r, &trace_num_r, sizeof(trace_num_r))) {
	    	close(fd_meta_r);
	    	return false;
	    }

	    total_trace_num += trace_num_r;
	    close(fd_meta_r);

	    int iRet = unlink(fname_meta_r.c_str());
	    if(0 > iRet) {
    		Warn("Delete meta file of reduce : %d ERROR\n", i);
	    }
	}

    int fd_meta = open(fname_meta.c_str(), O_WRONLY | O_CREAT | O_TRUNC, ACCESSPERMS);
    if(fd_meta < 0) {
    	return false;
    }
    if(sizeof(total_trace_num) != write(fd_meta, &total_trace_num, sizeof(total_trace_num))) {
    	close(fd_meta);
    	return false;
    }

    close(fd_meta);
    return true;
}


#if 0
bool IndexOrderWriter::WriteIndexMeta(const std::string &fname_base) {
    std::string meta_filename = fname_base;
    meta_filename += ".meta";

    int fd_meta = open(meta_filename.c_str(), O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IRGRP | S_IROTH);
    if(fd_meta < 0) {
        Err("Write Index Meta ERROR\n");
    }

#if 0
    if(sizeof(int) != write(fd_meta, &real_key_num_, sizeof(real_key_num_))) {
        close(fd_meta);
        return false;
    }


    for(int i = 0; i < real_key_num_; ++i) {
        int ncode = index_attr_.keycode_arr[i];
        if(sizeof(int) != write(fd_meta, &ncode, sizeof(ncode))) {
            close(fd_meta);
            return false;
        }
    }
#endif

    if(sizeof(total_trace_num_) != write(fd_meta, &total_trace_num_, sizeof(total_trace_num_))) {
    	close(fd_meta);
    	return false;
    }

    close(fd_meta);
    return true;
}

#endif


bool IndexOrderWriter::InitWriteBuf() {
    max_buf_index_num_ = (MAX_BUF_SIZE << 20) / one_index_size_;
    int real_buf_size = max_buf_index_num_ * one_index_size_;
    index_buf_ = new char[real_buf_size];
    if(NULL == index_buf_) {
    	Err("New index buf error\n");
    	return false;
    }
    record_p_ = index_buf_;
    cur_buf_index_num_ = 0;

    init_key_first_ = true;
    max_key_first_num_ = (MAX_KEY_FIRST_BUF_SIZE << 20) / key_first_record_size_;
    key_first_buf_ = new char[key_first_record_size_ * max_key_first_num_];
    if(NULL == key_first_buf_) {
    	Err("New key first buf error\n");
    	return false;
    }
    record_p_key_first_ = key_first_buf_;
    cur_key_first_num_ = 0;

    cur_key_first_.key_buf = new char[key_first_size_];

    return true;
}


bool IndexOrderWriter::WriteKeyFirst(int fd, const KeyFirst &key_first) {
//    char *p = key_first_buf_;

    memcpy(record_p_key_first_, key_first.key_buf, key_first_size_);
    record_p_key_first_ += key_first_size_;
    memcpy(record_p_key_first_, &key_first.trace_num, sizeof(key_first.trace_num));
    record_p_key_first_ += sizeof(key_first.trace_num);
    memcpy(record_p_key_first_, &key_first.offset, sizeof(key_first.offset));
    record_p_key_first_ += sizeof(key_first.offset);

//	printf("write_key_first, key = %d, trace_num = %d, offset = %d\n", *((int*)key_first.key_buf), key_first.trace_num, key_first.offset);

    cur_key_first_num_ ++;
    if(cur_key_first_num_ >= max_key_first_num_) {
    	if(!FlushKeyFirst(fd)) {
            Err("Flush Key First ERROR\n");
            return false;
    	}
    }

    return true;
}

bool IndexOrderWriter::ReadKeyFirst(int fd, KeyFirst *key_first) {
	if(key_first_record_size_ != read(fd, key_first_buf_, key_first_record_size_))
		return false;

	char *p = key_first_buf_;
//	key_first->value = *((KWValue *)p);
	memcpy(key_first->key_buf, p, key_first_size_);
    p += key_first_size_;

    memcpy(&(key_first->trace_num), p, sizeof(uint64_t));
    p += sizeof(key_first->trace_num);

    memcpy(&(key_first->offset), p, sizeof(uint64_t));
    return true;
}

bool IndexOrderWriter::WriteKeyOther(const IndexElement &index) {
//    int tmp_size = sizeof(KWValue) * (real_key_num_ - 1);
//    memcpy(record_p_, &(index.kv_arr[1]), tmp_size); //TODO
	//    record_p_ += tmp_size;
	memcpy(record_p_, index.key_buf + key_first_size_, key_other_size_);
	record_p_ += key_other_size_;
    memcpy(record_p_, &(index.trace_id), sizeof(index.trace_id));
    record_p_ += sizeof(index.trace_id);

    cur_buf_index_num_ ++;
    cur_key_first_.trace_num ++;
    if(cur_buf_index_num_ >= max_buf_index_num_) {
        if(!Flush()) {
            Err("Flush Index ERROR\n");
            return false;
        }
    }
    return true;
}

bool IndexOrderWriter::Flush() {
    int write_size = cur_buf_index_num_ * one_index_size_;
    if(write_size != write(fd_key_oth_, index_buf_, write_size))
        return false;
    record_p_ = index_buf_;
    cur_buf_index_num_ = 0;

    return true;
}

bool IndexOrderWriter::FlushKeyFirst(int fd) {
	int write_size = cur_key_first_num_ * key_first_record_size_;

    if(write_size != write(fd, key_first_buf_, write_size))
        return false;

    record_p_key_first_ = key_first_buf_;
    cur_key_first_num_ = 0;
    return true;
}


