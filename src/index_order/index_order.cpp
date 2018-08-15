/*
 * seis_index_doc_hhj.cpp
 *
 *  Created on: Apr 22, 2018
 *      Author: wyd
 */



#include "index_order/index_order.h"

#include <unistd.h>
#include <sys/time.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>


#include "tbb/parallel_sort.h"

#include "util/util.h"
#include "util/util_io.h"
#include "util/util_string.h"
#include "util/util_log.h"
#include "job_para.h"
#include "index_order/index_order_writer.h"
#include "index_order/selector_order_hhj.h"
#include "head_data.h"




IndexOrder::IndexOrder(const IndexAttr &index_attr, HeadInfo *head_info) :
	Index(index_attr, head_info) {
	index_type_ = INDEX_TYPE_ORDER;
}
IndexOrder::~IndexOrder() {

}

bool IndexOrder::BuildIndex() {

    uint64_t trace_num = JobPara::GetTraceNum();

	printf("Begin to build ORDER index of key : \n");
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
    printf("Total trace num is : %llu\n", trace_num);

	struct timeval t_all_begin, t_all_end;
	gettimeofday(&t_all_begin, NULL);

    int key_num = index_attr_.keypos_arr.size();
    if(key_num > MAX_INDEX_KEY_NUM) { //TODO
        fprintf(stderr, "The number of index keys exceeds the ceiling of 5\n");
        exit(-1);
    }

	std::string index_dir = GetIndexFullDir(index_attr_, index_type_);

	if(0 > access(index_dir.c_str(), F_OK | R_OK | W_OK)) {
		RecursiveRemove(index_dir);
		bool bRet = RecursiveMakeDir(index_dir);
		if(!bRet) {
			printf("Make index dir error\n");
		}
	}

//	int total_key_num = TOTAL_KEY_NUM;
//    int *head_buf = new int[total_key_num];
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
        	fprintf(stderr,"read head error\n");fflush(stderr);
        	exit(-1);
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
//        	KWValue key_tmp;
//        	key_tmp.int_value = head_buf[*it];
//        	index.kv_arr.push_back(key_tmp);
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
    IndexOrderWriter *index_writer = new IndexOrderWriter(index_attr_, head_info_);
    index_writer->OpenWrite(0);
    for(std::vector<IndexElement>::iterator it = index_buf.begin(); it != index_buf.end(); ++it) {

    	index_writer->WriteOneIndex(*it);
    	if(it->key_buf != NULL) {
    		delete it->key_buf;
    		it->key_buf = NULL;
    	}
    }
    index_writer->CloseWrite();
    index_writer->Merge(1);

    gettimeofday(&t_end, NULL);
    float time_write = CALTIME(t_begin, t_end);
    printf("Write out over, write time is : %f s\n", time_write);fflush(stdout);


    gettimeofday(&t_all_end, NULL);
    float time_all = CALTIME(t_all_begin, t_all_end);
    printf("Build ORDER index over, Time Elapsed is : %f s\n", time_all);fflush(stdout);

    delete index_writer;
	return true;
}


bool IndexOrder::Remove() {
	std::string index_dir = GetIndexFullDir(index_attr_, index_type_);
	return RecursiveRemove(index_dir);
}


#if 0

/*
 * static methods
 */

bool IndexOrder::IsValid(const IndexAttr &index_attr) {
	std::string index_dir = GetIndexRootDir();

	for(int i = 0; i < index_attr.keypos_arr.size(); ++i) {
		index_dir += "/" + ToString(index_attr.keypos_arr[i]);
		if(!index_attr.asc_arr_[i]) {
			index_dir += "_d";
		}
	}

	std::string meta_name = index_dir + "/index.meta";
	int iRet = access(meta_name.c_str(), F_OK | R_OK);
	if(0 > iRet) { // access meta error
		return false;
	} else {
		return true;
	}
}

std::string IndexOrder::GetIndexRootDir() {

	std::string index_dir = INDEX_DIR;
	std::string trace_num = JobPara::GetPara("TraceNum");
	std::string index_root_dir = index_dir + "/index_order/" + trace_num + "traces";

	return index_root_dir;
}

std::string IndexOrder::GetIndexFullDir(const IndexAttr &index_attr) {
	std::string index_dir = GetIndexRootDir();

//	std::vector<int> key_pos_arr = order.GetKeyPosArr();
	for(int i = 0; i < index_attr.keypos_arr.size(); ++i) {
		index_dir += "/" + ToString(index_attr.keypos_arr[i]);
		if(!index_attr.asc_arr_[i]) {
			index_dir += "_d";
		}
	}

	return index_dir;
}

typedef std::vector<std::string>  DirSuffix;

std::string GetFullDirPath(const std::string &index_dir, const DirSuffix &suffix) {
	std::string ret_path = index_dir;
	for(int i = 0; i < suffix.size(); ++i) {
		ret_path += "/" + suffix[i];
	}

	return ret_path;
}

IndexAttr GetIndexKeyIDArr(const IndexAttr &required_index_attr, const DirSuffix &suffix) {
	IndexAttr ret_attr = required_index_attr;
	for(int i = 0; i < suffix.size(); ++i) {

		std::string str_tmp = suffix[i];
		bool asc = true;;
		if(EndsWith(str_tmp, "_d")) {
			str_tmp = str_tmp.substr(0, str_tmp.size() - 2);
			asc = false;
		}

		ret_attr.keypos_arr.push_back(ToInt(str_tmp));
		ret_attr.asc_arr_.push_back(asc);
	}
	return ret_attr;
}


IndexAttr IndexOrder::FindIndex(const IndexAttr &index_attr) {
	IndexAttr null_ret;
	std::string index_dir = GetIndexRootDir();

	printf("index_root_dir in find = %s\n", index_dir.c_str());


	for(int i = 0; i < index_attr.keypos_arr.size(); ++i) {
		index_dir += "/" + ToString(index_attr.keypos_arr[i]);
		if(!index_attr.asc_arr_[i]) {
			index_dir += "_d";
		}
	}

	std::string meta_name = index_dir + "/index.meta";
	int iRet = access(meta_name.c_str(), F_OK | R_OK);
	if(0 == iRet) {
		return index_attr;
	} else {
		std::queue<DirSuffix> search_queue;
		DirSuffix empty_suf;
		search_queue.push(empty_suf);

		while(!search_queue.empty()) {
			DirSuffix cur_suffix = search_queue.front();
			search_queue.pop();
			std::string dir_path = GetFullDirPath(index_dir, cur_suffix);

			DIR *pdir = opendir(dir_path.c_str());
			if(NULL == pdir) {
				return null_ret;
			}
			dirent entry;
			dirent *result;
			int dret;
//			printf("dir_path = %s\n", dir_path.c_str());
			for(dret = readdir_r(pdir, &entry, &result);
					result != NULL && dret ==0;
					dret = readdir_r(pdir, &entry, &result)) {

				if(strcmp(entry.d_name, ".") == 0
						|| strcmp(entry.d_name, "..") == 0)
					continue;

//				printf("entry is : %s\n", entry.d_name);

				std::string full_entry_path = dir_path + "/" + entry.d_name;

				struct stat statBuf;
				int sret = stat (full_entry_path.c_str(), &statBuf);
				if(sret < 0) {
					closedir(pdir);
					return null_ret;
				}
				if(S_ISDIR(statBuf.st_mode)) {
//					std::string invalid_flag_name = dir_path + "/index.invalid";
//					if(0 == access(invalid_flag_name.c_str(), F_OK)) {
//						continue;
//					}
					meta_name = dir_path + "/" + entry.d_name + "/index.meta";
//					printf("meta_name = %s\n", meta_name.c_str());fflush(stdout);
					if(0 == access(meta_name.c_str(), F_OK | R_OK)) {
						closedir(pdir);
						cur_suffix.push_back(entry.d_name);
						return GetIndexKeyIDArr(index_attr, cur_suffix);
					} else {
						DirSuffix new_suffix = cur_suffix;
						new_suffix.push_back(entry.d_name);
						search_queue.push(new_suffix);
					}

				}
			}
			closedir(pdir);
		} //while queue

		return null_ret;
	} // else
}

#endif

#if 0
bool IndexOrder::InvalidateIndex(const SeisDataAttr &data_attr, int keycode) {
	std::string index_root_data = GetIndexRootDir(data_attr);
	std::string keycode_str = ToString(keycode);

	std::queue<DirSuffix> search_queue;
	DirSuffix empty_suf;
	search_queue.push(empty_suf);

	while(!search_queue.empty()) {
		DirSuffix cur_suffix = search_queue.front();
		search_queue.pop();
		std::string dir_path = GetFullDirPath(index_root_data, cur_suffix);

		DIR *pdir = opendir(dir_path.c_str());
		if(NULL == pdir) {
			return false;
		}

		dirent entry;
		dirent *result;
		int dret;

		for(dret = readdir_r(pdir, &entry, &result);
				result != NULL && dret ==0;
				dret = readdir_r(pdir, &entry, &result)) { //iterate in one dir

			if(strcmp(entry.d_name, ".") == 0
					|| strcmp(entry.d_name, "..") == 0)
				continue;


			std::string full_entry_path = dir_path + "/" + entry.d_name;

			struct stat statBuf;
			int sret = stat (full_entry_path.c_str(), &statBuf);
			if(sret < 0) {
				closedir(pdir);
				return false;
			}
			if(S_ISDIR(statBuf.st_mode)) {
				if(keycode_str == entry.d_name) {
					std::string invalid_flag_name = dir_path + "/" + entry.d_name + "/index.invalid";
					int fd = creat(invalid_flag_name.c_str(), 0755);
					if(0 > fd) {
						printf("create invalid flag error\n");
						closedir(pdir);
						return false;
					}
				} else {
//					printf("push new entry : %s\n", entry.d_name);fflush(stdout);
					DirSuffix new_suffix = cur_suffix;
					new_suffix.push_back(entry.d_name);
					search_queue.push(new_suffix);
				}
			}// is a dir
		}
		closedir(pdir);
	}
	return true;
}



bool IndexOrder::ValidOrRemove(const IndexAttr &index_attr) {
	std::string index_dir = GetIndexRootDir(index_attr.data_attr);

	for(int i = 0; i < index_attr.keycode_arr.size(); ++i) {
		index_dir += "/" + ToString(index_attr.keycode_arr[i]);
		std::string invalid_flag_name = index_dir + "/index.invalid";
		int iRet = access(invalid_flag_name.c_str(), F_OK);
		if(0 == iRet) {
			RecursiveRemove(index_dir);
			return false;
		}
	}


	std::string meta_name = index_dir + "/index.meta";
	int iRet = access(meta_name.c_str(), F_OK | R_OK);
	if(0 > iRet) { // access meta error
		return false;
	} else {
		return true;
	}
}

#endif

