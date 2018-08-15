/*
 * index.cpp
 *
 *  Created on: May 1, 2018
 *      Author: wyd
 */



#include "index.h"

#include <fcntl.h>
#include <dirent.h>
#include <sys/stat.h>
#include <queue>
#include <string.h>
#include <stdio.h>


#include "job_para.h"
#include "util/util_string.h"



Index::Index(const IndexAttr &index_attr, HeadInfo *head_info) :
	index_attr_(index_attr), head_info_(head_info){
}


/*
 * static methods
 */

bool Index::IsValid(const IndexAttr &index_attr, std::string index_type_name) {
	std::string index_dir = GetIndexRootDir(index_type_name);

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

std::string Index::GetIndexRootDir(std::string index_type_name) {

//	std::string index_dir = INDEX_DIR;
	std::string index_dir = JobPara::GetPara("RootDir") + "/index_data";
	int key_num = ToInt(JobPara::GetPara("HeadKeyNum"));
	std::string index_root_dir = index_dir + "/" + index_type_name + "/" + ToString(key_num) + "keys_" + JobPara::GetDataRelatedName();

	if("index_order" != index_type_name) {
		std::string block_size = JobPara::GetPara("BlockSize");
		index_root_dir += "_" + block_size;
	}

	return index_root_dir;
}

std::string Index::GetIndexFullDir(const IndexAttr &index_attr, std::string index_type_name) {
	std::string index_dir = GetIndexRootDir(index_type_name);

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

#if 0

IndexAttr Index::FindIndex(const IndexAttr &index_attr, std::string index_type_name) {
	IndexAttr null_ret;
	std::string index_dir = GetIndexRootDir(index_type_name);

//	printf("index_root_dir in find = %s\n", index_dir.c_str());

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

IndexAttr Index::FindIndex(const IndexAttr &index_attr, std::string index_type_name) {
	IndexAttr null_ret;
	std::string index_dir = GetIndexRootDir(index_type_name);

//	printf("index_root_dir in find = %s\n", index_dir.c_str());

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
		return null_ret;
	} // else
}

void Swap(IndexAttr *p_index_attr, int i, int j) {
	int tmp_key_id = p_index_attr->keypos_arr[i];
	p_index_attr->keypos_arr[i] = p_index_attr->keypos_arr[j];
	p_index_attr->keypos_arr[j] = tmp_key_id;

	bool tmp_asc = p_index_attr->asc_arr_[i];
	p_index_attr->asc_arr_[i] = p_index_attr->asc_arr_[j];
	p_index_attr->asc_arr_[j] = tmp_asc;
}

void Permutation(int n, IndexAttr *p_index_attr, std::vector<IndexAttr> *p_ret) {

	int key_id_num = p_index_attr->keypos_arr.size();

	if(n == key_id_num) {
		printf("Permutation : ");
		for(int i = 0; i < p_index_attr->keypos_arr.size(); ++i) {
			printf("%d, ", p_index_attr->keypos_arr[i]);
		}
		printf("\n");

		p_ret->push_back(*p_index_attr);
		return;
	}

	for(int i = n; i < key_id_num; ++i) {
		Swap(p_index_attr, n, i);
		Permutation(n + 1, p_index_attr, p_ret);
		Swap(p_index_attr, n, i);
	}
}

IndexAttr Index::FindIndexIgnoreOrder(const IndexAttr &index_attr, std::string index_type_name, int strict_order_key_num) {

	printf("Search for index : ");
	for(int i = 0; i < index_attr.keypos_arr.size(); ++i) {
		printf("%d, ", index_attr.keypos_arr[i]);
	}
	printf("\n");

//	IndexAttr strict_order_attr;
//	for(int i = 0; i < strict_order_key_num; ++i) {
//		strict_order_attr.keypos_arr.push_back(index_attr.keypos_arr[i]);
//		strict_order_attr.asc_arr_.push_back(index_attr.asc_arr_[i]);
//	}

	std::vector<IndexAttr> all_permutation;

	IndexAttr tmp_attr = index_attr;
	Permutation(strict_order_key_num, &tmp_attr, &all_permutation);

	IndexAttr ret;

	for(int i = 0; i < all_permutation.size(); ++i) {
		ret = FindIndex(all_permutation[i], index_type_name);
		if(!ret.keypos_arr.empty()) {

			printf("Find Index : ");
			for(int i = 0; i < ret.keypos_arr.size(); ++i) {
				printf("%d, ", ret.keypos_arr[i]);
			}
			printf("\n");

			return ret;
		}
	}



	return ret;
}

