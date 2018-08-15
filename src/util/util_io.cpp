/*
 * gcache_io.cpp
 *
 *  Created on: Apr 24, 2016
 *      Author: wyd
 */

#include <dirent.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <iterator>
#include <string>
#include <vector>

#include "util/util_io.h"
#include "util/util_string.h"

bool RecursiveRemove(const std::string &path) {

	DIR *dir = opendir(path.c_str());
	if(dir == NULL) {
//		errno = GBM_ERR_IN_ERRNO;
		return false;
	}
	struct dirent entry;
	struct dirent *result;
	int dret;
	for(dret = readdir_r(dir, &entry, &result);
			result != NULL && dret ==0;
			dret = readdir_r(dir, &entry, &result)) {

		if(strcmp(entry.d_name, ".") == 0
				|| strcmp(entry.d_name, "..") == 0)
			continue;

		std::string entry_path = path + "/" + entry.d_name;

		struct stat statBuf;
		int sret = stat (entry_path.c_str(), &statBuf);
		if(sret < 0) {
//			gbm_errno = GBM_ERR_IN_ERRNO;
			closedir(dir);
			return false;
		}
		if(S_ISDIR(statBuf.st_mode)) {
			bool bRet = RecursiveRemove(entry_path);
			if(!bRet) {
				closedir(dir);
				return false;
			}
		} else {

			int uret = unlink(entry_path.c_str());
			if(uret < 0) {
//				gbm_errno = GBM_ERR_IN_ERRNO;
				closedir(dir);
				return false;
			}
		} // isfile

	} //all files and dirs

	closedir(dir);

	int rret = rmdir(path.c_str());
	if(rret < 0) {
//		gbm_errno = GBM_ERR_IN_ERRNO;
		return false;
	}

	return true;
}

bool RecursiveMakeDir(const std::string &dir_path) {
	std::vector<std::string> paths = SplitString(dir_path, "/");
	std::string path = "";
	for(std::vector<std::string>::iterator it_path = paths.begin(); it_path != paths.end(); ++ it_path) {
		path = path + "/" + *it_path;

		if(0 > access(path.c_str(), F_OK )) {
			if(0 > mkdir(path.c_str(), 0777)) {
				return false;
			}
		}
	}
	return true;
}

#define MAX_WRITE_SIZE 1073741824 //1G

int64_t SafeWrite(int fd, const char *buf, int64_t size) {
	int64_t ret_len = 0;
	int64_t left_size = size;
	while(left_size > 0) {
		int write_size = left_size > MAX_WRITE_SIZE ? MAX_WRITE_SIZE : left_size;
		int write_ret = write(fd, buf + ret_len, write_size);
		if(write_ret < 0)
			return -1;

		ret_len += write_ret;
		left_size -= write_ret;
	}

	return ret_len;
}
#undef MAX_WRITE_SIZE


#define MAX_READ_SIZE 1073741824 //1G

int64_t SafeRead(int fd, char *buf, int64_t size) {
	int64_t ret_len = 0;
	int64_t left_size = size;
	while(left_size > 0) {
		int read_size = left_size > MAX_READ_SIZE ? MAX_READ_SIZE : left_size;
		int read_ret = read(fd, buf + ret_len, read_size);
		if(read_ret < 0)
			return -1;
		else if(read_ret == 0)
			break;
		else {
			ret_len += read_ret;
			left_size -= read_ret;
		}
	}
	return ret_len;
}

#undef MAX_READ_SIZE
