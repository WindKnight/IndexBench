/*
 * gcache_io.h
 *
 *  Created on: Apr 24, 2016
 *      Author: wyd
 */

#ifndef GCACHE_IO_H_
#define GCACHE_IO_H_

#include <sys/types.h>
#include <string>

bool RecursiveRemove(const std::string &path);

bool RecursiveMakeDir(const std::string &dir_path);

int64_t SafeWrite(int fd, const char *buf, int64_t size) ;

int64_t SafeRead(int fd, char *buf, int64_t size);


#endif /* GCACHE_IO_H_ */
