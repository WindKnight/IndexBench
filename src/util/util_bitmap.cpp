/*
 * seis_util_bitmap.cpp
 *
 *  Created on: Jun 7, 2017
 *      Author: wzb
 */


#include "util/util_bitmap.h"
#include <stdio.h>
#include <string.h>

#include "util/util_log.h"


Bitmap::Bitmap(int64_t length) {
	if(length > 0) {
		max_length_ = length;
		buf_size_ = (length >> SHIFT) + 1;
		buf_ = new uint8_t[buf_size_];
		memset(buf_, 0, buf_size_);
	} else {
		max_length_ = 0;
		buf_size_ = 0;
		buf_ = NULL;
	}
}
Bitmap::~Bitmap(){
	if(NULL != buf_) {
		delete buf_;
		buf_ = NULL;
	}
}

void Bitmap::SetAll() {
	memset(buf_, 0xff, buf_size_);
}

void Bitmap::ClearAll() {
	memset(buf_, 0x00, buf_size_);
}

bool Bitmap::SetBit(uint64_t pos){

	if(pos > max_length_) {
		Err("pos : %llu is out of the bitmap's range\n", pos);
		return false;
	}
	int64_t index = pos >> SHIFT;
	int offset = pos & MASK;
	buf_[index] |= 1 << offset;

	return true;

}
bool Bitmap::ClearBit(uint64_t pos){

	if(pos > max_length_) {
		Err("pos : %llu is out of the bitmap's range\n", pos);
		return false;
	}

	int64_t index = pos >> SHIFT;
	int offset = pos & MASK;
	buf_[index] &= ~(1 << offset);

	return true;

}
bool Bitmap::IsSet(uint64_t pos){

	if(pos > max_length_) {
		Err("pos : %llu is out of the bitmap's range\n", pos);
		return false;
	}

	int64_t index = pos >> SHIFT;
	int offset = pos & MASK;
	uint8_t tmp = buf_[index] & (1 << offset);
	return tmp == 0 ? false : true;

}

void Bitmap::And(Bitmap *bit_map) {
	for(int64_t i = 0; i < buf_size_; ++i) {
		buf_[i] &= bit_map->buf_[i];
	}
}

void Bitmap::PrintBitmap() {
	printf("bitmap is :\n");
	for(int i = 0; i < max_length_; ++i) {
		if(IsSet(i)) {
			printf("1");
		} else {
			printf("0");
		}
	}
	printf("\n");
}



