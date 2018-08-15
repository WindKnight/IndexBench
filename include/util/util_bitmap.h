/*
 * util_bitmap.h
 *
 *  Created on: Jun 7, 2017
 *      Author: wzb
 */

#ifndef UTIL_BITMAP_H_
#define UTIL_BITMAP_H_


#include <stdint.h>


#define SHIFT	3
#define MASK	7

class Bitmap {
public:
	Bitmap(int64_t length);
	~Bitmap();

	void ClearAll();
	void SetAll();

	bool SetBit(uint64_t pos);
	bool ClearBit(uint64_t pos);
	bool IsSet(uint64_t pos);

	void And(Bitmap *bit_map);
	void PrintBitmap();

private:
	uint8_t *buf_;
	int64_t buf_size_;
	int64_t max_length_;
};



#endif /* UTIL_BITMAP_H_ */
