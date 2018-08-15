/*
 * util.h
 *
 *  Created on: Oct 27, 2016
 *      Author: zch
 */

#ifndef UTIL_H_
#define UTIL_H_

#include <vector>
#include <sys/time.h>
#include <stdint.h>
#include <time.h>
#include <stdlib.h>
#include <stdio.h>


#define CALTIME(time_begin, time_end) \
	time_end.tv_sec - time_begin.tv_sec + 1.0 * (time_end.tv_usec - time_begin.tv_usec) / 1000000

template <class T>
void Shuffle(T *buffer, uint32_t num) {
	struct timeval t;
	gettimeofday(&t, NULL);
	int64_t seed = t.tv_sec * 1000000 + t.tv_usec;
	srand(seed);

	for(int64_t i = 0; i < num; i++) {
		int64_t rand1 = rand();
		int64_t rand2 = rand();
		int64_t rand3 = rand();

		int64_t big_rand = (rand1 << 32) | (rand2 << 16) | rand3;

		big_rand = big_rand % num;
		T tmp = buffer[i];
		buffer[i] = buffer[big_rand];
		buffer[big_rand] = tmp;
	}
}

template <typename T>
void Swap(std::vector<T> *p_attr, int i, int j) {
	T tmp = (*p_attr)[i];
	(*p_attr)[i] = (*p_attr)[j];
	(*p_attr)[j] = tmp;

}


template <typename T>
void Permutation(int n, std::vector<T> *input_arr, std::vector<std::vector<T> > *all_permutation) {

	int enum_num = input_arr->size();

	if(n == enum_num) {
		printf("Permutation : ");
		for(int i = 0; i < input_arr->size(); ++i) {
			printf("%d, ", input_arr[i]);
		}
		printf("\n");

		all_permutation->push_back(*input_arr);
		return;
	}

	for(int i = n; i < enum_num; ++i) {
		Swap(input_arr, n, i);
		Permutation(n + 1, input_arr, all_permutation);
		Swap(input_arr, n, i);
	}
}



void ClearCache();







#endif /* UTIL_H_ */
