/*
 * util.h
 *
 *  Created on: May 24, 2018
 *      Author: wyd
 */


#include "util/util.h"
#include "job_para.h"
#include <stdio.h>

void ClearCache() {

	std::string clear_cache = JobPara::GetPara("ClearCache");
	if("true" == clear_cache) {
		printf("Clear Cache...\n");
		system("clr_cache");
	}
}

