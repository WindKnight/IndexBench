/***************************************************************************
* Copyright (c) 2013, R&D Center of BGP
* All rights reserved.
*
* Filename: gbm_log.cpp
* Abstract: Portal Access Layer of GPP.
*
* author         : zch
* Created on     : Dec 18, 2013
 ***************************************************************************/

#include <stdarg.h>
#include <stdlib.h>
#include <cstdio>
#include <ctime>
#include <string>

#include "util/util_log.h"

std::string g_logAppTag;

#define GBM_FORMAT_OUTPUT(str) \
        do {                                    \
                va_list ap;                     \
                int size = 128;         \
                char* buf = (char*)malloc(size);        \
                while(1) {                                              \
                        va_start(ap, fmt);                              \
                        int bytes = vsnprintf(buf, size, fmt, ap);      \
                        va_end(ap);                                                                     \
                        if(bytes < size) {                                                      \
                                str = buf;                                                              \
                                break;                                                                  \
                        }                                                                                       \
                        size = bytes + 1;                                                       \
                        buf = (char*)realloc(buf, size);                        \
                }                                                                                               \
                free(buf);                                                                              \
        } while(0);


int Warn(const char *fmt, ...) {

        std::string str;
        GBM_FORMAT_OUTPUT(str);

        std::string finalStr = GetTimeString() + std::string(" WARN ") + g_logAppTag + ": " + str;

        printf("%s\n", finalStr.c_str());
        fflush(stdout);
        return finalStr.length();
}

int Err(const char *fmt, ...) {

	 std::string str;
	GBM_FORMAT_OUTPUT(str);

	std::string finalStr = GetTimeString() + std::string(" ERROR ") + g_logAppTag + ": " + str;

	printf("%s\n", finalStr.c_str());
	fflush(stdout);
	return finalStr.length();

}

int Info(const char *fmt, ...) {
	 std::string str;
	GBM_FORMAT_OUTPUT(str);

	std::string finalStr = GetTimeString() + std::string(" INFO ")  + g_logAppTag + ": " + str;

	printf("%s\n", finalStr.c_str());
	fflush(stdout);
	return finalStr.length();
}

int Debug(const char *fmt, ...) {
	 std::string str;
	GBM_FORMAT_OUTPUT(str);

	std::string finalStr = GetTimeString() + std::string(" DEBUG ")  + g_logAppTag + ": " + str;

	printf("%s\n", finalStr.c_str());
	fflush(stdout);
	return finalStr.length();
}

std::string GetTimeString() {

	time_t rawtime;
	time(&rawtime);

	struct tm* timeInfo = localtime(&rawtime);
	char timeBuf[32];
	strftime(timeBuf, 32, "%y/%m/%d %H:%M:%S", timeInfo);
	return std::string(timeBuf);
}

void SetLogAppTag(const std::string& appTag) {
	g_logAppTag = appTag;
}
