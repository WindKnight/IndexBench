/***************************************************************************
* Copyright (c) 2013, R&D Center of BGP
* All rights reserved.
*
* Filename: gbm_log.h
* Abstract: Portal Access Layer of GPP.
*
* author         : zch
* Created on     : Dec 18, 2013
 ***************************************************************************/


#ifndef UTIL_LOG_H_
#define UTIL_LOG_H_

#include <string>

#define PRINTL printf("file : %s, line : %d\n", __FILE__, __LINE__);fflush(stdout);


extern std::string g_logAppTag;

extern "C" {

        int Warn(const char *fmt, ...);
        int Err(const char *fmt, ...);
        int Info(const char *fmt, ...);
        int Debug(const char *fmt, ...);

        std::string GetTimeString();

        void SetLogAppTag(const std::string& appTag);

}


#endif /* UTIL_LOG_H_ */
