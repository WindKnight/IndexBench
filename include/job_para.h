/*
 * JobPara.h
 *
 *  Created on: Apr 18, 2018
 *      Author: wyd
 */

#ifndef JOBPARA_H_
#define JOBPARA_H_


#include <map>
#include <string>
#include <stdint.h>

typedef std::map<std::string, std::string> ParaMap;

class JobPara {
public:

	static bool ParsePara(int argc, char **argv);
	static std::string GetPara(const std::string &name);
	static std::string GetDataRelatedName();
	static int64_t GetTraceNum();
	static int64_t GetValueNumber(int key_id);
private:

	static ParaMap *para_map_;
};






#endif /* JOBPARA_H_ */
