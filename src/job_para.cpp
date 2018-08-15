/*
 * JobPara.cpp
 *
 *  Created on: Apr 18, 2018
 *      Author: wyd
 */



#include "job_para.h"

#include <stdio.h>
#include <string.h>
#include <getopt.h>
#include <vector>

#include "util/util_string.h"
#include "util/util_log.h"

bool JobPara::ParsePara(int argc, char **argv) {
	para_map_ = new ParaMap();
    std::string my_conf_filename;
    const char *short_options = "f:"; //use : if there is option followed
    const struct option long_options[] = {
            {"conf", 1, NULL, 'f'},
            {0, 0, 0, 0}
    };
    char c;
    while((c = getopt_long(argc, argv, short_options, long_options, NULL)) != -1) {
        switch(c) {
        case 'f' :
            my_conf_filename = optarg;
            break;
        default:
            break;
        }
    }

    FILE *fConf = fopen(my_conf_filename.c_str(),"r");
    if(NULL == fConf) {
        printf("conf does not exist\n");fflush(stdout);
        return false;
    }
    char tmp[256];

    while (NULL != fgets(tmp,256,fConf)) {
        int len = strlen(tmp);
        if(tmp[len - 1] == '\n')
            tmp[len - 1] = '\0';

        std::string str_tmp = tmp;
        size_t comment_pos = str_tmp.find_first_of("#");
        if(comment_pos != std::string::npos) {
            str_tmp = Trimmed(str_tmp.substr(0, comment_pos));
        }
        if(str_tmp.empty())
            continue;
        std::vector<std::string> tmpVec = SplitString(str_tmp , "=");
        if(tmpVec.size() > 1) {
            std::string key = Trimmed(tmpVec[0]);
            std::string value = Trimmed(tmpVec[1]);

            para_map_->insert(std::make_pair(key,value));
        }
    }
    fclose(fConf);
    return true;
}

std::string JobPara::GetPara(const std::string &name) {
	ParaMap::const_iterator it = para_map_->find(name);
	if(it != para_map_->end()) {
		return it->second;
	}

	return "";
}


std::string JobPara::GetDataRelatedName() {
	std::string data_order = GetPara("DataOrder");
	std::string data_related_name;

	if(data_order == "SOURCE,TRACE") {
		std::string shot_num = GetPara("ShotNum");
		std::string trace_num_per_shot = GetPara("TraceNumPerShot");

		data_related_name = shot_num + "shots_with_" + trace_num_per_shot + "traces";

	} else if(data_order == "CMP_LINE,CMP,TRACE") {
		std::string cmp_line_num = GetPara("CMPLineNum");
		std::string cmp_num = GetPara("CMPNum");
		std::string trace_num_per_cmp_line = GetPara("TraceNumPerCMPLine");

		data_related_name = cmp_line_num + "lines_"
				+ cmp_num + "cmps_with_"
				+ trace_num_per_cmp_line + "traces";
	} else {
		int64_t trace_num = GetTraceNum();
		data_related_name = ToString(trace_num) + "traces";
	}

	return data_related_name;

}

int64_t JobPara::GetValueNumber(int key_id) {
	std::string data_order = GetPara("DataOrder");

	if(data_order == "SOURCE,TRACE") {
		int shot_num = ToInt(GetPara("ShotNum"));
		int trace_num_per_shot = ToInt(GetPara("TraceNumPerShot"));

		switch (key_id) {
		case 0 :
			return shot_num;
			break;
		case 1 :
			return trace_num_per_shot;
			break;
		default :
			return 5000;
		}
	} else if(data_order == "CMP_LINE,CMP,TRACE") {
		int cmp_line_num = ToInt(GetPara("CMPLineNum"));
		int cmp_num = ToInt(GetPara("CMPNum"));
		int trace_num_per_cmp_line = ToInt(GetPara("TraceNumPerCMPLine"));

		switch (key_id) {
		case 1 :
			return trace_num_per_cmp_line;
			break;
		case 2 :
			return cmp_line_num;
			break;
		case 3 :
			return cmp_num;
			break;
		default :
			return 5000;
		}
	} else {
		int cmp_line_num = ToInt(GetPara("CMPLineNum"));
		int cmp_num = ToInt(GetPara("CMPNum"));
		int trace_num_per_cmp_line = ToInt(GetPara("TraceNumPerCMPLine"));

		switch (key_id) {
		case 1 :
			return trace_num_per_cmp_line;
			break;
		case 2 :
			return cmp_line_num;
			break;
		case 3 :
			return cmp_num;
			break;
		default :
			return 5000;
		}
	}
}

int64_t JobPara::GetTraceNum() {
	std::string data_order = JobPara::GetPara("DataOrder");
	int64_t trace_num;

	if(data_order == "SOURCE,TRACE") {
		int shot_num = ToInt(JobPara::GetPara("ShotNum"));
		int trace_num_per_shot = ToInt(JobPara::GetPara("TraceNumPerShot"));

		trace_num = shot_num;
		trace_num = trace_num * trace_num_per_shot;

	} else {
		int cmp_line_num = ToInt(JobPara::GetPara("CMPLineNum"));
		int cmp_num = ToInt(JobPara::GetPara("CMPNum"));
		int trace_num_per_cmp_line = ToInt(JobPara::GetPara("TraceNumPerCMPLine"));

		trace_num = cmp_line_num * cmp_num;
		trace_num = trace_num * trace_num_per_cmp_line;
	}

	return trace_num;
}




ParaMap* JobPara::para_map_ = NULL;
