/*
 * make_data.cpp
 *
 *  Created on: Apr 19, 2018
 *      Author: wyd
 */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "sql_manager.h"
#include "head_data.h"
#include "util/util_string.h"
#include "util/util_io.h"
#include "util/util_log.h"
#include "util/util.h"
#include "job_para.h"


/*
 * key_id	type	value			meaning
 *
 * 0		uint	0-99			shot
 * 1		uint	0-99999			trace
 * 2		uint	0-4999			cmp_line
 * 3		uint	0-4999			cmp
 * 4		float	-1000.0-1000.0	offset
 * 5		float	-180.0-180.0	angle
 */





HeadInfo::HeadInfo(const HeadType &head_type) :
	head_type_(head_type){
	head_size_ = 0;
	int key_offset = 0;
	for(int i = 0; i < head_type_.head_type.size(); ++i) {
		int key_size = (head_type_.head_type[i] & 0x00ff);
		key_size_arr_.push_back(key_size);
		key_offset_arr_.push_back(key_offset);
		head_size_ += key_size;
		key_offset += key_size;
	}
}
HeadInfo::~HeadInfo() {

}

KeyType HeadInfo::GetKeyType(int key_id) const {
	return head_type_.head_type[key_id];
}
int HeadInfo::GetKeySize(int key_id) const {
	return key_size_arr_[key_id];
}
int HeadInfo::GetHeadSize() const {
	return head_size_;
}

int HeadInfo::GetKeyOffset(int key_id) const {
	return key_offset_arr_[key_id];
}

int HeadInfo::GetKeyNum() {
	return key_size_arr_.size();
}

HeadData::HeadData(HeadInfo *head_info) :
	head_info_(head_info){

	total_key_num_ = head_info_->GetKeyNum();
	head_size_ = head_info_->GetHeadSize();
}
HeadData::~HeadData() {

}

std::string HeadData::GetHeadName(HeadInfo *head_info) {
	std::string head_data_dir = JobPara::GetPara("RootDir") + "/head_data";
//	std::string head_data_dir = HEAD_DATA_DIR;

	if(0 > access(head_data_dir.c_str(), F_OK | R_OK | W_OK)) {
		RecursiveRemove(head_data_dir);
		bool bRet = RecursiveMakeDir(head_data_dir);
		if(!bRet) {
			Err("Make index dir error\n");
		}
	}

//	std::string data_order = JobPara::GetPara("DataOrder");

	int key_num = head_info->GetKeyNum();
	std::string head_name = head_data_dir + "/head_" + ToString(key_num) + "keys_" + JobPara::GetDataRelatedName();

	return head_name;
}

std::string HeadData::GetSqlTableName(HeadInfo *head_info) {
	int key_num = head_info->GetKeyNum();
	std::string head_name;
	head_name += "head_" + ToString(key_num) + "keys_" + JobPara::GetDataRelatedName();

	return head_name;
}


void PrintProgress(float progress) {
	int int_p = progress;
	printf("progress : %d\%\n", int_p);
}


void HeadData::InsertToSql(SqlManager *sql_man, uint32_t *p, int64_t total_trace_id) {
	std::string insert_query = "insert into " + GetSqlTableName(head_info_);

	std::string column_list = "(";

	for(int i = 0; i < 6; ++i) {
		column_list += key_name[i];
		column_list += ",";
	}

	for(int i = 6; i < total_key_num_; ++i) {
		column_list += "tmp_" + ToString(i) + ", ";
	}
	column_list += "trace_id)";
	insert_query += column_list;
	insert_query += "values ";
	std::string value_list = "(" + ToString(p[0]) + ","
			+ ToString(p[1]) + ","
			+ ToString(p[2]) + ","
			+ ToString(p[3]) + ","
			+ Float2Str(((float*)p)[4]) + ","
			+ Float2Str(((float*)p)[5]) + ",";

	for(int i = 6; i < total_key_num_; ++i) {
		value_list += ToString(p[i]) + ",";
	}
	value_list += Int642Str(total_trace_id + 1) + ")";
	insert_query += value_list;

	sql_man->Query(insert_query);
}

bool HeadData::MakeData(bool make_sql) {

//	printf("head_size_ = %d\n", head_size_);
//	int head_size = 4 * sizeof(uint) + 2 * sizeof(float);

	printf("Making data...\n");
	std::string head_name = GetHeadName(head_info_);
	if(0 == access(head_name.c_str(), F_OK | R_OK)) {
		struct stat f_stat;
		int ret = stat(head_name.c_str(), &f_stat);

		int64_t trace_num =  JobPara::GetTraceNum();
		int64_t total_head_size = head_size_;
		total_head_size *= trace_num;

//		printf("total_head_size = %lld, file_size = %lld\n", total_head_size, f_stat.st_size);
		if(total_head_size == f_stat.st_size)
			return true;
		else {
			ret = remove(head_name.c_str());
			if(0 != ret) {
				Err("remove invalid head data file : %s error\n", head_name.c_str());
				return false;
			}
		}
	}

	std::string sql_table_name = GetSqlTableName(head_info_);

	int fd = open(head_name.c_str(), O_WRONLY | O_TRUNC | O_CREAT, 0644);

	SqlManager *sql_man = NULL;

	if(make_sql) {
		sql_man = new SqlManager();
		if(!sql_man->Init()) {
			Err("sql_manager init error\n");
			return false;
		}

		std::string create_table_query = "create table if not exists ";
		create_table_query += sql_table_name;
		 std::string column_name_list =
				"(shot int not null, "
				"trace int not null, "
				"cmp_line int not null, "
				"cmp int not null, "
				"offset float not null, "
				"angel float not null,";
		for(int i = 6; i < total_key_num_; ++i) {
			column_name_list += "tmp_" + ToString(i) + " int not null, ";
		}

		column_name_list += "trace_id bigint not null auto_increment, primary key (trace_id))";
		create_table_query += column_name_list;
		create_table_query += "ENGINE=InnoDB DEFAULT CHARSET=utf8";

		printf("create table query = %s\n", create_table_query.c_str());

		sql_man->Query(create_table_query);
	}


	std::string data_order = JobPara::GetPara("DataOrder");

	int64_t total_trace_id = 0;

	if(data_order == "SOURCE,TRACE") { //Source Trace

		int shot_num = ToInt(JobPara::GetPara("ShotNum"));
		int trace_num_per_shot = ToInt(JobPara::GetPara("TraceNumPerShot"));

		int head_len = total_key_num_ * trace_num_per_shot;
		uint32_t *head_buf = new uint32_t[head_len];


//		uint32_t shot_num = (trace_num + TRACE_NUM_IN_ONE_SHOT - 1) / TRACE_NUM_IN_ONE_SHOT;
		uint32_t *shot_buf = new uint32_t[shot_num];
		for(uint32_t i = 0; i < shot_num; ++i) {
			shot_buf[i] = i;
		}
	//	Shuffle(shot_buf, shot_num);

//		int64_t left_trace_num = trace_num;
		uint32_t *trace_buf = new uint32_t[trace_num_per_shot];
		for(uint32_t i = 0; i < trace_num_per_shot; ++i) {
			trace_buf[i] = i;
		}

		for(int shot_i = 0; shot_i < shot_num; ++shot_i) {
			uint32_t *p = head_buf;

	//		Shuffle(trace_buf, TRACE_NUM_IN_ONE_SHOT);
			srand(time(NULL));
			for(int trace_id = 0; trace_id < trace_num_per_shot; ++trace_id, p += total_key_num_) {
				p[0] = shot_buf[shot_i];
				p[1] = trace_buf[trace_id];
				p[2] = rand() % 5000;
				p[3] = rand() % 5000;
				((float*)p)[4] = rand() % 1000 + (1.0 * (rand() % 1000) / 1000.0);
				((float*)p)[5] = rand() % 180 + (1.0 * (rand() % 1000) / 1000.0);

				for(int column_i = 6; column_i < total_key_num_; ++column_i) {
					p[column_i] = rand() % 5000;
				}

				if(make_sql) {
					InsertToSql(sql_man, p, total_trace_id);
				}

				total_trace_id ++;
			}

			int write_size = trace_num_per_shot * total_key_num_ * sizeof(head_buf[0]);
			SafeWrite(fd, (char*)head_buf, write_size);
			float progress = 1.0 * shot_i / shot_num * 100;
			PrintProgress(progress);

//			left_trace_num -= trace_num_this_shot;

		}
		delete head_buf;
		delete trace_buf;
		delete shot_buf;
	} else if(data_order == "CMP_LINE,CMP,TRACE") {

		int cmp_line_num = ToInt(JobPara::GetPara("CMPLineNum"));
		int cmp_num = ToInt(JobPara::GetPara("CMPNum"));
		int trace_num_per_cmp_line = ToInt(JobPara::GetPara("TraceNumPerCMPLine"));


		int head_len = total_key_num_ * trace_num_per_cmp_line;
		uint32_t *head_buf = new uint32_t[head_len];


		for(int cmp_line_i = 0; cmp_line_i < cmp_line_num; ++cmp_line_i) {
			for(int cmp_i = 0; cmp_i < cmp_num; ++cmp_i) {

				uint32_t *p = head_buf;

				srand(time(NULL));
				for(int trace_id = 0; trace_id < trace_num_per_cmp_line; ++trace_id, p += total_key_num_) {
					p[0] = rand() % 5000;
					p[1] = trace_id;
					p[2] = cmp_line_i;
					p[3] = cmp_i;
					((float*)p)[4] = rand() % 1000 + (1.0 * (rand() % 1000) / 1000.0);
					((float*)p)[5] = rand() % 180 + (1.0 * (rand() % 1000) / 1000.0);

					for(int column_i = 6; column_i < total_key_num_; ++column_i) {
						p[column_i] = rand() % 5000;
					}

					if(make_sql) {
						InsertToSql(sql_man, p, total_trace_id);
					}
					total_trace_id ++;
				}

				int write_size = trace_num_per_cmp_line * total_key_num_ * sizeof(head_buf[0]);
				SafeWrite(fd, (char*)head_buf, write_size);
			}
			float progress = 1.0 * cmp_line_i / cmp_line_num * 100;
			PrintProgress(progress);
		}

		delete head_buf;
	} else {
		uint32_t cmp_line_num = ToInt(JobPara::GetPara("CMPLineNum"));
		uint32_t cmp_num = ToInt(JobPara::GetPara("CMPNum"));
		uint32_t trace_num_per_cmp_line = ToInt(JobPara::GetPara("TraceNumPerCMPLine"));


		int head_len = total_key_num_ * trace_num_per_cmp_line;
		uint32_t *head_buf = new uint32_t[head_len];

		uint32_t *cmp_line_buf = new uint32_t[cmp_line_num];
		for(uint32_t i = 0; i < cmp_line_num; ++i) {
			cmp_line_buf[i] = i;
		}
		Shuffle(cmp_line_buf, cmp_line_num);


		uint32_t *cmp_buf = new uint32_t[cmp_num];
		for(uint32_t i = 0; i < cmp_num; ++i) {
			cmp_buf[i] = i;
		}
		Shuffle(cmp_buf, cmp_num);

		uint32_t *trace_buf = new uint32_t[trace_num_per_cmp_line];
		for(uint32_t i = 0; i < trace_num_per_cmp_line; ++i) {
			trace_buf[i] = i;
		}
		Shuffle(trace_buf, trace_num_per_cmp_line);


		for(int cmp_line_i = 0; cmp_line_i < cmp_line_num; ++cmp_line_i) {
			for(int cmp_i = 0; cmp_i < cmp_num; ++cmp_i) {

				uint32_t *p = head_buf;

				srand(time(NULL));
				for(int trace_id = 0; trace_id < trace_num_per_cmp_line; ++trace_id, p += total_key_num_) {
					p[0] = rand() % 5000;
					p[1] = trace_buf[trace_id];
					p[2] = cmp_line_buf[cmp_line_i];
					p[3] = cmp_buf[cmp_i];
					((float*)p)[4] = rand() % 1000 + (1.0 * (rand() % 1000) / 1000.0);
					((float*)p)[5] = rand() % 180 + (1.0 * (rand() % 1000) / 1000.0);

					for(int column_i = 6; column_i < total_key_num_; ++column_i) {
						p[column_i] = rand() % 5000;
					}

					if(make_sql) {
						InsertToSql(sql_man, p, total_trace_id);
					}
					total_trace_id ++;
				}

				int write_size = trace_num_per_cmp_line * total_key_num_ * sizeof(head_buf[0]);
				SafeWrite(fd, (char*)head_buf, write_size);
			}
			float progress = 1.0 * cmp_line_i / cmp_line_num * 100;
			PrintProgress(progress);
		}

		delete head_buf;
		delete cmp_line_buf;
		delete cmp_buf;
		delete trace_buf;
	}
	if(make_sql) {
		sql_man->Close();
		delete sql_man;
	}
	close(fd);
	return true;
}


HeadDataReader::HeadDataReader(HeadInfo *head_info) :
		head_info_(head_info){
#if 0
	buf_ = NULL;
#endif
}
HeadDataReader::~HeadDataReader() {
#if 0
	if(NULL != buf_) {
		delete buf_;
		buf_ = NULL;
	}
#endif
}

bool HeadDataReader::Open() {
	filename_ = HeadData::GetHeadName(head_info_);
	fd_ = open(filename_.c_str(), O_RDONLY);
	if(0 > fd_) {
		Err("Open file : %s for read error\n", filename_.c_str());
		return false;
	}

	one_head_size_ = head_info_->GetHeadSize();
	total_trace_num_ = JobPara::GetTraceNum();
	trace_id_ = 0;

#if 0
	buf_trace_num_ = BUF_TRACE_NUM;
	buf_len_ = buf_trace_num_ * one_head_size_;
	buf_ = new char[buf_len_];
	if(NULL == buf_) {
		return false;
	}
	left_trace_num_ = total_trace_num_;

	buf_start_id_ = -1;
	buf_end_id_ = -1;
#endif

	return true;
}

bool HeadDataReader::Seek(int64_t trace_id) {
#if 0
	if(trace_id >= buf_start_id_ && trace_id <= buf_end_id_) {
		trace_id_ = trace_id;
		offset_ = one_head_size_ * (trace_id - buf_start_id_);
	} else {
		int64_t offset_in_buf = trace_id % buf_trace_num_;
		buf_start_id_ = trace_id - offset_in_buf;
		buf_end_id_ = buf_start_id_ + buf_trace_num_;
		buf_end_id_ = buf_end_id_ < total_trace_num_ ? buf_end_id_ : (total_trace_num_ - 1);
		int read_trace_num = buf_end_id_ - buf_start_id_ + 1;
		int read_size = read_trace_num * one_head_size_;
		int64_t ret = SafeRead(fd_, buf_, read_size);
		if (ret != read_size) {
			Err("Read head data error\n");
			return false;
		}
	}
#endif

	if(trace_id >= total_trace_num_ || trace_id < 0) {
		Err("invalid trace_id\n");
		return false;
	}

	int64_t offset = trace_id * one_head_size_;
	int64_t ret = lseek(fd_, offset, SEEK_SET);
	if(0 > ret) {
		Err("Seek error\n");
		return false;
	}
	return true;

}

bool HeadDataReader::Next(void *head) {
#if 0
	if(0 == trace_id_ % buf_trace_num_) {
		int read_trace_num = buf_trace_num_ < left_trace_num_ ? buf_trace_num_ : left_trace_num_;
		int read_size = read_trace_num * one_head_size_;
		int64_t ret = SafeRead(fd_, buf_, read_size);
		if (ret != read_size) {
			Err("Read head data error\n");
			return false;
		}
		left_trace_num_ -= read_trace_num;

		buf_start_id_ = trace_id_;
		buf_end_id_ = buf_start_id_ + read_trace_num - 1;
		offset_ = 0;
	}

	memcpy(head, buf_ + offset_, one_head_size_);
	offset_ += one_head_size_;
	trace_id_ ++;

	return true;
#endif

	int ret = read(fd_, head, one_head_size_);
	if(ret != one_head_size_) {
		Err("read head error\n");
		return false;
	}
	trace_id_ ++;
	return true;
}

#if 0
bool HeadDataReader::Get(int64_t trace_id, void *head) {
	if(trace_id >= total_trace_num_ || trace_id < 0) {
		Err("invalid trace_id\n");
		return false;
	}
}
#endif

bool HeadDataReader::HasNext() {
	return trace_id_ < total_trace_num_;
}
void HeadDataReader::Close() {
	close(fd_);
#if 0
	if(NULL != buf_) {
		delete buf_;
		buf_ = NULL;
	}
	return true;
#endif
}

void HeadDataReader::PrintHead(int64_t trace_id) {

	char head[2048];

	this->Seek(trace_id);
	this->Next(head);

	int *p = (int*)head;
	for(int i = 0; i < 4; i++) {
		printf("key[%d] = %d, ", i, p[i]);
	}
	printf("\n");
}


