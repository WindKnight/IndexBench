#include <stdio.h>
#include "job_para.h"
#include "head_data.h"

#include "util/util_string.h"
#include "util/util_log.h"

#include "index_order/index_order.h"
#include "index_order/selector_order_hhj.h"
#include "index_btree/index_btree.h"
#include "index_btree/selector_btree.h"
#include "index_btree/index_btree_reader.h"
#include "order.h"
#include "row_filter_by_key.h"

#include "job/index_test_job.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <util/util.h>

//#include <iostream>
//   #include <sstream>
//   #include <memory>
//   #include <string>
//   #include <stdexcept>
//   #include "mysql_connection.h"
//   #include "mysql_driver.h"
//   #include "cppconn/driver.h"
#include "mysql/mysql.h"
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




void printResult(MYSQL *mysqlPrint)//打印结果集(此处传入指针，而非内容)
{
    MYSQL_RES * result;
    int numFields = 0;
    int numRows = 0;
    MYSQL_FIELD * field;
    MYSQL_ROW row;
    int i = 0;
    result = mysql_store_result(mysqlPrint);//将查询的全部结果读取到客户端
    numFields = mysql_num_fields(result);//统计结果集中的字段数
    numRows = mysql_num_rows(result);//统计结果集的行数
    while(field = mysql_fetch_field(result))//返回结果集中的列信息(字段)
        printf("%s\t", field->name);

    printf("\n");
    if(result)
    {
        while(row = mysql_fetch_row(result))//返回结果集中行的记录
        {
            for(i = 0; i < numFields; i++)
            {
                printf("%s\t", row[i]);
            }
            printf("\n");
        }
    }
    mysql_free_result(result);//释放result空间，避免内存泄漏
}

int main(int argc, char **argv){

	JobPara::ParsePara(argc, argv);

	/*
	 * make data
	 * */

	HeadType head_type;
	head_type.head_type.push_back(UINT);
	head_type.head_type.push_back(INT);
	head_type.head_type.push_back(UINT);
	head_type.head_type.push_back(UINT);
	head_type.head_type.push_back(FLOAT);
	head_type.head_type.push_back(FLOAT);

	int head_key_num = ToInt(JobPara::GetPara("HeadKeyNum"));
	for(int i = 6; i < head_key_num; ++i) {
		head_type.head_type.push_back(INT);
	}

	HeadInfo *head_info = new HeadInfo(head_type);


	HeadData *head_data = new HeadData(head_info);
	head_data->MakeData(true);
	delete head_data;


#if 0
	int fd = open("/data/wyd/IndexBench/head_data/head_6keys_100lines_500cmps_with_1000traces", O_RDONLY);
	int64_t buf_size = 1 << 30;
	buf_size *= 2;
	char *buf = new char[buf_size];
	struct stat f_stat;

	fstat(fd, &f_stat);
	int64_t f_size = f_stat.st_size;

	struct timeval t_begin, t_end;

	/**************  Test 1  ******************/

	ClearCache();

	lseek(fd, 0, SEEK_SET);

	printf("Test 1, directly read\n");

	gettimeofday(&t_begin, NULL);
	read(fd, buf, f_size);
	gettimeofday(&t_end, NULL);
	float time = CALTIME(t_begin, t_end);

	float speed = 1.0 * f_size / 1024 / 1024 / time;

	printf("time 1 = %10.5f, speed = %10.5f MB/s\n", time, speed);

	/**************  Test 2  ******************/

	ClearCache();

	lseek(fd, 0, SEEK_SET);

	printf("Test 2\n");

	gettimeofday(&t_begin, NULL);

	for(int64_t i = 0; i < f_size; ++i) {
		read(fd, buf + i, 1);
	}

	gettimeofday(&t_end, NULL);
	time = CALTIME(t_begin, t_end);
	speed = 1.0 * f_size / 1024 / 1024 / time;

	printf("time 2 = %10.5f, speed = %10.5f MB/s\n", time, speed);

	/**************  Test 3  ******************/

	ClearCache();

	lseek(fd, 0, SEEK_SET);

	printf("Test 3\n");

	gettimeofday(&t_begin, NULL);

	for(int64_t i = 0; i < f_size; ++i) {
		lseek(fd, i, SEEK_SET);
		read(fd, buf + i, 1);
	}

	gettimeofday(&t_end, NULL);
	time = CALTIME(t_begin, t_end);
	speed = 1.0 * f_size / 1024 / 1024 / time;

	printf("time 3 = %10.5f, speed = %10.5f MB/s\n", time, speed);

	/**************  Test 4  ******************/

	printf("Test 4\n");


	int block_size = 1;

	while(block_size <= 4096) {

		ClearCache();

		lseek(fd, 0, SEEK_SET);

		int block_num = f_size / block_size;

		gettimeofday(&t_begin, NULL);

		for(int64_t i = 0, offset = 0; i < block_num; ++i, offset += block_size) {
			read(fd, buf + offset, block_size);
		}

		gettimeofday(&t_end, NULL);
		time = CALTIME(t_begin, t_end);
		speed = 1.0 * block_size * block_num / 1024 / 1024 / time;

		printf("block_size = %d, time = %10.5f, speed = %10.5f MB/s\n", block_size, time, speed);

		block_size <<= 1;
	}

	/**************  Test 5  ******************/

	printf("Test 5\n");


	block_size = 1;

	while(block_size <= 4096) {

		ClearCache();

		lseek(fd, 0, SEEK_SET);

		int block_num = f_size / block_size;

		gettimeofday(&t_begin, NULL);

		for(int64_t i = 0, offset = 0; i < block_num; ++i, offset += block_size) {
			lseek(fd, offset, SEEK_SET);
			read(fd, buf + offset, block_size);
		}

		gettimeofday(&t_end, NULL);
		time = CALTIME(t_begin, t_end);
		speed = 1.0 * block_size * block_num / 1024 / 1024 / time;

		printf("block_size = %d, time = %10.5f, speed = %10.5f MB/s\n", block_size, time, speed);

		block_size <<= 1;
	}


#endif



#if 1

	IndexTestJob *test_job = new IndexTestJob(head_info);
	std::vector<std::string> index_type_arr = SplitString(JobPara::GetPara("IndexType"), ",");
	for(int i = 0; i < index_type_arr.size(); ++i) {
		test_job->AddIndexType(index_type_arr[i]);
	}

	SharedPtr<RowFilterByKey> row_filter = new RowFilterByKey();



/*	row_filter->FilterBy(Key(2, 53, 82, 1, 1)).FilterBy(Key(2, 55, 73, 1, 1)).
			FilterBy(Key(2, 45, 63, 1, 1)).FilterBy(Key(2, 33, 51, 1, 1)).
			FilterBy(Key(2, 21, 39, 1, 1));*/




//	row_filter->FilterBy(Key(2, 3, 6, 1, 1)).FilterBy(Key(2, 2, 5, 1, 1));

//
//	row_filter->FilterBy(Key(3, 37, 45, 1, 1)).FilterBy(Key(3, 15, 23, 1, 1)).FilterBy(Key(3, 4, 12, 1, 1))
//			.FilterBy(Key(3, 15, 23, 1, 1)).FilterBy(Key(3, 2, 10, 1, 1));

//	row_filter->FilterBy(Key(3, 6, 17, 1, 1)).FilterBy(Key(3, 47, 58, 1, 1));

//	row_filter->FilterBy(Key(1, 719, 788, 1, 1)).FilterBy(Key(1, 841, 910, 1, 1)).FilterBy(Key(1, 270, 339, 1, 1))
//		.FilterBy(Key(1, 854, 970, 1, 1)).FilterBy(Key(1, 754, 870, 1, 1)).FilterBy(Key(1, 124, 240, 1, 1))
//		.FilterBy(Key(1, 212, 328, 1, 1));


//	row_filter->FilterBy(Key(0, 96, 315, 1, 1)).FilterBy(Key(0, 2066, 2285, 1, 1));
//	row_filter->FilterBy(Key(2,30,60,1, 1)).FilterBy(Key(2, 40, 70, 1, 1));
//	row_filter->FilterBy(Key(2,30, 60, 1, 1)).FilterBy(Key(3,115,157,1, 1)).FilterBy(Key(2, 70, 90, 1, 1)).FilterBy(Key(3, 345, 409, 1, 1));
	row_filter->FilterBy(Key(1, 0, 17, 1, 1));


	SharedPtr<Order> order = new Order();

	order->OrderBy(1);

	TestUnit test_unit;
	test_unit.order = order;
	test_unit.row_filter = row_filter;

	int test_round = ToInt(JobPara::GetPara("TestRound"));

//	int i = 0;
//	while(i < test_round) {
//		test_job->AddTestUnit(test_unit);
//		++i;
//	}

	test_job->GenerateWorkload(test_round);
	test_job->Start();

#endif


#if 0
	char head[2048];


	HeadDataReader *head_read = new HeadDataReader(head_info);
	head_read->Open();
	head_read->Seek(46191988);
	head_read->Next(head);

	int *p = (int*)head;
	for(int i = 0; i < 4; i++) {
		printf("i = %d, key = %d\n", i, p[i]);
	}
#endif


#if 0
	   MYSQL *conn;

	   MYSQL_RES *res;

	   MYSQL_ROW row;

	  /* db configure*/

	   char *server = "localhost";

	   char *user = "root";

	   char *password = "112106gw";

	   char *database = "test";

	   int  port=3306;

	   conn = mysql_init(NULL);



	   /* Connect to database */

	   if (!mysql_real_connect(conn, server,

	         user, password, database, port, NULL, 0)) {

	      fprintf(stderr, "connect error: %s\n", mysql_error(conn));

	      exit(0);

	   }



	   /* send SQL query */

	   if (mysql_query(conn, "select * from seis_head")) {

	      fprintf(stderr, "%s\n", mysql_error(conn));

	      exit(0);

	   }

	   printResult(conn);


	   mysql_query(conn, "insert into seis_head "
			   "(trace_id, cmp_line, cmp, trace, shot, offset, angel)"
			   " values "
			   "(2,10,100,1001,20,11.0, 170.0)");


	   mysql_close(conn);
#endif



	return 0;
}
