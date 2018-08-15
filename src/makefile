

OPTFLAGS	= -O3 -Wall -rdynamic
CFLAGS		= -fPIC -pipe -g -Wall -D_REENTRANT $(OPTFLAGS)
CXXFLAGS	= -fPIC -pipe -g -Wall -D_REENTRANT $(OPTFLAGS)

CC		= gcc
CXX		= g++
CXXLINKER	=$(CXX)

INCPATH		+= -I. -I../include -I$(GPP)/include
LIBS		+= -L./  -rdynamic -L$(GPP)/lib -lgpptbb -lgpp

#INCPATH		+= -I$(MYSQLC_HOME)/include -I$(BOOST_HOME)/include
#LIBS		+= -L$(MYSQLC_HOME)/lib -lmysqlcppconn -L$(BOOST_HOME)/lib 

INCPATH		+= -I/usr/include -I$(BOOST_HOME)/include
LIBS		+= -L/usr/lib64/mysql -lmysqlclient -L$(BOOST_HOME)/lib 


AR		= ar rvcs
MKDIR		= mkdir -p

OBJECTS		= main.o		\
		job_para.o head_data.o data_type.o\
		util/util_io.o util/util_log.o util/util_string.o 	\
		util/util.o	 util/util_bitmap.o \
		order.o key.o row_filter_by_key.o 	\
		index.o	selector.o	\
		index_order/index_order.o index_order/index_order_writer.o  \
		index_order/index_order_reader.o index_order/selector_order_hhj.o\
		index_btree/index_btree_data_struct.o	\
		index_btree/index_btree.o index_btree/index_btree_writer.o \
		index_btree/index_btree_reader.o index_btree/selector_btree.o \
		index_btree_mt/index_btree_reader_mt.o index_btree_mt/selector_btree_mt.o \
		index_btree_seperate/selector_btree_seperate.o	\
		index_btree_sort/selector_btree_sort.o	index_btree_sort/index_btree_reader_sort.o \
		index_btree_nosort/selector_btree_nosort.o	\
		sql_manager.o 	\
		index_sql/index_sql.o index_sql/selector_sql.o index_sql/index_sql_reader.o\
		index_btree_sort_mt/selector_btree_sort_mt.o \
		index_btree_sort_mt/index_btree_reader_sort_mt.o \
		job/index_test_job.o
		
		

BINDIR		= ../bin
TARGET		= $(BINDIR)/index-bench
INCDIR		= 
LIBDIR		=

CHK_DIR_EXISTS	= test -d
DEL_FILE		= rm -f

first : all

.SUFFIXES: .o .c .cpp .cc .cxx .C

.cpp.o:
	$(CXX) -c $(CXXFLAGS) $(INCPATH) -o "$@" "$<"
.cc.o:
	$(CXX) -c $(CXXFLAGS) $(INCPATH) -o "$@" "$<"
.cxx.o:
	$(CXX) -c $(CXXFLAGS) $(INCPATH) -o "$@" "$<"
.C.o:
	$(CXX) -c $(CXXFLAGS) $(INCPATH) -o "$@" "$<"
.c.o:
	$(CC) -c $(CFLAGS) $(INCPATH) -o "$@" "$<"
	
all : $(TARGET)

$(TARGET) : $(OBJECTS)
	$(CHK_DIR_EXISTS) $(BINDIR) | $(MKDIR) $(BINDIR)
	$(CXXLINKER)  $(OBJECTS) $(LIBS) -o $(TARGET)
	
	
clean:
	-$(DEL_FILE) $(OBJECTS)
	-$(DEL_FILE) $(TARGET) 
	-$(DEL_FILE) *.core *.o
	-$(DEL_FILE) .depend
.depend:
	touch .depend
	makedepend -- $(INCPATH) -- -f .depend *.cpp *.c
include .depend

