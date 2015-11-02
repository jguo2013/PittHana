/**************************
   PittHN project
   global.h
   author:  Jie Guo (jig26@pitt.edu)
   version: 1.0 
**************************/
#ifndef _global
#define _global

#include <iostream>
#include <sstream>
#include <fstream>
#include <string>
#include <stdio.h>
#include <stdlib.h>
#include <deque>

using namespace std;

#define debug_mode 1

enum sch_op{write_t=0,r_query=1,m_query=2,a_query=3,del=4,undel=5,com_del=6,rollback_t=7,non=8};
enum link_op{l_insert=0, l_remove=1, l_search=2,
	           l_index=3,  l_update=4, l_delete=5};
enum log_op{insert_L1=0,insert_L2=1,commit=2,rollback=3,delete_suc=4,
	          delete_uns=5,update_L1=6,read_t=7,delete_t=8};
enum tb_att{att_id=0, att_name=1, att_phone=2};
enum tb_type{l1_used=0, l1_avail=1,rec_bimage=2};
enum tb_loc{l1_loc=0, l2_loc=1};
enum access_type{dic_file=0, column=1};
enum spaceop{add=0,substr=1,check=3};

#define TUPLE_LEN  32    
#define PAGE_LEN   512
#define MERGE_NUM  512

#define BYTE_PER_PAGE   512
#ifdef  debug_mode
#define TUPLES_PER_PAGE 1       
#else
#define TUPLES_PER_PAGE 12       
#endif
#define PAGE_PER_BLK    2
#define TUPLES_PER_BLK  (TUPLES_PER_PAGE*PAGE_PER_BLK)

#define L1_TUPLE_NUM    4
#define L2_TUPLE_NUM    8
#define GD_TUPLE_NUM    40000
#define L1_TH_TUPLE_NUM (L1_TUPLE_NUM/2)
#define L2_TH_TUPLE_NUM (L2_TUPLE_NUM/2)
#define GD_TH_TUPLE_NUM 10000

#define TABLE_PATH   "./table_dir"
#define COLUMN_PATH  "./column_dir"
#define ROW_PATH     "./row_dir"

typedef struct tuple{   
    unsigned int id;  			 
    string   client_name;
    string   phone;
}tuple_t;  

typedef struct page_info{
    bool           dirty;      
	  bool           in_mem;   //1: in memory;  0: in disk
    unsigned int   fix_cnt;
	  deque<tuple_t> tuples;
}page_info_t;
 
typedef struct tb_info{
	  string     table_name;
    bool       valid;        
	  unsigned int inmem_page;//page number in memory        	  
	  unsigned int assign_end;    
	  deque<page_info_t> page;
}tb_info_t;

typedef struct global_dic{    
    tb_loc       loc;         
    unsigned int addr[3];     
}global_dic_t;                

typedef struct l2_attr{
    unsigned int    id;
    string          content;   
}l2_attr_t;

extern ofstream fp1;

#endif
