/**************************
   PittHN project
   bm.h
   author:  Jie Guo (jig26@pitt.edu)
   version: 1.0 
**************************/

#ifndef l1_delta_ctrl
#define l1_delta_ctrl

#include <stdio.h>
#include <iostream>
#include <string>
#include <stdio.h>
#include <string.h>
#include "global.h"

using namespace std;
 
typedef struct tb_info{
	  string     table_name;
    bool       valid;      
	  deque<page_info_t> page;
	  unsigned int assign_start;
	  unsigned int assign_end;    
}tb_info_t;
             
class l1_delta_ctrl {
   
protected:
   unsigned int used_buf_size;
   unsigned int avail_buf_size;
   deque<tb_info_t>   l1_dic;  
         
public:

   l1_delta_ctrl();     
   ~l1_delta_ctrl();
   l1_delta_ctrl(unsigned int _s);                      
   
   	
   unsigned int RecycleSpace(unsigned int _n);					//_n: one page; return page numbers to be recycled
   bool OpTuple(link_op _op, string _tb, tuple_t _tp, deque<unsigned int> * _id);
   bool PopTuple(string *_s, tuple_t *_t); 
   void GroupRemoveTuple(string _tb);	
   deque<tb_info_t>::iterator GetTable(string _tb);
   deque<tb_info_t>::iterator InsertTable(tb_info_t _tb);
   void DeleteTable(deque<tb_info_t>::iterator _it);
   unsigned int OpAvailSpace(spaceop _op,unsigned int _n);
          
};

#endif
