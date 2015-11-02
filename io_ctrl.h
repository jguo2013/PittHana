/**************************
   PittHN project
   io_ctrl.h
   author:  Jie Guo (jig26@pitt.edu)
   version: 1.0  
**************************/
#ifndef _io_ctrl
#define _io_ctrl

#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include "global.h"
#include "io_ctrl.h"

using namespace std;

class io_ctrl {
	 
  char buf[BYTE_PER_PAGE];         
public:
	      
   io_ctrl();
   ~io_ctrl();     
          
   void GetTableNameFromDisk(deque<string> *_l,char *_c);   
	 unsigned int GetTableFromDisk(string _tb,deque<global_dic_t> * _pt);     
   unsigned int FlushTable2Disk(string _tb, deque<global_dic_t> * _t);
   
   void GetColumnNameFromDisk(deque<string> *_tb,deque<string> *_l);  
   void FlushColmn2Disk(string _tb,string _att, deque<l2_attr_t> * _col);   
   void GetColumnFromDisk(string _tb,string _att, deque<l2_attr_t> * _col);
   
   void CreateTableFile(string _tb);
   void RemoveTableFile(string _tb);
 
   void CreatColumnFile(string _tb,string _att);
   void RemoveColumnFile(string _tb,string _att);

   unsigned int GetRowNameFromDisk(string _tb,deque<tb_info_t> * _pt);    
   unsigned int FlushRow2Disk(string _tb, deque<tuple_t> * _pt, unsigned int _p);   
   void RemoveRowFile(string _tb);
   unsigned int GetRowFileFromDisk(string _tb, deque<tuple_t> * _q, unsigned int _n);                                                                                 
   string _itoa(unsigned int n);
   
   void CreateRowFile(string _tb);   
    
   string ConstrFileName(char * _c, string _s);    
};

#endif
