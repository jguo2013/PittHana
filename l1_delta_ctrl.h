/**************************
   PittHN project
   l1_delta_ctrl.h
   author:  Jie Guo (jig26@pitt.edu)
   version: 1.0 
**************************/
#ifndef _l1_delta_ctrl
#define _l1_delta_ctrl

#include <stdio.h>
#include <iostream>
#include <string>
#include <stdio.h>
#include <string.h>
#include <deque>
#include "io_ctrl.h"
#include "global.h"

using namespace std;

class l1_delta_ctrl {
   
protected:
   unsigned int used_size;
   unsigned int avail_size;   
   deque<tb_info_t> l1_dic;
   io_ctrl  io_int;
         
public:                    
   l1_delta_ctrl();  
   ~l1_delta_ctrl();
   
   unsigned int RecycleSpace(unsigned int _n);        
   void DeleteRowFile(string _tb);
   
   bool OpTuple(link_op _op, string _tb, tuple_t &_tp, deque<unsigned int> * _id);
   bool PopTuple(string &_s, tuple_t &_t); 
   bool GroupRemoveTuple(string _tb);	
   deque<tb_info_t>::iterator GetTable(string _tb);
   deque<tb_info_t>::iterator InsertTable(tb_info_t _tb);
   void DeleteTable(deque<tb_info_t>::iterator _it);  
   unsigned int OpAvailSpace(spaceop _op,unsigned int _n);
   unsigned int GetRowFile(string _tb, deque<tuple_t> * _q, unsigned int _n); 
   void UpdateRecency(deque<tb_info_t>::iterator it);
   	 	
   ostream & Print(ostream &os) const;
};
inline ostream & operator<<(ostream &os, const l1_delta_ctrl & _m) { return _m.Print(os);}                     
 
#endif
