/**************************
   PittHN project
   l2_delta_ctrl.h
   author:  Jie Guo (jig26@pitt.edu)
   version: 1.0 /11/28/2013
**************************/
#ifndef _l2_delta_ctrl
#define _l2_delta_ctrl

#include <iostream>
#include <new>
#include <string>
#include <deque>
#include "io_ctrl.h"
#include "global.h"

using namespace std;

typedef struct l2_meta{
	  bool             valid;       
    bool             dirty;      
    unsigned int     fix_cnt;
    unsigned int     ass_id;    
    string           table_name;   
    string           att_name;
    deque<l2_attr_t> col;
}l2_meta_t;

class l2_delta_ctrl {
   
protected:
   deque<l2_meta_t>  l2_dic_meta;
   int used_size;
   int avail_size;   
   io_ctrl  io_int;  
         
public:                    
   l2_delta_ctrl();   
   ~l2_delta_ctrl();
   
   void  GroupRemoveColumn(string _tb);
	 bool  OpColumn(link_op _op, string _tb,tuple_t _t,string _att, unsigned int &_n);
   string  GetColumn(string _tb,unsigned int _l,string _att);
	 
	 deque<l2_meta_t>::iterator SearchAtt(string _tb,string _att); 
	 deque<l2_attr_t>::iterator SearchColumn(deque<l2_attr_t> * _lh, string _cont, unsigned int _n, int flag);
	 		 	
   void InitL2Delta();
   
   unsigned int RecycleSpace(unsigned int _n);	 
	 bool GetColumnFromDisk(string _tb,string _att,deque<l2_attr_t> * col, int _n);   
   deque<l2_meta_t>::iterator CreateAtt(string _tb,string _att);           
   int OpAvailSpace(spaceop _op,unsigned int _n);                    			           	       	 	 
   void UpdateRecency(deque<l2_meta_t>::iterator it);
   string _itoa(unsigned int n);   	
   ostream & Print(ostream &os) const;
};
inline ostream & operator<<(ostream &os, const l2_delta_ctrl & _m) { return _m.Print(os);}                     
 
#endif
