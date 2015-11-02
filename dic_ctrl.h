/**************************
   PittHN project
   dic_ctrl.h
   author:  Jie Guo (jig26@pitt.edu)
   version: 1.0 
**************************/
#ifndef _dic_ctrl
#define _dic_ctrl

#include <sys/types.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <stdio.h>
#include <iostream>
#include <string>
#include <deque>
#include "global.h"
#include "l1_delta_ctrl.h"
#include "l2_delta_ctrl.h"
#include "io_ctrl.h"

using namespace std;

typedef struct dic_meta{  
    string                table_name;   
    deque<global_dic_t>   addr;
    bool                  dirty;
    bool                  valid;      
    deque<string>         att_name;
}dic_meta_t;
        
class dic_ctrl {
   
protected:
   deque<dic_meta_t> glb_dic_metadata;  
   unsigned int used_size;
   unsigned int avail_size; 
   int      mode;                     
   
   l1_delta_ctrl L1_ctrl;
   l2_delta_ctrl L2_ctrl;
   io_ctrl  io_int;  
           
public:
   dic_ctrl();
   dic_ctrl(unsigned int _us, unsigned int _as); 								
	 ~dic_ctrl();

   void SetMode(int _m);
   int  GetMode();
   
   int  OpTuple(string _tb, tuple_t _nt, log_op _op, tuple_t &_ot);   
   void InsertTuple2L2(string _tb,tuple_t _t);	
         
   void QueryTable(string _tb,sch_op _op,tuple_t _t,deque<tuple_t> * _q);	
   int  OpTable(string _tb,sch_op _op);                                                                     
   void RecoveryTable(log_op _op, tuple_t _t, string _tb);      

   deque<dic_meta_t>::iterator GetTable(string _tb);            
   void CreatTable(string _tb, dic_meta_t &_t);                 
   void ConstTable(string _tb, dic_meta_t &_t); 
    
   bool GetTupleFromL2(deque<global_dic_t>::iterator it,string _tb,tuple_t &_ot,deque<string> * _a);
   bool GetTupleFromL1(deque<global_dic_t>::iterator it,string _tb,tuple_t &_ot);
   bool DelTupleFromL1(deque<global_dic_t>::iterator it,string _tb,tuple_t &_ot);

   deque<dic_meta_t>::iterator GbOpLink(link_op _op, string _t, dic_meta_t _pt); 
   deque<global_dic_t>::iterator TbOpLink(link_op _op, tuple_t _t, deque<global_dic_t> * _pt); 
   	
   void InitDic();  
   void UpdateTableDic(deque<global_dic_t>::iterator _it,log_op _op,deque<unsigned> *_id);
   int  MergeL12L2Op(int _n);
   unsigned int OpAvailSpace(spaceop _op,unsigned int _n);      //return available space          	 
   unsigned int RecycleSpace(unsigned int _n);
   int GetTableFromDisk(string _t, deque<global_dic_t> * _lh);  //return the memory addr of table/column 
   void SetLoc(deque<global_dic_t>::iterator _it,deque<unsigned> * _q);
   	
   deque<dic_meta_t>::iterator UpdateRecency(deque<dic_meta_t>::iterator it);

   ostream & Print(ostream &os) const;
};

inline ostream & operator<<(ostream &os, const dic_ctrl & _d) { return _d.Print(os);}                     
#endif
