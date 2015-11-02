/**************************
   PittHN project
   sch_int.h
   author: 
   version: 1.0 
**************************/
#ifndef _sch_int
#define _sch_int

#include <stdio.h>
#include <iostream>
#include <string>
#include <stdio.h>
#include <string.h>
#include "global.h"
#include "dic_ctrl.h"
#include "recovery_ctrl.h"

using namespace std;

class dic_ctrl;
class recovery_ctrl;
             
class sch_int {
   
protected:
   unsigned int trans_no;
   sch_op   op;
   tuple_t  new_tuple;
   string   table_name;

   recovery_ctrl rec_ctrl;   
   dic_ctrl gd_ctrl;   
      
public:
   sch_int();
   ~sch_int(); 
    
   void SetTranNo(unsigned int _t);
   void SetOp(sch_op _op);    
   void SetTuple(tuple_t _t);
   void SetTableName(string _t);
   void SetMode(int _m);
    
   unsigned int GetTranNo();
   sch_op   GetOp();    
   tuple_t  GetTuple(); 
   string   SetTableName();   
   int      GetMode();
   
   void InitOp();   
   int QueryOp(deque<tuple_t> * _q);
   unsigned int GroupOp();          
   int  WriteOp();
   int  DeleteOp();   
   int  CommitOp();      
   int  AbortOp();
    
   ostream & Print(ostream &os) const;     
};

inline ostream & operator<<(ostream &os, const sch_int & _m) { return _m.Print(os);}                     

#endif
