/**************************
   PittHN project
   recovery_ctrl.h
   author:  Jie Guo (jig26@pitt.edu)
   version: 1.0 /11/28/2013
**************************/
#ifndef _recovery_ctrl
#define _recovery_ctrl

#include <stdio.h>
#include <iostream>
#include <fstream>
#include <string>
#include <stdio.h>
#include <string.h>
#include "global.h"

using namespace std;
 
typedef struct b_image{ 
    string   table_name;
    tuple_t  t; 
    log_op   l_op;  
    sch_op   op;
    unsigned int tran_no;
}b_image_t;
             
class recovery_ctrl {
   
protected:
   deque<b_image_t> tran_metadata;
   ofstream fp ;
   string   file_name; 
         
public:
   recovery_ctrl();   
   ~recovery_ctrl();  
                                
   bool OpenLogFile(); 
   void CloseLogFile();
         
	 void InsertLog(unsigned int _tr, string _tb,sch_op _op,tuple_t _tp,log_op _l);
   void PrintLog(unsigned int _tr, string _tb,sch_op _op,tuple_t _tp,log_op _l);  
   
   int ReleaseLog(unsigned int _tr, string &_t); 														 //0: release cpl; 1: return table name for deletion; 2: return for completion
	 int RollbackLog(unsigned int _tr, log_op &_op, tuple_t &_t, string &_tb); //0: cpl; 1: return log      
   string _itoa(unsigned int n);
   ostream & Print(ostream &os) const;   
};

inline ostream & operator<<(ostream &os, const recovery_ctrl & _m) { return _m.Print(os);}                     

#endif
