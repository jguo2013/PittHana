/**************************
   PittHN project
   sch_int.cc
   author:  Jie Guo (jig26@pitt.edu)
   version: 1.0 /04/04/2014 
**************************/
#include <sys/types.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <stdlib.h>
#include <iostream>
#include <string>
#include "sch_int.h"

using namespace std;

sch_int::sch_int(){}
sch_int::~sch_int(){}
   
void sch_int::SetTranNo(unsigned int _t)
{
	trans_no = _t; 
}

void sch_int::SetOp(sch_op _op)
{
	op = _op; 
}

void sch_int::SetTuple(tuple_t _t)
{   
	new_tuple.id = _t.id;
	new_tuple.client_name = _t.client_name; 
	new_tuple.phone = _t.phone; 		
}
    
void sch_int::SetTableName(string _t)
{
	table_name = _t;
}

void sch_int::SetMode(int _m)
{
	gd_ctrl.SetMode(_m);
}

unsigned int sch_int::GetTranNo()
{
	return(trans_no);
}

sch_op sch_int::GetOp()
{
	return(op);
}

tuple_t sch_int::GetTuple()
{
	return(new_tuple);
}

string sch_int::SetTableName()
{
	return(table_name);
}

int sch_int::GetMode()
{
	return(gd_ctrl.GetMode());
}

int sch_int::WriteOp()
{
	tuple_t old_tuple,tt; string s; 

	int ok = gd_ctrl.OpTuple(table_name,new_tuple,read_t,old_tuple);   
	   
	if(ok!=1) 
	{
		 old_tuple.id = new_tuple.id;//record id in the old tuple for log recording 
		 gd_ctrl.OpTuple(table_name,new_tuple,insert_L1,tt);
		 rec_ctrl.InsertLog(trans_no,table_name,op,old_tuple,insert_L1);
  }
	else 
	{
		 gd_ctrl.OpTuple(table_name,new_tuple,update_L1,tt);
	   rec_ctrl.InsertLog(trans_no,table_name,op,old_tuple,update_L1);
	}
	
	return(0);
}

int sch_int::DeleteOp()
{
	int ok = gd_ctrl.OpTable(table_name,del);
	if(ok = 1) 
	{
     deque<tuple_t> q;  deque<tuple_t>::iterator it;
	   gd_ctrl.QueryTable(table_name,a_query,new_tuple, &q);
	   for(it=q.begin();it!=q.end();it++)
	   {   rec_ctrl.InsertLog(trans_no,table_name,op,*it,delete_t);}	
     gd_ctrl.OpTable(table_name,com_del);	   		  
	}	
  else      
  {
	  rec_ctrl.InsertLog(trans_no,table_name,op,new_tuple,delete_uns);	  
	}	
	return(0);	  	 	
}
   
int sch_int::CommitOp()
{

	  rec_ctrl.PrintLog(trans_no,table_name,op,new_tuple,commit);	  
	  int incpl; string table;
	  do
	  {
	  	incpl = rec_ctrl.ReleaseLog(trans_no, table);

	  	if(incpl) gd_ctrl.OpTable(table,com_del);	  		
	  }	while(incpl);
	  return(0);	  	  
}

int sch_int::AbortOp()
{
	tuple_t temp_t;
	log_op  temp_op;
	string  temp_tb;
	unsigned int id; int incpl;

	rec_ctrl.PrintLog(trans_no,table_name,rollback_t,new_tuple,rollback);	  	
	do{
	    incpl = rec_ctrl.RollbackLog(trans_no, temp_op, temp_t, temp_tb);
	  	if(incpl) //roll back table or tuple
	  	{
         rec_ctrl.PrintLog(trans_no,temp_tb,rollback_t,temp_t,temp_op);	  		
	  		 gd_ctrl.RecoveryTable(temp_op, temp_t, temp_tb); 
	    }
	}	
	while(incpl);
  return(0);			
}
           
int sch_int::QueryOp( deque<tuple_t> * _q)
{
	gd_ctrl.QueryTable(table_name,op,new_tuple,_q);
	deque<tuple_t>::iterator it;
	for(it=(*_q).begin();it!=(*_q).end();it++)
	{
	 rec_ctrl.PrintLog(trans_no,table_name,op,*it,read_t);
	}		
	return((*_q).size());
}

unsigned int sch_int::GroupOp()
{
	deque<tuple_t> q; sch_int::QueryOp(&q);
	unsigned int n = q.size(); q.clear(); 
	return n;
}

void sch_int::InitOp()
{
	bool ok = rec_ctrl.OpenLogFile();
	if(!ok)
	{
	   cout << "Cannot open log file in sch_int::InitOp()!" << endl;
	   exit(-1);		 
	}
	gd_ctrl.InitDic(); 
}

ostream & sch_int::Print(ostream &os) const
{
   os << "curr trans info:\n" 
      << "                trans_no="<< trans_no <<"\n"
      << "                op="<< op <<"\n"
      << "                table= "<< table_name <<"\n"
      << "                tuple(id)= "<< new_tuple.id <<"\n"
      << "                tuple(name)= "<< new_tuple.client_name   <<"\n"
      << "                tuple(phone)= "<< new_tuple.phone << endl; 
   return os;
}	 
