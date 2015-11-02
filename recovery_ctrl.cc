/**************************
   PittHN project
   recovery_ctrl.cc
   author: Jie Guo (jig26@pitt.edu)
   version: 1.0 /04/03/2014 
**************************/
#include <sys/types.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <fstream>
#include <string>
#include "recovery_ctrl.h"

using namespace std;

recovery_ctrl::recovery_ctrl():file_name("logfile.txt"){}
recovery_ctrl::~recovery_ctrl(){}

string recovery_ctrl::_itoa(unsigned int n)
{
 ostringstream ostr; ostr << n;
 return(ostr.str());
}

bool recovery_ctrl::OpenLogFile()
{
	fp.open(file_name.c_str());
	bool ok = fp.is_open(); return(ok);
} 

void recovery_ctrl::CloseLogFile()
{
	fp.close();
}

void recovery_ctrl::InsertLog(unsigned int _tr, string _tb,sch_op _op,tuple_t _tp,log_op _l)
{
	b_image_t im;
	im.table_name = _tb; im.op = _op;
	im.l_op = _l;	       im.tran_no = _tr;
	im.t.id = _tp.id;    im.t.phone = _tp.phone;
	(im.t).client_name = _tp.client_name;		

  tran_metadata.push_back(im);
  recovery_ctrl::PrintLog(_tr,_tb,_op,_tp,_l);
  
}

int recovery_ctrl::ReleaseLog(unsigned int _tr, string &_t)
{
	 deque<b_image_t>::iterator it; int incpl = 1;  
	 
	 while(incpl)
	 {
      log_op l; string t; 
      for(it = tran_metadata.begin();it!=tran_metadata.end();it++)
      {
        	if((*it).tran_no ==_tr)
   		    {
   		      l = (*it).l_op; t = (*it).table_name;
   		      tran_metadata.erase(it); break;
   		    }  
   		}
   		if(l == delete_suc) {_t = t; break;}
      if(it==tran_metadata.end()){incpl=0;}
   }  return(incpl);
}

int recovery_ctrl::RollbackLog(unsigned int _tr, log_op &_op, tuple_t &_t, string &_tb)
{
	 int rec_entry; bool go_through = false; 
	 unsigned int rec_id;
	 do
	 {
	 	 rec_entry = 0; 
	   deque<b_image_t>::iterator it; 
	   if(tran_metadata.begin() != tran_metadata.end())it = tran_metadata.end()-1;
	   else it = tran_metadata.end();
     while(!go_through)
     {
   	   if(it != tran_metadata.end() && (*it).tran_no ==_tr)
   	   {
   	     if(rec_entry == 0)
   	     {
   	        _op = (*it).l_op; _tb = (*it).table_name;
   	        _t.id = (*it).t.id;
   	        _t.client_name = (*it).t.client_name;
   	        _t.phone = (*it).t.phone;
   	        if(it == tran_metadata.begin())
   	        {
   	         tran_metadata.erase(it); it = tran_metadata.begin();
   	        }
   	        else tran_metadata.erase(it);
   	     	  if(_op == insert_L1 || _op == update_L1) 
   	        {
               rec_entry = 1; //rollback insert/update
   	        }
   	        else
   	        { 
   	         rec_entry = 2;
   	         if(_op == delete_suc) {return(1);}
   	         else break;
   	        }   	        
   	     }
   	     else if(rec_entry == 1) 
   	     {
   	     	  if((*it).t.id == _t.id)tran_metadata.erase(it);
   	     }   	                 
   	   }
   	   if(it==tran_metadata.begin()) go_through = true;
   	   else it--;
     }
     if(rec_entry == 1) {return(1);}
   } 
   while (rec_entry != 0);
   
   return(0);	
}
	    
void recovery_ctrl::PrintLog(unsigned int _tr, string _tb,sch_op _op,tuple_t _tp,log_op _l)
{
	string s; s.clear();

  s = recovery_ctrl::_itoa(_tr); s.push_back(',');
  s.insert(s.size(),_tb); s.push_back(',');

  string c;
  switch (_op)
  {
  	case write_t: c = "write,"; break;
  	case del:     c = "delete,"; break; 
  	case r_query: c = "query by id,"; break;
  	case m_query: c = "query by area code,"; break;
  	case rollback_t: c = "rollback trans"; break;				
  } 
  s.insert(s.size(),c);

  c = recovery_ctrl::_itoa(_tp.id); c.push_back(',');
  c.insert(c.size(),_tp.client_name); c.push_back(','); 
  c.insert(c.size(),_tp.phone); c.push_back(','); 
  s.insert(s.size(),c); 

  switch (_l)
  {
  	case insert_L1: c = "insert L1 delta"; break;
  	case insert_L2: c = "insert L2 delta"; break; 
  	case commit:    c = "commit"; break;
  	case rollback:  c = "rollback"; break;
  	case delete_suc:c = "delete(succ)"; break;
  	case delete_uns:c = "delete(fail)"; break;  						
  	case update_L1: c = "update L1 delta"; break;
  	case read_t:    c = "read"; break;
  } 
  s.insert(s.size(),c); s.push_back('\n');
  fp << s;           	
}

ostream & recovery_ctrl::Print(ostream &os) const
{
   os << "recovery_ctrl info:\n" << endl; 
      
	 deque<b_image_t>::const_iterator it;
	 for(it=tran_metadata.begin();it!=tran_metadata.end();it++)
	 {
       os << "        table name: " << (*it).table_name << "\n"
          << "        trans no: " << (*it).tran_no << "\n"
          << "        log op: " << (*it).l_op << "\n"
          << "        sch op: " << (*it).op << "\n" 
          << "        tuple(id): " << (*it).t.id << "\n" 
          << "        tuple(name): " << (*it).t.client_name << "\n" 
          << "        tuple(phone): " << (*it).t.phone << endl; 
   }
   return os;
}	
