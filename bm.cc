/**************************
   PittHN project
   l1_delta_ctrl.cc
   author:  Jie Guo (jig26@pitt.edu)
   version: 1.0 /04/03/2014 
**************************/
#include <sys/types.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <stdlib.h>
#include <ctype.h>
#include <iostream>
#include <string>
#include "l1_delta_ctrl.h"

using namespace std;

l1_delta_ctrl::l1_delta_ctrl(){} 
l1_delta_ctrl::~l1_delta_ctrl(){} 
l1_delta_ctrl::~l1_delta_ctrl(unsigned int _s):avail_tuple_size(_s),used_tuple_size(0){}

bool l1_delta_ctrl::OpTuple(link_op _op, string _tb, tuple_t &_tp, deque<unsigned int> * _id)
{	
	bool ok = false;
	//find the existing tuple
	deque<tb_info_t>::iterator it = l1_delta_ctrl::GetTable(_tb);
	if(_op!=l_insert && it == l1_dic.end()) 
	{
		return(false);
	}
  else //insert a new tuple and insert a new table
  {    
  	tb_info_t t; 
  	t.assign_start = 0; t.assign_end = 0;
  	t.valid = true;   t.table_name = true;
  	page_info_t p;
  	p.offset = 0;     p.valid = 1;
  	p.fix_cnt = 0;    t.page.push_back(p);
  	it = l1_delta_ctrl::InsertTable(t);
  }
  
	if(_op==l_search || _op==l_delete || _op==l_update) 
	{

	   if((*it).page.size()<(*_id).at(0)){return(false);}		   
	   deque<page_info_t>::iterator it1=(*it).page.begin()+(*_id).at(0);
	   
	   deque<tuple_t>::iterator it2=(*it1).tuples.begin();
	   for(it2=(*it1).tuples.begin();it2!=(*it1).tuples.end();it2++)
	   {
	   	 if((*it2).id == (*_id).at(1))
	   	 {
	        switch(_op)
	        {
	        	 case l_search:
	        	 	    _tp.client_name = (*id2).client_name;
	        	 	    _tp.phone = (*id2).phone;break;
	        	 case l_update:
	        	 	    (*id2).client_name = _tp.client_name;
	        	 	    (*id2).phone = _tp.phone;break; 	        	 	
	        	 case l_delete:
	        	 	    (*it1).tuples.erase(it); 
	        	 	    l1_delta_ctrl::OpAvailSpace(add,1);break;	
	        	 	    
	        }	 return(true);  	 	  
	   	 }
	   }	
	   return(false);   	   
	}	
	
	
	if(_op==l_insert)
  {
	   deque<page_info_t>::iterator it1;
	   (*_id).clear();     
	   it1=(*it).page.begin()+(*it).assign_end; 
	   if((*it1).tuples.size()<TUPLES_PER_PAGE)  
	   {
	   	  (*it1).tuples.push_back(_tp);
	   	  (*_id).push_back((*it).assign_end); 
	   }
	   else if((*it).assign_start!=0)
	   {
	   	   it1=(*it).page.begin()+(*it).assign_start;
	   	  (*it1).tuples.push_back(_tp);  
	   	  (*_id).push_back((*it).assign_start); (*it).assign_start--;	   	  
	   }
	   else
	   {
  	     page_info_t p;
  	     p.offset = 0;     p.valid = 1;
  	     p.fix_cnt = 0;    p.tuples.push_back(_tb); 
  	     (*it).page.push_back(p); (*_id).push_back((*it).assign_end);
  	     (*it).assign_end++;  	
	   }	
	   
	   l1_delta_ctrl::OpAvailSpace(add,1); return(true);	 
  }
	
  return(true);    		
}

unsigned int l1_delta_ctrl::OpAvailSpace(spaceop _op,unsigned int _n)
{
	 switch (_op)
	 {
	 	   case add:    avail_size=avail_size+n;
	 	   	            used_size=used_size-n;  break;
	 	   case substr: avail_size=avail_size-n;
	 	   	            used_size=used_size+n;  break;	 	   	         
	 }
	 return(avail_size);
}                     

bool l1_delta_ctrl::GroupRemoveTuple(string _tb)
{
	deque<tb_info_t>::iterator it = l1_delta_ctrl::GetTable(_tb);
  if(it == l1_dic.end()) 
	{
		cout << "No table is found in l1_delta_ctrl::GroupRemoveTuple()" << endl;
		return(false);
	}

  deque<page_info_t>::iterator it1;	
	for(it1=(*it).page.begin();it!=(*it).page.end();it++)
	{
		 (*it1).tuples.clear();
	}
	(*it).page.clear(); l1_delta_ctrl::DeleteTable(it);
	return(true); 
}

bool l1_delta_ctrl::PopTuple(string &_s, tuple_t &_t)
{	
	deque<tb_info_t>::iterator it = l1_dic.begin();
		
	for(it=l1_dic.begin();it!=l1_dic.end();it++)
	{
		 deque<page_info_t>::iterator it1;
		 for(it1=(*it).page.begin();it1!=(*it).page.end();it1++)
		 {
		 	   if((*it1).tuples.size())
		 	   {
		 	   	  s = table_name;
		 	   	  _t.id = (*it1).tuples.at(0).id;
		 	   	  _t.client_name = (*it1).tuples.at(0).client_name;
		 	   	  _t.phone = (*it1).tuples.at(0).phone;
		 	   	  (*it1).tuples.pop_front();
		 	   	  if((*it1).tuples.size()==0)
		 	   	  {
		 	   	  	 if((*it).assign_start!=0)(*it).assign_start++;
		 	   	  }
		 	   	  return(true);
		 	   }
		 }
		
	}
	return(false); 		
}

deque<tb_info_t>::iterator l1_delta_ctrl::GetTable(string _tb)
{
	deque<tb_info_t>::iterator it;		
	for(it=l1_dic.begin();it!=l1_dic.end();it++)
	{
		 if((*it1).table_name.compare(_tb)){return(it);}
	}
	return(it); 			
}

deque<tb_info_t>::iterator l1_delta_ctrl::InsertTable(tb_info_t _tb)
{
	deque<tb_info_t>::iterator it = l1_dic.insert(l1_dic.size(),_tb);
	return(it);
}
void l1_delta_ctrl::DeleteTable(deque<tb_info_t>::iterator _it)
{
  l1_dic.erase(it);
}
   	