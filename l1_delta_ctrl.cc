/**************************
   PittHN project
   l1_delta_ctrl.cc
   author: Jie Guo (jig26@pitt.edu)
   version: 1.0 
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

l1_delta_ctrl::l1_delta_ctrl():avail_size(L1_TUPLE_NUM),used_size(0){} 
l1_delta_ctrl::~l1_delta_ctrl(){} 

bool l1_delta_ctrl::OpTuple(link_op _op, string _tb, tuple_t &_tp, deque<unsigned int> * _id)
{	
	bool ok = false;
	//find the existing tuple
	deque<tb_info_t>::iterator it = l1_delta_ctrl::GetTable(_tb);
	if(_op!=l_insert && it == l1_dic.end()) 
	{
		return(false);
	}
	//insert a new table and insert a new tuple
  if (_op==l_insert && it == l1_dic.end())
  {   
  	  tb_info_t t; t.assign_end = 0; t.inmem_page = 0; t.valid = true; t.table_name = _tb;
  	  it = l1_delta_ctrl::InsertTable(t);
  }
  //no tuple in the table
  if(_op==l_insert && (*it).page.size() == 0 && (*it).assign_end == 0)
  {
  	page_info_t p;  p.in_mem = 1; p.dirty = true;  
  	(*it).page.push_back(p);      (*it).inmem_page++;
  } 
	if(_op==l_search || _op==l_delete || _op==l_update) 
	{
     //if the page larger than total page number, report error
	   if((*it).page.size()<(*_id).at(0))
	   {
	   	 cout << "Page id is NOT correct in l1_delta_ctrl::OpTuple()!" << endl;
	   	 exit(-1);	   
	   }	
	   	   
	   deque<page_info_t>::iterator it1=(*it).page.begin()+(*_id).at(0);
	   //if the page is not in memory, need to read it from disk
	   if((*it1).in_mem == false)
	   {
	    l1_delta_ctrl::GetRowFile((*it).table_name,&((*it1).tuples),(*_id).at(0));
	    (*it1).in_mem = true;	(*it).inmem_page++;  	      
	   }
	   deque<tuple_t>::iterator it2;
	   for(it2=(*it1).tuples.begin();it2!=(*it1).tuples.end();it2++)
	   {
	   	 if((*it2).id == (*_id).at(1))
	   	 {
	        switch(_op)
	        {
	        	 case l_search:
	        	 	    _tp.client_name = (*it2).client_name;
	        	 	    _tp.phone = (*it2).phone; 
	        	 	    _tp.id = (*_id).at(1);
	        	 	    break;
	        	 case l_update:
	        	 	    (*it2).client_name = _tp.client_name;
	        	 	    (*it2).phone = _tp.phone; (*it1).dirty = true; break; 	        	 	
	        	 case l_delete:
	        	 	    (*it1).tuples.erase(it2); (*it1).dirty = true;
	        	 	    l1_delta_ctrl::OpAvailSpace(add,1);break;	
	        	 	    
	        }	 ok = true;  break;	 	  
	   	 }
	   }		   
	}	
	
	if(_op==l_insert)
  {	   
	   if(l1_delta_ctrl::OpAvailSpace(check,0)==0)
	   {
	   	  if(l1_delta_ctrl::RecycleSpace(L1_TH_TUPLE_NUM)==0)
	   	  {
	   	    cout << "Cannot recycle any space l1_delta_ctrl::OpTuple()!" << endl;
	   	    exit(-1);	   	  	 
	   	  }
	   }
	   deque<page_info_t>::iterator it1 =(*it).page.begin()+(*it).assign_end;
	   (*_id).clear();     
	   if((*it1).in_mem == false)//if the page is not in memory, retrieve it from disk 
     {
        l1_delta_ctrl::GetRowFile(_tb,&((*it1).tuples),(*it).assign_end); 
        (*it1).in_mem = true;	(*it).inmem_page++;    	 
     }
	   if((*it1).tuples.size()<TUPLES_PER_BLK) //if the page is not full 
	   {	   	  	
	   	  (*it1).tuples.push_back(_tp); (*it1).dirty = true;
	   }
	   else
	   {
  	    page_info_t p;   p.fix_cnt = 0;
  	    p.dirty = true; p.in_mem = true;     
  	    p.tuples.push_back(_tp); (*it).page.push_back(p); 
  	    if((*it).inmem_page)(*it).assign_end++; (*it).inmem_page++;  	
	   }	
	   (*_id).push_back((*it).assign_end); 
	   (*_id).push_back(_tp.id);	   
	   l1_delta_ctrl::OpAvailSpace(substr,1);  
  }
	l1_delta_ctrl::UpdateRecency(it); return(ok);    		
}

unsigned int l1_delta_ctrl::OpAvailSpace(spaceop _op,unsigned int _n)
{
	 switch (_op)
	 {
	 	   case add:    avail_size=avail_size+_n;
	 	   	            used_size=used_size-_n;  break;
	 	   case substr: avail_size=avail_size-_n;
	 	   	            used_size=used_size+_n;  break;	 	   	         
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

  deque<page_info_t>::iterator it1;	unsigned int n = 0; 
	for(it1=(*it).page.begin();it1!=(*it).page.end();it1++)
	{
		 n+= (*it1).tuples.size(); (*it1).tuples.clear();
	}
	(*it).page.clear(); l1_delta_ctrl::DeleteTable(it);

	l1_delta_ctrl::OpAvailSpace(add,n);
	return(true); 
}

bool l1_delta_ctrl::PopTuple(string &_s, tuple_t &_t)
{	

	//find the existing tuple, should be popped out from the end of link list
	deque<tb_info_t>::iterator it=l1_dic.end()-1; bool incpl=true; 
	while(incpl)		
	{
		 deque<page_info_t>::iterator it1 = (*it).page.begin()+(*it).assign_end; 	 	
		 bool incpl1=true;
		 do
		 {
		 	   if((*it1).tuples.size()!=0 && 
		 	   	  (*it1).in_mem == true && it1 != (*it).page.end())
		 	   {
		 	   	  _s = (*it).table_name;
		 	   	  _t.id = (*it1).tuples.at(0).id;
		 	   	  _t.client_name = (*it1).tuples.at(0).client_name;
		 	   	  _t.phone = (*it1).tuples.at(0).phone;
		 	   	  (*it1).tuples.pop_front();
		 	   	  l1_delta_ctrl::OpAvailSpace(add,1);
		 	   	  if((*it1).tuples.size()==0)
		 	   	  { 
		 	   	  	 (*it).page.erase(it1); (*it).inmem_page--;
		 	   	  	 if((*it).assign_end!=0)(*it).assign_end--;
		 	   	  }
		 	   	  return(true);
		 	   }  
		 	   if(it1 != (*it).page.begin())it1--;
		 	   else incpl1=false;
		 }		
		 while(incpl1);
		 if(it==l1_dic.begin()){incpl=false;}
		 else {it--;}
	}
	return(false); 		
}

deque<tb_info_t>::iterator l1_delta_ctrl::GetTable(string _tb)
{
	deque<tb_info_t>::iterator it;		
	for(it=l1_dic.begin();it!=l1_dic.end();it++)
	{
		 if((*it).table_name.compare(_tb)==0){break;}
	}
	return(it); 			
}

deque<tb_info_t>::iterator l1_delta_ctrl::InsertTable(tb_info_t _tb)
{
	l1_dic.push_front(_tb); io_int.CreateRowFile(_tb.table_name);
	return(l1_dic.begin());
}

void l1_delta_ctrl::DeleteTable(deque<tb_info_t>::iterator _it)
{
   l1_dic.erase(_it);
}

unsigned int l1_delta_ctrl::RecycleSpace(unsigned int _n)
{
	 //find out the least accessed table
	 unsigned int n = 0;
	 while(n<_n)
	 {
	     deque<tb_info_t>::iterator it= l1_dic.end()-1;	
	     //find out the lru table (tail)
	     while(l1_dic.size()!=0)
	     {
	    	  if((*it).inmem_page!=0 || it == l1_dic.begin()){break;}
	    	  it--;	
	     } 	     		
	     if(it==l1_dic.begin() && (*it).inmem_page==0)
	     {
		    cout << "Cannot find any table to recycle in l1_delta_ctrl::RecycleSpace()" << endl;
		    exit(-1);	 	 
	     }
	     deque<page_info_t>::iterator it1; unsigned int i=0;//find out page
	     for(it1=(*it).page.begin();it1!=(*it).page.end();it1++)
	     {
		     if((*it1).in_mem==true){break;} i++; //i: the page offset for the whole table
	     }	 		 	   
	     //find out the first in-memory page to release
	     if(it1==(*it).page.end())
	     {
		    cout << "Cannot find any page to recycle in l1_delta_ctrl::RecycleSpace()" << endl;
		    exit(-1);	 	 
	     }
	     if((*it1).dirty == true){io_int.FlushRow2Disk((*it).table_name,&((*it1).tuples),i);}
	     n += (*it1).tuples.size(); (*it1).dirty = false;
	          (*it1).tuples.clear();(*it1).in_mem = false;
	     //decrement in memory page number
       (*it).inmem_page--; 
   }
	 //increment available space
   unsigned int res = l1_delta_ctrl::OpAvailSpace(add,n); return(res);   	 
}

void l1_delta_ctrl::DeleteRowFile(string _tb)
{
     io_int.RemoveRowFile(_tb);	 
}

unsigned int l1_delta_ctrl::GetRowFile(string _tb, deque<tuple_t> * _q, unsigned int _n)
{
	 //check the available space is enough
   unsigned int avail = l1_delta_ctrl::OpAvailSpace(check,0);   	
	 //get space for L1 delta space area
   if(avail<L1_TH_TUPLE_NUM) l1_delta_ctrl::RecycleSpace(L1_TH_TUPLE_NUM);  	 
	 //control io ctrl to read file
   unsigned int n = io_int.GetRowFileFromDisk(_tb,_q,_n);      	 
	 //decrement available space
	 l1_delta_ctrl::OpAvailSpace(substr,n); 	 	
	 return(1);	
} 

void l1_delta_ctrl::UpdateRecency(deque<tb_info_t>::iterator it)
{  
    unsigned int i = it - l1_dic.begin();
    l1_dic.push_front(*it); //put at the front
    deque<tb_info_t>::iterator it1 = l1_dic.begin()+i+1;
    l1_dic.erase(it1);  
} 

ostream & l1_delta_ctrl::Print(ostream &os) const
{
   os << "l1_delta_ctrl info:\n" 
      << "        avail tuple size="<< avail_size <<"\n"
      << "        used tuple size= "<< used_size  <<endl; 
      
	 deque<tb_info_t>::const_iterator it;
	 for(it=l1_dic.begin();it!=l1_dic.end();it++)
	 {
       os << "        table name: " << (*it).table_name << "\n"
          << "        valid=" << (*it).valid << "\n"
          << "        in memory page number =" << (*it).inmem_page << "\n"
          << "        assign_end = " << (*it).assign_end << "\n" 
          << "        attached page: " << endl; 
            
	     deque<page_info_t>::const_iterator it1;
	     for(it1=(*it).page.begin();it1!=(*it).page.end();it1++)
	     {
          os << "                      dirty = " << (*it1).dirty << "\n" 
             << "                      in_mem= " << (*it1).in_mem << "\n" 
             << "                      tuples: " << endl;
	        deque<tuple_t>::const_iterator it2;
	        for(it2=(*it1).tuples.begin();it2!=(*it1).tuples.end();it2++)
          {
             os << "                      id=" << (*it2).id 
                << " name=" << (*it2).client_name 
                << " phone=" << (*it2).phone << endl;
          }           
	     }
	 } 
   return os;
}	