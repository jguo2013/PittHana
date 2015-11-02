/**************************
   PittHN project
   l2_delta_ctrl.cc
   author:  Jie Guo (jig26@pitt.edu)
   version: 1.0 
**************************/
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/utsname.h>
#include <stdio.h>
#include <iostream>
#include <stdlib.h>
#include <fstream>
#include <stdio.h>
#include <string.h>
#include <string>
#include "l2_delta_ctrl.h"
	                      
l2_delta_ctrl::l2_delta_ctrl():avail_size(L2_TUPLE_NUM),used_size(0){}
l2_delta_ctrl::~l2_delta_ctrl(){}

void l2_delta_ctrl::UpdateRecency(deque<l2_meta_t>::iterator it)
{  
    unsigned int i = it - l2_dic_meta.begin();
    l2_dic_meta.push_front(*it); 
    deque<l2_meta_t>::iterator it1 = l2_dic_meta.begin()+i+1;
    l2_dic_meta.erase(it1);  
}
   
bool l2_delta_ctrl::OpColumn(link_op _op, string _tb,tuple_t _t,string _att,unsigned int &_n)
{
	  deque<l2_meta_t>::iterator it = l2_delta_ctrl::SearchAtt(_tb,_att);

	  if(it==l2_dic_meta.end()) 
	  {
	  	 if(_op == l_insert) it = l2_delta_ctrl::CreateAtt(_tb,_att);
	  	 else return (false);
	  }	
	  //check if there is enough space to hold the data
	  int avail = l2_delta_ctrl::OpAvailSpace(check,0);
	  if(avail < 1 && _op == l_insert)
	  {   	    	   		
	  	l2_delta_ctrl::RecycleSpace(L2_TUPLE_NUM);	    	   		
	  }	     

	  if((*it).col.size()==0) 
  	{ 
  		l2_delta_ctrl::GetColumnFromDisk(_tb,_att,&((*it).col),(*it).ass_id);	
  	}

    string s1; bool ok = true;
    if(_att.compare("phone")==0) s1=_t.phone;
    else if(_att.compare("client_name")==0) s1=_t.client_name;
    	
    deque<l2_attr_t>::iterator it1;
    it1 = l2_delta_ctrl::SearchColumn(&((*it).col),s1,0,0);
    	        
    switch (_op)
    {
	    case l_insert:
	    	   if(it1==(*it).col.end())
	    	   {
	    	    l2_delta_ctrl::OpAvailSpace(substr,1);

	    	   	l2_attr_t t; t.id = (*it).ass_id; t.content = s1;
	    	   	(*it).col.push_back(t); (*it).ass_id=(*it).ass_id+1;
	    	   	it1==(*it).col.end()-1;	    	   	
	    	   }
           (*it).dirty = true; _n = (*it1).id; break;
	    case l_delete:	    	   
	    	   if(it1==(*it).col.end()) ok = false;
	    	   l2_delta_ctrl::OpAvailSpace(add,1);	
	    	   (*it).dirty = true; (*it).col.erase(it1);break;
      case l_search:
	    	   if(it1==(*it).col.end()) ok = false;      	
	         _n = (*it1).id;break;            	
    }
    //lru: cache placement policy
    l2_delta_ctrl::UpdateRecency(it); return(ok); 
}         

deque<l2_meta_t>::iterator l2_delta_ctrl::SearchAtt(string _tb,string _att)
{

  deque<l2_meta_t>::iterator it;  
  for(it = l2_dic_meta.begin();it!=l2_dic_meta.end();it++)
  {
  	 string s1 = (*it).table_name; string s2 = (*it).att_name;
     if(s1.compare(_tb) == 0 && s2.compare(_att) == 0) return(it);           	 
  }
  return(it); 	 
}

deque<l2_attr_t>::iterator l2_delta_ctrl::SearchColumn(deque<l2_attr_t> * _lh, string _cont, unsigned int _n, int flag)
{

  deque<l2_attr_t>::iterator it;  
  for(it = (*_lh).begin();it!=(*_lh).end();it++)
  {
  	 if(flag==0) //0: search by content
  	 { 
        if(_cont.compare((*it).content) == 0) return(it); 
     }
  	 if(flag==1) //1: search by id
  	 { 
        if(_n == (*it).id) return(it);
     }    	   	 
  }
  return(it); 	 
}
	 	
void l2_delta_ctrl::GroupRemoveColumn(string _tb)
{
  deque<l2_meta_t>::iterator it;  
  while(1)
  {
       int incpl = 0;
       for(it = l2_dic_meta.begin();it!=l2_dic_meta.end();it++)
       {
       	 string s1 = (*it).table_name;  
          if(s1.compare(_tb) == 0) 
          {
          	io_int.RemoveColumnFile(_tb,(*it).att_name);
          	l2_delta_ctrl::OpAvailSpace(add,(*it).col.size()); 
            (*it).col.clear(); l2_dic_meta.erase(it);         		
          	incpl = 1; break;
          }     	 
       }
       if(incpl==0)break;   
  }	  
}

string l2_delta_ctrl::GetColumn(string _tb,unsigned int _l,string _att)
{
    
    //search the attr in the link table
    deque<l2_meta_t>::iterator it; string s;
    it = l2_delta_ctrl::SearchAtt(_tb,_att);
    //check if the table is in memory
    if((*it).ass_id!=0 && (*it).col.size()==0)
    {
	    //check if there is enough space to hold the data
	    int avail = l2_delta_ctrl::OpAvailSpace(check,0);
	    if(avail < (*it).ass_id) l2_delta_ctrl::RecycleSpace((*it).ass_id);
	    //get the column table in main memory
	    l2_delta_ctrl::GetColumnFromDisk(_tb,_att,&((*it).col),(*it).ass_id);
  	}    
    //search the column content in the attr table    	
    deque<l2_attr_t>::iterator it1 = SearchColumn(&((*it).col),s,_l,1);	    	
    if(it1!=(*it).col.end()) s = (*it1).content;
    else  s.clear();  
    l2_delta_ctrl::UpdateRecency(it); return(s);
}          	
    
void l2_delta_ctrl::InitL2Delta()
{
   	
	 //read column file from disk
	 deque<string> tb; deque<string> att;
	 l2_dic_meta.clear();
	 io_int.GetColumnNameFromDisk(&tb,&att);
	 if(tb.size()!=0 && att.size()!=0)
	 {
	 	 unsigned int i=0;
	 	 while(i<tb.size())
	 	 {
	 	 	  l2_meta_t t;
	 	 	  t.table_name=tb.at(i); t.att_name=att.at(i);
	      t.valid=true;    t.dirty=false;                    
        io_int.GetColumnFromDisk(t.table_name,t.att_name,&(t.col));
        t.ass_id=t.col.size(); t.col.clear(); 
        l2_dic_meta.push_back(t); 
        i++;
	 	 }
	 }
	 tb.clear(); att.clear();  
}

unsigned int l2_delta_ctrl::RecycleSpace(unsigned int _n)
{
	unsigned int n=0;	
	while(_n>n)
	{
	  //search a column table in memory (lru)
	  deque<l2_meta_t>::iterator it = l2_dic_meta.end()-1;
	  //find out the lru table (tail)
	  while(l2_dic_meta.size()!=0)
	  {
	  	  if((*it).col.size()!=0 || it == l2_dic_meta.begin())
	  	    {break;}
	  	  it--;	
	  }    	
    if(it==l2_dic_meta.begin() && (*it).col.size()==0)
    {  
	  	 break;  
    }
    
    if((*it).dirty==true)
    {
  	  //flush the dirty column to disk
  	  io_int.FlushColmn2Disk((*it).table_name,(*it).att_name,&((*it).col));  
  	  (*it).dirty = false;
    }    
    unsigned int n1 = (*it).col.size(); (*it).col.clear();
    n += n1; 
  }
  
  l2_delta_ctrl::OpAvailSpace(add,n);  return(n);  
} 	

bool l2_delta_ctrl::GetColumnFromDisk(string _tb,string _att, deque<l2_attr_t>  * _col, int _n)
{
	 //check space is enough
	 int n = l2_delta_ctrl::OpAvailSpace(check,0);
	 //if not, recycle some space for read
	 if(n < _n+1) 
	 {  l2_delta_ctrl::RecycleSpace(_n+1);}
	 //call io_ctrl to read file into disk
	 io_int.GetColumnFromDisk(_tb,_att,_col);
	 l2_delta_ctrl::OpAvailSpace(substr,(*_col).size());
	 
	 return (true);
} 

deque<l2_meta_t>::iterator l2_delta_ctrl::CreateAtt(string _tb,string _att) //create a empty file in disk
{
	 //create a new entry
	 l2_meta_t t; t.col.clear();
	 t.table_name = _tb; t.att_name = _att;
   t.fix_cnt = 0;  t.ass_id = 0;  t.valid = true;	  
	 //create a empty column file in disk
	 io_int.CreatColumnFile(_tb,_att);
	 //insert in global link table
	 l2_dic_meta.push_front(t); 
	 deque<l2_meta_t>::iterator it = l2_dic_meta.begin();
	 return(it);		 
}

int l2_delta_ctrl::OpAvailSpace(spaceop _op,unsigned int _n)
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

ostream & l2_delta_ctrl::Print(ostream &os) const
{
   os << "l2_delta_ctrl info:\n" 
      << "        avail tuple size="<< avail_size << "\n"
      << "        used tuple size= "<< used_size  << endl; 
      
	 deque<l2_meta_t>::const_iterator it;
	 for(it=l2_dic_meta.begin();it!=l2_dic_meta.end();it++)
	 {
       os << "        table name= " << (*it).table_name << "\n"
          << "        att_name= "   << (*it).att_name << "\n" 
          << "        valid= " << (*it).valid << "\n"
          << "        dirty= " << (*it).dirty << "\n"
          << "        ass_id= " << (*it).ass_id << "\n"
          << "        col: " << endl; 
            
	     deque<l2_attr_t>::const_iterator it1;
	     for(it1=(*it).col.begin();it1!=(*it).col.end();it1++)
	     {
          os << "                      id = " << (*it1).id << "\n" 
             << "                      content = " << (*it1).content << endl;          
	     }
	 } 
   return os;
}	
  	  