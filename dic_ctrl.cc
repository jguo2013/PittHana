/**************************
   PittHN project
   dic_ctrl.cc
   author:  Jie Guo (jig26@pitt.edu)
   version: 1.0 
**************************/
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <stdio.h>
#include <iostream>
#include <sstream>
#include <fstream>
#include <string>
#include "dic_ctrl.h"

dic_ctrl::dic_ctrl():used_size(0),avail_size(GD_TUPLE_NUM){}
dic_ctrl::~dic_ctrl(){}
	
void dic_ctrl::SetMode(int _m)
{
	mode = _m;
}

int dic_ctrl::GetMode()
{
	return(mode);
}

int dic_ctrl::MergeL12L2Op(int _n) //_n: merged tuple number
{
	tuple_t temp_tp; 
	string  temp_tb;
		
	for(int i=0;i<_n;i++)
	{
	   bool ok = L1_ctrl.PopTuple(temp_tb,temp_tp);	 	
	   if(ok) 
	   {
	   	 dic_ctrl::InsertTuple2L2(temp_tb,temp_tp);
	   }
	   else	
	   {
	      cout << "cannot find tuple in dic_ctrl::MergeL12L2Op()!" << endl;
	      exit(-1);	 
	   } 		 		
	}
}
   	
int dic_ctrl::OpTuple(string _tb, tuple_t _nt, log_op _op, tuple_t &_ot)
{
	  deque<unsigned> id;  int res = 0; bool ok=false;
	     	
	  //get enough storage space to insert new table
	  if(_op == insert_L1 || _op == update_L1)
	  {
	     //get space for global dictionary
	     unsigned int avail = dic_ctrl::OpAvailSpace(check,0);
       if(!avail) 
       {
       	  unsigned int n = dic_ctrl::RecycleSpace(GD_TH_TUPLE_NUM); 
       	  if(n==0)
       	  {
	  	 	     cout << "No recycled tuple can be found in dic_ctrl::OpTuple()!" << endl;
	  	       exit(-1);       	  	
       	  }      	
       }	  
	     avail = L1_ctrl.OpAvailSpace(check,0);
	     //get space for L1 delta space area
       if(avail==0)                   
       {
       	  if(mode==0)      dic_ctrl::MergeL12L2Op(L1_TH_TUPLE_NUM);//0: column store     
	        else if(mode==1) L1_ctrl.RecycleSpace(L1_TH_TUPLE_NUM);  //1: row store	     
	     }	      
	  }
	  	
    //get global dic; if not in memory, get it from disk  
    deque<dic_meta_t>::iterator it = dic_ctrl::GetTable(_tb);
    //if the table does not exist,create one		  
	  if(it == glb_dic_metadata.end() && _op == insert_L1)     
	  {
	  	dic_meta_t temp; dic_ctrl::CreatTable(_tb,temp);
	  	it = dic_ctrl::GbOpLink(l_insert,_tb,temp);
	  }
	  if(it != glb_dic_metadata.end())
	  {
	     //get tuple dic from global dic 	
       deque<global_dic_t>::iterator it1;
 	     it1 = dic_ctrl::TbOpLink(l_search,_nt,&((*it).addr));
 	     	    	    
       switch (_op)
       {
       	case read_t :
       		   if(it1==(*it).addr.end()) 
       		   	  res=0;                              
       		   else if((*it1).loc == l1_loc)          
	               ok = dic_ctrl::GetTupleFromL1(it1,_tb,_ot);
	           else if((*it1).loc == l2_loc)          
	            	  ok = dic_ctrl::GetTupleFromL2(it1,_tb,_ot,&((*it).att_name));
	           if(ok==true) res=1; break;	
	       case insert_L1:   
	       	    L1_ctrl.OpTuple(l_insert,_tb,_nt,&id);    	   
	       	   it1 = dic_ctrl::TbOpLink(l_insert,_nt,&((*it).addr));//cannot find dic in file,create in link table
	       	   dic_ctrl::UpdateTableDic(it1,_op,&id);
	       	   (*it).dirty = true; dic_ctrl::OpAvailSpace(substr,1); break;	 
	       case update_L1:
	       	   dic_ctrl::SetLoc(it1,&id);
       		   if(it1==(*it).addr.end()) 
       		   {
	       	      it1 = dic_ctrl::TbOpLink(l_insert,_nt,&((*it).addr));//cannot find dic in file,create in link table
       	   	    L1_ctrl.OpTuple(l_insert,_tb,_nt,&id);
       	   	 }
	       	   else if((*it1).loc==l1_loc) L1_ctrl.OpTuple(l_update,_tb,_nt,&id);
	       	   else L1_ctrl.OpTuple(l_insert,_tb,_nt,&id);
	           dic_ctrl::UpdateTableDic(it1,insert_L1,&id); 	  	                   		   			    	   
             (*it).dirty = true; break;       
         case delete_t:
	            if(it1==(*it).addr.end()) 
	            {  res=-1;}       
	            else 
	            {  
	             tuple_t tp;  OpAvailSpace(add,1); //only discard tuple in L1 
	             if((*it1).loc == l1_loc) dic_ctrl::DelTupleFromL1(it1,_tb,_ot);	          	
	             it1 = dic_ctrl::TbOpLink(l_remove,_nt,&((*it).addr));
	            }    
	            (*it).dirty = true; break;      	
       }
    }
    else{res=-1;} return (res);
}
   
int dic_ctrl::OpTable(string _tb,sch_op _op)       //-1: no such table; 1: delete successfully  	
{
    dic_meta_t temp;
    deque<dic_meta_t>::iterator it = dic_ctrl::GetTable(_tb);	  
	  if(it == glb_dic_metadata.end()) return(-1);
	  if(_op==com_del)                               //commit delete table
	  {
	  	 dic_ctrl::OpAvailSpace(add,(*it).addr.size());
	  	 (*it).addr.clear(); glb_dic_metadata.erase(it);
	  	 L1_ctrl.GroupRemoveTuple(_tb);              //for model 0 & model 1: only delete tuples in memory
	  	 if(mode==1) L1_ctrl.DeleteRowFile(_tb);     //model 1: delete row file
	  	 if(mode==0) L2_ctrl.GroupRemoveColumn(_tb); //model 0: delete column file      
	  	 io_int.RemoveTableFile(_tb);                //for model 0 & model 1: delete table dic file	  
	  }                
	  return(1);
}    
	
void dic_ctrl::QueryTable(string _tb,sch_op _op,tuple_t _t,deque<tuple_t> * _q)
{
	  //find table
    deque<dic_meta_t>::iterator it = dic_ctrl::GetTable(_tb);	  
	  if(it != glb_dic_metadata.end()) 	  
	  {   //go over table
	      deque<global_dic_t>::iterator it1;   tuple_t t; 
	      for(it1=(*it).addr.begin();it1!=(*it).addr.end();it1++)
	      {
	      	 bool ok;  string s1 = _t.phone.substr(0,3);
	      	 //get tuple
	         if((*it1).loc == l1_loc) ok = dic_ctrl::GetTupleFromL1(it1,_tb,t);
	         else if((*it1).loc == l2_loc) ok = dic_ctrl::GetTupleFromL2(it1,_tb,t,&((*it).att_name));	         
	         if(!ok)
	         {
	          cout << "cannot find tuple in dic_ctrl::QueryTable()!" << endl;
	          exit(-1);	     	
	         }
	         //check if the tuple satisfy the requirement
	         switch (_op) 
	         {
	         	 //insert it to queue 
	         	 case a_query: (*_q).push_front(t); break; 
	         	 case r_query: if(t.id == _t.id) (*_q).push_front(t); break;
	         	 case m_query: string s2 = t.phone.substr(0,3); 
	         	               if(s1.compare(s2) == 0) (*_q).push_front(t); break;
	         }	     	  	 
	      }
	  }
}  

void dic_ctrl::RecoveryTable(log_op _op, tuple_t _t, string _tb) //0: cannot recovery  1: recovery 
{
	tuple_t tt;
	switch (_op)
	{
		case insert_L1:  dic_ctrl::OpTuple(_tb,_t,delete_t,tt);  break;
		case update_L1:  dic_ctrl::OpTuple(_tb,_t,update_L1,tt); break;
		case delete_t:   dic_ctrl::OpTuple(_tb,_t,insert_L1,tt); break;			
	}	
}

void dic_ctrl::InsertTuple2L2(string _tb,tuple_t _t)
{
	   //check the table and make sure the dic is in memory
	   deque<dic_meta_t>::iterator it = dic_ctrl::GetTable(_tb);
	   if(it==glb_dic_metadata.end())
	   {
	   	  cout << "Not table exist in dic_ctrl::InsertTuple2L2()!" << endl;
	   	  exit(-1);
	   }
	   //check the tuple in dic, if not, report error
     deque<global_dic_t>::iterator it1 = dic_ctrl::TbOpLink(l_search,_t,&((*it).addr));	
	   if(it1==(*it).addr.end()) 
	   {
	   	  cout << "Cannot find existing table in dic in dic_ctrl::InsertTuple2L2()!" << endl;
	   	  exit(-1);	   	 
	   }
 	   	
     //Get an attribute,insert in l2 delta ctrl 
     deque<unsigned int> loc; unsigned int i;
     deque<string>::iterator it2 = (*it).att_name.begin()+1;  
     for(;it2!=(*it).att_name.end();it2++)
     {
     	  L2_ctrl.OpColumn(l_insert,_tb,_t,(*it2),i); //return value id
        loc.push_back(i);
     }    
     dic_ctrl::UpdateTableDic(it1,insert_L2,&loc);      	
     (*it).dirty = true; 	       //set dirty flag
}

deque<dic_meta_t>::iterator dic_ctrl::GetTable(string _tb)
{    
    dic_meta_t temp;
    deque<dic_meta_t>::iterator it = dic_ctrl::GbOpLink(l_search,_tb,temp);	  
	  if(it==glb_dic_metadata.end()) {return(it);}	
	  	
	  if((*it).addr.size() == 0)   //the global dic is in not main memory
	  {
	  	int res = dic_ctrl::GetTableFromDisk(_tb,&((*it).addr)); //-1: no dic file; 0: dic file empty; 1: read all 
	  	if(res == -1 )                     
	  	{ 
	  	 	 cout << "No Dic file in dic_ctrl::InsertL1Tuple()!" << endl;
	  	   exit(-1);
	  	}	  	    	 	 
	  }
	  return(it);	  	
}

void dic_ctrl::ConstTable(string _tb, dic_meta_t &_t)
{
   _t.dirty = 0;      _t.table_name = _tb;
   string s = "id";   _t.att_name.push_back(s); 
   s = "client_name"; _t.att_name.push_back(s);
   s = "phone";       _t.att_name.push_back(s);
}

void dic_ctrl::CreatTable(string _tb, dic_meta_t &_t)
{
	 dic_ctrl::ConstTable(_tb,_t);
   io_int.CreateTableFile(_tb);  
}

bool dic_ctrl::GetTupleFromL1(deque<global_dic_t>::iterator it,string _tb,tuple_t &_ot)
{
	   //get page id and offset id from the entry
	   deque<unsigned int> id; dic_ctrl::SetLoc(it,&id);
	   //find the entry in L1_delta
	   bool ok = L1_ctrl.OpTuple(l_search,_tb,_ot,&id);	   
	   return(ok);
}

bool dic_ctrl::DelTupleFromL1(deque<global_dic_t>::iterator it,string _tb,tuple_t &_ot)
{
	   //get page id and offset id from the entry
	   deque<unsigned int> id; dic_ctrl::SetLoc(it,&id);
	   //find the entry in L1_delta
	   bool ok = L1_ctrl.OpTuple(l_delete,_tb,_ot,&id);	   
	   return(ok);
}
   	
bool dic_ctrl::GetTupleFromL2(deque<global_dic_t>::iterator it,string _tb,tuple_t &_ot,deque<string> * _a)
{
	   //get id from the entry
	   _ot.id = (*it).addr[0];
	   //get client_name from the entry
	   unsigned int n = (*it).addr[1];
	   _ot.client_name = L2_ctrl.GetColumn(_tb,n,(*_a).at(1));
	   //get phone from the entry
	   n = (*it).addr[2];
	   _ot.phone = L2_ctrl.GetColumn(_tb,n,(*_a).at(2));	
	   return(true);   	   
}
   	   	   	   
deque<global_dic_t>::iterator dic_ctrl::TbOpLink(link_op _op, tuple_t _t, deque<global_dic_t> * _pt)
{

  deque<global_dic_t>::iterator it;  	
	global_dic_t temp;   temp.addr[0] = _t.id; 
	//insert: put it in the back of the queue	
  if(_op == l_insert) 
  {  
  	(*_pt).push_back(temp); it = (*_pt).end()-1;
  	return(it);
  }

  for(it = (*_pt).begin();it!=(*_pt).end();it++)
  {
     if(_t.id == (*it).addr[0])
     {
     	 if(_op == l_search) {return(it);}
     	 if(_op == l_remove) {break;}
     }    	 
  }
  if(_op == l_remove)
  {
    (*_pt).erase(it); it = (*_pt).end();
  } 
  return(it);  
}

deque<dic_meta_t>::iterator dic_ctrl::GbOpLink(link_op _op, string _t, dic_meta_t _pt)
{
  deque<dic_meta_t>::iterator it;
  deque<dic_meta_t>::iterator it1 = glb_dic_metadata.end();
		
  if(_op == l_insert) 
  {
  	 glb_dic_metadata.push_front(_pt);
     it = glb_dic_metadata.begin(); return(it);
  }

  for(it=glb_dic_metadata.begin();it!=glb_dic_metadata.end();it++)
  {
     if((*it).table_name.compare(_t) == 0) break;
  }
  if(_op == l_search && it!=glb_dic_metadata.end()) 
  {
  	 it1 = dic_ctrl::UpdateRecency(it);
  }
  if(_op == l_remove)
  {
     glb_dic_metadata.erase(it); it1 = glb_dic_metadata.end();
  }
  return(it1);	
}

deque<dic_meta_t>::iterator dic_ctrl::UpdateRecency(deque<dic_meta_t>::iterator it)
{  
    deque<dic_meta_t>::iterator it1;
    unsigned int i = it - glb_dic_metadata.begin();
    glb_dic_metadata.push_front(*it); 
    it1 = glb_dic_metadata.begin()+i+1; glb_dic_metadata.erase(it1); 
    it1 = glb_dic_metadata.begin();
    return(it1);  
}

void dic_ctrl::UpdateTableDic(deque<global_dic_t>::iterator _it,log_op _op,deque<unsigned> * _id)
{
	   switch(_op)
	   {
	     case insert_L1: (*_it).loc = l1_loc; break;  
       case insert_L2: (*_it).loc = l2_loc; break;
	   }
	   (*_it).addr[1] = (*_id).at(0);
	   (*_it).addr[2] = (*_id).at(1);
}

unsigned int dic_ctrl::OpAvailSpace(spaceop _op,unsigned int _n)
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

void dic_ctrl::InitDic()
{	
	deque<string> l; deque<string>::iterator it;
	io_int.GetTableNameFromDisk(&l,TABLE_PATH);
	if(l.size()!=0)
	{ 
	   for(it=l.begin();it!=l.end();it++)
	   {
	   	  dic_meta_t dic; dic_ctrl::ConstTable(*it,dic);
        dic_ctrl::GbOpLink(l_insert,*it,dic); 		  
	   }
  }
  	
}
   
int dic_ctrl::GetTableFromDisk(string _t, deque<global_dic_t> * _lh)
{
	   //check if there is available memory space
     unsigned int n = dic_ctrl::OpAvailSpace(check,0);
     unsigned int n1;
      
	   //if not, flush table to disk 
     if(n < GD_TH_TUPLE_NUM)	
     {
     	 n = dic_ctrl::RecycleSpace(GD_TH_TUPLE_NUM); //return the total available tuple number
	   }
     if(n < GD_TH_TUPLE_NUM)	
     {
	  	 	 cout << "Not enough space is aquired in dic_ctrl::GetTableFromDisk()!" << endl;
	  	   exit(-1);
	   }	     	   
	   //read table from disk (first assume that there is no need to flush table)
	   n1 = io_int.GetTableFromDisk(_t,_lh);	   
	   //reduce available space
	   n = dic_ctrl::OpAvailSpace(substr,n1);
	   
     if(n==0) return(0);
	   else return(1);
}

void dic_ctrl::SetLoc(deque<global_dic_t>::iterator _it,deque<unsigned> * _q)
{
	  (*_q).clear();
	  (*_q).push_back((*_it).addr[1]);
	  (*_q).push_back((*_it).addr[2]);
} 
   
unsigned int dic_ctrl::RecycleSpace(unsigned int _n)
{
	 unsigned int total = 0;	 bool cpl = false; //already reach the table header
	 deque<dic_meta_t>::iterator it = glb_dic_metadata.end()-1;
	 	
	 //search for the lru accessed table	
	 while(total<_n)
	 {
	 	 unsigned int n = 0; 
	   //find out the lru table (tail)
	   while(glb_dic_metadata.size()!=0)//only search when there are table
	   {
	   	  if((*it).addr.size()!=0){n = (*it).addr.size(); break;}
	   	  if(it == glb_dic_metadata.begin()){break;}
	   	  it--;	
	   }
	   //cannot find any tuples to be released
	   if(n==0){return(0);} 	   
     //evict the lru table index
	   if((*it).dirty == true) //if dirty-> flush 
	   {
      n = io_int.FlushTable2Disk((*it).table_name,&((*it).addr));
      (*it).dirty = false; 	 		   	
	   }
	   //if not dirty table or after store them in disk, remove the table directly	    
	   (*it).addr.clear();
     
     total=total+n;
   }
   //increment the available space
   unsigned int res = dic_ctrl::OpAvailSpace(add,total);
   return(res);	 
} 

ostream & dic_ctrl::Print(ostream &os) const
{
   os << "dic_ctrl info:\n" 
      << "         avail tuple size="<< avail_size <<"\n" 
      << "         used tuple size ="<< used_size  <<endl; 
      
	 deque<dic_meta_t>::const_iterator it;
	 for(it=glb_dic_metadata.begin();it!=glb_dic_metadata.end();it++)
	 {
       os << "      table name: " << (*it).table_name
          << " dirty=" << (*it).dirty 
          << " attr name =" 
          << (*it).att_name.at(0) << ", " 
          << (*it).att_name.at(1) << ", "
          << (*it).att_name.at(2) << endl;
       os << "      entry is: " << endl;   
	     deque<global_dic_t>::const_iterator it1;
	     for(it1=(*it).addr.begin();it1!=(*it).addr.end();it1++)
	     {
           os << "         loc=" << (*it1).loc
              << " id=" << (*it1).addr[0] 
              << " add1=" << (*it1).addr[1]
              << " add2=" << (*it1).addr[2] << endl;
	     }
	 } 
	 os << L1_ctrl << endl << endl;
	 os << L2_ctrl << endl << endl;	 
   return os;
}
