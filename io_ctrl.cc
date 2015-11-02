/**************************
   PittHN project
   io_ctrl.cc
   author: Jie Guo (jig26@pitt.edu)
   version: 1.0 
**************************/
#include <sys/types.h>
#include <sys/time.h>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <sstream> 
#include <fstream>
#include <string>
#include <dirent.h>
#include <string.h>
#include <pthread.h>  
#include "io_ctrl.h"

using namespace std;

io_ctrl::io_ctrl(){}
io_ctrl::~io_ctrl(){}

void io_ctrl::GetTableNameFromDisk(deque<string> *_l, char *_c)
{     
    DIR * d; struct dirent *dir; (*_l).clear();
    d = opendir(_c);
    if (d)
    {
      while ((dir = readdir(d)) != NULL)
      {
         if(strcmp(dir->d_name,".")==1 && strcmp(dir->d_name,"..")==1) 
         {  
         	string s1 = dir->d_name ; (*_l).push_back(s1);
         }
      }  closedir(d);  
    }	
} 

string io_ctrl::ConstrFileName(char * _c, string _s) //constructed file name    
{
    string s = _c;  s.push_back('/');
    s.insert(s.size(),_s);
    return(s); 
}  	
unsigned int io_ctrl::GetTableFromDisk(string _tb,deque<global_dic_t> * _pt) //return tuple number    
{
    string s = io_ctrl::ConstrFileName(TABLE_PATH,_tb) ;  
    ifstream fp (s.c_str());
    if (fp.is_open())
    {
        string line; tb_loc loc;
        while ( getline (fp,line) )
        {
          global_dic_t t; unsigned int id,name,phone;        	         
          //use scanf to read integer
          char c[100]; line.copy(c,line.size(),0); c[line.size()]='\0'; 
          sscanf(c,"%d %u %u %u\n",&(t.loc),&id,&name,&phone);
          t.addr[0]=id; t.addr[1]=name;
          t.addr[2]=phone;         
      	  //insert into link list and set loc in L2 delta
          (*_pt).push_back(t);
        }
    }
    fp.close();  return((*_pt).size());
}

unsigned int io_ctrl::FlushTable2Disk(string _tb, deque<global_dic_t> * _t)
{
   ofstream fp; 
   string s = io_ctrl::ConstrFileName(TABLE_PATH,_tb); 
   fp.open (s.c_str());
  
   deque<global_dic_t>::iterator it;
   for(it = (*_t).begin();it!=(*_t).end();it++)	  
	 {  
    string s1,s2;
    s1 = _itoa((*it).loc);     s2.insert(s2.size(),s1); s2.push_back(' '); //insert loc
    s1 = _itoa((*it).addr[0]); s2.insert(s2.size(),s1); s2.push_back(' '); //insert id
    s1 = _itoa((*it).addr[1]); s2.insert(s2.size(),s1); s2.push_back(' '); //insert name
    s1 = _itoa((*it).addr[2]); s2.insert(s2.size(),s1); s2.push_back('\n');//insert phone
    fp << s2;
   }
   fp.close(); return((*_t).size());	  
}

string io_ctrl::_itoa(unsigned int n)
{
 ostringstream ostr; ostr << n;
 return(ostr.str());
}

void io_ctrl::GetColumnNameFromDisk(deque<string> *_tb,deque<string> *_l) 
{
    //open the directory when the column file is store
    DIR * d; struct dirent *dir; (*_l).clear();
    d = opendir(COLUMN_PATH);
    if (d)
    {
      while ((dir = readdir(d)) != NULL)
      {
         if(strcmp(dir->d_name,".")==1 && strcmp(dir->d_name,"..")==1) //exclude . ..
         {  
         	string s1 = dir->d_name ; int pos = s1.find('_');
         	string t = s1.substr(0,pos-1);
         	string c = s1.substr(pos+1,200); //assume the largest size of column file name is 200
         	(*_tb).push_back(t); (*_l).push_back(c); 
         }
      }  closedir(d);  
    }
}

void io_ctrl::FlushColmn2Disk(string _tb,string _att, deque<l2_attr_t> * _col)
{  
	  //construct a file name 
    string tn=_tb; tn.push_back('_');  tn.insert(tn.size(),_att);
    string s = io_ctrl::ConstrFileName(COLUMN_PATH, tn);     	  
	  //write the column to disk
    ofstream fp; fp.open (s.c_str());
    deque<l2_attr_t>::iterator it;
    for(it = (*_col).begin();it!=(*_col).end();it++)	  
	  {  
     string s1,s2; 
     s1 = io_ctrl::_itoa((*it).id); s2.insert(s2.size(),s1); s2.push_back(' ');
     s2.insert(s2.size(),(*it).content); s2.push_back('\n');
     fp << s2;
    }
   fp.close();    
}

void io_ctrl::GetColumnFromDisk(string _tb,string _att, deque<l2_attr_t> * _col)
{  
    string tn=_tb; tn.push_back('_');  tn.insert(tn.size(),_att);
    string s = io_ctrl::ConstrFileName(COLUMN_PATH, tn);     	  

    ifstream fp (s.c_str());
    if (fp.is_open())
    {
        string line;
        while ( getline (fp,line) )
        {
          l2_attr_t t; unsigned int id; char c[100],s[100];        	         
          line.copy(c,line.size(),0); c[line.size()]='\0'; 
          sscanf(c,"%d %s\n",&(t.id),s); t.content = s;         
          (*_col).push_back(t);
        }
    }
    fp.close();       
}

void io_ctrl::CreateTableFile(string _tb)
{
   string s = io_ctrl::ConstrFileName(TABLE_PATH,_tb); 
   ofstream fp; fp.open (s.c_str());
   fp.close(); 	
}

void io_ctrl::CreatColumnFile(string _tb,string _att)
{
	  //construct a file name 
    string tn=_tb; tn.push_back('_');  tn.insert(tn.size(),_att);
    string s = io_ctrl::ConstrFileName(COLUMN_PATH, tn);   	  
    ofstream fp; fp.open (s.c_str());fp.close();
}

void io_ctrl::RemoveTableFile(string _tb)
{
  string s = io_ctrl::ConstrFileName(TABLE_PATH,_tb);
  if( remove(s.c_str()) != 0 ) perror( "Error deleting file" );
  else cout << _tb << " is successfully deleted" << endl;
}

void io_ctrl::RemoveColumnFile(string _tb,string _att)
{
    string tn=_tb; tn.push_back('_');  tn.insert(tn.size(),_att);
    string s = io_ctrl::ConstrFileName(COLUMN_PATH, tn);
	  
  if( remove(s.c_str()) != 0 ) perror( "Error deleting file" );
  else cout << s << " is successfully deleted" << endl;
}

unsigned int io_ctrl::GetRowNameFromDisk(string _tb,deque<tb_info_t> * _pt)   
{
	//Find the table name already exist in dir 
	deque<string> tb;   unsigned int total = 0;
	io_ctrl::GetTableNameFromDisk(&tb,ROW_PATH);
	//find out the file size in for each file
	deque<string>::iterator it; unsigned int size,pn;
	for(it=tb.begin();it!=tb.end();it++) 
  {
	  tb_info_t tb1;  tb1.table_name = (*it);
	  tb1.valid = true; tb1.assign_end = 0;
	  tb1.inmem_page = 0;
 
  	string s = io_ctrl::ConstrFileName(ROW_PATH,(*it));
    FILE *fp = fopen(s.c_str(),"rb");
    size = ftell (fp);fclose(fp);  	
    //culculate the block number
    pn = size/(BYTE_PER_PAGE*PAGE_PER_BLK);
    if(size%(BYTE_PER_PAGE*PAGE_PER_BLK)) pn++;
    
	  for(int i=0;i<pn;i++) 
	  {
       page_info_t p; p.dirty = 0;   
       p.in_mem = false; p.fix_cnt = 0; 
       tb1.page.push_back(p);	  	 
       if(i!=pn-1){tb1.assign_end++;}//when reach the last page, do not increment the page number since the last page may not be full 
	  }
	  (*_pt).push_back(tb1); total += pn;   
  }
  return(total);
}

unsigned int io_ctrl::FlushRow2Disk(string _tb, deque<tuple_t> * _pt, unsigned int _p)
{
	//slotted page file format: <page header: page offset(4B),total size(4B)>
	deque<tuple_t>::iterator it=(*_pt).begin(); unsigned int ps;		
	//calculate page size
  if((*_pt).size()%TUPLES_PER_PAGE == 0) ps = (*_pt).size()/TUPLES_PER_PAGE;
  else ps = (*_pt).size()/TUPLES_PER_PAGE+1;
  //open a file and jump to page offset
  string s = io_ctrl::ConstrFileName(ROW_PATH,_tb); 
  FILE * fp = fopen (s.c_str(), "r+");
  fseek (fp,(BYTE_PER_PAGE*PAGE_PER_BLK*_p),0); 
    			
	for(int i=0;i<ps;i++)
	{
     int * p; int j; int pstart=8; int ptail=BYTE_PER_PAGE-4;//int pend=511; p1: the pointer to tail
     //put the tuple in a page
     for(j=0;j<TUPLES_PER_PAGE;j++)
     {
     	  if(it!=(*_pt).end())
     	  {
     	  	//store a tuple in the file
     	  	p = (int *)(&buf[pstart]); *p = (*it).id; //store 4B id in the first loc
     	  	int s = (*it).client_name.size();
     	  	(*it).client_name.copy(&buf[pstart+4],s,0);
     	  	if(s<18) buf[pstart+4+s] = '\0';          //add eof at the end of string  
     	  	(*it).phone.copy(&buf[pstart+22],10,0);
     	    //increment pt & update page header
     	  	p = (int *)(&buf[ptail]); *p = pstart;    //set the pointer to tuple      	    
     	    pstart+=32; ptail-=4; it++; 
     	  } 
     	  else {break;}
     }
     if(j!=0)//there are tuples in files, store it in file
     {
       //update the page header
       p = (int *)buf; *p = i+_p;
       p+= 1;  *p = j;
       //store in file
       fwrite(buf,sizeof(char),BYTE_PER_PAGE,fp);      	
     }     
	}  
	fclose(fp);	return((*_pt).size());   
}  

void io_ctrl::RemoveRowFile(string _tb)
{
	   //construct a path to the file
     string s = io_ctrl::ConstrFileName(ROW_PATH,_tb);  	   
	   //delete the file
     if( remove(s.c_str()) != 0 ) perror( "Error deleting file" );
     else cout << _tb << " is successfully deleted in io_ctrl::RemoveRowFile()" << endl;	   
}

unsigned int io_ctrl::GetRowFileFromDisk(string _tb, deque<tuple_t> * _q, unsigned int _n)
{
	//slotted page file format: <page header: page offset(4B),total size(4B)>
	//calculate page size
  unsigned int ps = PAGE_PER_BLK;
  //open a file and jump to page offset
  string s = io_ctrl::ConstrFileName(ROW_PATH,_tb);  
  FILE * fp = fopen (s.c_str(), "rb");
  if(!fp)
  {
		cout << "Cannot open a file in l1_delta_ctrl::GetRowFileFromDisk()" << endl;
		exit(-1);	   	 
  }
  fseek (fp,(BYTE_PER_PAGE*PAGE_PER_BLK*_n),0); 
    			
	for(int i=0;i<ps;i++)
	{
     int * p; int pstart=8; int ptail=BYTE_PER_PAGE-4; int * temp; 
     //read a page in buf & get tuple number
     int rs = fread (buf,1,BYTE_PER_PAGE,fp); 
     if(rs==0) break; temp=(int *)(buf+4);
     //insert a tuple to link list
     for(int j=0;j<(*temp);j++)
     {
     	  int * p = (int *)(buf+pstart); tuple_t t;
     	  t.id = *p; t.phone.insert(0,&buf[pstart+22],10);
     	  buf[pstart+22]= '\0'; //to add '\0' in case of 18 byte name length
     	  t.client_name = &(buf[pstart+4]);
     	  //increment pt & update page header
     	  (*_q).push_back(t); pstart+=32; p=(int *)(buf+ptail); ptail-=4;
     }
  } 
	unsigned int r = (*_q).size();
	fclose(fp);	return(r);	  
}

void io_ctrl::CreateRowFile(string _tb)
{
   string s = io_ctrl::ConstrFileName(ROW_PATH,_tb); 
   ofstream fp; fp.open (s.c_str());
   fp.close();	
}                                                                                 
