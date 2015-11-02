/**************************
   PittHN project
   main.cc
   author: Jie Guo (jig26@pitt.edu)
   argument: --> <T, table, op, tuple>
   version: 1.0 /04/03/2014 
**************************/
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/types.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <stdlib.h>
#include <iostream>
#include <string>
#include <sstream>
#include "global.h"
#include "sch_int.h"

#define debug_input 1
#define inpipe  "/tmp/TO_DM"
#define outpipe "/tmp/TO_SCHEDULER"

using namespace std;
ofstream fp1; int ifd,ofd;

string _itoa(unsigned int n)
{
 ostringstream ostr; ostr << n;
 return(ostr.str());
}

int ReadOp(string &_s)
{
   char buf[64]; 
   while(1)
   {
    int n = read(ifd,buf,64);  _s = buf; 
    if(buf[0]<='9' && buf[0]>='0')
    {  cout << "ReadOp: buf is " << _s << endl; return(n);}
   }
} 

void 	SendRes2Sch(string _s)
{
   write(ofd,_s.c_str(),_s.size());fsync(ofd);
}
             
int main(int argc, char * argv[]) { //argv: file name, mode  
                                                     
    sch_int dm; deque<tuple_t> q; string line; int incpl = 1;string ret_str;
    /** for debug purpose: record the operation in file**/
    fp1.open ("log4debug.txt");       
    //set the mode
    dm.InitOp();   
    if(strcmp(argv[2],"column_store")==0) dm.SetMode(0);
    else if(strcmp(argv[2],"row_store")==0) dm.SetMode(1);

    unlink(inpipe); unlink(outpipe);mkfifo(inpipe, 0666); mkfifo(outpipe, 0666);
    ifd = open(inpipe,O_RDONLY|O_NONBLOCK);  if (ifd == -1) {perror("open in");exit(-1);}
    ofd = open(outpipe, O_RDWR|O_NONBLOCK);if (ofd == -1) {perror("open out");exit(-1);}                  	
	  #ifdef debug_input 
    //open the script file
    ifstream fp (argv[1]); 
    if (fp.is_open())
    {
        while ( getline (fp,line) )
        {
    #else
        while (incpl)
        {
          ReadOp(line);
    #endif    
    
          int i = 0; string s[6]; int res;//parse file   
          while(line.size() && i!=6)
          {      
            int size = line.find(" ",0);
            s[i] = line.substr(0,size); line.erase(0,size+1);
            i++; 
          } 
          unsigned int t_no = atoi(s[0].c_str()); 
          dm.SetTranNo(t_no); //set trans no 
          if(s[1].compare("close")==0)
          {incpl=0;}          
          if(s[1].compare("write")==0) dm.SetOp(write_t);
          if(s[1].compare("r_query")==0) dm.SetOp(r_query);
          if(s[1].compare("a_query")==0) dm.SetOp(a_query);
          if(s[1].compare("m_query")==0 ||
          	  s[1].compare("g_query")==0) dm.SetOp(m_query);
          if(s[1].compare("del")==0)dm.SetOp(del);
          dm.SetTableName(s[2]); tuple_t t;
          if(s[1].compare("write")==0)
          {  t.id =atoi(s[3].c_str()); t.client_name =s[4]; t.phone = s[5]; }
          if(s[1].compare("r_query")==0){t.id = atoi(s[3].c_str());}
          if(s[1].compare("m_query")==0 ||
          	  s[1].compare("g_query")==0){t.phone = s[3];}
          dm.SetTuple(t); q.clear();           
          
          if(s[1].compare("write")==0) {res=dm.WriteOp();} 	          
          else if(s[1].compare("r_query")==0 || s[1].compare("m_query")==0 ||
          	       s[1].compare("g_query")==0 || s[1].compare("a_query")==0)
          {res=dm.QueryOp(&q);} 
          if(s[1].compare("del")==0 ) {res=dm.DeleteOp();}
          if(s[1].compare("commit")==0 ) {res=dm.CommitOp();}	
          if(s[1].compare("abort")==0 ) {res=dm.AbortOp();}

          ret_str = _itoa(res); 
          if(s[1].compare("r_query")==0 || s[1].compare("m_query")==0 || 
          	 s[1].compare("g_query")==0 || s[1].compare("a_query")==0)
          {
             for(deque<tuple_t>::iterator it=q.begin();it!=q.end();it++)
             {
             	  ret_str.push_back(' ');
             	  ret_str.insert(ret_str.size(),_itoa((*it).id));ret_str.push_back(' ');
                ret_str.insert(ret_str.size(),(*it).client_name);ret_str.push_back(' ');
                ret_str.insert(ret_str.size(),(*it).phone);                     
             }                       	 
          }
          #ifndef debug_input
          SendRes2Sch(ret_str); 
          #endif            	
        }
	  #ifdef debug_input
    }
    fp.close();
    #endif    
    /** for debug purpose: record the operation in file **/    
    fp1.close();  unlink(inpipe); unlink(outpipe); 
}
