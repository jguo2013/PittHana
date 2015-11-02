/**************************
   PittHN project
   stat.h
   author:  Jie Guo (jig26@pitt.edu)
   version: 1.0 
**************************/
#ifndef _stat
#define _stat

#include <stdio.h>
#include <unistd.h>
#include <iostream>
#include <fstream>
#include "global.h"

class stat {
	
protected:
   ofstream stat_pt;
   string   file_name; 
           
public:
	      
   stat();
   stat(const char * c);   
   ~stat();     
                                                                        
 	 void WriteStatFile(char * s);                                
   void OpenStatFile(); 
   void CloseStatFile();   
};

#endif
