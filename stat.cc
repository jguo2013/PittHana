/**************************
   PittHN project
   stat.cc
   author:  Jie Guo (jig26@pitt.edu) 
   version: 1.0  
**************************/
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
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
#include "stat.h"
#include "global.h"

using namespace std;

stat::stat(){}
stat::~stat(){}
	
stat::stat(const char * c)
{ 
	file_name.insert(0,c);
}

void stat::OpenStatFile()
{
	stat_pt.open(file_name.c_str());
} 

void stat::WriteStatFile(char * s)
{
	stat_pt << s;
} 

void stat::CloseStatFile(char * s)
{
	stat_pt.close();
}