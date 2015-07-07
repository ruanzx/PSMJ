
#pragma once 
#include <windows.h>

class Barrier
{
	short              myMaxThreads;
	short              myNumThreads; 
	CRITICAL_SECTION   myMutex; 
	HANDLE			   myBarrier; // mannual reset event
public:
	Barrier( short num );       
	~Barrier();                            
	 
	void   Wait();
	void   Release();
} ;
