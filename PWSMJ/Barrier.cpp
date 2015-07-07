
#include "stdafx.h"
#include "Barrier.h"  

Barrier::Barrier( short num ) : myMaxThreads(num), myNumThreads(0) 
{ 
	InitializeCriticalSection(&myMutex);
	myBarrier = CreateEvent(NULL, TRUE, TRUE, NULL); // Manual reset event

	ResetEvent(myBarrier);
}

Barrier::~Barrier()
{  
	DeleteCriticalSection(&myMutex);
	CloseHandle(myBarrier);
}


void  Barrier::Wait()
{ 
	EnterCriticalSection(&myMutex); 

	if(++myNumThreads >= myMaxThreads)
	{
		myNumThreads = 0;

		// we have enough, clear waiting threads
		PulseEvent(myBarrier); 
		LeaveCriticalSection(&myMutex);
		return;
	}
	else
	{
		LeaveCriticalSection(&myMutex);
	}
 
	WaitForSingleObject(myBarrier, INFINITE); 
}
 
void  Barrier::Release()
{ 
	EnterCriticalSection(&myMutex); 
	myNumThreads = 0;  
	PulseEvent(myBarrier); 
	LeaveCriticalSection(&myMutex); 
}
