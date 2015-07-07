
// 
// Name: Mutex.cpp : implementation file 
// Author: hieunt
// Description: Helpers for sync
//

#include "stdafx.h"
#include "Mutex.h"

MUTEX::MUTEX()
{ 
	mutex = CreateMutex(NULL, FALSE, NULL); 
}

MUTEX::~MUTEX()
{  
	CloseHandle(mutex);
}

BOOL MUTEX::Lock()
{  
	if(WaitForSingleObject(mutex, INFINITE)==WAIT_OBJECT_0)
		return TRUE;
	return FALSE; 
}

BOOL MUTEX::TryLock()
{ 
	if( WaitForSingleObject(mutex, 0)==WAIT_OBJECT_0 )
		return TRUE;
	return FALSE; 
}

BOOL MUTEX::UnLock()
{ 
	if( ReleaseMutex(mutex) )
		return TRUE;
	return FALSE; 
}