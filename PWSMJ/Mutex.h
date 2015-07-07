
// 
// Name: Mutex.h : implementation file 
// Author: hieunt
// Description: Helpers for sync
//

#pragma once
   

class MUTEX 
{ 
public: 
	MUTEX();
	~MUTEX();
	BOOL Lock();
	BOOL TryLock();
	BOOL UnLock(); 
private: 
	HANDLE mutex; 
}; 
 