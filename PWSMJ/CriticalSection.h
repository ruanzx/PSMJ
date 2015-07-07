// 
// Name: CriticalSection.h : header file 
// Author: hieunt
// Description: Helpers for sync
//

#pragma once 
 
class CriticalSection 
{ 
public: 
	CriticalSection();
	~CriticalSection();
	VOID Lock(); 
	BOOL TryLock(); 
	VOID UnLock(); 
private: 
	CRITICAL_SECTION    myCriticalSection;
}; 
 