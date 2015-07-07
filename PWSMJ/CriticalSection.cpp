// 
// Name: CriticalSection.cpp : implementation file 
// Author: hieunt
// Description: Helpers for sync
//

#include "stdafx.h"
#include "CriticalSection.h"

CriticalSection::CriticalSection()
{ 
	InitializeCriticalSection(&myCriticalSection);
}

CriticalSection::~CriticalSection()
{  
	DeleteCriticalSection(&myCriticalSection);
}

BOOL CriticalSection::TryLock()
{
	return TryEnterCriticalSection(&myCriticalSection);
}

VOID CriticalSection::Lock()
{  
	EnterCriticalSection(&myCriticalSection); 
}

VOID CriticalSection::UnLock()
{ 
	LeaveCriticalSection(&myCriticalSection);
}