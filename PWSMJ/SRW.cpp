
// 
// Name: SRW.h : implementation file 
// Author: hieunt
// Description: Helpers for sync
//


#include "stdafx.h"
#include "SRW.h"
 
SRW::SRW(void)
{ 
	InitializeSRWLock(slimRWL);
}
 
SRW::~SRW(void)
{

}
 
VOID SRW::LockRead(void)
{
	AcquireSRWLockShared(slimRWL); 
}
 
VOID SRW::UnLockRead(void)
{
	ReleaseSRWLockShared(slimRWL); 
}
 
VOID SRW::LockWrite(void)
{
	AcquireSRWLockExclusive(slimRWL); 
}
  
VOID SRW::UnLockWrite(void)
{
	ReleaseSRWLockExclusive(slimRWL); 
}
