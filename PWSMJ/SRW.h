
// 
// Name: SRW.h : implementation file 
// Author: hieunt
// Description: Helpers for sync
//


#pragma once
class SRW
{
	PSRWLOCK slimRWL;
public:
	SRW(void);
	~SRW(void);
	VOID LockRead(void);
	VOID LockWrite(void);
	VOID UnLockRead(void);
	VOID UnLockWrite(void);
};

