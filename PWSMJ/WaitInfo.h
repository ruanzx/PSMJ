#pragma once
#include "DataTypes.h"
//////////////////////////////////////////////////////////////////////////
// Notifies the parent window that the thread has been terminated      //
////////////////////////////////////////////////////////////////////////// 
#define UWM_THREAD_TERMINATED_MSG _T("UWM_THREAD_TERMINATED-{F7113F80-6D03-11d3-9FDD-006067718D04}")

class WaitInfo 
{
public:
	WaitInfo();
	~WaitInfo();

	VOID RequestNotification(HANDLE hWaitThread, CWnd *tell, OPERATIONS vOperation);  
	static UINT UWM_THREAD_TERMINATED;
protected:
	HANDLE m_Handle; // thread to wait 
	HANDLE m_hWait;
	CWnd * m_Notify; // window to notify  
	OPERATIONS m_Operation;
	static DWORD WINAPI WaiterEx(LPVOID p); 
	DWORD WINAPI Waiter();
};
