#include "stdafx.h"
#include "WaitInfo.h"

// User Windows Message
//#define UWM_THREAD_TERMINATED  WM_APP + 1U 
UINT WaitInfo::UWM_THREAD_TERMINATED = ::RegisterWindowMessage(UWM_THREAD_TERMINATED_MSG);

WaitInfo::WaitInfo() 
{
	m_Handle = NULL; 
	m_Notify = NULL; 
	m_hWait = NULL; 
}

WaitInfo::~WaitInfo()
{
	CloseHandle(m_hWait);
}  

// Spawns a waiter thread 
VOID WaitInfo::RequestNotification(HANDLE handle, CWnd *tell, OPERATIONS vOperation)
{  
	m_Handle = handle; 
	m_Notify = tell; 
	m_Operation = vOperation;
	m_hWait = CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)WaiterEx, this, NULL, NULL);  
}


DWORD WINAPI WaitInfo::WaiterEx(LPVOID p) 
{
	((WaitInfo *)p)->Waiter();
	return 0;
}

// Waits for the thread to complete and notifies the parent 
DWORD WINAPI WaitInfo::Waiter()
{ 
	WaitForSingleObject(m_Handle, INFINITE); 

	// It is the responsibility of the parent window to perform a
	// CloseHandle operation on the handle. Otherwise there will be
	// a handle leak. 
	m_Notify->PostMessage(UWM_THREAD_TERMINATED, (WPARAM)m_Operation, (LPARAM)m_Handle); 
	return 0;
}  
