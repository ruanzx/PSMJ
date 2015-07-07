// 
// Name: WmiService.h : implementation file 
// Author: hieunt
// Description: Helpers for working with Windows Management Instrumentation (WMI)
//				Start the WMI service automaticlly
//
// Helpers for WMI 
// http://www.dinop.com/vc/service_ctrl.html (ja)

#include "stdafx.h"
#include "WmiService.h"

/// <summary>
/// Initializes a new instance of the <see cref="WmiServiceThread"/> class.
/// </summary>
WmiServiceThread::WmiServiceThread()
{
	_bCancel = FALSE;
}

/// <summary>
/// Determines whether the specified b save is cancel.
/// </summary>
/// <param name="bSave">The b save.</param>
/// <param name="bNewValue">Is new value?</param>
/// <returns></returns>
BOOL WmiServiceThread::IsCancel(BOOL bSave, BOOL bNewValue)
{
	BOOL	ret;

	_secbCancel.Lock();
	if(bSave)
	{
		_bCancel = bNewValue;
		ret = TRUE;
	}
	else
		ret = _bCancel;
	_secbCancel.Unlock();

	return	ret;
}

/// <summary>
/// Easies the start stop.
/// </summary>
/// <param name="pszName">Name of the PSZ.</param>
/// <param name="b">enable/disable.</param>
/// <returns></returns>
BOOL WmiServiceThread::EasyStartStop(LPCTSTR pszName, BOOL b)
{
	BOOL			ret = FALSE;
	BOOL			bRet = FALSE;
	SC_HANDLE		hManager = NULL;
	SC_HANDLE		hService = NULL;
	SERVICE_STATUS	sStatus;

	hManager = OpenSCManager(NULL,NULL,GENERIC_EXECUTE);
	if(hManager == NULL)
	{
		DebugPrint(_T("WmiServiceThread::EasyStartStop(): OpenSCManager Fail"));
		return FALSE;
	}

	hService = OpenService(hManager, pszName, SERVICE_START | SERVICE_QUERY_STATUS);
	if(hService == NULL)
	{
		if(hManager){CloseServiceHandle(hManager);}
		DebugPrint(_T("WmiServiceThread::EasyStartStop(): OpenService Fail"));
		return FALSE;
	}

	ZeroMemory(&sStatus,sizeof(SERVICE_STATUS));
	bRet = QueryServiceStatus(hService,&sStatus);
	if(bRet == FALSE)
	{
		if(hService){CloseServiceHandle(hService);}
		if(hManager){CloseServiceHandle(hManager);}
		DebugPrint(_T("WmiServiceThread::EasyStartStop(): QueryServiceStatus Fail"));
		return FALSE;
	}

	if(sStatus.dwCurrentState == SERVICE_RUNNING)
	{
		if(hService){CloseServiceHandle(hService);}
		if(hManager){::CloseServiceHandle(hManager);}
		DebugPrint(_T("WmiServiceThread::EasyStartStop(): sStatus.dwCurrentState=SERVICE_RUNNING"));
		return TRUE;
	}

	CString cstr;
	cstr.Format(_T("sStatus.dwCurrentState:%08X"), sStatus.dwCurrentState);
	DebugPrint(cstr);

	DebugPrint(_T("StartService - 1"));
	bRet = ::StartService(hService, NULL, NULL);

	DebugPrint(_T("QueryServiceStatus - 1"));
	int count = 0;
	while(::QueryServiceStatus(hService, &sStatus))
	{ 
		if(count >= 4)
		{
			break;
		}

		if(sStatus.dwCurrentState == SERVICE_RUNNING)
		{
			DebugPrint(_T("StartService Completed : SERVICE_RUNNING"));
			if(hService){::CloseServiceHandle(hService);}
			if(hManager){::CloseServiceHandle(hManager);}
			return TRUE;
		}

		::Sleep(100 * count);
		DebugPrint(_T("Sleep"));
		count++;
	}

	// http://msdn.microsoft.com/en-us/library/windows/desktop/bb762153(v=vs.85).aspx
	//DESCRIPTION:
	//  SC is a command line program used for communicating with the
	//  Service Control Manager and services.
	//USAGE:
	//   sc <server> [command] [service name] <option1> <option2>...

	DebugPrint(_T("sc config Winmgmt start=auto"));

	ShellExecute(NULL, NULL, _T("sc"), _T("config Winmgmt start=auto"), NULL, SW_HIDE);
	count = 0;
	DebugPrint(_T("QueryServiceStatus - 2"));

	while(::QueryServiceStatus(hService, &sStatus))
	{ 
		DebugPrint(_T("StartService - 2"));
		::StartService(hService, NULL, NULL);


		// aptemt 10 times
		if(count >= 10)
		{
			break;
		}

		if(sStatus.dwCurrentState == SERVICE_RUNNING)
		{
			DebugPrint(_T("StartService Completed : SERVICE_RUNNING"));
			if(hService){::CloseServiceHandle(hService);}
			if(hManager){::CloseServiceHandle(hManager);}
			return TRUE;
		}

		::Sleep(500);
		DebugPrint(_T("Sleep"));
		count++;
	}

	if(hService){::CloseServiceHandle(hService);}
	if(hManager){::CloseServiceHandle(hManager);}
	return FALSE;
}



BOOL WmiService::EasyStartStop(LPCTSTR pszName, BOOL bStart)
{
	WmiServiceThread cThread; 
	return	cThread.EasyStartStop(pszName, bStart);
}

BOOL WmiService::EasyStart(LPCTSTR pszName)
{
	return	EasyStartStop(pszName, TRUE);
}

BOOL WmiService::EasyStop(LPCTSTR pszName)
{
	return	EasyStartStop(pszName, FALSE);
}

BOOL WmiService::EasyRestart(LPCTSTR pszName)
{
	BOOL ret;
	WmiServiceThread	cThread;

	ret = cThread.EasyStartStop(pszName, FALSE);
	if(ret)
		ret = cThread.EasyStartStop(pszName, TRUE);

	return	ret;
}

/// <summary>
/// Determines whether is service running.
/// </summary>
/// <param name="pszName">Name of the PSZ.</param>
/// <returns></returns>
BOOL WmiService::IsServiceRunning(LPCTSTR pszName)
{
	BOOL			ret;
	BOOL			bRet;
	SC_HANDLE		hManager;
	SC_HANDLE		hService;
	SERVICE_STATUS	sStatus;

	ret = FALSE;
	hManager = NULL;
	hService = NULL;
	while(1)			
	{
		hManager = OpenSCManager(NULL,NULL,GENERIC_EXECUTE);
		//ATLASSERT(hManager);
		if(hManager == NULL)
			break;

		hService = OpenService(hManager,pszName,SERVICE_QUERY_STATUS);
		//ATLASSERT(hService);
		if(hService == NULL)
			break;

		::ZeroMemory(&sStatus,sizeof(SERVICE_STATUS));
		bRet = QueryServiceStatus(hService,&sStatus);
		//ATLASSERT(bRet);
		if(bRet == FALSE)
			break;

		if(sStatus.dwCurrentState == SERVICE_RUNNING)
			ret = true;

		break;	 
	}

	if(hService)
		CloseServiceHandle(hService);
	if(hManager)
		CloseServiceHandle(hManager);

	return	ret;
}