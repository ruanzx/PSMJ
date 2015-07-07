// 
// Name: WmiService.h : implementation file 
// Author: hieunt
// Description: Helpers for working with Windows Management Instrumentation (WMI)
//				Start the WMI service automaticlly
//
// Helpers for WMI 
// http://www.dinop.com/vc/service_ctrl.html (ja)

#pragma once 

#include <winsvc.h>
#include "atlstr.h"



class WmiServiceThread
{
public:
	WmiServiceThread(); 
private: 
	BOOL					_bCancel;			 
	CComAutoCriticalSection	_secbCancel;		 
public:
	BOOL IsCancel(BOOL bSave=FALSE, BOOL bNewValue=FALSE);
	BOOL EasyStartStop(LPCTSTR pszName, BOOL b); 
};

class WmiService
{ 
public:
	BOOL EasyStartStop(LPCTSTR pszName, BOOL bStart);
	BOOL EasyStart(LPCTSTR pszName);
	BOOL EasyStop(LPCTSTR pszName);
	BOOL EasyRestart(LPCTSTR pszName);
	BOOL IsServiceRunning(LPCTSTR pszName); 
};
