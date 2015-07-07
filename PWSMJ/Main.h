
// 
// Name: Main.h 
// Author: hieunt
// Description: main header file for the PWSMJ application
//

#pragma once

#ifndef __AFXWIN_H__
#error "include 'stdafx.h' before including this file for PCH"
#endif

#include "resource.h"		// main symbols
#include "WMI.h"
#include "Loggers.h"
#include "CpuUsage.h"

// CPWSMJApp:
// See PWSMJ.cpp for the implementation of this class
// 

class CPWSMJApp : public CWinApp
{
public:
	CPWSMJApp();

	// Overrides
public:
	virtual BOOL InitInstance();
	 
	// Implementation


	// Get Cpu Performance states and Idle states, call from here because CoInitializeEx problems
	VOID UpdateCpuStateFromBiosInfo(WMI_ProcessorBiosInfo P_BiosStateTable);
	
	// This function checks the token of the calling thread to see if the caller
	// belongs to the Administrators group.
	BOOL IsCurrentUserLocalAdministrator();

	DECLARE_MESSAGE_MAP()
};

extern CPWSMJApp theApp;