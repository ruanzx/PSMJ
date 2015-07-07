
// 
// Name: PWSMJ.cpp 
// Author: hieunt
// Description: : Entry point, defines the class behaviors for the PWSMJ application
// 

#include "stdafx.h"
#include "Main.h"
#include "CPWSMJDlg.h"

// To find out the source code which generates memory leaks, we simply have to use DEBUG_NEW MFC macro.
// http://msdn.microsoft.com/en-us/library/tz7sxz99(VS.80).aspx

#ifdef _DEBUG
#define new DEBUG_NEW
#endif

// CPWSMJApp 
BEGIN_MESSAGE_MAP(CPWSMJApp, CWinApp)
	ON_COMMAND(ID_HELP, &CWinApp::OnHelp)
END_MESSAGE_MAP()


// CPWSMJApp construction

CPWSMJApp::CPWSMJApp()
{
	// support Restart Manager
	m_dwRestartManagerSupportFlags = AFX_RESTART_MANAGER_SUPPORT_RESTART;

	// TODO: add construction code here,
	// Place all significant initialization in InitInstance
}

// The one and only CPWSMJApp object 
CPWSMJApp theApp;

// Global variables 
WMI_ProcessorStatus g_P_StatesTable; // CPU P States table get from WMI and BIOS
volatile DWORD g_Sample_Interval; // Refresh interval
volatile DWORD g_Gui_Interval; // Refresh GUI interval
volatile DOUBLE g_ProcessUsage; // Current process cpu utilitzion
volatile DOUBLE g_ProcessPower; // Current process power
volatile DOUBLE g_PackagePower; // Processor package power
volatile DOUBLE g_CapPower; // Process cap power
volatile DWORD g_TimeBase; // Cap interval time

std::vector<ComboBoxItem *> g_ComboBoxBufferItems;
std::vector<ComboBoxItem *> g_ComboBoxThreadNumItems;
UINT g_ThreadNum[] = {0, 1, 2, 4, 8, 16, 32};
Loggers g_Logger(DEFAULT_WORK_SPACE); // log cpu info
CpuUsage usage;

// CPWSMJApp initialization

BOOL CPWSMJApp::InitInstance()
{
	// InitCommonControlsEx() is required on Windows XP if an application
	// manifest specifies use of ComCtl32.dll version 6 or later to enable
	// visual styles.  Otherwise, any window creation will fail.
	INITCOMMONCONTROLSEX InitCtrls;
	InitCtrls.dwSize = sizeof(InitCtrls);

	// Set this to include all the common control classes you want to use
	// in your application.
	InitCtrls.dwICC = ICC_WIN95_CLASSES;
	InitCommonControlsEx(&InitCtrls);

	CWinApp::InitInstance();


	AfxEnableControlContainer();

	// Create the shell manager, in case the dialog contains
	// any shell tree view or shell list view controls.
	CShellManager *pShellManager = new CShellManager;

	// Activate "Windows Native" visual manager for enabling themes in MFC controls
	CMFCVisualManager::SetDefaultManager(RUNTIME_CLASS(CMFCVisualManagerWindows));

	// Standard initialization
	// If you are not using these features and wish to reduce the size
	// of your final executable, you should remove from the following
	// the specific initialization routines you do not need
	// Change the registry key under which our settings are stored
	// TODO: You should modify this string to be something appropriate
	// such as the name of your company or organization

	//SetRegistryKey(_T("Local AppWizard-Generated Applications"));

	////////////////////////////////////////////////////////////////////////////////
	if(!IsCurrentUserLocalAdministrator())
	{
		ShowMB(L"Current User does not belong Administrator group\nSome feature in program will not work properly"); 
	}
	///////////////////////////////////////////////////////////////////////////////////

	//Typically, the COM library is initialized on a thread only once.
	//Because there is no way to control the order in which in-process servers are loaded or unloaded, 
	//do not call CoInitialize, CoInitializeEx, or CoUninitialize from the DllMain function. 


	//////////////////////////////////////////////////////////////////////////
	// Some issues may have with WMI
	// Comment these below line to fix
	// https://msdn.microsoft.com/en-us/library/windows/desktop/ms695279(v=vs.85).aspx
	//////////////////////////////////////////////////////////////////////////

	RC rc;

	//Must making the following guarantees:
	//1. You will access each COM object from a single thread; 
	//   you will not share COM interface pointers between multiple threads.
	//2. The thread will have a message loop
	HRESULT hres = CoInitializeEx(NULL, COINIT_APARTMENTTHREADED | COINIT_DISABLE_OLE1DDE); //COINIT_DISABLE_OLE1DDE: Setting this flag avoids some overhead associated with Object Linking and Embedding (OLE) 1.0, an obsolete technology.
	if (FAILED(hres))
	{ 
		DebugPrint(_T("CoUninitialize(): Failed to initialize COM library")); 
		CoUninitialize();
	}
	else
	{
		WMI myWmi; 

		rc = myWmi.GetPstateTable(g_P_StatesTable);
		if(rc==SUCCESS) 
		{
			WMI_KernelIdleStates  C_StatesTable;
			WMI_ProcessorBiosInfo P_BiosStateTable; 
			//
			rc = myWmi.GetCstateTable(C_StatesTable);

			if(rc==SUCCESS)
			{
				rc = myWmi.GetProcessorBiosInfoTable(P_BiosStateTable); 
			}
			else { DebugPrint(PrintErrorRC(rc)); }

			if(rc==SUCCESS) { 
				UpdateCpuStateFromBiosInfo(P_BiosStateTable); 
			}
			else{ 
				DebugPrint(PrintErrorRC(rc)); 
			}
		}
		else
		{
			DebugPrint(L"Main::InitInstance(): Cannot Initialize WMI");
			DebugPrint(PrintErrorRC(rc));
		}

		//For every successful call to CoInitializeEx, you must call CoUninitialize before the thread exits. 
		CoUninitialize();
	} 

	///////////////////////////////////////////////////////////////////////////////// 
	// Create commbobox buffer items
	ComboBoxItem *item;
	item = new ComboBoxItem (L"4KB" , 4096);g_ComboBoxBufferItems.push_back(item); 
	item = new ComboBoxItem(L"8KB", 8192);g_ComboBoxBufferItems.push_back(item); 
	item = new ComboBoxItem(L"16KB", 16384);g_ComboBoxBufferItems.push_back(item); 
	item = new ComboBoxItem(L"32KB", 32768);g_ComboBoxBufferItems.push_back(item); 
	item = new ComboBoxItem(L"64KB", 65536);g_ComboBoxBufferItems.push_back(item); 
	item = new ComboBoxItem(L"128KB", 131072);g_ComboBoxBufferItems.push_back(item);  
	item = new ComboBoxItem(L"256KB", 262144);g_ComboBoxBufferItems.push_back(item); 
	item = new ComboBoxItem(L"512KB", 524288);g_ComboBoxBufferItems.push_back(item);  
	item = new ComboBoxItem(L"1MB", 1048576);g_ComboBoxBufferItems.push_back(item);  
	item = new ComboBoxItem(L"2MB", 2097152);g_ComboBoxBufferItems.push_back(item); 
	item = new ComboBoxItem(L"4MB", 4194304); g_ComboBoxBufferItems.push_back(item);
	item = new ComboBoxItem(L"8MB", 8388608); g_ComboBoxBufferItems.push_back(item);
	item = new ComboBoxItem(L"16MB", 16777216); g_ComboBoxBufferItems.push_back(item);
	item = new ComboBoxItem(L"32MB", 33554432); g_ComboBoxBufferItems.push_back(item);
	item = new ComboBoxItem(L"64MB", 67108864); g_ComboBoxBufferItems.push_back(item);
	item = new ComboBoxItem(L"128MB", 134217728); g_ComboBoxBufferItems.push_back(item);
	//
	BOOL IsDefaulWorkSpaceExist  = FALSE;
	IsDefaulWorkSpaceExist = DirectoryExist(DEFAULT_WORK_SPACE);
	if(!IsDefaulWorkSpaceExist)
	{
		CString cstr;
		cstr.Format(_T("Create working space: %s"), DEFAULT_WORK_SPACE);
		DebugPrint(cstr); 
		CreateDirectoryW(DEFAULT_WORK_SPACE, NULL);
	}

	//////////////////////////////////////////////////////////////////////////

	// Open main dialog
	CPWSMJDlg dlg;
	m_pMainWnd = &dlg; 

	INT_PTR nResponse = dlg.DoModal();

	if (nResponse == IDOK)
	{
		// Handle when the dialog is dismissed with OK   
	}
	else if (nResponse == IDCANCEL)
	{
		// Handle when the dialog is dismissed with Cancel 
	}
	else if (nResponse == -1)
	{
		TRACE(traceAppMsg, 0, "Warning: dialog creation failed, so application is terminating unexpectedly.\n");
		TRACE(traceAppMsg, 0, "Warning: if you are using MFC controls on the dialog, you cannot #define _AFX_NO_MFC_CONTROLS_IN_DIALOGS.\n");
	}

	// Delete the shell manager created above.
	if (pShellManager != NULL)
	{
		delete pShellManager;
	}

	// Since the dialog has been closed, return FALSE so that we exit the
	//  application, rather than start the application's message pump.
	return FALSE;
}


/// <summary>
/// Updates the cpu state from bios information.
/// </summary>
/// <param name="P_BiosStateTable">The p_ bios state table.</param>
VOID CPWSMJApp::UpdateCpuStateFromBiosInfo(WMI_ProcessorBiosInfo P_BiosStateTable)
{ 
	for(int i=0; i<g_P_StatesTable.PerfStates.Count; i++)
	{
		if(g_P_StatesTable.PerfStates.State[i].Flags==1) // from WMI, only flag=1 are have power values
		{
			for(int j=0; j<P_BiosStateTable.Count; j++)
			{ 
				if(g_P_StatesTable.PerfStates.State[i].Frequency==P_BiosStateTable.State[j].Frequency)
				{
					g_P_StatesTable.PerfStates.State[i].Power = P_BiosStateTable.State[j].Power;
					g_P_StatesTable.PerfStates.State[i].BmLatency = P_BiosStateTable.State[j].BmLatency;
					g_P_StatesTable.PerfStates.State[i].Latency = P_BiosStateTable.State[j].Latency;
					break;
				}
			}
		} 
		else if(g_P_StatesTable.PerfStates.State[i].Flags==2)
		{
			g_P_StatesTable.PerfStates.State[i].Power = 0;
			g_P_StatesTable.PerfStates.State[i].BmLatency = 0;
			g_P_StatesTable.PerfStates.State[i].Latency = 0;
		} 
	}
}


/*------------------------------------------------------------------------- 
IsCurrentUserLocalAdministrator ()

This function checks the token of the calling thread to see if the caller
belongs to the Administrators group.

Return Value:  TRUE if the caller is an administrator on the local machine.
Otherwise, FALSE.
--------------------------------------------------------------------------*/
BOOL CPWSMJApp::IsCurrentUserLocalAdministrator()
{
	BOOL   fReturn         = FALSE;
	DWORD  dwStatus;
	DWORD  dwAccessMask;
	DWORD  dwAccessDesired;
	DWORD  dwACLSize;
	DWORD  dwStructureSize = sizeof(PRIVILEGE_SET);
	PACL   pACL            = NULL;
	PSID   psidAdmin       = NULL;

	HANDLE hToken              = NULL;
	HANDLE hImpersonationToken = NULL;

	PRIVILEGE_SET   ps;
	GENERIC_MAPPING GenericMapping;

	PSECURITY_DESCRIPTOR     psdAdmin           = NULL;
	SID_IDENTIFIER_AUTHORITY SystemSidAuthority = SECURITY_NT_AUTHORITY;


	/*
	Determine if the current thread is running as a user that is a member of
	the local admins group.  To do this, create a security descriptor that
	has a DACL which has an ACE that allows only local aministrators access.
	Then, call AccessCheck with the current thread's token and the security
	descriptor.  It will say whether the user could access an object if it
	had that security descriptor.  Note: you do not need to actually create
	the object.  Just checking access against the security descriptor alone
	will be sufficient.
	*/
	const DWORD ACCESS_READ  = 1;
	const DWORD ACCESS_WRITE = 2;


	__try
	{

		/*
		AccessCheck() requires an impersonation token.  We first get a primary
		token and then create a duplicate impersonation token.  The
		impersonation token is not actually assigned to the thread, but is
		used in the call to AccessCheck.  Thus, this function itself never
		impersonates, but does use the identity of the thread.  If the thread
		was impersonating already, this function uses that impersonation context.
		*/
		if (!OpenThreadToken(GetCurrentThread(), TOKEN_DUPLICATE|TOKEN_QUERY, TRUE, &hToken))
		{
			if (GetLastError() != ERROR_NO_TOKEN)
				__leave;

			if (!OpenProcessToken(GetCurrentProcess(), TOKEN_DUPLICATE|TOKEN_QUERY, &hToken))
				__leave;
		}

		if (!DuplicateToken (hToken, SecurityImpersonation, &hImpersonationToken))
			__leave;

		/*
		Create the binary representation of the well-known SID that
		represents the local administrators group.  Then create the security
		descriptor and DACL with an ACE that allows only local admins access.
		After that, perform the access check.  This will determine whether
		the current user is a local admin.
		*/
		if (!AllocateAndInitializeSid(&SystemSidAuthority, 2,
			SECURITY_BUILTIN_DOMAIN_RID,
			DOMAIN_ALIAS_RID_ADMINS,
			0, 0, 0, 0, 0, 0, &psidAdmin))
			__leave;

		psdAdmin = LocalAlloc(LPTR, SECURITY_DESCRIPTOR_MIN_LENGTH);
		if (psdAdmin == NULL)
			__leave;

		if (!InitializeSecurityDescriptor(psdAdmin, SECURITY_DESCRIPTOR_REVISION))
			__leave;

		// Compute size needed for the ACL.
		dwACLSize = sizeof(ACL) + sizeof(ACCESS_ALLOWED_ACE) +
			GetLengthSid(psidAdmin) - sizeof(DWORD);

		pACL = (PACL)LocalAlloc(LPTR, dwACLSize);
		if (pACL == NULL)
			__leave;

		if (!InitializeAcl(pACL, dwACLSize, ACL_REVISION2))
			__leave;

		dwAccessMask= ACCESS_READ | ACCESS_WRITE;

		if (!AddAccessAllowedAce(pACL, ACL_REVISION2, dwAccessMask, psidAdmin))
			__leave;

		if (!SetSecurityDescriptorDacl(psdAdmin, TRUE, pACL, FALSE))
			__leave;

		/*
		AccessCheck validates a security descriptor somewhat; set the group
		and owner so that enough of the security descriptor is filled out to
		make AccessCheck happy.
		*/
		SetSecurityDescriptorGroup(psdAdmin, psidAdmin, FALSE);
		SetSecurityDescriptorOwner(psdAdmin, psidAdmin, FALSE);

		if (!IsValidSecurityDescriptor(psdAdmin))
			__leave;

		dwAccessDesired = ACCESS_READ;

		/*
		Initialize GenericMapping structure even though you
		do not use generic rights.
		*/
		GenericMapping.GenericRead    = ACCESS_READ;
		GenericMapping.GenericWrite   = ACCESS_WRITE;
		GenericMapping.GenericExecute = 0;
		GenericMapping.GenericAll     = ACCESS_READ | ACCESS_WRITE;

		if (!AccessCheck(psdAdmin, hImpersonationToken, dwAccessDesired,
			&GenericMapping, &ps, &dwStructureSize, &dwStatus,
			&fReturn))
		{
			fReturn = FALSE;
			__leave;
		}
	}
	__finally
	{
		// Clean up.
		if (pACL) LocalFree(pACL);
		if (psdAdmin) LocalFree(psdAdmin);
		if (psidAdmin) FreeSid(psidAdmin);
		if (hImpersonationToken) CloseHandle (hImpersonationToken);
		if (hToken) CloseHandle (hToken);
	}

	return fReturn;
}