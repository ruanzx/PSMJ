// 
// Name: MsrDriver.cpp : implementation file  
// Author: hieunt
// Description: Interface with hardware to archive power infos
//

#include "stdafx.h"
#include "MsrDriver.h"

/// <summary>
/// Initializes a new instance of the <see cref="MsrDriver"/> class.
/// </summary>
MsrDriver::MsrDriver()
{
	hDriver = INVALID_HANDLE_VALUE;
	szDriverPath = new TCHAR[MAX_PATH];
	driverName = "\\\\.\\WinRing0_1_2_0";
	serviceName = "WinRing0_1_2_0";
	serviceDisplayName = "WinRing0_1_2_0";
	b64BitOS = Is64bitOS(); // for chosing which MSR driver for load
	bDriverInitialized = FALSE;
}

/// <summary>
/// Finalizes an instance of the <see cref="MsrDriver"/> class.
/// </summary>
MsrDriver::~MsrDriver()
{
	if(hDriver!=INVALID_HANDLE_VALUE) 
		Close(); 

	delete szDriverPath;
} 

/// <summary>
/// Closes driver.
/// </summary>
VOID MsrDriver::Close()
{ 
	//if (hDriver != INVALID_HANDLE_VALUE) 
	//	CloseHandle(hDriver); 

	if(!RemoveDriver())
	{ 
		DebugPrint(L"MsrDriver::Close(): RemoveDriver() fail");
	}

	bDriverInitialized = FALSE; 
}

/// <summary>
/// Opens driver.
/// </summary>
/// <returns></returns>
BOOL MsrDriver::Open()
{ 
	if(hDriver!=INVALID_HANDLE_VALUE)
		return FALSE;

	if(!bDriverInitialized)
	{
		if(!Initialize())
		{ 
			DebugPrint(L"MsrDriver::Open(): Cannot Initialize driver");
		} 
	}

	return TRUE;
}


/// <summary>
/// Determines whether this driver is open.
/// </summary>
/// <returns></returns>
BOOL MsrDriver::IsOpen()
{
	if(hDriver!=INVALID_HANDLE_VALUE)
		return TRUE;
	return FALSE;
}

/// <summary>
/// Initializes driver.
/// </summary>
/// <returns></returns>
BOOL MsrDriver::Initialize() 
{  
	BOOL bResult;

	// If the driver is not running, install it 
	if (bDriverInitialized == FALSE)
	{		
		//PrintErrorMessage(L"Initialize() >> GetDriverPath()", GetLastError());
		GetDriverPath();

		//PrintErrorMessage(L"Initialize() >> GetDriverPath() >> OK", GetLastError());
		bResult = InstallDriver(szDriverPath, true); 
		if (!bResult)
		{ 
			DebugPrint(L"MsrDriver::Initialize(): Cannot install driver");
			return FALSE;
		}

		bResult = StartDriver(); 
		if (!bResult)
		{ 
			DebugPrint(L"MsrDriver::Initialize(): Cannot start driver");  
			return FALSE;
		}

		hDriver = CreateFile(driverName,
			GENERIC_READ | GENERIC_WRITE,
			FILE_SHARE_READ | FILE_SHARE_WRITE,
			NULL,
			OPEN_EXISTING,
			FILE_ATTRIBUTE_NORMAL,
			NULL);

		if (hDriver == INVALID_HANDLE_VALUE)
		{
			DebugPrint(L"MsrDriver::Initialize(): hDriver=INVALID_HANDLE_VALUE");  
			return FALSE;
		}
	}


	bDriverInitialized = TRUE;

	return TRUE; 
}

/// <summary>
/// Installs the driver.
/// </summary>
/// <param name="pszDriverPath">The PSZ driver path.</param>
/// <param name="IsDemandLoaded">Is demand loaded.</param>
/// <returns></returns>
BOOL _stdcall MsrDriver::InstallDriver(PWSTR pszDriverPath, BOOL IsDemandLoaded)
{
	//printf("Install driver 1\n");

	SC_HANDLE hSCManager;
	SC_HANDLE hService;

	// Remove any previous instance of the driver 
	//printf("Remove any driver exist\n");
	if(!RemoveDriver())
	{ 
		DebugPrint(L"MsrDriver::InstallDriver(): Remove Fail"); 
	}

	hSCManager = OpenSCManager(NULL, NULL, SC_MANAGER_ALL_ACCESS);

	if (hSCManager)
	{
		// Install the driver

		hService = CreateService(hSCManager,
			serviceName,
			serviceDisplayName,
			SERVICE_ALL_ACCESS,
			SERVICE_KERNEL_DRIVER,
			(IsDemandLoaded == TRUE) ? SERVICE_DEMAND_START : SERVICE_SYSTEM_START,
			SERVICE_ERROR_NORMAL,
			pszDriverPath,
			NULL,
			NULL,
			NULL,
			NULL,
			NULL);

		//PrintErrorMessage(L"InstallDriver() >> CreateService() ", GetLastError());
		CloseServiceHandle(hSCManager);

		if (hService == NULL)
		{  
			DebugPrint(L"MsrDriver::InstallDriver(): hService == NULL");
			return FALSE;
		}
	}
	else
		return FALSE;

	CloseServiceHandle(hService);

	return TRUE;
}


/// <summary>
/// Removes the driver.
/// </summary>
/// <returns></returns>
BOOL _stdcall MsrDriver::RemoveDriver()
{
	SC_HANDLE hSCManager;
	SC_HANDLE hService;
	LPQUERY_SERVICE_CONFIG pServiceConfig;
	DWORD dwBytesNeeded;
	DWORD cbBufSize;
	BOOL bResult;

	/* Stop current running driver */

	if(!StopDriver())
	{
		DebugPrint(L"MsrDriver::RemoveDriver(): StopDriver() fail"); 
	}

	hSCManager = OpenSCManager(NULL, NULL, SC_MANAGER_ALL_ACCESS);

	if (!hSCManager) 
		return FALSE; 

	hService = OpenService(hSCManager, serviceName, SERVICE_ALL_ACCESS);
	errorCode = GetLastError();
	if(errorCode!=ERROR_SUCCESS)
	{
		//PrintErrorMessage(L"RemoveDriver() >> OpenService() ", errorCode);
	}
	if(errorCode == ERROR_SERVICE_DOES_NOT_EXIST)
	{
		/* This service has been deleted*/
		//PrintErrorMessage(L"RemoveDriver() >> OpenService() -> Has been delete ",errorCode);
		CloseServiceHandle(hService);
		return TRUE;
	}

	CloseServiceHandle(hSCManager);

	if (!hService) 
		return FALSE; 

	bResult = QueryServiceConfig(hService, NULL, 0, &dwBytesNeeded);
	errorCode = GetLastError();
	if(errorCode!=ERROR_SUCCESS)
	{
		//PrintErrorMessage(L"RemoveDriver() >> QueryServiceConfig() ",errorCode);
	}
	if (errorCode == ERROR_INSUFFICIENT_BUFFER)
	{
		cbBufSize = dwBytesNeeded;
		pServiceConfig = (LPQUERY_SERVICE_CONFIG)malloc(cbBufSize);
		bResult = QueryServiceConfig(hService, pServiceConfig, cbBufSize, &dwBytesNeeded);

		if (!bResult)
		{
			free(pServiceConfig);
			CloseServiceHandle(hService);
			return bResult;
		}

		// If service is set to load automatically, don't delete it!
		if (pServiceConfig->dwStartType == SERVICE_DEMAND_START) 
			bResult = DeleteService(hService); 
	}

	CloseServiceHandle(hService);

	return bResult;
}

/// <summary>
/// Starts the driver.
/// </summary>
/// <returns></returns>
BOOL _stdcall MsrDriver::StartDriver()
{
	SC_HANDLE hSCManager;
	SC_HANDLE hService;
	BOOL bResult;

	hSCManager = OpenSCManager(NULL, NULL, SC_MANAGER_ALL_ACCESS);

	if (hSCManager)
	{
		hService = OpenService(hSCManager, serviceName, SERVICE_ALL_ACCESS);

		//PrintErrorMessage(L"StartDriver() >> OpenService()", GetLastError());  

		CloseServiceHandle(hSCManager);

		if (hService)
		{
			bResult = StartService(hService, 0, NULL);

			DWORD error = GetLastError();

			//PrintErrorMessage(L"StartDriver() >>  StartService()", error); 

			switch (error)
			{
			case ERROR_DRIVER_BLOCKED:
				DebugPrint(L"MsrDriver::StartDriver(): ERROR_DRIVER_BLOCKED: This driver has been blocked from loading"); 
				break;
			case ERROR_SERVICE_ALREADY_RUNNING:
				DebugPrint(L"MsrDriver::StartDriver(): ERROR_SERVICE_ALREADY_RUNNING");  
				bResult = TRUE;
				break;
			default:
				break;
			}

			if(!bResult)
			{  
				DebugPrint(L"MsrDriver::StartDriver(): Cannot start driver"); 
				return bResult;
			}

			//PrintErrorMessage(L"StartDriver() started Successfully", GetLastError()); 
			CloseServiceHandle(hService);
		}
		else
			return FALSE;
	}
	else
		return FALSE;

	return bResult;
}

/// <summary>
/// Stops the driver.
/// </summary>
/// <returns></returns>
BOOL _stdcall MsrDriver::StopDriver()
{
	SC_HANDLE hSCManager;
	SC_HANDLE hService;
	SERVICE_STATUS ServiceStatus;
	BOOL bResult;

	//printf("StopDriver() come in\n");

	hSCManager = OpenSCManager(NULL, NULL, SC_MANAGER_ALL_ACCESS);

	if (hSCManager)
	{
		hService = OpenService(hSCManager, serviceName, SERVICE_ALL_ACCESS);

		//PrintErrorMessage(L"StopDriver() >> OpenService()", GetLastError());

		CloseServiceHandle(hSCManager);

		if (hService)
		{
			bResult = ControlService(hService, SERVICE_CONTROL_STOP, &ServiceStatus);
			errorCode = GetLastError();

			if(errorCode==ERROR_SERVICE_NOT_ACTIVE)
			{
				/* The service has not been started */
				bResult = TRUE;
				//PrintErrorMessage(L"StopDriver() >> ControlService()", errorCode);
			} 
			CloseServiceHandle(hService);
		}
		else
			return FALSE; 
		//PrintErrorMessage(L"Driver Stopped", GetLastError()); 
	}
	else
		return FALSE;

	return bResult;
}


/// <summary>
/// Is 64bits operation system.
/// </summary>
/// <returns></returns>
BOOL MsrDriver::Is64bitOS()
{ 
	typedef void (WINAPI *PGNSI)(LPSYSTEM_INFO);
	typedef BOOL (WINAPI *PGPI)(DWORD, DWORD, DWORD, DWORD, PDWORD);

	OSVERSIONINFOEX osInfo;
	SYSTEM_INFO si;
	BOOL bOsVersionInfoEx;

	ZeroMemory(&si, sizeof(SYSTEM_INFO));
	ZeroMemory(&osInfo, sizeof(OSVERSIONINFOEX));
	osInfo.dwOSVersionInfoSize = sizeof(OSVERSIONINFOEX);

	if( !(bOsVersionInfoEx = GetVersionEx((OSVERSIONINFO *) &osInfo)))
		return FALSE;

	//PGPI  pGPI;
	PGNSI pGNSI;
	pGNSI = (PGNSI) GetProcAddress(GetModuleHandle(TEXT("kernel32.dll")), "GetNativeSystemInfo");

	if(NULL!=pGNSI)
		pGNSI(&si);
	else
		GetSystemInfo(&si);

	if(si.wProcessorArchitecture==PROCESSOR_ARCHITECTURE_IA64   
		|| si.wProcessorArchitecture==PROCESSOR_ARCHITECTURE_AMD64
		|| si.wProcessorArchitecture==PROCESSOR_ARCHITECTURE_IA32_ON_WIN64) 
		return TRUE; 
	else 
		return FALSE;  
} 

/// <summary>
/// Gets the driver path.
/// </summary>
/// <returns></returns>
BOOL MsrDriver::GetDriverPath()
{ 
	// GetCurrentDirectory may be the problem because it get the path to current folder call it
	//GetCurrentDirectory(1024, szWinIoDriverPath);

	GetModuleFileName(NULL, szDriverPath, MAX_PATH);

	// LPWSTR full = new TCHAR[MAX_PATH];
	//LPWSTR root = new TCHAR[MAX_PATH];
	//LPWSTR dir = new TCHAR[MAX_PATH];
	//LPWSTR fileName = new TCHAR[MAX_PATH];
	//LPWSTR fileNameNoExt = new TCHAR[MAX_PATH];
	//_wsplitpath(szWinIoDriverPath, root, dir, fileNameNoExt, NULL);  
	//_wfullpath(full, L"", MAX_PATH);

	TCHAR* ptrEnd; 
	if((ptrEnd = _tcsrchr(szDriverPath, '\\')) != NULL )
	{
		*ptrEnd = '\0';

		/* Check OS version */
		if(b64BitOS)
			wcscat_s(szDriverPath, MAX_PATH, L"\\WinRing0\\WinRing0x64.sys"); 
		else
			wcscat_s(szDriverPath, MAX_PATH, L"\\WinRing0\\WinRing0.sys");
		//_tcscat_s(szWinIoDriverPath, MAX_PATH, _T("\\WinRing0\\WinRing0.sys"));
	}

	CString cstr;
	cstr.Format(_T("MsrDriver::GetDriverPath(): %s"), szDriverPath);
	DebugPrint(cstr); 

	return TRUE;
}

/// <summary>
/// Read MSR by thread affinity.
/// </summary>
/// <param name="index">The MSR address.</param>
/// <param name="buffer">The buffer.</param>
/// <param name="coreIdx">Index of the core.</param>
/// <returns></returns>
BOOL MsrDriver::RdmsrTx(UINT index,  UINT64  &buffer, UINT coreIdx)
{
	if(hDriver==INVALID_HANDLE_VALUE)
	{    
		buffer = 0; 
		return FALSE;
	}

	/************************************************************************/
	/* Each MSR are a 64 bit word. STS have the following structure:        */
	/* Bits: 63-48 (E), 47-32 (D), 31-24 (C), 23-16 (B), 15-0 (A)           */
	/*       63-------edx------32, 31 ---------eax -------- 0               */
	/************************************************************************/

	DWORD   dwByteReturned;  
	DWORD_PTR dwCoreAffinity, dwPrevCoreAffinity, dwSystemAffinity;
	HANDLE hProcess = GetCurrentProcess();
	HANDLE hThread = GetCurrentThread();

	// Get currrent affinity
	GetProcessAffinityMask(hProcess, &dwPrevCoreAffinity, &dwSystemAffinity);

	// Get core affinity mask
	dwCoreAffinity = cpuTopology.CoreAffinityMask(coreIdx);

	// Set thread to new core
	DWORD_PTR dwThreadAffinity = SetThreadAffinityMask(hThread, dwCoreAffinity); 
	 
	//SetThreadAffinityMask(GetCurrentThread(), 1 << coreIdx);
	BOOL result = DeviceIoControl(hDriver, IOCTL_OLS_READ_MSR, &index, sizeof(index), &buffer, sizeof(buffer), &dwByteReturned, NULL);

	// Restore previous affinity
	SetThreadAffinityMask(hThread, dwPrevCoreAffinity); 

	if(!result)
	{
		CString cstr;
		DWORD errorCode = GetLastError();
		cstr.Format(_T("MsrDriver::Rdmsr(3): Error=%d, %s"), errorCode, GetErrorMessage(errorCode));
		DebugPrint(cstr); 
	}

	return result;  
}

/// <summary>
/// Read MSR by 32-bit register.
/// </summary>
/// <param name="index">The MSR address.</param>
/// <param name="eax">The eax register.</param>
/// <param name="edx">The edx register.</param>
/// <returns></returns>
BOOL MsrDriver::Rdmsr(UINT index,  UINT &eax, UINT &edx)  
{
	if(hDriver==INVALID_HANDLE_VALUE)
	{    
		eax = 0; // low register
		edx = 0; // high register
		return FALSE;
	}

	//BYTE	buffer[8] = {0};

	/************************************************************************/
	/* Each MSR are a 64 bit word. STS have the following structure:        */
	/* Bits: 63-48 (E), 47-32 (D), 31-24 (C), 23-16 (B), 15-0 (A)           */
	/*       63-------edx------32, 31 ---------eax -------- 0               */
	/************************************************************************/
	UINT64  buffer = 0; //Each MSR are a 64 bit word
	DWORD   dwByteReturned;

	BOOL result = DeviceIoControl(hDriver, IOCTL_OLS_READ_MSR, &index, sizeof(index), &buffer, sizeof(buffer), &dwByteReturned, NULL);

	if(!result)
	{
		CString cstr;
		DWORD errorCode = GetLastError();
		cstr.Format(_T("MsrDriver::Rdmsr(3): Error=%d, %s"), errorCode, GetErrorMessage(errorCode));
		DebugPrint(cstr); 
	}

	if(result)
	{ 
		edx = (UINT)((buffer >> 32) & 0xFFFFFFFF);  /* shifting right 32 bit then Bitwise AND with  0xFFFFFFFF to take high part */
		eax = (UINT)(buffer & 0xFFFFFFFF); /* AND with  0xFFFFFFFF to take low part */
	} 

	return result;  
}


BOOL MsrDriver::RdmsrTx(UINT index,  UINT &eax, UINT &edx, UINT coreIdx)
{
	if(hDriver==INVALID_HANDLE_VALUE)
	{    
		eax = 0; // low register
		edx = 0; // high register
		return FALSE;
	}

	//BYTE	buffer[8] = {0};

	/************************************************************************/
	/* Each MSR are a 64 bit word. STS have the following structure:        */
	/* Bits: 63-48 (E), 47-32 (D), 31-24 (C), 23-16 (B), 15-0 (A)           */
	/*       63-------edx------32, 31 ---------eax -------- 0               */
	/************************************************************************/
	UINT64  buffer = 0; //Each MSR are a 64 bit word
	DWORD   dwByteReturned;

	DWORD_PTR dwCoreAffinity, dwPrevCoreAffinity, dwSystemAffinity;
	HANDLE hProcess = GetCurrentProcess();
	HANDLE hThread = GetCurrentThread();

	// Get currrent affinity
	GetProcessAffinityMask(hProcess, &dwPrevCoreAffinity, &dwSystemAffinity);

	// Get core affinity mask
	dwCoreAffinity = cpuTopology.CoreAffinityMask(coreIdx);

	// Set thread to new core
	DWORD_PTR dwThreadAffinity = SetThreadAffinityMask(hThread, dwCoreAffinity);
	//SetThreadAffinityMask(GetCurrentThread(), 1 << coreIdx);
	BOOL result = DeviceIoControl(hDriver, IOCTL_OLS_READ_MSR, &index, sizeof(index), &buffer, sizeof(buffer), &dwByteReturned, NULL);

	// Restore previous affinity
	SetThreadAffinityMask(hThread, dwPrevCoreAffinity); 

	if(!result)
	{
		CString cstr;
		DWORD errorCode = GetLastError();
		cstr.Format(_T("MsrDriver::Rdmsr(4): Error=%d, %s"), errorCode, GetErrorMessage(errorCode));
		DebugPrint(cstr); 
	}

	if(result)
	{ 
		edx = (UINT)((buffer >> 32) & 0xFFFFFFFF);  /* shifting right 32 bit then Bitwise AND with  0xFFFFFFFF to take high part */
		eax = (UINT)(buffer & 0xFFFFFFFF); /* AND with  0xFFFFFFFF to take low part */
	} 

	return result;  
}

BOOL MsrDriver::Rdmsr(UINT index,  UINT64  &buffer) 
{
	if(hDriver==INVALID_HANDLE_VALUE)
	{    
		buffer = 0; 
		return FALSE;
	}

	/************************************************************************/
	/* Each MSR are a 64 bit word. STS have the following structure:        */
	/* Bits: 63-48 (E), 47-32 (D), 31-24 (C), 23-16 (B), 15-0 (A)           */
	/*       63-------edx------32, 31 ---------eax -------- 0               */
	/************************************************************************/

	DWORD   dwByteReturned; 
	BOOL result = DeviceIoControl(hDriver, IOCTL_OLS_READ_MSR, &index, sizeof(index), &buffer, sizeof(buffer), &dwByteReturned, NULL);

	if(!result)
	{
		CString cstr;
		DWORD errorCode = GetLastError();
		cstr.Format(_T("MsrDriver::Rdmsr(2): Error=%d, %s"), errorCode, GetErrorMessage(errorCode));
		DebugPrint(cstr); 
	}

	return result;  
}

/// <summary>
/// Write MSR value.
/// </summary>
/// <param name="index">The MSR address.</param>
/// <param name="Value">The value.</param>
/// <returns></returns>
BOOL MsrDriver::Wrmsr(UINT index, UINT64 Value)
{
	if(hDriver==INVALID_HANDLE_VALUE)
	{     
		return FALSE;
	}

	UINT edx = (UINT)((Value >> 32) & 0xFFFFFFFF);  /* shifting right 32 bit then Bitwise AND with  0xFFFFFFFF to take high part */
	UINT eax = (UINT)(Value & 0xFFFFFFFF); /* AND with  0xFFFFFFFF to take low part */

	UINT buf[3];
	buf[0] = index;
	buf[1] = eax;
	buf[2] = edx;

	DWORD dwByteReturned; 
	BOOL result = DeviceIoControl(hDriver, IOCTL_OLS_WRITE_MSR, buf, sizeof(buf), NULL, 0, &dwByteReturned, NULL);

	if(!result)
	{
		CString cstr;
		DWORD errorCode = GetLastError();
		cstr.Format(_T("MsrDriver::Wrmsr(2): Error=%d, %s"), errorCode, GetErrorMessage(errorCode));
		DebugPrint(cstr); 
	}

	return result;
} 

/// <summary>
/// Write MSR value.
/// </summary>
/// <param name="index">The MSR address.</param>
/// <param name="eax">The eax.</param>
/// <param name="edx">The edx.</param>
/// <returns></returns>
BOOL MsrDriver::Wrmsr(UINT index, UINT eax, UINT edx)
{
	if(hDriver==INVALID_HANDLE_VALUE)
	{    
		eax = 0;
		edx = 0;
		return FALSE;
	}

	UINT buf[3];
	buf[0] = index;
	buf[1] = eax;
	buf[2] = edx;

	DWORD dwByteReturned; 
	BOOL result = DeviceIoControl(hDriver, IOCTL_OLS_WRITE_MSR, buf, sizeof(buf), NULL, 0, &dwByteReturned, NULL);

	if(!result)
	{
		CString cstr;
		DWORD errorCode = GetLastError();
		cstr.Format(_T("MsrDriver::Wrmsr(3): Error=%d, %s"), errorCode, GetErrorMessage(errorCode));
		DebugPrint(cstr); 
	}

	return result;
} 

BYTE MsrDriver::ReadIoPort(UINT port)
{
	if (hDriver == INVALID_HANDLE_VALUE)
		return 0;

	UINT Value = 0;
	DWORD dwByteReturned;
	BOOL result = DeviceIoControl(hDriver, IOCTL_OLS_READ_IO_PORT_BYTE, &port, sizeof(port), &Value, sizeof(Value), &dwByteReturned, NULL);

	if(!result)
	{
		CString cstr;
		DWORD errorCode = GetLastError();
		cstr.Format(_T("MsrDriver::ReadIoPort(): Error=%d, %s"), errorCode, GetErrorMessage(errorCode));
		DebugPrint(cstr); 
	}

	return (BYTE)(Value & 0xFF);
}


VOID MsrDriver::WriteIoPort(UINT port, BYTE Value)
{
	if (hDriver == INVALID_HANDLE_VALUE)
		return;

#pragma  pack(1)
	typedef struct 
	{
		UINT PortNumber;
		BYTE Value; 
	} INPUT_VALUE;
#pragma 

	INPUT_VALUE input;
	input.PortNumber = port;
	input.Value = Value;

	DWORD dwByteReturned;
	BOOL result = DeviceIoControl(hDriver, IOCTL_OLS_WRITE_IO_PORT_BYTE, &input, sizeof(input), NULL, 0, &dwByteReturned, NULL);

	if(!result)
	{
		CString cstr;
		DWORD errorCode = GetLastError();
		cstr.Format(_T("MsrDriver::WriteIoPort(): Error=%d, %s"), errorCode, GetErrorMessage(errorCode));
		DebugPrint(cstr); 
	}
}


UINT MsrDriver::GetPciAddress(BYTE bus, BYTE device, BYTE Function) 
{
	return (UINT)(((bus & 0xFF) << 8) | ((device & 0x1F) << 3) | (Function & 7));
}

BOOL MsrDriver::ReadPciConfig(UINT pciAddress, UINT regAddress, UINT &Value)
{
	if(hDriver == INVALID_HANDLE_VALUE || (regAddress&3)!=0)
	{
		Value = 0;
		return FALSE;
	}

	UINT buf[2];
	buf[0] = pciAddress;
	buf[1] = regAddress;

	DWORD dwByteReturned;
	BOOL result = DeviceIoControl(hDriver, IOCTL_OLS_READ_PCI_CONFIG, buf, sizeof(buf), &Value, sizeof(Value), &dwByteReturned, NULL);

	if(!result)
	{
		CString cstr;
		DWORD errorCode = GetLastError();
		cstr.Format(_T("MsrDriver::ReadPciConfig(): Error=%d, %s"), errorCode, GetErrorMessage(errorCode));
		DebugPrint(cstr); 
	}

	return result;
}

BOOL MsrDriver::WritePciConfig(UINT pciAddress, UINT regAddress, UINT Value)
{
	if(hDriver == INVALID_HANDLE_VALUE || (regAddress&3)!=0)
	{
		return FALSE;
	}

	UINT buf[2];
	buf[0] = pciAddress;
	buf[1] = regAddress;

	DWORD dwByteReturned;
	BOOL result = DeviceIoControl(hDriver, IOCTL_OLS_WRITE_PCI_CONFIG, buf, sizeof(buf), NULL, 0, &dwByteReturned, NULL);

	if(!result)
	{
		CString cstr;
		DWORD errorCode = GetLastError();
		cstr.Format(_T("MsrDriver::WritePciConfig(): Error=%d, %s"), errorCode, GetErrorMessage(errorCode));
		DebugPrint(cstr); 
	}

	return result;
}