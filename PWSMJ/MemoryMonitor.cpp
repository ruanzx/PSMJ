// 
// Name: MemoryMonitor.cpp : implementation file  
// Author: hieunt
// Description: Monitor free memory
//
#include "stdafx.h"
#include "MemoryMonitor.h"

/// <summary>
/// Initializes a new instance of the <see cref="MemoryMonitor"/> class.
/// </summary>
MemoryMonitor::MemoryMonitor( ) 
{ 
	 statex.dwLength = sizeof (statex);
}

/// <summary>
/// Finalizes an instance of the <see cref="MemoryMonitor"/> class.
/// </summary>
MemoryMonitor::~MemoryMonitor()
{

}

/// <summary>
/// Gets the memory information.
/// </summary>
/// <returns></returns>
BOOL MemoryMonitor::GetMemoryInfo()
{ 
	statex.dwLength = sizeof (statex);
	if(GlobalMemoryStatusEx (&statex))
	{
		MemoryInfo.TotalMemory = statex.ullTotalPhys / (1024 * 1024);
		MemoryInfo.FreeMemory  = statex.ullAvailPhys / (1024 * 1024);
		return TRUE;
	}
	else
	{
		//Log::Error("GlobalMemoryStatusEx() Error: %d", GetLastError());
		return FALSE;
	}
}

/// <summary>
/// Gets the memory usage.
/// </summary>
/// <param name="mem">The memory.</param>
/// <param name="vmem">The vmem.</param>
/// <returns></returns>
BOOL MemoryMonitor::GetMemoryUsage(UINT64 mem, UINT64  vmem)
{
	PROCESS_MEMORY_COUNTERS pmc;
	if(GetProcessMemoryInfo(GetCurrentProcess(), &pmc, sizeof(pmc)))
	{
		mem = pmc.WorkingSetSize;
		vmem = pmc.PagefileUsage;
		return TRUE;
	}
	return FALSE;
}