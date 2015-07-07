// 
// Name: MemoryMonitor.cpp : implementation file  
// Author: hieunt
// Description: Monitor free memory
//

#pragma once   

#include <psapi.h>
#pragma comment(lib, "Psapi.lib")

typedef struct MEMORY_INFO
{
	UINT64 TotalMemory;
	UINT64 FreeMemory;
} MEMORY_INFO;

class MemoryMonitor 
{  
public: 
	MemoryMonitor();
	~MemoryMonitor();   

	BOOL GetMemoryInfo();
	BOOL GetMemoryUsage(UINT64 mem, UINT64  vmem);
	

private: 
	MEMORYSTATUSEX statex;
	MEMORY_INFO    MemoryInfo;
}; 
 