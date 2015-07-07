#pragma once

#include <comdef.h>
#include <Wbemidl.h>
#include <propvarutil.h>
#pragma comment(lib, "wbemuuid.lib")  
#include "WmiService.h" 
 
class WMI
{
public:
	WMI(void);
	~WMI(void);
	RC GetPstateTable(WMI_ProcessorStatus &table);
	RC GetCstateTable(WMI_KernelIdleStates &table);
	RC GetProcessorBiosInfoTable(WMI_ProcessorBiosInfo &table);
private:
	RC GetCpuPStates();
	RC GetCpuCStates();
	RC GetCpuInfos();
	RC GetDiskInfo(); 
	RC GetPerformanceCounter();  
	RC GetProcessorBiosInfo();
private:
	HRESULT hres;

	/* Variables */ 
	WMI_ProcessorStatus m_PStates; 
	WMI_KernelIdleStates m_CStates; 
	WMI_ProcessorBiosInfo m_PBI;

	WmiService cService;
	BOOL isInitWmi;
	BOOL isReady;
	RC rc;
};



