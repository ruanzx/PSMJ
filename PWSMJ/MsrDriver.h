// 
// Name: MsrDriver.cpp : implementation file  
// Author: hieunt
// Description: Interface with hardware to archive power infos
//

#pragma once

#include <winioctl.h>
#include <winsvc.h>
#include "CpuTopology.h"

// Define the various device type values.  
// Note that values used by Microsoft Corporation are in the range 0-32767, 
// and 32768-65535 are reserved for use by customers.
const UINT OLS_TYPE      =    40000;
#define OLS_DRIVER_ID         _T("WinRing0_1_2_0") 

// Macro definition for defining IOCTL and FSCTL function control codes.
// Note that function codes 0-2047 are reserved for Microsoft Corporation,
// and 2048-4095 are reserved for customers.

const UINT IOCTL_OLS_GET_REFCOUNT = CTL_CODE(OLS_TYPE, 0x801, METHOD_BUFFERED, FILE_ANY_ACCESS);
const UINT IOCTL_OLS_GET_DRIVER_VERSION = CTL_CODE(OLS_TYPE, 0x800, METHOD_BUFFERED, FILE_ANY_ACCESS);
const UINT IOCTL_OLS_READ_MSR  CTL_CODE(OLS_TYPE, 0x821, METHOD_BUFFERED, FILE_ANY_ACCESS);
const UINT IOCTL_OLS_WRITE_MSR = CTL_CODE(OLS_TYPE, 0x822, METHOD_BUFFERED, FILE_ANY_ACCESS); 
const UINT IOCTL_OLS_READ_IO_PORT_BYTE = CTL_CODE(OLS_TYPE, 0x833, METHOD_BUFFERED, FILE_READ_ACCESS);
const UINT IOCTL_OLS_WRITE_IO_PORT_BYTE = CTL_CODE(OLS_TYPE, 0x836, METHOD_BUFFERED, FILE_WRITE_ACCESS);
const UINT IOCTL_OLS_READ_PCI_CONFIG = CTL_CODE(OLS_TYPE, 0x851, METHOD_BUFFERED, FILE_READ_ACCESS);
const UINT IOCTL_OLS_WRITE_PCI_CONFIG = CTL_CODE(OLS_TYPE, 0x852, METHOD_BUFFERED, FILE_WRITE_ACCESS);
const UINT InvalidPciAddress = 0xFFFFFFFF;

#define IA32_HWP_CAPABILITIES		    0x771
/* Package RAPL Domain */
#define MSR_RAPL_POWER_UNIT    0x606 
#define MSR_PKG_ENERGY_STATUS  0x611  
#define MSR_PKG_POWER_INFO     0x614  
#define MSR_PKG_PERF_STATUS	   0x613
#define MSR_PKG_POWER_LIMIT    0x610

/* PP0 RAPL Domain */
#define MSR_PP0_POWER_LIMIT		0x638 // Sandy Bridge & JakeTown specific 'Running Average Power Limit' MSR's.
#define MSR_PP0_ENERGY_STATUS	0x639
#define MSR_PP0_POLICY			0x63A
#define MSR_PP0_PERF_STATUS		0x63B

/* PP1 RAPL Domain, may reflect to uncore devices */
#define MSR_PP1_POWER_LIMIT		0x640
#define MSR_PP1_ENERGY_STATUS	0x641
#define MSR_PP1_POLICY			0x642

/* DRAM RAPL Domain */
#define MSR_DRAM_POWER_LIMIT		0x618
#define MSR_DRAM_ENERGY_STATUS		0x619
#define MSR_DRAM_PERF_STATUS		0x61B
#define MSR_DRAM_POWER_INFO		    0x61C

/* RAPL UNIT BITMASK */
#define POWER_UNIT_OFFSET	0
#define POWER_UNIT_MASK		0x0F

#define ENERGY_UNIT_OFFSET	0x08
#define ENERGY_UNIT_MASK	0x1F00

#define TIME_UNIT_OFFSET	0x10
#define TIME_UNIT_MASK		0xF000

/*------------------------------------*/
#define IA32_THERM_STATUS		    0x19c 
#define	IA32_PERF_STATUS		    0x198 
#define	IA32_TIME_STAMP_COUNTER		0x10
#define	IA32_PLATFORM_ID		    0x17
#define	IA32_MPERF			        0xe7
#define	IA32_APERF			        0xe8

#define	IA32_CLOCK_MODULATION		0x19a
#define	IA32_THERM_INTERRUPT		0x19b

#define	IA32_MISC_ENABLE		    0x1a0
#define	IA32_ENERGY_PERF_BIAS		0x1b0
#define	IA32_PKG_THERM_INTERRUPT 	0x1b2
#define	IA32_FIXED_CTR1			    0x30a
#define	IA32_FIXED_CTR2			    0x30b
#define	IA32_FIXED_CTR_CTRL		    0x38d
#define	IA32_PERF_GLOBAL_STATUS		0x38e
#define	IA32_PERF_GLOBAL_CTRL		0x38f
#define	IA32_PERF_GLOBAL_OVF_CTRL	0x390
#define	MSR_CORE_C3_RESIDENCY		0x3fc
#define	MSR_CORE_C6_RESIDENCY		0x3fd
#define	MSR_FSB_FREQ			    0xcd   /* limited use - not for i7 */
#define	MSR_PLATFORM_INFO		    0xce   /* limited use - MinRatio for i7 but Max for Yonah	*/
#define	MSR_TURBO_RATIO_LIMIT		0x1ad  /* limited use - not for Penryn or older */


#define IA32_PACKAGE_THERM_STATUS   0x1B1
#define	SMBIOS_PROCINFO_STRUCTURE	4
#define	SMBIOS_PROCINFO_INSTANCE	0
#define	SMBIOS_PROCINFO_EXTCLK		0x12
#define	SMBIOS_PROCINFO_CORES		0x23
#define	SMBIOS_PROCINFO_THREADS		0x25 
#define	MSR_TEMPERATURE_TARGET		0x1a2 /* TjMax limited use - not for Penryn or older	*/


/* Intel MSRs. Some also available on other CPUs */
#define MSR_IA32_TSC		       0x10
#define MSR_IA32_PLATFORM_ID	   0x17

#define MSR_IA32_PERFCTR0          0xc1
#define MSR_IA32_PERFCTR1          0xc2
#define MSR_FSB_FREQ		       0xcd

#define MSR_MTRRcap		           0x0fe
#define MSR_IA32_BBL_CR_CTL        0x119

#define MSR_IA32_SYSENTER_CS	    0x174
#define MSR_IA32_SYSENTER_ESP	    0x175
#define MSR_IA32_SYSENTER_EIP	    0x176

#define MSR_IA32_MCG_CAP            0x179
#define MSR_IA32_MCG_STATUS         0x17a
#define MSR_IA32_MCG_CTL            0x17b

#define MSR_IA32_EVNTSEL0           0x186
#define MSR_IA32_EVNTSEL1           0x187

#define MSR_IA32_DEBUGCTLMSR        0x1d9
#define MSR_IA32_LASTBRANCHFROMIP   0x1db
#define MSR_IA32_LASTBRANCHTOIP     0x1dc
#define MSR_IA32_LASTINTFROMIP      0x1dd
#define MSR_IA32_LASTINTTOIP        0x1de

#define MSR_IA32_PEBS_ENABLE		0x3f1
#define MSR_IA32_DS_AREA		    0x600
#define MSR_IA32_PERF_CAPABILITIES	0x345

#define MSR_MTRRfix64K_00000	0x250
#define MSR_MTRRfix16K_80000	0x258
#define MSR_MTRRfix16K_A0000	0x259
#define MSR_MTRRfix4K_C0000	0x268
#define MSR_MTRRfix4K_C8000	0x269
#define MSR_MTRRfix4K_D0000	0x26a
#define MSR_MTRRfix4K_D8000	0x26b
#define MSR_MTRRfix4K_E0000	0x26c
#define MSR_MTRRfix4K_E8000	0x26d
#define MSR_MTRRfix4K_F0000	0x26e
#define MSR_MTRRfix4K_F8000	0x26f
#define MSR_MTRRdefType		0x2ff

#define MSR_IA32_MC0_CTL        0x400
#define MSR_IA32_MC0_STATUS     0x401
#define MSR_IA32_MC0_ADDR       0x402
#define MSR_IA32_MC0_MISC       0x403

#define MSR_P6_PERFCTR0			0xc1
#define MSR_P6_PERFCTR1			0xc2
#define MSR_P6_EVNTSEL0			0x186
#define MSR_P6_EVNTSEL1			0x187

/* Intel defined MSRs. */
#define MSR_IA32_P5_MC_ADDR		    0
#define MSR_IA32_P5_MC_TYPE		    1
#define MSR_IA32_PLATFORM_ID		0x17
#define MSR_IA32_EBL_CR_POWERON		0x2a

#define MSR_IA32_APICBASE               0x1b
#define MSR_IA32_APICBASE_BSP           (1<<8)
#define MSR_IA32_APICBASE_ENABLE        (1<<11)
#define MSR_IA32_APICBASE_BASE          (0xfffff<<12)


/* Intel Core-based CPU performance counters */
#define MSR_CORE_PERF_FIXED_CTR0	    0x309
#define MSR_CORE_PERF_FIXED_CTR1	    0x30a
#define MSR_CORE_PERF_FIXED_CTR2	    0x30b
#define MSR_CORE_PERF_FIXED_CTR_CTRL	0x38d
#define MSR_CORE_PERF_GLOBAL_STATUS	    0x38e
#define MSR_CORE_PERF_GLOBAL_CTRL	    0x38f
#define MSR_CORE_PERF_GLOBAL_OVF_CTRL	0x390

/* CPU Features */
#define CPU_FEATURE_MMX			0x00000001		// MMX Instruction Set
#define CPU_FEATURE_SSE			0x00000002		// SSE Instruction Set
#define CPU_FEATURE_SSE2		0x00000004		// SSE2 Instruction Set
#define CPU_FEATURE_SSE3		0x00000008		// SSE3 Instruction Set
#define CPU_FEATURE_SSE41		0x00000010		// SSE41 Instruction Set
#define CPU_FEATURE_SSE42		0x00000020		// SSE42 Instruction Set
#define CPU_FEATURE_EM64T		0x00000040		// 64Bit Support
#define CPU_FEATURE_HTT			0x00000080		// HyperThreading
#define CPU_FEATURE_MOBILE		0x00000100		// Mobile CPU
#define CPU_FEATURE_MSR			0x00000200		// MSR Support
class MsrDriver 
{
private:
	HANDLE     hDriver; 
	CString    pathName;
	BOOL       b64BitOS;
	BOOL       bDriverInitialized;
	CString    driverName; 
	CString    serviceName;
	CString    serviceDisplayName;
	LPWSTR     szDriverPath;  // length for current directory + "\\msr.sys"
	DWORD      errorCode; 

	BOOL Initialize();
	BOOL _stdcall InstallDriver(PWSTR pszDriverPath, BOOL IsDemandLoaded);
	BOOL _stdcall RemoveDriver();
	BOOL _stdcall StartDriver();
	BOOL _stdcall StopDriver();

	BOOL Is64bitOS();
	BOOL GetDriverPath();
	CpuTopology cpuTopology;
	
public: 
	MsrDriver();
	~MsrDriver();

	BOOL IsDriverReady()
	{
		BOOL bReady = TRUE;
		WIN32_FIND_DATA m_data;
		HANDLE hFile = FindFirstFile(szDriverPath, &m_data);

		if(hFile==INVALID_HANDLE_VALUE) //file not found
		{
			bReady = FALSE;
		} 
		FindClose(hFile);

		return bReady;
	}

	BOOL Open();
	BOOL IsOpen(); 
	VOID Close(); 
	BOOL RdmsrTx(UINT index,  UINT64  &buffer, UINT coreIdx); 
	BOOL Rdmsr(UINT index,  UINT64  &buffer);
	BOOL Rdmsr(UINT index,  UINT &eax, UINT &edx); 
	BOOL RdmsrTx(UINT index,  UINT &eax, UINT &edx, UINT coreIdx);    
	BOOL Wrmsr(UINT index, UINT eax, UINT edx);
	BOOL Wrmsr(UINT index, UINT64 Value);
	BYTE ReadIoPort(UINT port);
	VOID WriteIoPort(UINT port, BYTE Value);
	UINT GetPciAddress(BYTE bus, BYTE device, BYTE Function);
	BOOL ReadPciConfig(UINT pciAddress, UINT regAddress, UINT &Value);
	BOOL WritePciConfig(UINT pciAddress, UINT regAddress, UINT Value); 
};
 