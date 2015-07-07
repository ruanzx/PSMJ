// 
// Name: CPU.cpp
// Author: hieunt
// Description: Get information of processor by reading MSR
//

#include "stdafx.h"
#include "CPU.h"

/// <summary>
/// Initializes a new instance of the <see cref="CPU"/> class.
/// </summary>
CPU::CPU()
{ 
	DebugPrint(L"Start read MSR, MSR = Model Specific Register"); 
	ThermalStatus.TjMax = 0;
	ThermalStatus.TSlope = 1; // TSlope [°C] Temperature slope of the digital thermal sensor

	AccumulatedPackageThrottledTime = 0;
	AccumulatedPP0ThrottledTime = 0;
	myRing.Open(); 

	FrequencyStatus.TscFrequency = GetCurrentTscFrequency();
	SetPerfGlobalCounterBits();
	SetFixedCounterBits();  
}

/// <summary>
/// Finalizes an instance of the <see cref="CPU"/> class.
/// </summary>
CPU::~CPU()
{ 
	ClearFixedCounterBits();
	ClearPerfGlobalCounterBits();

	myRing.Close(); // Close MSR driver
}  

/// <summary>
/// Determines whether MSR driver ready.
/// </summary>
/// <returns></returns>
BOOL CPU::IsDriverReady()
{
	return	myRing.IsDriverReady();
}

/// <summary>
/// Gets the state of the busy p-state.
/// </summary>
/// <param name="busyPercent">The busy percent.</param>
/// <param name="maximumNonTurboRatio">The maximum non turbo ratio.</param>
/// <returns></returns>
DWORD CPU::GetBusyPState(DWORD busyPercent, DWORD maximumNonTurboRatio)
{
	return (DWORD)(((maximumNonTurboRatio + 0.5) * busyPercent) / 100);
}

/// <summary>
/// Gets the busy percent.
/// </summary>
/// <param name="mPerf">The m-perf value.</param>
/// <param name="aPerf">The a-perf value.</param>
/// <returns></returns>
DWORD CPU::GetBusyPercent(UINT64 mPerf, UINT64 aPerf)
{
	return DWORD((aPerf * 100) / mPerf);
}

BOOL CPU::GetBasicInfos()
{
	BOOL bResult = TRUE; 
	char buffer[1024]; 

	GetCpuIdInfos(0, IdInfo);
	memset(buffer, 0, 1024);
	int maxCpuId;
	((int *)buffer)[0] = IdInfo.array[1];
	((int *)buffer)[1] = IdInfo.array[3];
	((int *)buffer)[2] = IdInfo.array[2];

	if (strncmp(buffer, "GenuineIntel", 4 * 3) != 0)
	{  
		DebugPrint(_T("CPU::GetBasicInfos() : Unsupported processor. Only Intel(R) processors are supported (Atom(R) and microarchitecture codename Nehalem, Westmere, Sandy Bridge and Ivy Bridge).")); 
		bResult = FALSE;
	}
	maxCpuId = IdInfo.array[0];

	GetCpuIdInfos(1, IdInfo); 


	/*
	EAX (Intel):
	31    28 27            20 19    16 1514 1312 11     8 7      4 3      0
	+--------+----------------+--------+----+----+--------+--------+--------+
	|########|Extended family |Extmodel|####|type|familyid|  model |stepping|
	+--------+----------------+--------+----+----+--------+--------+--------+ 
	*/

	BasicInfo.Family = (((IdInfo.array[0]) >> 8) & 0xf) | ((IdInfo.array[0] & 0xf00000) >> 16); // 0x6: Intel core

	// model = (cpu_feat_eax >> 4) & 0xF;
	BasicInfo.Model = (((IdInfo.array[0]) & 0xf0) >> 4) | ((IdInfo.array[0] & 0xf0000) >> 12); // Current Micro Architecture Sandy Brige
	BasicInfo.OriginalCpuModel = BasicInfo.Model; // Save current machine model

	// stepping = cpu_feat_eax & 0xF;
	BasicInfo.Stepping = (IdInfo.array[0]) & 0x0F;
	BasicInfo.ApicID = ((IdInfo.array[1]) >> 24) & 0xFF;

	//DWORD hasModelSpecificRegisters  = (cpuidInfo.array[3]) & 0x20;
	//DWORD hasTimeStampCounter  = (cpuidInfo.array[3]) & 0x10;

	if (maxCpuId >= 0xa)  //11
	{
		// get counter related info
		GetCpuIdInfos(0xa, IdInfo);
		UINT perfmon_version = BitExtractUInt32(IdInfo.array[0], 0, 7);
		UINT core_gen_counter_num_max = BitExtractUInt32(IdInfo.array[0], 8, 15);
		UINT core_gen_counter_width = BitExtractUInt32(IdInfo.array[0], 16, 23);
		if (perfmon_version > 1)
		{
			UINT core_fixed_counter_num_max = BitExtractUInt32(IdInfo.array[3], 0, 4);
			UINT core_fixed_counter_width = BitExtractUInt32(IdInfo.array[3], 5, 12);
		}
	}

	if (BasicInfo.Family != 6)
	{
		DebugPrint(_T("CPU::GetBasicInfos() : Unsupported processor. Only Intel(R) processors are supported (Atom(R) and microarchitecture codename Nehalem, Westmere, Sandy Bridge and Ivy Bridge).")); 
		bResult = FALSE;
	}

	if (BasicInfo.Model == CORE_2) 
	{
		BasicInfo.Model = CORE_1;
	}

	if (BasicInfo.Model == ATOM_2 
		|| BasicInfo.Model == ATOM_CENTERTON 
		|| BasicInfo.Model == ATOM_BAYTRAIL
		|| BasicInfo.Model == ATOM_AVOTON) 
	{
		BasicInfo.Model = ATOM_1;
	}

	if (BasicInfo.Model == NEHALEM_EP
		|| BasicInfo.Model == NEHALEM_EX
		|| BasicInfo.Model == CLARKDALE
		|| BasicInfo.Model == WESTMERE_EP 
		|| BasicInfo.Model == WESTMERE_EX) 
	{
		BasicInfo.Model = NEHALEM;
	}

	if (BasicInfo.Model == JAKETOWN) 
	{
		BasicInfo.Model = SANDY_BRIDGE;
	}

	if (BasicInfo.Model == IVYTOWN) 
	{
		BasicInfo.Model = IVY_BRIDGE;
	}

	if (BasicInfo.Model == HASWELL_ULT 
		|| BasicInfo.Model == HASWELL_2
		|| BasicInfo.Model == HASWELL_E) 
	{
		BasicInfo.Model = HASWELL;
	}

	isCpuModelSupport = IsCpuModelSupported(BasicInfo.Model);
	if(!isCpuModelSupport)
	{
		// death
		bResult = FALSE;
		return FALSE; // Current CPU model is not support
	}

	// Check RAPL available or not
	isPackageEnergyMetricsSupport = IsPackageEnergyMetricsAvailable(BasicInfo.OriginalCpuModel);

	const UINT64 busFreq = (
		BasicInfo.Model == SANDY_BRIDGE 
		|| BasicInfo.Model == JAKETOWN 
		|| BasicInfo.Model == IVYTOWN 
		|| BasicInfo.Model == IVY_BRIDGE
		|| BasicInfo.Model == HASWELL
		|| BasicInfo.OriginalCpuModel == ATOM_AVOTON
		) ? (100000000ULL) : (133333333ULL);

	BasicInfo.BusFrequency = busFreq;

	// Intel Sandy brige, Core Voltage (R/O)  P-state core voltage can be computed by MSR_PERF_STATUS[37:32] * (float) 1/(2^13).
	UINT64 freq;
	myRing.Rdmsr(MSR_PLATFORM_INFO, freq); 
	BasicInfo.NominalFrequency = ((freq >> 8) & 255) * busFreq;

	/* Get Logical Core Infomation */ 
	DWORD size = sizeof(SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX);
	char *slpi = new char[sizeof(SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX)]; 
	DWORD res = GetLogicalProcessorInformationEx(RelationAll, (PSYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX)slpi, &size);

	while (res == FALSE)
	{
		delete[] slpi;

		if (GetLastError() == ERROR_INSUFFICIENT_BUFFER)
		{
			slpi = new char[size];
			res = GetLogicalProcessorInformationEx(RelationAll, (PSYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX)slpi, &size);
		}
		else
		{
			CString cstr;
			DWORD errorCode = GetLastError();
			cstr.Format(_T("CPU::GetBasicInfos(): Error in Windows function 'GetLogicalProcessorInformationEx', ErrorCode=%d, %s"), errorCode, GetErrorMessage(errorCode));
			DebugPrint(cstr); 

			bResult = FALSE;
		}
	}

	DWORD numCores = 0;
	DWORD threadsPerCore = 0; 
	char *baseSlpi = slpi;
	PSYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX pi = NULL;

	for ( ; slpi < baseSlpi + size; slpi += pi->Size)
	{
		pi = (PSYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX)slpi;
		if (pi->Relationship == RelationProcessorCore)
		{
			threadsPerCore = (pi->Processor.Flags == LTP_PC_SMT) ? 2 : 1; 
			numCores += threadsPerCore;
		}
	}

	if (numCores != GetActiveProcessorCount(ALL_PROCESSOR_GROUPS))
	{ 
		CString cstr;
		DWORD errorCode = GetLastError();
		cstr.Format(_T("CPU::GetBasicInfos(): Error in processor group size counting, ErrorCode=%d, %s"), errorCode, GetErrorMessage(errorCode));
		DebugPrint(cstr);

		DebugPrint(_T("CPU::GetBasicInfos(): Make sure your binary is compiled for 64-bit: using 'x64' platform configuration"));

		bResult = FALSE;
	}

	BasicInfo.NumberOfPhysicalCore = numCores/threadsPerCore;
	BasicInfo.NumberOfLogicalCore = numCores;
	BasicInfo.NumberOfThreadsPerPhysicalCore = threadsPerCore;


	// Init core thermal status
	ThermalStatus.Core = new THERMAL_CORE[BasicInfo.NumberOfPhysicalCore];

	// Energy status for each core
	EnergyStatus.Core = new ENERGY_CORE[BasicInfo.NumberOfPhysicalCore]; 


	return bResult; 
}

/// <summary>
/// Get the Cpus usage.
/// </summary>
/// <returns></returns>
double CPUUsage()
{
	//http://www.cyberforum.ru/cpp-beginners/thread594820.html

	typedef NTSTATUS (NTAPI* pfNtQuerySystemInformation) (
		IN SYSTEM_INFORMATION_CLASS SystemInformationClass,
		OUT PVOID SystemInformation,
		IN ULONG SystemInformationLength,
		OUT PULONG ReturnLength OPTIONAL
		);

	static pfNtQuerySystemInformation NtQuerySystemInformation = NULL;

	if(NtQuerySystemInformation == NULL)
	{
		HMODULE ntDLL = ::GetModuleHandle(L"ntdll.dll");
		NtQuerySystemInformation = (pfNtQuerySystemInformation)GetProcAddress(ntDLL ,"NtQuerySystemInformation");
	}

	static SYSTEM_PROCESSOR_PERFORMANCE_INFORMATION lastInfo = {0};    
	SYSTEM_PROCESSOR_PERFORMANCE_INFORMATION curInfo = {0};

	ULONG retsize;

	NtQuerySystemInformation(SystemProcessorPerformanceInformation, &curInfo, sizeof(curInfo), &retsize);    

	double cpuUsage = -1;

	if(lastInfo.KernelTime.QuadPart != 0 || lastInfo.UserTime.QuadPart != 0)
	{
		cpuUsage = 100.0 - double(curInfo.IdleTime.QuadPart - lastInfo.IdleTime.QuadPart) / 
			double(curInfo.KernelTime.QuadPart - lastInfo.KernelTime.QuadPart + curInfo.UserTime.QuadPart - lastInfo.UserTime.QuadPart) * 100.0;
	}

	lastInfo = curInfo;

	return cpuUsage;    
}

/// <summary>
/// Gets the cpu usage.
/// </summary>
/// <returns></returns>
INT CPU::GetCpuUsage()
{ 
	DWORD processorCount = BasicInfo.NumberOfPhysicalCore;

	UINT64 lastTime = 0, lastSystemTime = 0;

	FILETIME now;
	FILETIME creationTime, exitTime, kernelTime, userTime;
	UINT64 systemTime, time;
	UINT64 systemTimeDelta, timeDelta;
	INT cpuUsage = 0;


	GetSystemTimeAsFileTime(&now);

	if (!GetProcessTimes(GetCurrentProcess(), &creationTime, &exitTime, &kernelTime, &userTime))
	{
		// We don't assert here because in some cases (such as in the Task Manager)
		// we may call this function on a process that has just exited but we have
		// not yet received the notification.
		return 0;
	}

	systemTime = (FileTime2UTC(&kernelTime) + FileTime2UTC(&userTime)) / processorCount;
	time = FileTime2UTC(&now);

	if ((lastSystemTime == 0) || (lastTime == 0))
	{
		// First call, just set the last values.
		lastSystemTime = systemTime;
		lastTime = time;
		return 0;
	}

	systemTimeDelta = systemTime - lastSystemTime;
	timeDelta = time - lastTime;

	//ASSERT(timeDelta != 0);

	if (timeDelta == 0)
		return -1;

	// We add time_delta / 2 so the result is rounded.
	cpuUsage = (int)((systemTimeDelta * 100 + timeDelta / 2) / timeDelta); 
	lastSystemTime = systemTime;
	lastTime = time;

	return cpuUsage;
} 

/// <summary>
/// Gets the power infos.
/// </summary>
VOID CPU::GetPowerInfos()
{  
	if(IsPackageEnergyMetricsAvailable(BasicInfo.Model)) // is cpu support RAPL
	{  
		/************************************************************************/
		/*  Reading package power unit                                          */
		/************************************************************************/
		UINT64 raplPowerUnit = 0; 
		if(	myRing.Rdmsr(MSR_RAPL_POWER_UNIT, raplPowerUnit))
		{  
			// Power Units (bits 3:0): Power related information (in Watts) is based on the multiplier, 1/ 2^PU; where PU is
			// an unsigned integer represented by bits 3:0. Default value is 0011b, indicating power unit is in 1/8 Watts
			// increment.
			PowerUnit.PowerUnit = 1./(double(1ULL<<BitExtractUInt64(raplPowerUnit, 0, 3)));    

			// Energy Status Units (bits 12:8): Energy related information (in Joules) is based on the multiplier, 1/2^ESU;
			// where ESU is an unsigned integer represented by bits 12:8. Default value is 10000b, indicating energy status
			// unit is in 15.3 micro-Joules increment.
			PowerUnit.EnergyStatusUnit = 1./(double(1ULL<<BitExtractUInt64(raplPowerUnit, 8, 12))); // ESU

			// Time Units (bits 19:16): Time related information (in Seconds) is based on the multiplier, 1/ 2^TU; where TU
			// is an unsigned integer represented by bits 19:16. Default value is 1010b, indicating time unit is in 976 microseconds
			// increment.
			PowerUnit.TimeUnits =1./(double(1ULL<<BitExtractUInt64(raplPowerUnit, 16, 19))); 
		}
		else
		{ 
			DebugPrint(_T("CPU::GetPowerInfos(): Read MSR_RAPL_POWER_UNIT fail")); 
		}
		/************************************************************************/
		/*  Reading package power info                                          */
		/************************************************************************/ 
		//MSR_PKG_POWER_INFO is a read-only MSR. It reports the package power range 
		//information for RAPL usage. This MSR provides maximum/minimum values (derived 
		//from electrical specification), thermal specification power of the package domain. It 
		//also provides the largest possible time window for software to program the RAPL interface.
		UINT64 packagePowerInfo = 0; 
		if(	myRing.Rdmsr(MSR_PKG_POWER_INFO, packagePowerInfo))
		{  
			//Intel Vol. 3B 14-35, page 2398

			// Thermal Spec Power (bits 14:0): The unsigned integer value is the equivalent of thermal specification power
			// of the package domain. The unit of this field is specified by the “Power Units” field of MSR_RAPL_POWER_UNIT.
			PkgPowerInfo.ThermalSpecPower  =  DOUBLE(BitExtractUInt64(packagePowerInfo, 0, 14))*PowerUnit.PowerUnit;
			//printf("Package thermal spec power (TDP): %.3fW\n", PkgPowerInfo.ThermalSpecPower);

			// Minimum Power (bits 30:16): The unsigned integer value is the equivalent of minimum power derived from
			// electrical spec of the package domain. The unit of this field is specified by the “Power Units” field of
			// MSR_RAPL_POWER_UNIT.
			PkgPowerInfo.MinimumPower =  DOUBLE(BitExtractUInt64(packagePowerInfo, 16, 30))*PowerUnit.PowerUnit;
			//printf("Package minimum power: %.3f W\n", PkgPowerInfo.MinimumPower);

			// Maximum Power (bits 46:32): The unsigned integer value is the equivalent of maximum power derived from
			// the electrical spec of the package domain. The unit of this field is specified by the “Power Units” field of
			// MSR_RAPL_POWER_UNIT.
			PkgPowerInfo.MaximumPower =  DOUBLE(BitExtractUInt64(packagePowerInfo, 32, 46))*PowerUnit.PowerUnit;
			//printf("Package maximum power: %.3f W\n", PkgPowerInfo.MaximumPower);

			// Maximum Time Window (bits 53:48): The unsigned integer value is the equivalent of largest acceptable
			// value to program the time window of MSR_PKG_POWER_LIMIT. The unit of this field is specified by the “Time
			// Units” field of MSR_RAPL_POWER_UNIT. 
			PkgPowerInfo.MaximumTimeWindow = DOUBLE( BitExtractUInt64(packagePowerInfo, 48, 53)) * PowerUnit.TimeUnits;
			//printf("Package maximum time window: %.3f s\n",PkgPowerInfo.MaximumTimeWindow ); 
		}
		else
		{ 
			DebugPrint(_T("CPU::GetPowerInfos(): Read MSR_PKG_POWER_INFO fail")); 
		}
		/************************************************************************/
		/*  Reading package power limit                                         */
		/************************************************************************/ 
		// MSR_PKG_POWER_LIMIT allows a software agent to define power limitation for the package domain. Power limitation
		// is defined in terms of average power usage (Watts) over a time window specified in MSR_PKG_POWER_LIMIT.
		// Two power limits can be specified, corresponding to time windows of different sizes. Each power limit provides
		// independent clamping control that would permit the processor cores to go below OS-requested state to meet the
		// power limits. A lock mechanism allow the software agent to enforce power limit settings. Once the lock bit is set,
		// the power limit settings are static and un-modifiable until next RESET.
		UINT64 packagePowerLimit = 0;  
		if( myRing.Rdmsr(MSR_PKG_POWER_LIMIT, packagePowerLimit))
		{ 
			//Intel Vol. 3B 14-35, page 2397
			// Package Power Limit #1(bits 14:0): Sets the average power usage limit of the package domain corresponding
			// to time window # 1. The unit of this field is specified by the “Power Units” field of
			// MSR_RAPL_POWER_UNIT. 
			PkgPowerLimit.PowerLimit1=DOUBLE(BitExtractUInt64(packagePowerLimit, 0, 14) * PowerUnit.PowerUnit);
			//printf("Package Power Limit #1: %.3f W\n", PkgPowerLimit.PowerLimit1);

			//Enable Power Limit #1(bit 15): 0 = disabled; 1 = enabled 
			PkgPowerLimit.EnablePowerLimit1 = BitExtractUInt64(packagePowerLimit, 15, 15);
			//printf("Enable Power Limit #1: %s\n", PkgPowerLimit.EnablePowerLimit1==0 ? "disabled" : "enabled");

			//Package Clamping Limitation #1 (bit 16): Allow going below OS-requested P/T state setting during time
			//window specified by bits 23:17.
			PkgPowerLimit.ClampingLimitation1 = BitExtractUInt64(packagePowerLimit, 16, 16);
			//printf("Package Clamping Limitation #1: %.3f\n", PkgPowerLimit.ClampingLimitation1);

			//Time Window for Power Limit #1 (bits 23:17): Indicates the time window for power limit #1
			//Time limit = 2^Y * (1.0 + Z/4.0) * Time_Unit
			//Here “Y” is the unsigned integer value represented. by bits 21:17, “Z” is an unsigned integer represented by
			//bits 23:22. “Time_Unit” is specified by the “Time Units” field of MSR_RAPL_POWER_UNIT.
			DWORD Y = BitExtractUInt64(packagePowerLimit, 17, 21);
			DWORD Z = BitExtractUInt64(packagePowerLimit, 22, 23);
			PkgPowerLimit.TimeWindowForPowerLimit1 =  pow(2, Y) * (1.0 + Z/ (4.0)) * PowerUnit.TimeUnits;
			//printf("Time Window for Power Limit #1: %.3f\n", PkgPowerLimit.TimeWindowForPowerLimit1);

			//Package Power Limit #2(bits 46:32): Sets the average power usage limit of the package domain corresponding
			//to time window # 2. The unit of this field is specified by the “Power Units” field of
			//MSR_RAPL_POWER_UNIT.
			PkgPowerLimit.PowerLimit2=DOUBLE(BitExtractUInt64(packagePowerLimit, 32, 46) * PowerUnit.PowerUnit);
			//printf("Package Power Limit #2: %.3fW\n", PkgPowerLimit.PowerLimit2);

			// Enable Power Limit #2(bit 47): 0 = disabled; 1 = enabled.
			PkgPowerLimit.EnablePowerLimit2 = BitExtractUInt64(packagePowerLimit, 47, 47);
			//printf("Enable Power Limit #2: %s\n", PkgPowerLimit.EnablePowerLimit2==0 ? "disabled" : "enabled");

			// Package Clamping Limitation #2 (bit 48): Allow going below OS-requested P/T state setting during time
			// window specified by bits 23:17.
			PkgPowerLimit.ClampingLimitation2 = BitExtractUInt64(packagePowerLimit, 48, 48);
			//printf("Package Clamping Limitation #2: %.3f\n", PkgPowerLimit.ClampingLimitation2);

			//Time Window for Power Limit #2 (bits 55:49): Indicates the time window for power limit #2
			//Time limit = 2^Y * (1.0 + Z/4.0) * Time_Unit
			//Here “Y” is the unsigned integer value represented. by bits 53:49, “Z” is an unsigned integer represented by
			//bits 55:54. “Time_Unit” is specified by the “Time Units” field of MSR_RAPL_POWER_UNIT. This field may have
			//a hard-coded value in hardware and ignores values written by software.
			Y = BitExtractUInt64(packagePowerLimit, 49, 53);
			Z = BitExtractUInt64(packagePowerLimit, 54, 55);
			PkgPowerLimit.TimeWindowForPowerLimit2 =  pow(2, Y) * (1.0 + Z/ (4.0)) * PowerUnit.TimeUnits;
			//printf("Time Window for Power Limit #2: %.3f\n", PkgPowerLimit.TimeWindowForPowerLimit2); 

			// Lock (bit 63): If set, all write attempts to this MSR are ignored until next RESET.
			PkgPowerLimit.Lock = BitExtractUInt64(packagePowerLimit, 63, 63);
			//printf("Package Power limit LOCK: %s\n", PkgPowerLimit.Lock==1 ? "True" : "False");  
		}
		else
		{ 
			DebugPrint(_T("CPU::GetPowerInfos(): Read MSR_PKG_POWER_LIMIT fail"));
		}
		/************************************************************************/
		/* Read PP0 Power limit MSR                                              */
		/************************************************************************/
		UINT64 pp0PowerLimit = 0; 

		if(	myRing.Rdmsr(MSR_PP0_POWER_LIMIT, pp0PowerLimit))
		{ 
			//Power Limit (bits 14:0): Sets the average power usage limit of the respective power plane domain. The unit
			//of this field is specified by the “Power Units” field of MSR_RAPL_POWER_UNIT.
			Pp0PowerLimit.PowerLimit=DOUBLE(BitExtractUInt64(pp0PowerLimit, 0, 14) * PowerUnit.PowerUnit);
			//printf("PP0 Power Limit: %.3fW\n", Pp0PowerLimit.PowerLimit);

			//Enable Power Limit (bit 15): 0 = disabled; 1 = enabled.
			Pp0PowerLimit.EnablePowerLimit = BitExtractUInt64(pp0PowerLimit, 15, 15);
			//printf("PP0 Enable Power Limit: %s\n", Pp0PowerLimit.EnablePowerLimit==0 ? "disabled" : "enabled");

			// Clamping Limitation (bit 16): Allow going below OS-requested P/T state setting during time window specified
			// by bits 23:17.
			Pp0PowerLimit.ClampingLimitation = BitExtractUInt64(pp0PowerLimit, 16, 16);
			//printf("PP0 Clamping Limitation: %.3f\n", Pp0PowerLimit.ClampingLimitation);

			//Time Window for Power Limit (bits 23:17): Indicates the length of time window over which the power limit
			//#1 The numeric value encoded by bits 23:17 is represented by the product of 2^Y *F; where F is a single-digit
			//decimal floating-point value between 1.0 and 1.3 with the fraction digit represented by bits 23:22, Y is an
			//unsigned integer represented by bits 21:17. The unit of this field is specified by the “Time Units” field of
			//MSR_RAPL_POWER_UNIT.
			DWORD Y = BitExtractUInt64(pp0PowerLimit, 17, 21);
			DOUBLE F = BitExtractUInt64(pp0PowerLimit, 22, 23);
			Pp0PowerLimit.TimeWindowForPowerLimit =  pow(2, Y) * F * PowerUnit.TimeUnits;
			//printf("PP0 Time Window for Power Limit: %.3f\n", Pp0PowerLimit.TimeWindowForPowerLimit);

			// Lock (bit 31): If set, all write attempts to the MSR and corresponding policy
			// MSR_PP0_POLICY/MSR_PP1_POLICY are ignored until next RESET.
			Pp0PowerLimit.Lock = BitExtractUInt64(pp0PowerLimit, 31, 31);
			//printf("PP0 Power Limit LOCK: %s\n", Pp0PowerLimit.Lock==1 ? "True" : "False"); 
		}
		else
		{ 
			DebugPrint(_T("CPU::GetPowerInfos(): Read MSR_PP0_POWER_LIMIT fail"));
		}


		/************************************************************************/
		/* Read DRAM Power Info MSR                                              */
		/************************************************************************/
		UINT64 dramPowerInfo = 0;  
		if(	myRing.Rdmsr(MSR_DRAM_POWER_INFO, dramPowerInfo))
		{
			//Thermal Spec Power (bits 14:0): The unsigned integer value is the equivalent of thermal specification power
			//of the DRAM domain. The unit of this field is specified by the “Power Units” field of MSR_RAPL_POWER_UNIT.
			DramPowerInfo.ThermalSpecPower=DOUBLE(BitExtractUInt64(dramPowerInfo, 0, 14) * PowerUnit.PowerUnit);
			//printf("DRAM Thermal Spec Power: %.3fW\n", DramPowerInfo.ThermalSpecPower);

			//Minimum Power (bits 30:16): The unsigned integer value is the equivalent of minimum power derived from
			//electrical spec of the DRAM domain. The unit of this field is specified by the “Power Units” field of
			//MSR_RAPL_POWER_UNIT.
			DramPowerInfo.MinimumPower=DOUBLE(BitExtractUInt64(dramPowerInfo, 16, 30) * PowerUnit.PowerUnit);
			//printf("DRAM Minimum Power: %.3fW\n", DramPowerInfo.MinimumPower);

			//Maximum Power (bits 46:32): The unsigned integer value is the equivalent of maximum power derived from
			//the electrical spec of the DRAM domain. The unit of this field is specified by the “Power Units” field of
			//MSR_RAPL_POWER_UNIT.
			DramPowerInfo.MaximumPower=DOUBLE(BitExtractUInt64(dramPowerInfo, 32, 46) * PowerUnit.PowerUnit);
			//printf("DRAM Maximum Power: %.3fW\n", DramPowerInfo.MaximumPower);

			//Maximum Time Window (bits 53:48): The unsigned integer value is the equivalent of largest acceptable
			//value to program the time window of MSR_DRAM_POWER_LIMIT. The unit of this field is specified by the “Time
			//Units” field of MSR_RAPL_POWER_UNIT.
			DramPowerInfo.MaximumTimeWindow=DOUBLE(BitExtractUInt64(dramPowerInfo, 48, 53) * PowerUnit.TimeUnits);
			//printf("DRAM Maximum Time Window: %.3fW\n", DramPowerInfo.MaximumTimeWindow); 
		}
		else
		{
			//printf("DRAM Power Info does not support\n");
			DebugPrint(_T("CPU::GetPowerInfos(): Read MSR_DRAM_POWER_INFO fail"));
		}
		/************************************************************************/
		/* Read DRAM Power limit MSR                                              */
		/************************************************************************/
		UINT64 dramPowerLimit = 0;  
		if(	myRing.Rdmsr(MSR_DRAM_POWER_LIMIT, dramPowerLimit))
		{
			//DRAM Power Limit #1(bits 14:0): Sets the average power usage limit of the DRAM domain corresponding to
			//time window # 1. The unit of this field is specified by the “Power Units” field of MSR_RAPL_POWER_UNIT.
			DramPowerLimit.PowerLimit=DOUBLE(BitExtractUInt64(dramPowerLimit, 0, 14) * PowerUnit.PowerUnit);
			//printf("DRAM Power Limit: %.3fW\n", DramPowerLimit.PowerLimit);

			//Enable Power Limit (bit 15): 0 = disabled; 1 = enabled.
			DramPowerLimit.EnablePowerLimit = BitExtractUInt64(dramPowerLimit, 15, 15);
			//printf("DRAM Enable Power Limit: %s\n", DramPowerLimit.EnablePowerLimit==0 ? "disabled" : "enabled");

			//Time Window for Power Limit (bits 23:17): Indicates the length of time window over which the power limit
			//The numeric value encoded by bits 23:17 is represented by the product of 2^Y *F; where F is a single-digit
			//decimal floating-point value between 1.0 and 1.3 with the fraction digit represented by bits 23:22, Y is an
			//unsigned integer represented by bits 21:17. The unit of this field is specified by the “Time Units” field of
			//MSR_RAPL_POWER_UNIT.  
			DWORD  Y = BitExtractUInt64(dramPowerLimit, 17, 21);
			DOUBLE	F = BitExtractUInt64(dramPowerLimit, 22, 23);
			DramPowerLimit.TimeWindowForPowerLimit =  pow(2, Y) * F * PowerUnit.TimeUnits;
			//printf("DRAM Time Window for Power Limit: %.3f\n", DramPowerLimit.TimeWindowForPowerLimit);

			// Lock (bit 31): If set, all write attempts to the MSR and corresponding policy
			// MSR_PP0_POLICY/MSR_PP1_POLICY are ignored until next RESET.
			DramPowerLimit.Lock = BitExtractUInt64(dramPowerLimit, 31, 31);
			//printf("DRAM Power Limit LOCK: %s\n", DramPowerLimit.Lock==1 ? "True" : "False");  
		}
		else 
		{
			//printf("DRAM Power Limit Info does not support\n");
			DebugPrint(_T("CPU::GetPowerInfos(): Read MSR_DRAM_POWER_LIMIT fail"));
		}
		/************************************************************************/
		/* MSR_PKG_PERF_STATUS MSR                                              */
		/************************************************************************/
		//MSR_PKG_PERF_STATUS is a read-only MSR. It reports the total time for which the package was throttled due to
		//the RAPL power limits. Throttling in this context is defined as going below the OS-requested P-state or T-state. It
		//has a wrap-around time of many hours.
		// SANDY BRIGE is not supported
		UINT64 packagePerformaceStatus = 0;  
		if(myRing.Rdmsr(MSR_PKG_PERF_STATUS, packagePerformaceStatus))
		{
			//Intel Vol. 3B 14-35, page 2399
			//Accumulated Package Throttled Time (bits 31:0): The unsigned integer value represents the cumulative
			//time (since the last time this register is cleared) that the package has throttled. The unit of this field is specified
			//by the “Time Units” field of MSR_RAPL_POWER_UNIT.
			AccumulatedPackageThrottledTime = DOUBLE(BitExtractUInt64(packagePerformaceStatus, 0, 31)) * PowerUnit.TimeUnits;
			//printf("Accumulated Package Throttled Time: %.3f\n", AccumulatedPackageThrottledTime ); 
		}
		else
		{
			//printf("MSR_PKG_PERF_STATUS does not support\n" );
			DebugPrint(_T("CPU::GetPowerInfos(): Read MSR_PKG_PERF_STATUS fail. Not support"));
		}
		/************************************************************************/
		/* MSR_PP0_PERF_STATUS MSR                                              */
		/************************************************************************/
		//MSR_PP0_PERF_STATUS is a read-only MSR. It reports the total time for which the PP0 domain was throttled due
		//to the power limits. This MSR is supported only in server platform. Throttling in this context is defined as going
		//below the OS-requested P-state or T-state.
		UINT64 pp0PerformaceStatus = 0; 
		if(	myRing.Rdmsr(MSR_PP0_PERF_STATUS, pp0PerformaceStatus))
		{
			//Intel Vol. 3B 14-35, page 2341

			//Accumulated PP0 Throttled Time (bits 31:0): The unsigned integer value represents the cumulative time
			//(since the last time this register is cleared) that the PP0 domain has throttled. The unit of this field is specified
			//by the “Time Units” field of MSR_RAPL_POWER_UNIT.
			AccumulatedPP0ThrottledTime = DOUBLE(BitExtractUInt64(pp0PerformaceStatus, 0, 31)) * PowerUnit.TimeUnits;
			//printf("Accumulated PP0 Throttled Time: %.3f\n", AccumulatedPP0ThrottledTime ); 
		}
		else
		{
			//printf("MSR_PP0_PERF_STATUS does not support\n" );
			DebugPrint(_T("CPU::GetPowerInfos(): Read MSR_PP0_PERF_STATUS fail. Not support"));
		}
		/************************************************************************/
		/* Package & pp0 energy status                                          */
		/************************************************************************/ 
		// Get DRAM status 
		UINT64 dramEnergyInfo = 0; 
		if(	myRing.Rdmsr(MSR_DRAM_ENERGY_STATUS, dramEnergyInfo))
		{
			DOUBLE dramEnergyBefore =  DOUBLE(BitExtractUInt64(dramEnergyInfo, 0, 31)) * PowerUnit.EnergyStatusUnit; 
			//printf("Dram Energy before: %.9fJ\n", dramEnergyBefore);  
		}
		else
		{
			//printf("Dram Energy does not support\n" );
			DebugPrint(_T("CPU::GetPowerInfos(): Read MSR_DRAM_ENERGY_STATUS fail. DRAM Energy does not support"));
		} 
	} 
}

/// <summary>
/// Gets the current PP0 enery status.
/// </summary>
/// <param name="coreIdx">Index of the core.</param>
/// <returns></returns>
DOUBLE CPU::GetCurrentPp0EneryStatus(UINT coreIdx)
{
	//MSR_PP0_ENERGY_STATUS/MSR_PP1_ENERGY_STATUS is a read-only MSR. It reports the actual energy use for
	//the respective power plane domain. This MSR is updated every ~1msec.

	UINT64 reg = 0; 
	if(	myRing.RdmsrTx(MSR_PP0_ENERGY_STATUS, reg, coreIdx))
	{
		DOUBLE pp0EnergyConsumed = DOUBLE(BitExtractUInt64(reg, 0, 31)) * PowerUnit.EnergyStatusUnit;
		return pp0EnergyConsumed;
	}
	else
		DebugPrint(_T("CPU::GetCurrentPp0EneryStatus(): Read MSR_PP0_ENERGY_STATUS fail"));
	return 0;
}

/// <summary>
/// Gets the current package enery status.
/// </summary>
/// <returns></returns>
DOUBLE CPU::GetCurrentPackageEneryStatus()
{
	// Read energy status
	// MSR_PKG_ENERGY_STATUS is a read-only MSR. It reports the actual energy use for the package domain. This
	// MSR is updated every ~1msec. It has a wraparound time of around 60 secs when power consumption is high, and
	// may be longer otherwise. 
	// Total Energy Consumed (bits 31:0): The unsigned integer value represents the total amount of energy
	// consumed since that last time this register is cleared. The unit of this field is specified by the “Energy Status
	// Units” field of MSR_RAPL_POWER_UNIT. 

	UINT64 reg = 0; 
	if(myRing.Rdmsr(MSR_PKG_ENERGY_STATUS, reg))
	{
		DOUBLE packageEnergyConsumed =  DOUBLE(BitExtractUInt64(reg, 0, 31)) * PowerUnit.EnergyStatusUnit; 
		return packageEnergyConsumed; 
	}
	else
		DebugPrint(_T("CPU::GetCurrentPackageEneryStatus(): Read MSR_PKG_ENERGY_STATUS fail"));
	return 0;
}


/// <summary>
/// Gets the turbo limit.
/// </summary>
/// <returns></returns>
BOOL CPU::GetTurboLimit()
{
	UINT64 reg;

	for(int i=0; i<10; i++)
	{
		PlatformInfo.TurboLimitRatio[i] = 0;
	}

	if( myRing.Rdmsr(MSR_TURBO_RATIO_LIMIT, reg))
	{
		PlatformInfo.TurboLimitRatio[0] = PlatformInfo.MaximumEfficiencyRatio; // MinimumRatio
		PlatformInfo.TurboLimitRatio[1] = PlatformInfo.MaximumNonTurboRatio;  
		PlatformInfo.TurboLimitRatio[2] = BitExtractUInt64(reg, 56, 63); // MaxRatio 8 Core active
		PlatformInfo.TurboLimitRatio[3] = BitExtractUInt64(reg, 48, 55); // MaxRatio 7 Core active
		PlatformInfo.TurboLimitRatio[4] = BitExtractUInt64(reg, 40, 47); // MaxRatio 6 Core active
		PlatformInfo.TurboLimitRatio[5] = BitExtractUInt64(reg, 32, 39); // MaxRatio 5 Core active
		PlatformInfo.TurboLimitRatio[6] = BitExtractUInt64(reg, 24, 31); // MaxRatio 4 Core active
		PlatformInfo.TurboLimitRatio[7] = BitExtractUInt64(reg, 16, 23); // MaxRatio 3 Core active
		PlatformInfo.TurboLimitRatio[8] = BitExtractUInt64(reg, 8, 15); // MaxRatio 2 Core active
		PlatformInfo.TurboLimitRatio[9] = BitExtractUInt64(reg, 0, 7); // MaxRatio 1 Core active 

		/* 	TurboBoost[9] usually the maximum value */
	}
	else
	{
		DebugPrint(_T("CPU::GetTurboLimit(): Read MSR_TURBO_RATIO_LIMIT fail"));
	}
	return TRUE;
}

/// <summary>
/// Gets the platform information.
/// </summary>
VOID CPU::GetPlatformInfo()
{
	UINT64 reg;
	if( myRing.Rdmsr(MSR_PLATFORM_INFO, reg))
	{ 
		/* The is the ratio of the frequency that invariant TSC runs at:  Frequency = ratio * 100 MHz.*/
		PlatformInfo.MaximumNonTurboRatio = BitExtractUInt64(reg, 8, 15);
		/* 1, indicates that Programmable Ratio Limits for Turbo mode is enabled, 0, indicates Programmable Ratio Limits for Turbo mode is disabled. */
		PlatformInfo.ProgrammableRatioLimitForTurboMode  = BitExtractUInt64(reg, 28, 28); 
		/* 1, indicates that TDP Limits for Turbo mode are programmable,  0, indicates TDP Limit for Turbo mode is not programmable. */ 
		PlatformInfo.ProgrammableTDPLimitForTurboMode = BitExtractUInt64(reg, 29, 29);  
		/* The is the minimum ratio (maximum efficiency) that the processor can operates, in units of 100MHz. */
		PlatformInfo.MaximumEfficiencyRatio = BitExtractUInt64(reg, 40, 47);  

		GetTurboLimit();
	}
	else
	{
		//chMB("Cannot get MSR_PLATFORM_INFO"); 
		DebugPrint(_T("CPU::GetPlatformInfo(): Read MSR_PLATFORM_INFO fail"));
	}
}

/// <summary>
/// Sets the turbo disable mode.
/// </summary>
/// <param name="bEnable">enable/disable.</param>
/// <returns></returns>
BOOL CPU::SetTurboDisableMode(BOOL bEnable)
{
	BOOL result = TRUE;

	UINT64 reg = IA32_MISC_ENABLE;
	//printf("IA32_MISC_ENABLE\n ");
	//BitPrint(IA32_MISC_ENABLE);

	if(bEnable)
		BitSet(reg,  38);
	else
		BitClear(reg, 38); 

	//printf("reg\n ");
	//BitPrint(reg);

	if(!myRing.Wrmsr(IA32_MISC_ENABLE, reg))
	{
		DebugPrint(_T("CPU::SetTurboDisableMode(): Write IA32_MISC_ENABLE fail"));
		result = FALSE;
	}
	return result;
}

/// <summary>
/// Sets the speed step mode.
/// </summary>
/// <param name="bEnable">enable/disable.</param>
/// <returns></returns>
BOOL CPU::SetSpeedStepMode(BOOL bEnable)
{
	BOOL result = TRUE;

	UINT64 reg = IA32_MISC_ENABLE;

	if(bEnable)
		BitSet(reg,  16);
	else
		BitClear(reg, 16); 

	if(!myRing.Wrmsr(IA32_MISC_ENABLE, reg))
	{
		DebugPrint(_T("CPU::SetSpeedStepMode(): Write IA32_MISC_ENABLE fail"));
		result = FALSE;
	}
	return result;
}


/// <summary>
/// Gets the misc infos.
/// </summary>
/// <returns></returns>
BOOL CPU::GetMiscInfos()
{
	UINT64 reg;  
	BOOL result = TRUE;
	if(myRing.Rdmsr(IA32_MISC_ENABLE, reg))
	{
		MiscInfo.EnhancedIntelSpeedStepTechnologyEnable = BitExtractUInt64(reg, 16, 16);
		MiscInfo.LimitCPUIDMaxval = BitExtractUInt64(reg, 22, 22);
		MiscInfo.TurboModeDisable = BitExtractUInt64(reg, 38, 38); 
	}
	else
	{
		//printf("Cannot get IA32_MISC_ENABLE\n ");
		DebugPrint(_T("CPU::GetMiscInfos(): Read IA32_MISC_ENABLE fail"));
		result = FALSE;
	} 

	return result;
}


/// <summary>
/// Get the tj info.
/// </summary>
VOID CPU::GetTjMax()
{  
	ThermalStatus.TjMax = 100;
	UINT64 reg;  
	// Retreive the Thermal Junction Max. Fallback to 100°C if not available.
	if(	myRing.Rdmsr(MSR_TEMPERATURE_TARGET, reg))
	{
		// Temperature Target (R). 
		// The minimum temperature at which PROCHOT# will be asserted.
		// The value is degree C.
		ThermalStatus.TjMax = BitExtractUInt64(reg, 16, 23);
	} 
	else
	{
		DebugPrint(_T("CPU::GetTjMax(): Read MSR_TEMPERATURE_TARGET fail"));
	}
}

/// <summary>
/// Gets the package thermal status.
/// </summary>
VOID CPU::GetPackageThermalStatus()
{ 
	UINT64 reg;   
	// Retreive the Thermal Junction Max. Fallback to 100°C if not available.
	if(ThermalStatus.TjMax==0) { GetTjMax();  }

	if(	myRing.Rdmsr(IA32_PACKAGE_THERM_STATUS, reg))
	{

		// Package digital temperature reading in 1 degree Celsius
		// relative to the package TCC activation temperature.
		// 0: Package TCC Activation temperature,
		// 1: (PTCC Activation - 1) , etc. See the processor’s data sheet for details regarding PTCC activation.
		// A lower reading in the Package Digital Readout field (bits 22:16) indicates a higher actual temperature.
		ThermalStatus.Package.DigitalReadout = BitExtractUInt64(reg, 16, 22); 
		ThermalStatus.Package.TemperatureInDegreesCelsius = ThermalStatus.TjMax - ThermalStatus.TSlope * ThermalStatus.Package.DigitalReadout;
	}
	else
	{
		DebugPrint(_T("CPU::GetPackageThermalStatus(): Read IA32_PACKAGE_THERM_STATUS fail"));
	}
}


/// <summary>
/// Gets the time stamp counter multiplier.
/// </summary>
/// <returns></returns>
UINT64 CPU::GetTimeStampCounterMultiplier()
{ 
	DWORD timeStampCounterMultiplier=0;
	UINT eax, edx;
	if(BasicInfo.Model==ATOM_1 ||BasicInfo.Model==CORE_1)
	{ 
		if(	myRing.Rdmsr(IA32_PERF_STATUS, eax, edx))
		{
			timeStampCounterMultiplier = ((edx >> 8) & 0x1f) + 0.5 * ((edx >> 14) & 1);
			return timeStampCounterMultiplier;
		}
		else
		{
			DebugPrint(_T("CPU::GetTimeStampCounterMultiplier(): Read IA32_PERF_STATUS fail"));
		}
	} 

	if(BasicInfo.Model==NEHALEM
		||BasicInfo.Model==SANDY_BRIDGE
		||BasicInfo.Model==IVY_BRIDGE
		||BasicInfo.Model==HASWELL)
	{
		if(	myRing.Rdmsr(MSR_PLATFORM_INFO, eax, edx))
		{
			timeStampCounterMultiplier = (eax >> 8) & 0xff;
			return timeStampCounterMultiplier;
		}
	} 
	return timeStampCounterMultiplier;
}

/// <summary>
/// Gets the current TSC frequency.
/// </summary>
/// <returns></returns>
DOUBLE CPU::GetCurrentTscFrequency()
{
	//From http://www.cs.helsinki.fi/linux/linux-kernel/2001-37/0256.html
	/*
	* $Id: MHz.c,v 1.4 2001/05/21 18:58:01 davej Exp $
	* This file is part of x86info.
	* (C) 2001 Dave Jones.
	*
	* Licensed under the terms of the GNU GPL License version 2.
	*
	* Estimate CPU MHz routine by Andrea Arcangeli <andrea@suse.de>
	* Small changes by David Sterba <sterd9am@ss1000.ms.mff.cuni.cz>
	*
	*/

	UINT64 cycles[2];		/* must be 64 bit */

	UINT64 microseconds;	    /* total time taken */ 
	LARGE_INTEGER m_liPerfFreq; // Counts per second
	LARGE_INTEGER m_liPerfStart; // Starting count
	QueryPerformanceFrequency(&m_liPerfFreq); 

	QueryPerformanceCounter(&m_liPerfStart);

	/* we don't trust that this is any specific length of time */
	/* 1 sec will cause rdtsc to overlap multiple times perhaps. 100msecs is a good spot */
	cycles[0] = RDTSC();
	Sleep(100);
	cycles[1] = RDTSC(); 
	LARGE_INTEGER liPerfNow;
	QueryPerformanceCounter(&liPerfNow);
	microseconds =  ((liPerfNow.QuadPart - m_liPerfStart.QuadPart) * 1000000) / m_liPerfFreq.QuadPart ;

	UINT64 elapsed = 0;
	if (cycles[1] < cycles[0])
	{
		//printf("c0 = %llu   c1 = %llu",cycles[0],cycles[1]);
		elapsed = UINT32_MAX - cycles[0];
		elapsed = elapsed + cycles[1];
		//printf("c0 = %llu  c1 = %llu max = %llu elapsed=%llu\n",cycles[0], cycles[1], UINT32_MAX,elapsed);
	}
	else
	{
		elapsed = cycles[1] - cycles[0];
		//printf("\nc0 = %llu  c1 = %llu elapsed=%llu\n",cycles[0], cycles[1],elapsed);
	}

	DOUBLE mhz = elapsed / microseconds; 

	//printf("%llg MHz processor (estimate).  diff cycles=%llu  microseconds=%llu \n", mhz, elapsed, microseconds);
	//printf("%g  elapsed %llu  microseconds %llu\n",mhz, elapsed, microseconds);
	return (mhz);
}

/// <summary>
/// Set the perf global counter bits.
/// </summary>
/// <returns></returns>
BOOL CPU::SetPerfGlobalCounterBits()
{
	BOOL result = TRUE;
	UINT64 reg = 0;  
	// MSR_PERF_GLOBAL_CTRL enables/disables event counting for all or any combination of fixed-function PMCs
	// (MSR_PERF_FIXED_CTRx) or general-purpose PMCs via a single WRMSR.
	if(	myRing.Rdmsr(IA32_PERF_GLOBAL_CTRL, reg))
	{
		DWORD fixedCtr0=0, fixedCtr1=0, fixedCtr2=0;

		fixedCtr0 = BitExtractUInt64(reg, 32, 32);
		if(fixedCtr0 != 0) 
		{
			//printf("Warning: CPU#%02d: Fixed Counter #0 is already activated.\n");  
		}
		fixedCtr1 = BitExtractUInt64(reg, 33, 33);
		if(fixedCtr1 != 0) 
		{
			//printf("Warning: CPU#%02d: Fixed Counter #1 is already activated.\n");  
		}

		fixedCtr2 = BitExtractUInt64(reg, 34, 34);
		if(fixedCtr2 != 0)
		{
			//printf("Warning: CPU#%02d: Fixed Counter #2 is already activated.\n");  
		}

		UINT64 globalCtrReg = reg;
		BitSet(globalCtrReg, 33);
		BitSet(globalCtrReg, 34);
		// - Set the global counter bits
		if(	myRing.Wrmsr(IA32_PERF_GLOBAL_CTRL, globalCtrReg))
		{
			//printf("Set IA32_PERF_GLOBAL_CTRL OK.\n");  
			result = TRUE;
		}
		else
		{
			//printf("Set IA32_PERF_GLOBAL_CTRL FAIL.\n"); 
			DebugPrint(_T("CPU::SetPerfGlobalCounterBits(): Write IA32_PERF_GLOBAL_CTRL fail"));
			result = FALSE;
		}
	}
	else
	{ 
		DebugPrint(_T("CPU::SetPerfGlobalCounterBits(): Read IA32_PERF_GLOBAL_CTRL fail"));
		result = FALSE;
	}

	return result; 
}

/// <summary>
/// Clears the perf global counter bits.
/// </summary>
/// <returns></returns>
BOOL CPU::ClearPerfGlobalCounterBits()
{
	BOOL result = TRUE;
	UINT64 reg = 0;  
	// MSR_PERF_GLOBAL_CTRL enables/disables event counting for all or any combination of fixed-function PMCs
	// (MSR_PERF_FIXED_CTRx) or general-purpose PMCs via a single WRMSR.
	if(	myRing.Rdmsr(IA32_PERF_GLOBAL_CTRL, reg))
	{
		DWORD fixedCtr0=0, fixedCtr1=0, fixedCtr2=0;

		fixedCtr0 = BitExtractUInt64(reg, 32, 32);
		if(fixedCtr0 != 0) 
		{
			//printf("Warning: CPU#%02d: Fixed Counter #0 is already activated.\n");  
		}

		fixedCtr1 = BitExtractUInt64(reg, 33, 33);
		if(fixedCtr1 != 0) 
		{
			//printf("Warning: CPU#%02d: Fixed Counter #1 is already activated.\n");  
		}

		fixedCtr2 = BitExtractUInt64(reg, 34, 34);
		if(fixedCtr2 != 0) 
		{
			//printf("Warning: CPU#%02d: Fixed Counter #2 is already activated.\n");  
		}

		UINT64 globalCtrReg = reg;
		BitClear(globalCtrReg, 32);
		BitClear(globalCtrReg, 33);
		BitClear(globalCtrReg, 34);
		// - Set the global counter bits
		if(	myRing.Wrmsr(IA32_PERF_GLOBAL_CTRL, globalCtrReg))
		{
			//printf("Clear IA32_PERF_GLOBAL_CTRL OK.\n"); 
			result = TRUE;
		}
		else
		{ 
			DebugPrint(_T("CPU::ClearPerfGlobalCounterBits(): Write IA32_PERF_GLOBAL_CTRL fail"));
			result = FALSE;
		}
	}
	else
	{ 
		DebugPrint(_T("CPU::ClearPerfGlobalCounterBits(): Read IA32_PERF_GLOBAL_CTRL fail"));
		result = FALSE;
	}

	return result; 
}

/// <summary>
/// Sets the fixed counter bits.
/// </summary>
/// <returns></returns>
BOOL  CPU::SetFixedCounterBits()
{
	BOOL result = TRUE;
	UINT64 reg = 0; 

	//Fixed-Function-Counter Control Register (R/W)
	//Counter increments while the results of ANDing respective enable bit in
	//IA32_PERF_GLOBAL_CTRL with the corresponding OS or USR bits in this MSR is true.
	if(	myRing.Rdmsr(IA32_FIXED_CTR_CTRL, reg))
	{
		UINT64 fixedCtrCtrlReg = reg;

		// EN0_OS: Enable Fixed Counter 0 to count while CPL = 0.
		DWORD EN0_OS = BitExtractUInt64(reg, 0, 0);

		// EN0_Usr: Enable Fixed Counter 0 to count while CPL > 0.
		DWORD EN0_Usr = BitExtractUInt64(reg, 1, 1); 

		//AnyThread: When set to 1, it enables counting the associated event conditions
		//occurring across all logical processors sharing a processor core. When set to 0, the
		//counter only increments the associated  event conditions occurring in the logical
		//processor which programmed the MSR.
		DWORD AnyThread_1 = BitExtractUInt64(reg, 2, 2); 

		//EN0_PMI: Enable PMI when fixed counter 0 overflows.
		DWORD EN0_PMI = BitExtractUInt64(reg, 3, 3); 

		//EN1_OS: Enable Fixed Counter 1to count while CPL = 0.
		DWORD EN1_OS = BitExtractUInt64(reg, 4, 4); 

		//EN1_Usr: Enable Fixed Counter 1 to count  while CPL > 0.
		DWORD EN1_Usr = BitExtractUInt64(reg, 5, 5); 

		//AnyThread: When set to 1, it enables counting the associated event conditions
		//occurring across all logical processors sharing a processor core. When set to 0, the
		//counter only increments the associated event conditions occurring in the logical
		//processor which programmed the MSR.
		DWORD AnyThread_2 = BitExtractUInt64(reg, 6, 6); 

		// EN1_PMI: Enable PMI when fixed counter 1 overflows.
		DWORD EN1_PMI = BitExtractUInt64(reg, 7, 7); 

		// EN2_OS: Enable Fixed Counter 2 to count while CPL = 0.
		DWORD EN2_OS = BitExtractUInt64(reg, 8, 8);  

		//EN2_Usr: Enable Fixed Counter 2 to count while CPL > 0.
		DWORD EN2_Usr = BitExtractUInt64(reg, 9, 9);  

		//AnyThread: When set to 1, it enables counting the associated event conditions
		//occurring across all logical processors sharing a processor core. When set to 0, the
		//counter only increments the associated event conditions occurring in the logical
		//processor which programmed the MSR.
		DWORD AnyThread_3 = BitExtractUInt64(reg, 10, 10);  

		//EN2_PMI: Enable PMI when fixed counter 2 overflows.
		DWORD EN2_PMI = BitExtractUInt64(reg, 11, 11);  

		/************************************************************************/
		/* Set Bit counter                                                      */
		/************************************************************************/

		// EN1_OS & EN2_OS
		BitSet(fixedCtrCtrlReg, 8); BitSet(fixedCtrCtrlReg, 4);
		// EN1_Usr & EN2_Usr
		BitSet(fixedCtrCtrlReg, 5); BitSet(fixedCtrCtrlReg, 9);
		// Per core AnyThread_EN1 & AnyThread_EN2
		BitSet(fixedCtrCtrlReg, 2); BitSet(fixedCtrCtrlReg, 6);

		// Per thread AnyThread_EN1 & AnyThread_EN2
		// BitClear(fixedCtrCtrlReg, 2); BitClear(fixedCtrCtrlReg, 6);

		// - Set the fixed counter bits
		if(	myRing.Wrmsr(IA32_FIXED_CTR_CTRL, fixedCtrCtrlReg))
		{
			//printf("Set IA32_FIXED_CTR_CTRL OK.\n");
			result = TRUE;
		}
		else
		{	 
			DebugPrint(_T("CPU::SetFixedCounterBits(): Write IA32_FIXED_CTR_CTRL fail"));
			result = FALSE;
		}
	}
	else
	{ 
		DebugPrint(_T("CPU::SetFixedCounterBits(): Read IA32_FIXED_CTR_CTRL fail"));
		result = FALSE;
	} 

	return result;  
}

/// <summary>
/// Clears the fixed counter bits.
/// </summary>
/// <returns></returns>
BOOL  CPU::ClearFixedCounterBits()
{
	BOOL result = TRUE;
	UINT64 reg = 0; 

	//Fixed-Function-Counter Control Register (R/W)
	//Counter increments while the results of ANDing respective enable bit in
	//IA32_PERF_GLOBAL_CTRL with the corresponding OS or USR bits in this MSR is true.
	if(	myRing.Rdmsr(IA32_FIXED_CTR_CTRL, reg))
	{
		UINT64 fixedCtrCtrlReg = reg;

		// EN0_OS: Enable Fixed Counter 0 to count while CPL = 0.
		DWORD EN0_OS = BitExtractUInt64(reg, 0, 0);

		// EN0_Usr: Enable Fixed Counter 0 to count while CPL > 0.
		DWORD EN0_Usr = BitExtractUInt64(reg, 1, 1); 

		//AnyThread: When set to 1, it enables counting the associated event conditions
		//occurring across all logical processors sharing a processor core. When set to 0, the
		//counter only increments the associated  event conditions occurring in the logical
		//processor which programmed the MSR.
		DWORD AnyThread_1 = BitExtractUInt64(reg, 2, 2); 

		//EN0_PMI: Enable PMI when fixed counter 0 overflows.
		DWORD EN0_PMI = BitExtractUInt64(reg, 3, 3); 

		//EN1_OS: Enable Fixed Counter 1to count while CPL = 0.
		DWORD EN1_OS = BitExtractUInt64(reg, 4, 4); 

		//EN1_Usr: Enable Fixed Counter 1 to count  while CPL > 0.
		DWORD EN1_Usr = BitExtractUInt64(reg, 5, 5); 

		//AnyThread: When set to 1, it enables counting the associated event conditions
		//occurring across all logical processors sharing a processor core. When set to 0, the
		//counter only increments the associated event conditions occurring in the logical
		//processor which programmed the MSR.
		DWORD AnyThread_2 = BitExtractUInt64(reg, 6, 6); 

		// EN1_PMI: Enable PMI when fixed counter 1 overflows.
		DWORD EN1_PMI = BitExtractUInt64(reg, 7, 7); 

		// EN2_OS: Enable Fixed Counter 2 to count while CPL = 0.
		DWORD EN2_OS = BitExtractUInt64(reg, 8, 8);  

		//EN2_Usr: Enable Fixed Counter 2 to count while CPL > 0.
		DWORD EN2_Usr = BitExtractUInt64(reg, 9, 9);  

		//AnyThread: When set to 1, it enables counting the associated event conditions
		//occurring across all logical processors sharing a processor core. When set to 0, the
		//counter only increments the associated event conditions occurring in the logical
		//processor which programmed the MSR.
		DWORD AnyThread_3 = BitExtractUInt64(reg, 10, 10);  

		//EN2_PMI: Enable PMI when fixed counter 2 overflows.
		DWORD EN2_PMI = BitExtractUInt64(reg, 11, 11);  

		/************************************************************************/
		/* Set Bit counter                                                      */
		/************************************************************************/

		// EN1_OS & EN2_OS
		BitClear(fixedCtrCtrlReg, 8); BitClear(fixedCtrCtrlReg, 4);
		// EN1_Usr & EN2_Usr
		BitClear(fixedCtrCtrlReg, 5); BitClear(fixedCtrCtrlReg, 9);
		// Per core AnyThread_EN1 & AnyThread_EN2
		BitClear(fixedCtrCtrlReg, 2); BitClear(fixedCtrCtrlReg, 6);


		// - Set the fixed counter bits
		if(	myRing.Wrmsr(IA32_FIXED_CTR_CTRL, fixedCtrCtrlReg))
		{
			//printf("Clear IA32_FIXED_CTR_CTRL OK.\n");
			result = TRUE;
		}
		else
		{	 
			DebugPrint(_T("CPU::ClearFixedCounterBits(): Write IA32_FIXED_CTR_CTRL fail"));
			result = FALSE;
		}
	}
	else
	{ 
		DebugPrint(_T("CPU::ClearFixedCounterBits(): Read IA32_FIXED_CTR_CTRL fail"));
		result = FALSE;
	} 

	return result; 
}


/// <summary>
/// Gets the current MSR frequency.
/// </summary>
/// <returns></returns>
UINT64 CPU::GetCurrentMsrFrequency()
{
	return BasicInfo.BusFrequency * GetCurrentMultiplier(BasicInfo.Model, 0);
}

/// <summary>
/// Gets the current MSR frequency.
/// </summary>
/// <param name="multiplier">The multiplier.</param>
/// <returns></returns>
UINT64 CPU::GetCurrentMsrFrequency(DWORD multiplier)
{
	return BasicInfo.BusFrequency * multiplier;
}

/// <summary>
/// Gets the current MSR frequency in string formated.
/// </summary>
/// <returns></returns>
LPWSTR CPU::GetCurrentMsrFrequencyStr()
{
	LPWSTR szBuff = new TCHAR[100];
	DWORD multiplier= (DWORD)GetCurrentMultiplier(BasicInfo.Model, 0);
	UINT64 freq = (BasicInfo.BusFrequency * multiplier) / 1000000;
	swprintf(szBuff, 100, L" %d|%d|%d   ", freq, multiplier ,  DWORD(BasicInfo.BusFrequency/1000000));
	return szBuff;
}

/// <summary>
/// Gets the current cpu multiplier.
/// </summary>
/// <param name="model">The model.</param>
/// <param name="coreIdx">Index of the core.</param>
/// <returns></returns>
DOUBLE CPU::GetCurrentMultiplier(DWORD model, UINT coreIdx)
{
	DOUBLE multiplier=0;
	UINT eax, edx;
	if(myRing.RdmsrTx(IA32_PERF_STATUS, eax, edx, coreIdx))
	{
		// UInt8 currentMultiplier = (rdmsr64(MSR_IA32_PERF_STS) >> 8);
		if(model==NEHALEM)
		{
			multiplier = eax & 0xff; 
		}
		else if(model==SANDY_BRIDGE ||model==IVY_BRIDGE ||model==HASWELL)
		{
			multiplier = (eax >> 8) & 0xff; 
		}
		else
		{
			multiplier = ((eax >> 8) & 0x1f) + 0.5 * ((eax >> 14) & 1); 
		}
	}
	else
	{
		DebugPrint(_T("CPU::GetCurrentMultiplier(): Read IA32_PERF_STATUS fail"));
	}
	return multiplier;
}

/// <summary>
/// Gets the current CPU multiplier in string formated.
/// </summary>
/// <returns></returns>
LPWSTR CPU::GetMultiplierStr()
{
	// Need to call GetPlatformInfo() & GetTurboLimit() first 

	LPWSTR szPlatformInfo = new TCHAR[255];
	wsprintfW(szPlatformInfo, L"Min: %d, Max: %d, Turbo: %d", 
		PlatformInfo.MaximumEfficiencyRatio, 
		PlatformInfo.MaximumNonTurboRatio,
		PlatformInfo.TurboLimitRatio[9]);
	return szPlatformInfo;
}

/// <summary>
/// Gets the time stamp counter.
/// </summary>
/// <param name="coreIdx">Index of the core.</param>
/// <returns></returns>
UINT64 CPU::GetTimeStampCounter(UINT coreIdx)
{
	UINT64 reg = 0;
	UINT64 Value = 0;
	if(	myRing.RdmsrTx(IA32_TIME_STAMP_COUNTER, reg, coreIdx))
	{
		Value = BitExtractUInt64(reg, 0, 63); 
	}
	else
	{ 
		DebugPrint(_T("CPU::GetTimeStampCounter(): Read IA32_TIME_STAMP_COUNTER fail"));
	}
	return Value;
}

/// <summary>
/// Gets the current cycles frequency.
/// </summary>
VOID CPU::GetCurrentCyclesFrequency()
{   
	UINT64 reg;   
	UINT coreIdx = 0;
	// Unhalted Cycles Core 
	//////////////////////////////////////////////////////////////////////////
	if(myRing.RdmsrTx(IA32_APERF, reg, coreIdx))  // UCC 
		FrequencyStatus.Cycles.C0[1].ACNT = BitExtractUInt64(reg, 0, 63);
	else
		DebugPrint(_T("CPU::GetCurrentCyclesFrequency(): Read IA32_APERF fail"));

	// Reference Cycles
	//////////////////////////////////////////////////////////////////////////
	if(myRing.RdmsrTx(IA32_MPERF, reg, coreIdx))// URC 
		FrequencyStatus.Cycles.C0[1].MCNT = BitExtractUInt64(reg, 0, 63);
	else 
		DebugPrint(_T("CPU::GetCurrentCyclesFrequency(): Read IA32_MPERF fail"));

	// Time stamp counter
	//////////////////////////////////////////////////////////////////////////
	if(myRing.RdmsrTx(IA32_TIME_STAMP_COUNTER, reg, coreIdx))
		FrequencyStatus.Cycles.TSC[1] = BitExtractUInt64(reg, 0, 63); 
	else 
		DebugPrint(_T("CPU::GetCurrentCyclesFrequency(): Read IA32_TIME_STAMP_COUNTER fail"));

	// Delta ACNT
	//////////////////////////////////////////////////////////////////////////
	if(FrequencyStatus.Cycles.C0[1].ACNT > FrequencyStatus.Cycles.C0[0].ACNT)
		FrequencyStatus.Delta.C0.ACNT = FrequencyStatus.Cycles.C0[1].ACNT - FrequencyStatus.Cycles.C0[0].ACNT;
	else
		FrequencyStatus.Delta.C0.ACNT = FrequencyStatus.Cycles.C0[0].ACNT - FrequencyStatus.Cycles.C0[1].ACNT;

	// Delta MCNT
	//////////////////////////////////////////////////////////////////////////
	if(FrequencyStatus.Cycles.C0[1].MCNT > FrequencyStatus.Cycles.C0[0].MCNT)
		FrequencyStatus.Delta.C0.MCNT = FrequencyStatus.Cycles.C0[1].MCNT - FrequencyStatus.Cycles.C0[0].MCNT;
	else 
		FrequencyStatus.Delta.C0.MCNT = FrequencyStatus.Cycles.C0[0].MCNT - FrequencyStatus.Cycles.C0[1].MCNT; 

	FrequencyStatus.Delta.TSC = FrequencyStatus.Cycles.TSC[1] - FrequencyStatus.Cycles.TSC[0];

	//////////////////////////////////////////////////////////////////////////


	// Computes average core frequency when not in powersaving C0-state 
	// (also taking Intel Turbo Boost technology into account) 
	// return Fraction of nominal frequency (if >1.0 then Turbo was working during the measurement)
	// activeRelativeFreqRatio = ACNT / MCNT  
	//if(States.Delta.C0.MCNT!=0)
	//	States.ActiveRelativeRatio = DOUBLE(States.Delta.C0.ACNT) / DOUBLE(States.Delta.C0.MCNT); // relative ratio
	//else 
	//	States.ActiveRelativeRatio = 0;

	if(FrequencyStatus.Cycles.C0[1].MCNT!=0)
		FrequencyStatus.ActiveRelativeRatio = DOUBLE(FrequencyStatus.Cycles.C0[1].ACNT) / DOUBLE(FrequencyStatus.Cycles.C0[1].MCNT); // relative ratio
	else 
		FrequencyStatus.ActiveRelativeRatio = 0;

	// Computes average core frequency also taking Intel Turbo Boost technology into account 
	//  fraction of nominal frequency
	// relativeFreqRatio = ACNT / TSC 
	//States.RelativeRatio = DOUBLE(States.Delta.C0.ACNT) / DOUBLE(States.Delta.TSC); 
	FrequencyStatus.RelativeRatio = DOUBLE(FrequencyStatus.Cycles.C0[1].ACNT) / DOUBLE(FrequencyStatus.Cycles.TSC[1]); 

	// Computes average core frequency when NOT IN powersaving C0-state 
	// (also taking Intel Turbo Boost technology into account)
	// activeAverageFreq = tscFreq * (ACNT / MCNT) = tscFreq * activeRelativeFreq 
	FrequencyStatus.ActiveAverageFrequency = FrequencyStatus.NominalFrequency * FrequencyStatus.ActiveRelativeRatio;

	// Computes average core frequency also taking Intel Turbo Boost technology into account 
	// averageFreq = tscFreq * (ACNT / TSC) = tscFreq * relativeFreq;  
	FrequencyStatus.AverageFrequency = FrequencyStatus.NominalFrequency * FrequencyStatus.RelativeRatio;  

	// Idle percent
	FrequencyStatus.PercentHalted = 100 * (1.0 - FrequencyStatus.RelativeRatio); 
	// Busy percent
	//States.PercentUnhalted = (DOUBLE(States.Delta.C0.ACNT) * 100) / DOUBLE(States.Delta.C0.MCNT);
	FrequencyStatus.PercentUnhalted = (DOUBLE(FrequencyStatus.Cycles.C0[1].ACNT) * 100) / DOUBLE(FrequencyStatus.Cycles.C0[1].MCNT);

	//States.TscFrequency = GetCurrentTscFrequency();
	//////////////////////////////////////////////////////////////////////////
	FrequencyStatus.Cycles.C0[0].ACNT = FrequencyStatus.Cycles.C0[1].ACNT;  
	FrequencyStatus.Cycles.C0[0].MCNT = FrequencyStatus.Cycles.C0[1].MCNT;  
	FrequencyStatus.Cycles.TSC[0] = FrequencyStatus.Cycles.TSC[1];  
	//////////////////////////////////////////////////////////////////////////


	/************************************************************************/
	/* Idle                                                                 */
	/************************************************************************/
	// https://software.intel.com/en-us/articles/measuring-the-halted-state/
	// The halted state refers to the state in which the CPU is NOT running. 
	// Idle time, as measured by the operating system, will be greater than or equal to halted time. 
	// The CPU can be in a lower power state when it is halted. The more the CPU is halted, 
	// the more likely it is that the CPU enters deeper power saving states (such as C3 or C6).
	//
	//  %halted = 100 * 'halted cycles' / 'total cycles' 
	// 1. Total cycles = TSC_at_interval_end - TSC_at_interval_begin
	// 2. Total cycles = Elapsed_time_in_seconds * TSC_frequency
	//
	// One of the fixed counters, CPU_CLK_UNHALTED.REF, counts the 'reference cycles in the unhalted state'. 
	// CPU_CLK_UNHALTED.REF increments at the TSC (Time Stamp Counter) frequency while the CPU is unhalted.
	// The TSC frequency doesn't vary. We can compute 
	// %halted = 100 * (1.0 - (CPU_CLK_UNHALTED.REF / total_cycles))

	//DOUBLE totalCycles = TSC;
	//DOUBLE unhaltedCycles = ACNT; 
	//DOUBLE haltedPercent = 100 * (1.0 - (unhaltedCycles / totalCycles));

	/************************************************************************/
	/*  Busy                                                                */
	/************************************************************************/
	// Average frequency = TSC_frequency * (CPU_CLK_UNHALTED.THREAD / CPU_CLK_UNHALTED.REF)
	// https://software.intel.com/en-us/articles/measuring-the-average-unhalted-frequency
	// CPU_CLK_UNHALTED.REF ticks advances only in C0 state (normal active state operation). 
	// In all other C-states this counter is not incrementing
	// This means that this method of calculating average frequency works under load 
	// when a core does not enter power-saving C-states.

	//DOUBLE tscFreq = cpuBasicInfo.NominalFrequency; 
	//DOUBLE activeRelativeFreq = ACNT / MCNT
	//DOUBLE relativeFreq = ACNT / TSC
	//DOUBLE activeAverageFreq =  tscFreq * (ACNT / MCNT)
	//DOUBLE averageFreq = tscFreq * (ACNT / TSC)  
}

/// <summary>
/// Gets the core thermal status.
/// </summary>
VOID  CPU::GetCoreThermalStatus()
{  
	if(ThermalStatus.TjMax==0) { GetTjMax(); }

	UINT64 reg;   

	for (int coreIdx=0; coreIdx < BasicInfo.NumberOfPhysicalCore; coreIdx++)
	{ 
		if(myRing.RdmsrTx(IA32_THERM_STATUS, reg, coreIdx))
		{

			//Digital Readout (bits 22:16, RO) — Digital temperature reading in 1 degree Celsius relative to the TCC
			//activation temperature.
			//0: TCC Activation temperature,
			//1: (TCC Activation - 1) , etc. See the processor’s data sheet for details regarding TCC activation.
			//A lower reading in the Digital Readout field (bits 22:16) indicates a higher actual temperature.
			ThermalStatus.Core[coreIdx].DigitalReadout = BitExtractUInt64(reg, 16, 22);

			//Resolution in Degrees Celsius (bits 30:27, RO) — Specifies the resolution (or tolerance) of the digital
			//thermal sensor. The value is in degrees Celsius. It is recommended that new threshold values be offset from the
			//current temperature by at least the resolution + 1 in order to avoid hysteresis of interrupt generation.
			ThermalStatus.Core[coreIdx].ResolutionInDegreesCelsius = BitExtractUInt64(reg, 27, 30);

			// Reading Valid (bit 31, RO) — Indicates if the digital readout in bits 22:16 is valid. The readout is valid if
			// bit 31 = 1.
			ThermalStatus.Core[coreIdx].Valid = BitExtractUInt64(reg, 31, 31); 
			ThermalStatus.Core[coreIdx].TemperatureInDegreesCelsius = ThermalStatus.TjMax - ThermalStatus.TSlope * ThermalStatus.Core[coreIdx].DigitalReadout;
		}
		else
		{
			DebugPrint(_T("CPU::GetCoreThermalStatus(): Read IA32_THERM_STATUS fail"));
		}
	} 
}

/// <summary>
/// Gets the FSB frequency.
/// </summary>
/// <returns></returns>
DWORD CPU::GetFsbFrequency() 
{
	UINT64 reg;  
	DWORD FSB=0;
	if(myRing.Rdmsr(MSR_FSB_FREQ, reg))
	{
		// MSR_FSB_FREQ : Intel Core 2

		// Scaleable Bus Speed(RO)
		// This field indicates the intended scaleable bus clock speed for
		// processors based on Intel Core microarchitecture:
		// 101B: 100 MHz (FSB 400) = 5
		// 001B: 133 MHz (FSB 533) = 1
		// 011B: 167 MHz (FSB 667) = 3
		// 010B: 200 MHz (FSB 800) = 2
		// 000B: 267 MHz (FSB 1067) = 0
		// 100B: 333 MHz (FSB 1333) = 4

		// 133.33 MHz should be utilized if performing calculation with System Bus Speed when encoding is 001B.
		// 166.67 MHz should be utilized if performing calculation with System Bus Speed when encoding is 011B. 
		// 266.67 MHz should be utilized if performing calculation with System Bus Speed when encoding is 000B.
		// 333.33 MHz should be utilized if performing calculation with System Bus Speed when encoding is 100B.

		DWORD ScaleableBusSpeed = BitExtractUInt64(reg, 0, 2);

		switch (ScaleableBusSpeed)
		{
		case 5: // 101B
			FSB = 400;
			break;
		case 1: // 001B
			FSB = 533;
			break;
		case 3: // 011B
			FSB = 667;
			break;
		case 2: // 010B
			FSB = 800;
			break;
		case 0: // 000B
			FSB = 1067;
			break;
		case 4: // 100B
			FSB = 1067;
			break;
		default:
			FSB = 0;
			break;
		}
	}
	int aaa=0;
	return FSB; 
}

UINT CPU::GetFsbFrequency2()
{
	//msr = rdmsr(IA32_PERF_STS);
	//int busratio_min=(msr.lo >> 24) & 0x1f;
	//int busratio_max=(msr.hi >> (40-32)) & 0x1f;

	int max_states=8;
	int busratio_step=2;

	int coreIndex = 0;
	SetThreadAffinityMask(GetCurrentThread(), 1 << coreIndex);
	UINT eax=0, edx=0;
	if( myRing.Rdmsr(IA32_PERF_STATUS, eax, edx))
	{
		UINT busRatioMin = (edx >> 24) & 0x1f; // low
		UINT busRatioMax = (eax >> (40-32)) & 0x1f; // high

		UINT vid_min=edx & 0x3f;

		if( myRing.Rdmsr(MSR_IA32_PLATFORM_ID, eax, edx))
		{
			UINT vid_max=edx & 0x3f;

			int clock_max=100 * busRatioMax;
			int clock_min=100 * busRatioMin;


			int power_max=35000;
			int power_min=16000;
			int num_states=(busRatioMax-busRatioMin)/busratio_step;

			while (num_states > max_states-1) {
				busratio_step <<= 1;
				num_states >>= 1;
			}
			//printf("adding %x P-States between busratio %x and %x, incl. P0\n", num_states+1, busRatioMin, busRatioMax);

			int vid_step=(vid_max-vid_min)/num_states;
			int power_step=(power_max-power_min)/num_states;
			int clock_step=(clock_max-clock_min)/num_states;


			int current_busratio=busRatioMin+((num_states-1)*busratio_step);
			int current_vid=vid_min+((num_states-1)*vid_step);
			int current_power=power_min+((num_states-1)*power_step);
			int current_clock=clock_min+((num_states-1)*clock_step);
			int i;
			for (i=0;i<num_states; i++) {
				//len_ps += acpigen_write_PSS_package(current_clock /*mhz*/, current_power /*mW*/, 0 /*lat1*/, 0 /*lat2*/, (current_busratio<<8)|(current_vid) /*control*/, (current_busratio<<8)|(current_vid) /*status*/);
				current_busratio -= busratio_step;
				current_vid -= vid_step;
				current_power -= power_step;
				current_clock -= clock_step;
				int acccc=0;
			}

			int cc=0;
		} 
	} 

	//printf("Warning: No supported FSB frequency. Assuming 200MHz\n");
	return 200;
}
 
/// <summary>
/// RDTSC instruction — An instruction used to read the time-stamp counter.
/// </summary>
/// <returns></returns>
UINT64 CPU::RDTSC()
{
	UINT64 result = 0; 
	// Windows
#if _MSC_VER>= 1600
	result = __rdtsc();
#endif

	return result; 
}


/// <summary>
/// RDTSCPs this instance.
/// </summary>
/// <returns></returns>
UINT64 CPU::RDTSCP()
{
	UINT64 result = 0;

	// Windows
#if _MSC_VER>= 1600
	unsigned int Aux;
	result = __rdtscp(&Aux);
#endif

	return result;
}


/// <summary>
/// Gets the tick count RDTSCP.
/// </summary>
/// <param name="multiplier">The multiplier.</param>
/// <returns></returns>
UINT64 CPU::GetTickCountRDTSCP(UINT64 multiplier)
{
	return (multiplier * RDTSCP()) / GetNominalFrequency();
}

/// <summary>
/// Gets the nominal frequency.
/// </summary>
/// <returns></returns>
UINT64 CPU::GetNominalFrequency()
{
	// TODO: Get from CpuID 
	UINT coreIdx = 0;
	UINT64 before = 0, after = 0; 

	myRing.RdmsrTx(IA32_TIME_STAMP_COUNTER, before, coreIdx); 

	// Sleep for 100 ms 
	Sleep(100);

	myRing.RdmsrTx(IA32_TIME_STAMP_COUNTER, after, coreIdx); 

	return  (after-before);  
}


INT32 CPU::ToMilliseconds(INT32 Value)
{
	INT32 low, high;
	high = Value / 1000;
	low = Value % 1000;
	return (high * 976 + low * 976 / 1000);
}

INT32 CPU::ToMillijoules(INT32 Value)
{
	INT32 low, high;
	high = Value / 10000;
	low = Value % 10000;
	return (high * 153 + low * 153 / 10000);
}
LPWSTR CPU::GetFamilyName(DWORD family)
{
	switch(family)
	{
	case 6: 
		return L"Intel"; 
	}
	return L"Unknown";
}

/// <summary>
/// Gets the name of the cpu code.
/// </summary>
/// <param name="model">The model.</param>
/// <returns></returns>
LPWSTR CPU::GetCpuCodeName(DWORD model)
{
	switch(model)
	{
	case NEHALEM_EP:
	case NEHALEM:
		return L"Nehalem/Nehalem-EP";
	case ATOM_1:
		return L"Atom(tm)";
	case CLARKDALE:
		return L"Westmere/Clarkdale";
	case WESTMERE_EP:
		return L"Westmere-EP";
	case NEHALEM_EX:
		return L"Nehalem-EX";
	case WESTMERE_EX:
		return L"Westmere-EX";
	case SANDY_BRIDGE:
		return L"Sandy Bridge";
	case JAKETOWN:
		return L"Sandy Bridge-EP/Jaketown";
	case IVYTOWN:
		return L"Ivy Bridge-EP/EN/EX/Ivytown";
	case IVY_BRIDGE:
		return L"Ivy Bridge";
	case HASWELL:
		return L"Haswell";
	}
	return L"Unknown";
}


/// <summary>
/// Gets the cpu identifier infos.
/// </summary>
/// <param name="leaf">The leaf.</param>
/// <param name="info">The information.</param>
VOID CPU::GetCpuIdInfos(int leaf, CPU_ID & info)
{  
	__cpuid(info.array, leaf); 
}

/// <summary>
/// Gets the CPU current voltage.
/// </summary>
/// <returns></returns>
DOUBLE CPU::GetCurrentVoltage()
{ 
	//Intel Sandy brige, Core Voltage (R/O)  P-state core voltage can be computed by MSR_PERF_STATUS[37:32] * (float) 1/(2^13).
	//The Intel documentation is a little flaky and has a typo.
	//[47..32] is correct.
	//[37..32] is wrong.
	UINT64 reg = 0;
	DOUBLE currentVoltage = 0;
	if(myRing.Rdmsr(IA32_PERF_STATUS, reg))
		currentVoltage = DOUBLE(BitExtractUInt64(reg, 32, 47)) / DOUBLE(1 << 13);
	else
		DebugPrint(_T("CPU::GetCurrentVoltage(): Read IA32_PERF_STATUS fail"));

	return currentVoltage;
}

/// <summary>
/// Determines whether package thermal metrics available
/// </summary>
/// <param name="model">The model.</param>
/// <returns></returns>
BOOL CPU::IsPackageThermalMetricsAvailable(DWORD model)  
{
	return IsPackageEnergyMetricsAvailable(model);
}

/// <summary>
/// Determines whether package energy metrics available
/// </summary>
/// <param name="model">The model.</param>
/// <returns></returns>
BOOL CPU::IsPackageEnergyMetricsAvailable(DWORD model)  const
{
	return (  
		model == SANDY_BRIDGE 
		|| model == IVY_BRIDGE
		|| model == HASWELL  
		|| model == JAKETOWN
		); 
} 

/// <summary>
/// Determines whether cpu model supported.
/// </summary>
/// <param name="model">The model.</param>
/// <returns></returns>
BOOL CPU::IsCpuModelSupported(DWORD model)
{
	//return (
	//	model == NEHALEM
	//	|| model == SANDY_BRIDGE
	//	|| model == IVY_BRIDGE
	//	|| model == HASWELL
	//	|| model == CORE_1
	//	);


	return (   model == NEHALEM_EP
		|| model == NEHALEM_EX
		|| model == WESTMERE_EP
		|| model == WESTMERE_EX
		|| model == ATOM_1
		|| model == CLARKDALE
		|| model == SANDY_BRIDGE
		|| model == JAKETOWN
		|| model == IVY_BRIDGE
		|| model == HASWELL
		|| model == IVYTOWN
		);
} 

/// <summary>
/// Gets the current energy status.
/// </summary>
VOID CPU::GetCurrentEnergyStatus()
{   
	// Package energy
	////////////////////////////////////////////////////////////////////////// 
	EnergyStatus.Package.Status.Energy[1] = GetCurrentPackageEneryStatus();

	// Core energy
	for (int coreIdx=0; coreIdx < BasicInfo.NumberOfPhysicalCore; coreIdx++)
	{
		EnergyStatus.Core[coreIdx].Status.Energy[1] = GetCurrentPp0EneryStatus(coreIdx);  
	}

	// Time measure
	//////////////////////////////////////////////////////////////////////////
	LONGLONG nowInMs = stw.NowInMilliseconds();
	EnergyStatus.Package.Status.MeasureTime[1] = nowInMs;
	for (int coreIdx=0; coreIdx < BasicInfo.NumberOfPhysicalCore; coreIdx++)
	{  
		EnergyStatus.Core[coreIdx].Status.MeasureTime[1] = nowInMs; 
	}

	// Calculate delta package
	//////////////////////////////////////////////////////////////////////////
	EnergyStatus.Package.Delta.Energy = EnergyStatus.Package.Status.Energy[1] - EnergyStatus.Package.Status.Energy[0];
	EnergyStatus.Package.Delta.MeasureTime = EnergyStatus.Package.Status.MeasureTime[1] - EnergyStatus.Package.Status.MeasureTime[0];

	// Power & energy
	EnergyStatus.Package.Power = EnergyStatus.Package.Delta.Energy / (EnergyStatus.Package.Delta.MeasureTime / 1000); // unit in Second
	EnergyStatus.Package.Energy = EnergyStatus.Package.Delta.Energy;

	// Calculate delta core
	//////////////////////////////////////////////////////////////////////////
	for (int coreIdx=0; coreIdx < BasicInfo.NumberOfPhysicalCore; coreIdx++)
	{
		EnergyStatus.Core[coreIdx].Delta.Energy = EnergyStatus.Core[coreIdx].Status.Energy[1] - EnergyStatus.Core[coreIdx].Status.Energy[0];
		EnergyStatus.Core[coreIdx].Delta.MeasureTime = EnergyStatus.Core[coreIdx].Status.MeasureTime[1] - EnergyStatus.Core[coreIdx].Status.MeasureTime[0];

		// Power & energy
		EnergyStatus.Core[coreIdx].Power = EnergyStatus.Core[coreIdx].Delta.Energy / (EnergyStatus.Core[coreIdx].Delta.MeasureTime / 1000); // unit in Second
		EnergyStatus.Core[coreIdx].Energy = EnergyStatus.Core[coreIdx].Delta.Energy;
	}

	// Save current status
	EnergyStatus.Package.Status.Energy[0] = EnergyStatus.Package.Status.Energy[1];
	EnergyStatus.Package.Status.MeasureTime[0] = EnergyStatus.Package.Status.MeasureTime[1];
	for (int coreIdx=0; coreIdx < BasicInfo.NumberOfPhysicalCore; coreIdx++)
	{
		EnergyStatus.Core[coreIdx].Status.Energy[0] = EnergyStatus.Core[coreIdx].Status.Energy[1];
		EnergyStatus.Core[coreIdx].Status.MeasureTime[0] = EnergyStatus.Core[coreIdx].Status.MeasureTime[1];
	} 
}