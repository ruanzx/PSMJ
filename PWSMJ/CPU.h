// 
// Name: CPU.h
// Author: hieunt
// Description: Get information of processor by reading MSR 
//				RAPL: 2nd Generation Intel® Core™ Processor or later, older processors not supported
//

#pragma once 

#include "MsrDriver.h" 
#include "BitHelpers.h"
#include "StopWatch.h"
#include "Winternl.h" // NtQuerySystemInformation function

union CPU_ID
{
	int array[4];
	struct { int eax,ebx,ecx,edx; } reg ;
};  

 
/// <summary>
/// Brief Identifiers of supported CPU models
/// </summary>
enum  SUPPORT_MODEL_CPU
{
	CORE_1 = 15, // 0x15 - Intel Core 2 (65nm)
	CORE_2 = 23, // 0x17 - Intel Core 2 (45nm) 

	ATOM_1 = 28, // 0x1C - Intel Atom (45nm)
	ATOM_2 = 53,
	ATOM_CENTERTON = 54,
	ATOM_BAYTRAIL = 55,
	ATOM_AVOTON = 77,

	NEHALEM = 30, // 0x1E - Intel Core i5, i7 LGA1156 (45nm)
	NEHALEM_EP = 26, // 0x1A - Intel Core i7 LGA1366 (45nm)
	NEHALEM_EX = 46, // 0x2E - Intel Xeon Processor 7500 series (45nm)

	CLARKDALE = 37, // 0x25 - Intel Core i3, i5, i7 LGA1156 (32nm)
	WESTMERE_EP = 44, // 0x2C - Intel Core i7 LGA1366 (32nm) 6 Core 
	WESTMERE_EX = 47, // 0x2F - Intel Xeon Processor (32nm)

	SANDY_BRIDGE = 42, // 0x2A - Intel Core i5, i7 2xxx LGA1155 (32nm)
	JAKETOWN = 45, // 0x2D - Next Generation Intel Xeon, i7 3xxx LGA2011 (32nm)

	IVY_BRIDGE = 58, // 0x3A - Intel Core i5, i7 3xxx LGA1155 (22nm)
	IVYTOWN = 62, //0x3E - Intel Core i7 4xxx LGA2011 (22nm)

	HASWELL = 60, // 0x3C - Intel Core i5, i7 4xxx LGA1150 (22nm)
	HASWELL_ULT = 69, // 0x45
	HASWELL_2 = 70, // 0x46
	HASWELL_E = 63, // 0x3F - Intel Xeon E5-2600/1600 v3, Core i7-59xx , LGA2011-v3, Haswell-E (22nm)

	UNKNOWN
};

/// <summary>
/// DRAM power infos
/// </summary>
typedef struct DRAM_POWER_INFO
{ 
	DOUBLE ThermalSpecPower;
	DOUBLE MinimumPower;
	DOUBLE MaximumPower;
	DOUBLE MaximumTimeWindow; 

	DRAM_POWER_INFO()
	{ 
		ThermalSpecPower = 0;
		MinimumPower = 0;
		MaximumPower = 0;
		MaximumTimeWindow = 0; 
	}
}DRAM_POWER_INFO;

/// <summary>
/// 
/// </summary>
typedef struct DRAM_POWER_LIMIT
{ 
	DOUBLE PowerLimit;
	DWORD  EnablePowerLimit; 
	DOUBLE TimeWindowForPowerLimit; 
	DWORD  Lock; 

	DRAM_POWER_LIMIT()
	{ 
		PowerLimit = 0;
		EnablePowerLimit = 0; 
		TimeWindowForPowerLimit = 0; 
		Lock = 0; 
	}
}DRAM_POWER_LIMIT;

/// <summary>
/// CPU package power infos
/// </summary>
typedef struct PACKAGE_POWER_INFO
{ 
	DOUBLE ThermalSpecPower; // TDP
	DOUBLE MinimumPower;
	DOUBLE MaximumPower;
	DOUBLE MaximumTimeWindow;

	PACKAGE_POWER_INFO()
	{ 
		ThermalSpecPower = 0;
		MinimumPower = 0;
		MaximumPower = 0;
		MaximumTimeWindow = 0;
	}

}PACKAGE_POWER_INFO;

/// <summary>
/// CPU package power limit infos
/// </summary>
typedef struct PACKAGE_POWER_LIMIT
{ 
	DOUBLE PowerLimit1;
	DWORD  EnablePowerLimit1;
	DOUBLE ClampingLimitation1;
	DOUBLE TimeWindowForPowerLimit1;

	DOUBLE PowerLimit2;
	DWORD  EnablePowerLimit2;
	DOUBLE ClampingLimitation2;
	DOUBLE TimeWindowForPowerLimit2;
	DWORD  Lock;

	PACKAGE_POWER_LIMIT()
	{ 
		PowerLimit1 = 0;
		EnablePowerLimit1 = 0;
		ClampingLimitation1 = 0;
		TimeWindowForPowerLimit1 = 0;

		PowerLimit2 = 0;
		EnablePowerLimit2 = 0;
		ClampingLimitation2 = 0;
		TimeWindowForPowerLimit2 = 0;
		Lock = 0;
	}

}PACKAGE_POWER_LIMIT;

/// <summary>
/// CPU PP0 power infos
/// </summary>
typedef struct PP0_POWER_LIMIT
{ 
	DOUBLE  PowerLimit;
	DWORD   EnablePowerLimit;
	DOUBLE  ClampingLimitation;
	DOUBLE  TimeWindowForPowerLimit; 
	DWORD   Lock;

	PP0_POWER_LIMIT()
	{ 
		PowerLimit = 0;
		EnablePowerLimit = 0;
		ClampingLimitation = 0;
		TimeWindowForPowerLimit = 0; 
		Lock = 0;
	}

}PP0_POWER_LIMIT;

/// <summary>
/// CPU power unit infos
/// </summary>
typedef struct POWER_UNIT
{ 
	DOUBLE PowerUnit;   // Watts Per PowerUnit
	DOUBLE EnergyStatusUnit;  // Joules Per EnergyUnit
	DOUBLE TimeUnits;  

	POWER_UNIT()
	{ 
		PowerUnit = 0;   
		EnergyStatusUnit = 0; 
		TimeUnits = 0; 
	}
} POWER_UNIT;

/// <summary>
/// CPU Basic infos
/// </summary>
typedef struct BASIC_INFO
{ 
	DWORD  Model;
	DWORD  Family;
	DWORD  Stepping;
	DWORD  ApicID;
	DWORD  OriginalCpuModel;
	DWORD  PerfmonVersion;
	DWORD  CoreGenCounterNumMax;
	DWORD  CoreGenCounterWidth;
	DWORD  CoreFixedCounterNumMax;
	DWORD  CoreFixedCounterWidth; 
	UINT64 BusFrequency;
	UINT64 NominalFrequency;
	DWORD  NumberOfPhysicalCore;
	DWORD  NumberOfLogicalCore;
	DWORD  NumberOfThreadsPerPhysicalCore; 

	BASIC_INFO()
	{ 
		Model = 0;
		Family = 0;
		Stepping = 0;
		ApicID = 0;
		OriginalCpuModel = 0;
		PerfmonVersion = 0;
		CoreGenCounterNumMax = 0;
		CoreGenCounterWidth = 0;
		CoreFixedCounterNumMax = 0;
		CoreFixedCounterWidth = 0; 
		BusFrequency = 0;
		NominalFrequency = 0;
		NumberOfPhysicalCore = 0;
		NumberOfLogicalCore = 0;
		NumberOfThreadsPerPhysicalCore = 0; 
	}

}BASIC_INFO;

typedef struct MISC_INFO
{ 
	DWORD EnhancedIntelSpeedStepTechnologyEnable; //Thread (R/W)

	DWORD LimitCPUIDMaxval; // Thread (R/W) 
	//When set to 1 on processors that support Intel Turbo Boost
	//Technology, the turbo mode feature is disabled and the IDA_Enable
	//feature flag will be clear (CPUID.06H: EAX[1]=0).
	//When set to a 0 on processors that support IDA, CPUID.06H:
	//EAX[1] reports the processor’s support of turbo mode is enabled.
	//Note: the power-on default value is used by BIOS to detect
	//hardware support of turbo mode. If power-on default value is 1,
	//turbo mode is available in the processor. If power-on default value
	//is 0, turbo mode is not available.
	DWORD TurboModeDisable; // Package (R/W) 
}MISC_INFO;

typedef struct PLATFORM_INFO
{
	/************************************************************************/
	/*  MSR_PLATFORM_INFO                                                   */
	/************************************************************************/

	// The is the ratio of the frequency that invariant TSC runs at:  Frequency = ratio * 100 MHz.
	DWORD MaximumNonTurboRatio; 

	// 1, indicates that Programmable Ratio Limits for Turbo mode is enabled, 
	// 0, indicates Programmable Ratio Limits for Turbo mode is disabled. 
	DWORD ProgrammableRatioLimitForTurboMode;

	// 1, indicates that TDP Limits for Turbo mode are programmable,  
	// 0, indicates TDP Limit for Turbo mode is not programmable. 
	DWORD ProgrammableTDPLimitForTurboMode; 

	/* The is the minimum ratio (maximum efficiency) that the processor can operates, in units of 100MHz. */
	DWORD MaximumEfficiencyRatio; 

	/* Turbo multiplier */
	DWORD TurboLimitRatio[10];

	PLATFORM_INFO()
	{
		MaximumNonTurboRatio = 0;
		ProgrammableRatioLimitForTurboMode = 0;
		ProgrammableTDPLimitForTurboMode = 0;
		MaximumEfficiencyRatio = 0; 
	}
}PLATFORM_INFO;

typedef struct THERMAL_PACKAGE
{    
	DWORD  DigitalReadout;
	DWORD  TemperatureInDegreesCelsius;

	THERMAL_PACKAGE()
	{  
		DigitalReadout = 0;
		TemperatureInDegreesCelsius = 0;
	}
}THERMAL_PACKAGE;

typedef struct THERMAL_CORE
{  
	DWORD  DigitalReadout;
	DWORD  ResolutionInDegreesCelsius;
	DWORD  TemperatureInDegreesCelsius;
	BOOL   Valid; 
	BOOL   IsHot;

	THERMAL_CORE()
	{  
		DigitalReadout = 0;
		ResolutionInDegreesCelsius = 0;
		TemperatureInDegreesCelsius = 0;
		Valid = 0; 
		IsHot = 0;
	}
}THERMAL_CORE;

typedef struct THERMAL_STATUS
{
	THERMAL_PACKAGE  Package;
	THERMAL_CORE     *Core;   
	DWORD  TSlope;
	DWORD  TjMax;
}THERMAL_STATUS;

typedef	struct FREQUENCY_STATUS
{
	// Current[1] - Previous[0] 
	struct 
	{
		struct
		{
			UINT64 ACNT; // Unhalted Core (IA32_APERF -> C0_ACNT)
			UINT64 MCNT; // Reference Cycles (IA32_MPERF -> C0_MCNT)
		} C0[2]; 
		UINT64	TSC[2];
	} Cycles;

	struct {
		struct
		{
			UINT64 ACNT; // IA32_APERF
			UINT64 MCNT; // IA32_MPERF
		} C0; 
		UINT64 TSC;
	} Delta;

	DOUBLE  NominalFrequency;
	DOUBLE  TscFrequency;
	DOUBLE  RelativeRatio;
	DOUBLE	ActiveRelativeRatio; 
	DOUBLE  AverageFrequency;
	DOUBLE  ActiveAverageFrequency;
	DOUBLE  PercentHalted;
	DOUBLE  PercentUnhalted;
} FREQUENCY_STATUS;

typedef struct ENERGY_PACKAGE
{
	struct 
	{ 
		DOUBLE	Energy[2];  
		DOUBLE  MeasureTime[2];
	} Status; 

	struct 
	{  
		DOUBLE	Energy;  
		DOUBLE  MeasureTime;
	} Delta; 

	DOUBLE  Power;
	DOUBLE	Energy; 
}ENERGY_PACKAGE;

typedef struct ENERGY_CORE
{
	struct 
	{ 
		DOUBLE	Energy[2];  
		DOUBLE  MeasureTime[2];
	} Status; 

	struct 
	{  
		DOUBLE	Energy;  
		DOUBLE  MeasureTime;
	} Delta; 

	DOUBLE  Power;
	DOUBLE	Energy; 
}ENERGY_CORE;
 
typedef struct ENERGY_STATUS
{
	ENERGY_CORE    *Core;
	ENERGY_PACKAGE  Package; 
}ENERGY_STATUS;

class CPU 
{ 
public: 
	CPU();
	~CPU();
	BOOL IsDriverReady(); 

	VOID GetPowerInfos();

	BOOL GetBasicInfos();
	VOID GetPlatformInfo();
	BOOL GetTurboLimit();

	BOOL GetMiscInfos();

	BOOL SetTurboDisableMode(BOOL bEnable);
	BOOL SetSpeedStepMode(BOOL bEnable);
	DWORD GetFsbFrequency();

	VOID GetCurrentCyclesFrequency();
	UINT64 GetCurrentMsrFrequency();
	UINT64 GetCurrentMsrFrequency(DWORD multiplier);
	LPWSTR GetCurrentMsrFrequencyStr();
	DOUBLE GetCurrentMultiplier(DWORD cpuModel, UINT coreIdx);
	LPWSTR GetMultiplierStr(); 


	DWORD GetBusyPState(DWORD busyPercent, DWORD maximumNonTurboRatio);
	DWORD GetBusyPercent(UINT64 mPerf, UINT64 aPerf);

	BOOL SetPerfGlobalCounterBits();
	BOOL ClearPerfGlobalCounterBits();

	BOOL SetFixedCounterBits();
	BOOL ClearFixedCounterBits();

	DOUBLE GetCurrentTscFrequency(); 

	UINT GetFsbFrequency2();
	BOOL IsCpuModelSupported(DWORD model);  
	UINT64 GetTimeStampCounter(UINT core); // Get by Core
	UINT64 GetTimeStampCounterMultiplier();

	VOID GetCpuIdInfos(int leaf, CPU_ID & info); 
	BOOL IsPackageEnergyMetricsAvailable(DWORD model) const;
	BOOL IsPackageThermalMetricsAvailable(DWORD model);

	LPWSTR GetCpuCodeName(DWORD model);
	LPWSTR GetFamilyName(DWORD family);
	UINT64 RDTSC();
	UINT64 RDTSCP();
	UINT64 GetTickCountRDTSCP(UINT64 multiplier);
	UINT64 GetNominalFrequency();
	DOUBLE GetCurrentVoltage();
	DOUBLE GetCurrentPackageEneryStatus();
	DOUBLE GetCurrentPp0EneryStatus(UINT coreIdx);

	VOID  GetTjMax();
	VOID  GetCoreThermalStatus();
	VOID  GetPackageThermalStatus();

	INT32 ToMillijoules(INT32 Value);
	INT32 ToMilliseconds(INT32 Value); 

	VOID GetCurrentEnergyStatus();
	INT GetCpuUsage();


	/* Variables */ 
private:
	MsrDriver myRing;
	BOOL isPackageEnergyMetricsSupport; // RAPL power available or not?
	BOOL isCpuModelSupport; // RAPL power available or not?
public:
	StopWatch             stw;
	CPU_ID                IdInfo;
	BASIC_INFO            BasicInfo; 
	FREQUENCY_STATUS      FrequencyStatus;
	ENERGY_STATUS         EnergyStatus;
	THERMAL_STATUS        ThermalStatus;

	/************************************************************************/
	/*  MSR_PLATFORM_INFO                                                   */
	/************************************************************************/
	PLATFORM_INFO       PlatformInfo; 
	MISC_INFO           MiscInfo;

	/************************************************************************/
	/* RAPL (running average power limit)                                   */
	/************************************************************************/

	POWER_UNIT           PowerUnit;         /* MSR_RAPL_POWER_UNIT */ 
	PACKAGE_POWER_INFO   PkgPowerInfo;      /* MSR_PKG_POWER_INFO */ 
	PACKAGE_POWER_LIMIT  PkgPowerLimit;     /* MSR_PKG_POWER_LIMIT */ 
	PP0_POWER_LIMIT      Pp0PowerLimit; 	/* MSR_PPO_POWER_LIMIT */ 
	DRAM_POWER_INFO      DramPowerInfo;     /* MSR_PKG_POWER_INFO */ 
	DRAM_POWER_LIMIT     DramPowerLimit; 	/* MSR_DRAM_POWER_LIMIT */  

	DOUBLE	AccumulatedPackageThrottledTime; /* MSR_PKG_PERF_STATUS */ 
	DOUBLE	AccumulatedPP0ThrottledTime;  	/* MSR_PP0_PERF_STATUS */


};
