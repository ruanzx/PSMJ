// 
// Name: PowerSetting.cpp : implementation file 
// Author: hieunt
// Description:  Use for Windows Power Management 
//				 
//

#pragma once

#include "stdafx.h" 

// {4D92D9C1-579F-4F93-B882-9698B8A0AFA9}
static const GUID GUID_POWER_AWARE_SMJ =  { 0x4d92d9c1, 0x579f, 0x4f93, { 0xb8, 0x82, 0x96, 0x98, 0xb8, 0xa0, 0xaf, 0xa9 } }; 
static const LPWSTR GUID_POWER_AWARE_SMJ_NAME = L"Power Aware SMJ";
static const LPWSTR GUID_POWER_AWARE_SMJ_DESCRIPTION = L"Power Capping Sort Merge Join";

enum PowerSource
{
	Power_DC,
	Power_AC,
	Power_Undefined = 255
};

/************************************************************************/
/*  CPU infomations												        */
/************************************************************************/
typedef struct _PROCESSOR_POWER_INFORMATION 
{  
	/* The system processor number.*/
	ULONG Number; 

	/* The maximum specified clock frequency of the system processor, in megahertz. */
	ULONG MaxMhz;  

	/* The processor clock frequency, in megahertz. This number is the maximum 
	specified processor clock frequency multiplied by the current processor throttle. */
	ULONG CurrentMhz; 

	/* The limit on the processor clock frequency, in megahertz. 
	This number is the maximum specified processor clock frequency 
	multiplied by the current processor thermal throttle limit.   */
	ULONG MhzLimit;  

	/* The maximum idle state of this processor. */
	ULONG MaxIdleState;  

	/* The current idle state of this processor. */
	ULONG CurrentIdleState; 
} PROCESSOR_POWER_INFORMATION,  *PPROCESSOR_POWER_INFORMATION; 


//typedef struct CorePacking
//{
//	DWORD core;
//};

/************************************************************************/
/*  Hard Disk Settings    (GUID_DISK_SUBGROUP)                          */
/************************************************************************/
typedef struct HardDiskSettings
{   
	// Specifies (in seconds) how long we wait after the last disk access
	// before we power off the disk. 
	// GUID_DISK_POWERDOWN_TIMEOUT
	// Change to 0 mean NEVER
	DWORD PowerdownTimeout;

	// Specifies the amount of contiguous disk activity time to ignore when
	// calculating disk idleness.
	// GUID_DISK_BURST_IGNORE_THRESHOLD
	DWORD BurstIgnoreThreshold;

} HardDiskSettings;

/************************************************************************/
/*  Video Settings                                                      */
/*  Specifies the subgroup which will contain all of the video          */
/*  settings for a single policy.										*/
/*  GUID_VIDEO_SUBGROUP                                                 */
/************************************************************************/
typedef struct VideoSettings
{
	// Specifies (in seconds) how long we wait after the last user input has been
	// recieved before we power off the video.
	// GUID_VIDEO_POWERDOWN_TIMEOUT
	DWORD PowerdownTimeout;

	// Specifies whether adaptive display dimming is turned on or off.
	// GUID_VIDEO_ANNOYANCE_TIMEOUT
	DWORD AnnoyanceTimeout;

	// Specifies how much adaptive dim time out will be increased by.
	// GUID_VIDEO_ADAPTIVE_PERCENT_INCREASE
	DWORD AdaptivePercentIncrease;

	// Specifies (in seconds) how long we wait after the last user input has been
	// recieved before we dim the video.
	// GUID_VIDEO_DIM_TIMEOUT
	DWORD DimTimeout; 

	// Specifies if the operating system should use adaptive timers (based on
	// previous behavior) to power down the video,
	// GUID_VIDEO_ADAPTIVE_POWERDOWN
	DWORD AdaptivePowerdown;

	// Monitor brightness policy when in normal state
	// {aded5e82-b909-4619-9949-f5d71dac0bcb}
	// GUID_DEVICE_POWER_POLICY_VIDEO_BRIGHTNESS 
	DWORD VideoBrightness;

	// Monitor brightness policy when in dim state
	// {f1fbfde2-a960-4165-9f88-50667911ce96}
	// GUID_DEVICE_POWER_POLICY_VIDEO_DIM_BRIGHTNESS
	DWORD VideoDimBrightness; 

	// Specifies if the operating system should use ambient light sensor to change
	// disply brightness adatively.
	// {FBD9AA66-9553-4097-BA44-ED6E9D65EAB8}
	// GUID_VIDEO_ADAPTIVE_DISPLAY_BRIGHTNESS 
	DWORD AdaptiveDisplayBrightness;


} VideoSettings;

/************************************************************************/
/*  CPU Idle states                                                     */
/************************************************************************/
typedef struct ProcessorCStates
{
	// Specifies processor power settings for CState policy data
	//DWORD IdleStatePolicy;

	// Specifies if idle state promotion and demotion values should be scaled based on the current peformance state
	// Value:
	// 1 = Enable
	// 0 = Disable
	DWORD AllowScaling;

	// Specifies if idle states should be disabled
	// Value:
	// 0 = Disable
	// 1 = Enable
	DWORD IdleDisable;

	// Specifies the deepest idle state type that should be used. 
	// If this value is set to zero, this setting is ignored. 
	// Values higher than supported by the processor then this setting has no effect.
	//DWORD StateMaximum;

	// Specifies the time that elapsed since the last idle state promotion 
	// or demotion before idle states may be promoted or demoted again (in microseconds).
	// Value: 1 microseconds -> 200000 microseconds
	DWORD TimeCheck; //µ

	// Specifies the upper busy threshold that must be met before 
	// demoting the processor to a lighter idle state (in percentage).
	// Value range: 0% - 100%
	DWORD DemoteThreshold; 

	// Specifies the lower busy threshold that must be met before 
	// promoting the processor to a deeper idle state (in percentage).
	// Value range: 0% - 100%;
	DWORD PromoteThreshold; 


} ProcessorCStates;

/************************************************************************/
/*  CPU Performance states                                              */
/************************************************************************/
typedef struct ProcessorPStates
{
	// Specifies processor power settings for PerfState policy data
	//DWORD PerfStatePolicy;

	// Specifies the increase busy percentage threshold that must be met before increasing the processor performance state.
	// Value range: 0% - 100%
	DWORD IncreaseThreshold;

	// Specifies the decrease busy percentage threshold that must be met before decreasing the processor performance state.
	// Value range: 0% - 100%
	DWORD DecreaseThreshold; 

	// Specifies, either as ideal, single or rocket, how aggressive performance states should be selected 
	// when increasing the processor performance state.
	// Value: 
	//   Ideal (0): A target performance state is selected based on calculating which performance state 
	//          decreases the processor utilization to just below the value of the Processor Performance Increase Threshold setting.
	//   Single (1): The next highest performance state (compared to the current performance state) is selected
	//   Rocket (2): The highest performance state is selected.
	DWORD IncreasePolicy;

	// Specifies, either as ideal, single or rocket, how aggressive performance states should 
	// be selected when decreasing the processor performance state.
	// Value:
	//   Ideal (0): A target performance state is selected based on calculating which performance state 
	//              increases the processor utilization to just above the value of the Processor Performance Decrease Threshold setting.
	//   Single (1): The next lowest performance state (compared to the current performance state) is selected.
	//   Rocket (2): The lowest performance state is selected. 
	DWORD DecreasePolicy;


	// Specifies the time, in milliseconds, that must expire before considering a change in the processor performance states or parked core set.
	DWORD TimeCheck; // Value range: 1ms - 5000ms
	// Specifies, in milliseconds, the minimum amount of time that must elapse after 
	// the last processor performance state change before increasing the processor performance state.
	// Value range: 1ms - 100ms;
	DWORD IncreaseTime; 

	// Specifies, in milliseconds, the minimum amount of time that must elapse after 
	// the last processor performance state change before increasing the processor performance state.
	// Value range: 1ms - 100ms;
	DWORD DecreaseTime; 

	// Specifies how the processor should manage performance and efficiency tradeoffs when boosting frequency above the maximum.
	// Value:
	//   PROCESSOR_PERF_BOOST_POLICY_DISABLED = 0
	//   PROCESSOR_PERF_BOOST_POLICY_MAX  = 100
	DWORD BoostPolicy;


	// Specifies how a processor opportunistically increases frequency above the maximum when operating contitions allow it to do so safely.
	// Value:
	//   PROCESSOR_PERF_BOOST_MODE_DISABLED = 0
	//   PROCESSOR_PERF_BOOST_MODE_ENABLED = 1
	//   PROCESSOR_PERF_BOOST_MODE_AGGRESSIVE = 2
	//   PROCESSOR_PERF_BOOST_MODE_EFFICIENT_ENABLED = 3
	//   PROCESSOR_PERF_BOOST_MODE_EFFICIENT_AGGRESSIVE = 4
	//   PROCESSOR_PERF_BOOST_MODE_MAX = PROCESSOR_PERF_BOOST_MODE_EFFICIENT_AGGRESSIVE = 4  
	// DWORD BoostMode;

} ProcessorPStates;


class PowerSettings 
{ 
public:
	/************************************************************************/ 
	// Specifies a percentage (between 0 and 100) that the processor frequency
	// should not drop below.  For example, if this value is set to 50, then the
	// processor frequency will never be throttled below 50 percent of its
	// maximum frequency by the system. 
	/************************************************************************/ 
	DWORD MinThrottle;  // Value range; 0% - 100%

	/************************************************************************/
	// Specifies a percentage (between 0 and 100) that the processor frequency
	// should never go above.  For example, if this value is set to 80, then
	// the processor frequency will never be throttled above 80 percent of its
	// maximum frequency by the system.
	/************************************************************************/
	DWORD MaxThrottle; // Value range; 0% - 100%

	/************************************************************************/
	// Processor Performance Control Policy Constants
	// The processor performance control policy constants indicate the processor 
	// performance control algorithm applied to a power scheme. 
	// http://msdn.microsoft.com/en-us/library/windows/desktop/aa373183(v=vs.85).aspx
	// PO_THROTTLE_ADAPTIVE (Value = 3)
	// PO_THROTTLE_CONSTANT (Value = 1)
	// PO_THROTTLE_DEGRADE (Value = 2)
	// PO_THROTTLE_NONE (Value = 0)
	/************************************************************************/
	DWORD ThrottlePolicyAc;

	/************************************************************************/
	// Throttle states can be used. However, the processor throttle state does not change adaptively. 
	// When enabled, the Minimum Processor State and Maximum Processor State settings can be used to 
	// lock the system processors into a specific processor throttle state.                                                                     */
	/************************************************************************/
	DWORD AllowThrottling; 

	// Specifies active vs passive cooling.  Although not directly related to
	// processor settings, it is the processor that gets throttled if we're doing
	// passive cooling, so it is fairly strongly related.
	// {94D3A615-A899-4AC5-AE2B-E4D8F634367F}
	// GUID_SYSTEM_COOLING_POLICY 
	// Value:
	// 0 Passive The system reduces the processor performance before it enables active cooling features such as fans.
	// 1 Active The system enables active cooling features such as fans before it reduces the processor performance. 
	DWORD SytemCoolingPolicy;

	ProcessorPStates PerfState;
	ProcessorCStates IdleState;
	HardDiskSettings Disk;
	VideoSettings    Video;

	LPWSTR SchemeName;
	LPWSTR SchemeDescription;

	PowerSettings()
	{ 
		MinThrottle = 0;
		MaxThrottle = 0; 
		AllowThrottling = 0; // disable
		ThrottlePolicyAc = PO_THROTTLE_NONE;
		SchemeName = new TCHAR[255];
		SchemeDescription = new TCHAR[255];
	}

	~PowerSettings()
	{

	} 
private:
	/* Variables */ 

};
 