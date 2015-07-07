// 
// Name: PowerSchemes.cpp : implementation file 
// Author: hieunt
// Description:  Use for Windows Power Management 
//				 We can change power policy and CPU frequency by using WPM
//

#include "stdafx.h"
#include "PowerSchemes.h"

/// <summary>
/// Initializes a new instance of the <see cref="PowerSchemes"/> class.
/// </summary>
PowerSchemes::PowerSchemes()
{    
	/************************************************************************/
	/*  System infomations                                                  */
	/************************************************************************/
	GetSystemInfo(&sysInfo); 

	/************************************************************************/
	/*  Power plan infomations                                              */
	/************************************************************************/
	mySchemeGuid = new GUID; 
	*mySchemeGuid = GUID_POWER_AWARE_SMJ;

	mySchemeName = new TCHAR[MAX_PATH]; 
	mySchemeName = GUID_POWER_AWARE_SMJ_NAME; 

	mySchemeDescription = new TCHAR[MAX_PATH];
	mySchemeDescription = GUID_POWER_AWARE_SMJ_DESCRIPTION;  

	// Power status check
	isPsValid = FALSE; 
}

/// <summary>
/// Finalizes an instance of the <see cref="PowerSchemes"/> class.
/// </summary>
PowerSchemes::~PowerSchemes()
{   
	delete mySchemeName;
	delete mySchemeDescription;
} 

/// <summary>
/// Applies the custom power plan.
/// </summary>
/// <returns></returns>
BOOL PowerSchemes::ApplyCustomPowerPlan()
{
	BOOL bSuccess = TRUE;

	/************************************************************************/
	/* Create power request                                                 */
	/************************************************************************/   
	powerReasonContext = new REASON_CONTEXT;
	powerReasonContext->Version = POWER_REQUEST_CONTEXT_VERSION;
	powerReasonContext->Flags = POWER_REQUEST_CONTEXT_SIMPLE_STRING; // POWER_REQUEST_CONTEXT_DETAILED_STRING
	powerReasonContext->Reason.SimpleReasonString = L"Power capping for SMJ";

	hPowerRequest = PowerCreateRequest(powerReasonContext);

	// The calling process continues to run instead of being suspended or terminated 
	// by process lifetime management mechanisms. When and how long the process 
	// is allowed to run depends on the operating system and power policy settings.
	//
	// Value: 
	//  PowerRequestDisplayRequired   : The display remains on even if there is no user input for an extended period of time
	//  PowerRequestSystemRequired    : The system continues to run instead of entering sleep after a period of user inactivity
	//  PowerRequestAwayModeRequired  : Continues to run but turns off audio and video to give the appearance of sleep
	//  PowerRequestExecutionRequired : (win 8 or later)The calling process continues to run 
	//                                  instead of being suspended or terminated by process 
	//                                  lifetime management mechanisms
	if(!PowerSetRequest(hPowerRequest, PowerRequestSystemRequired)) 
	{
		//printf("PowerSetRequest failed\n"); 
	}

	DWORD mySchemeIndex;
	*mySchemeGuid = GUID_POWER_AWARE_SMJ;
	if( !IsPowerSchemeExist(*mySchemeGuid, mySchemeIndex))
	{
		/* Not exist */
		bSuccess&=CreateCustomPowerPlan();
	} 

	/************************************************************************/
	/*  Apply new power scheme                                              */
	/************************************************************************/
	bSuccess&=ApplyPowerScheme(*mySchemeGuid);

	bSuccess&=GetCurrentPowerSettings(); 

	return bSuccess;
}

/// <summary>
/// Delete the custom power plan.
/// </summary>
VOID PowerSchemes::ClearCustomPowerPlan()
{
	if(!PowerClearRequest(hPowerRequest, PowerRequestSystemRequired))
	{
		//printf("PowerClearRequest failed\n");
	}
	CloseHandle(hPowerRequest); 

	*mySchemeGuid = GUID_MIN_POWER_SAVINGS;
	ApplyPowerScheme(*mySchemeGuid);

	/************************************************************************/
	/* Second: Delete application power scheme                                     */
	/************************************************************************/
	if(!DeleteScheme(GUID_POWER_AWARE_SMJ))
	{
		//printf("DeleteScheme failed\n");
	} 
}


/// <summary>
/// Creates the custom power plan.
/// </summary>
/// <returns></returns>
BOOL PowerSchemes::CreateCustomPowerPlan()
{
	BOOL bSuccess = TRUE;

	/* Duplicates an existing power scheme. */  
	*mySchemeGuid = GUID_POWER_AWARE_SMJ;
	mySchemeName = GUID_POWER_AWARE_SMJ_NAME;
	mySchemeDescription = GUID_POWER_AWARE_SMJ_DESCRIPTION;
	//swprintf(mySchemeName, MAX_PATH, L"%s", L"Power Aware SMJ");  

	DWORD errorCode = PowerDuplicateScheme(NULL, &GUID_MIN_POWER_SAVINGS, &mySchemeGuid);
	switch (errorCode)
	{
	case ERROR_INVALID_PARAMETER:
		//printf("PowerDuplicateScheme ERROR_INVALID_PARAMETER\n");
		break;
	case ERROR_SUCCESS:
		//printf("PowerDuplicateScheme OK\n"); 
		break;
	case ERROR_ALREADY_EXISTS: 
		//printf("Power Plan already exists \n"); 
		break;
	default:
		break;
	}  

	if(!SetSchemeName(*mySchemeGuid, mySchemeName))
	{
		//printf("SetSchemeName failed\n");
		bSuccess = FALSE;
	} 

	if(!SetSchemeDescription(*mySchemeGuid, mySchemeDescription))
	{
		//printf("SetSchemeDescription failed\n");
		bSuccess = FALSE;
	} 

	return bSuccess;
}

/// <summary>
/// Restores the individual default power scheme settings.
/// </summary>
/// <returns></returns>
BOOL PowerSchemes::RestoreIndividualDefaultPowerSchemeSettings()
{
	/*   GUID_MIN_POWER_SAVINGS      = High performance                        */
	/*   GUID_MAX_POWER_SAVINGS      = Power saver                             */
	/*   GUID_TYPICAL_POWER_SAVINGS  = Balanced                                */
	BOOL IsSuccess = FALSE; 

	if(CompareScheme(*mySchemeGuid, GUID_MIN_POWER_SAVINGS))
	{ 
		IsSuccess = PowerRestoreIndividualDefaultPowerScheme(&GUID_MIN_POWER_SAVINGS); 
	}

	else if(CompareScheme(*mySchemeGuid, GUID_MAX_POWER_SAVINGS))
	{ 
		IsSuccess = PowerRestoreIndividualDefaultPowerScheme(&GUID_MAX_POWER_SAVINGS); 
	}

	else if(CompareScheme(*mySchemeGuid, GUID_TYPICAL_POWER_SAVINGS))
	{ 
		IsSuccess = PowerRestoreIndividualDefaultPowerScheme(&GUID_TYPICAL_POWER_SAVINGS); 
	}

	else if(CompareScheme(*mySchemeGuid, GUID_POWER_AWARE_SMJ))
	{ 
		IsSuccess = PowerRestoreIndividualDefaultPowerScheme(&GUID_MIN_POWER_SAVINGS); 
	} 

	return IsSuccess;
}

/// <summary>
/// Gets the current setting for display.
/// </summary>
/// <returns></returns>
BOOL PowerSchemes::GetCurrentSettingForDisplay()
{
	BOOL bSuccess = TRUE;
	bSuccess &= GetActiveScheme(mySchemeGuid); 
	bSuccess &= GetCurrentPowerSettings(); 
	return bSuccess;
}


/// <summary>
/// Gets the name of the throttle policy.
/// </summary>
/// <param name="throttle">The throttle ID.</param>
/// <returns></returns>
LPWSTR PowerSchemes::GetThrottleName(DWORD throttle)
{
	switch (throttle)
	{
	case PO_THROTTLE_NONE:

		// No processor performance control is applied. 
		// This policy always runs the processor at its highest possible performance level. 
		// This policy will not engage processor clock throttling, except in response to thermal events. 
		return L"None"; 
	case PO_THROTTLE_CONSTANT:
		// Does not allow the processor to use any high voltage performance states. 
		// This policy will not engage processor clock throttling, except in response to thermal events. 
		return L"Constant"; 
	case PO_THROTTLE_DEGRADE:
		// Does not allow the processor to use any high voltage performance states. 
		// This policy will engage processor clock throttling when the battery is below a certain threshold, 
		// if the C3 state is not being utilized, or in response to thermal events.
		return L"Degrade"; 
	case PO_THROTTLE_ADAPTIVE:
		// Attempts to match the performance of the processor to the current demand. 
		// This policy will use both high and low voltage and frequency states. 
		// This policy will lower the performance of the processor to the lowest voltage
		// available whenever there is insufficient demand to justify a higher voltage. 
		// This policy will engage processor clock throttling if the C3 state 
		// is not being utilized, and in response to thermal events.
		return L"Adaptive";
	default:
		return L"Unknown"; 
	} 
} 

/// <summary>
/// Gets the name of the policy.
/// </summary>
/// <param name="policy">The policy.</param>
/// <returns></returns>
LPWSTR PowerSchemes::GetPolicyName(DWORD policy)
{
	// IncreasePolicy:
	//   Ideal (0): A target performance state is selected based on calculating which performance state 
	//          decreases the processor utilization to just below the value of the Processor Performance Increase Threshold setting.
	//   Single (1): The next highest performance state (compared to the current performance state) is selected
	//   Rocket (2): The highest performance state is selected.

	// DecreasePolicy
	//   Ideal (0): A target performance state is selected based on calculating which performance state 
	//              increases the processor utilization to just above the value of the Processor Performance Decrease Threshold setting.
	//   Single (1): The next lowest performance state (compared to the current performance state) is selected.
	//   Rocket (2): The lowest performance state is selected. 
	switch (policy)
	{
	case PERFSTATE_POLICY_CHANGE_IDEAL:
		return L"Ideal"; 
	case PERFSTATE_POLICY_CHANGE_SINGLE:
		return L"Single"; 
	case PERFSTATE_POLICY_CHANGE_ROCKET:
		return L"Rocket"; 
	default:
		return L"Unknown"; 
	}   
}

/// <summary>
/// Determines whether os is supported
/// </summary>
/// <returns></returns>
BOOL PowerSchemes::IsSupportOS()
{
	/*  
	// Get OS version 
	// http://msdn.microsoft.com/en-us/library/windows/desktop/ms724834(v=vs.85).aspx
	*/
	BOOL bSupport = FALSE;
	OSVERSIONINFO osInfo;
	memset(&osInfo, 0, sizeof(OSVERSIONINFO));
	osInfo.dwOSVersionInfoSize = sizeof(OSVERSIONINFO);
	GetVersionEx(&osInfo);

	if(osInfo.dwPlatformId==VER_PLATFORM_WIN32_NT)
	{
		/* The operating system is Windows 7, Windows Server 2008, 
		Windows Vista, Windows Server 2003, Windows XP, or Windows 2000 */

		bSupport = TRUE; 
	} 

	if (osInfo.dwMajorVersion >= 6)
	{ 
		/* Windows Vista or newer 
		Starting with Windows Vista, power management configuration of the system's processor is controlled
		through the GUID_PROCESSOR_SETTINGS_SUBGROUP power settings subgroup. 
		Use the PowerEnumerate function to enumerate individual settings. 
		*/

		bSupport = TRUE; 
	}
	else
	{
		bSupport = FALSE;
	}

	return bSupport;
}

/// <summary>
/// Cleanings this instance.
/// </summary>
/// <returns></returns>
BOOL PowerSchemes::Cleaning()
{ 

	/************************************************************************/
	/* First: Restore system power scheme                                         */
	/************************************************************************/


	return TRUE; 
}


/// <summary>
/// Schedules the cpu.
/// </summary>
/// <param name="currentPower">The current power.</param>
/// <returns></returns>
BOOL PowerSchemes::ScheduleCpu(DWORD currentPower)
{
	BOOL bSuccess = TRUE;

	//GetCpuMhz();

	//The use of a maximum and minimum policy value provides for the most flexible yet 
	// simple expression of processor policy, such as the static use of any 
	// single performance state within the range of states supported by the hardware.

	//mySettings.MinThrottle = 40;
	//mySettings.MaxThrottle = 40;
	// 
	SetCurrentPowerSettings();

	if(!ApplyPowerScheme(*mySchemeGuid))
	{
		//printf("ApplyPowerScheme failed\n");
		bSuccess = FALSE;
	}

	//Cleaning(); 
	return  bSuccess;
}


/// <summary>
/// Gets the current system power policy.
/// </summary>
/// <returns></returns>
BOOL PowerSchemes::GetCurrentSystemPowerPolicy()
{
	BOOL bSuccess = TRUE;
	SYSTEM_POWER_POLICY  powerPolicy; 

	DWORD status = CallNtPowerInformation(SystemPowerPolicyCurrent, NULL, 0, &powerPolicy,  sizeof(SYSTEM_POWER_POLICY) );
	switch (status)
	{
	case STATUS_BUFFER_TOO_SMALL:
		//printf("The output buffer is of insufficient size to contain the data to be returned. \n");
		bSuccess = FALSE;
		break;
	case STATUS_ACCESS_DENIED:
		//printf("The caller had insufficient access rights to perform the requested action. \n");
		bSuccess = FALSE;
		break;
	case STATUS_SUCCESS: 
		//printf("OK: %d\n", 0); 
		bSuccess = TRUE;
		break; 
	default:
		//PrintErrorMessage(status);
		bSuccess = FALSE;
		break; 
	} 

	return bSuccess;
}  

/// <summary>
/// Gets the cpu power information.
/// </summary>
/// <param name="MaxMhz">The maximum MHZ.</param>
/// <param name="CurrentMhz">The current MHZ.</param>
/// <param name="MhzLimit">The MHZ limit.</param>
/// <returns></returns>
BOOL PowerSchemes::GetCpuPowerInfo(DWORD &MaxMhz, DWORD &CurrentMhz, DWORD &MhzLimit)
{ 
	BOOL bSuccess = TRUE;

	// Find out how many processors we have in the system   
	DWORD numCore = sysInfo.dwNumberOfProcessors;
	PROCESSOR_POWER_INFORMATION *cpuInfo = new PROCESSOR_POWER_INFORMATION[numCore];

	// get CPU stats                                              
	int status = CallNtPowerInformation(ProcessorInformation, NULL, 0, &cpuInfo[0],  numCore * sizeof(PROCESSOR_POWER_INFORMATION) );
	switch (status)
	{
	case STATUS_BUFFER_TOO_SMALL:
		//printf("The output buffer is of insufficient size to contain the data to be returned. \n");
		bSuccess = FALSE;
		break;
	case STATUS_ACCESS_DENIED:
		//printf("The caller had insufficient access rights to perform the requested action. \n");
		bSuccess = FALSE;
		break;
	case STATUS_SUCCESS: 
		//printf("Number: %d\n", cpuInfo->Number); 
		//printf("MaxMhz: %d\n", cpuInfo->MaxMhz);
		//printf("CurrentMhz: %d \n", cpuInfo->CurrentMhz);
		//printf("MhzLimit: %d\n", cpuInfo->MhzLimit);
		//printf("MaxIdleState: %d\n", cpuInfo->MaxIdleState);
		//printf("CurrentIdleState: %d\n", cpuInfo->CurrentIdleState);


		MaxMhz = cpuInfo->MaxMhz;
		CurrentMhz = cpuInfo->CurrentMhz;
		MhzLimit = cpuInfo->MhzLimit;
		break; 
	}  

	return bSuccess;
}


/// <summary>
/// Gets the power capabilitites.
/// </summary>
/// <returns></returns>
BOOL PowerSchemes::GetPowerCapabilitites()
{
	BOOL bSuccess = TRUE;
	SYSTEM_POWER_CAPABILITIES spc;
	// get system power settings                                                                                             
	if (!GetPwrCapabilities(&spc))
	{
		//perror("GetPwrCapabilities failed\n"); 
		bSuccess = FALSE;
	}

	// print power settings                                                                                                  
	//std::cout << "System Power Capabilities:" << std::endl;
	//std::cout << "Processor throttle: " << (spc.ProcessorThrottle ? "enabled" : "disabled") << std::endl;
	//std::cout << "Processor minimum throttle: " << static_cast<int>(spc.ProcessorMinThrottle) << '%' << std::endl;
	//std::cout << "Processor maximum throttle: " << static_cast<int>(spc.ProcessorMaxThrottle) << '%' << std::endl;

	return bSuccess;
}

/// <summary>
/// Gets the friendly name of the power scheme.
/// </summary>
/// <param name="schemeGuid">The scheme unique identifier.</param>
/// <returns></returns>
LPWSTR PowerSchemes::GetFriendlyName(GUID schemeGuid)
{
	/************************************************************************/
	/* PowerReadFriendlyName  http://msdn.microsoft.com/en-us/library/aa372740.aspx
	/************************************************************************/
	LPWSTR friendlyName = new TCHAR[MAX_PATH];
	UCHAR schemeName[1024]; 
	DWORD size = sizeof(schemeName); 
	DWORD errorCode = PowerReadFriendlyName(NULL, &schemeGuid, &NO_SUBGROUP_GUID, NULL, schemeName, &size);
	if (ERROR_SUCCESS!=errorCode) 
		friendlyName = NULL;
	else 
	{ 
		wcscpy(friendlyName, (LPWSTR)schemeName); 
	} 
	return friendlyName;
}


/// <summary>
/// Gets the friendly name of the power scheme.
/// </summary>
/// <param name="schemeGuid">The scheme unique identifier.</param>
/// <param name="friendlyName">Name of the friendly.</param>
/// <returns></returns>
BOOL PowerSchemes::GetFriendlyName(GUID schemeGuid, LPWSTR friendlyName) 
{  
	/************************************************************************/
	/* PowerReadFriendlyName  http://msdn.microsoft.com/en-us/library/aa372740.aspx
	/************************************************************************/
	UCHAR schemeName[1024];
	DWORD size = sizeof(schemeName); 
	DWORD errorCode = PowerReadFriendlyName(NULL, &schemeGuid, &NO_SUBGROUP_GUID, NULL, schemeName, &size);
	if ( ERROR_SUCCESS!=errorCode) 
		friendlyName = NULL;
	else 
	{ 
		wcscpy(friendlyName, (LPWSTR)schemeName); 
	} 

	return ERROR_SUCCESS==errorCode;
}

/// <summary>
/// Gets the friendly name of the active power scheme.
/// </summary>
/// <returns></returns>
LPWSTR PowerSchemes::GetActiveFriendlyName()
{
	GUID *activeScheme = new GUID;
	GetActiveScheme(activeScheme);
	LPWSTR activeSchemeName = new TCHAR[MAX_PATH];
	GetActiveFriendlyName(activeScheme, activeSchemeName);
	//	delete activeScheme;
	return activeSchemeName;
}

/// <summary>
/// Gets the friendly name of the active power scheme.
/// </summary>
/// <param name="schemeGuid">The scheme unique identifier.</param>
/// <param name="friendlyName">Friendly name to take.</param>
/// <returns></returns>
BOOL PowerSchemes::GetActiveFriendlyName(GUID *schemeGuid, LPWSTR friendlyName) 
{  
	/************************************************************************/
	/* PowerReadFriendlyName http://msdn.microsoft.com/en-us/library/aa372740.aspx                                                           */
	/************************************************************************/

	UCHAR schemeName[1024];
	DWORD size = sizeof(schemeName);  

	DWORD errorCode = PowerReadFriendlyName(NULL, schemeGuid, &NO_SUBGROUP_GUID, NULL, schemeName, &size);

	if ( ERROR_SUCCESS!= errorCode) 
		friendlyName = NULL;
	else 
	{ 
		wcscpy(friendlyName, (LPWSTR)schemeName); 
	} 

	return ERROR_SUCCESS== errorCode;
}


/// <summary>
/// Gets the scheme description.
/// </summary>
/// <param name="scheme">The scheme.</param>
/// <param name="description">The description.</param>
/// <returns></returns>
BOOL PowerSchemes::GetSchemeDescription(GUID scheme, LPWSTR  description) 
{  
	/************************************************************************/
	/* PowerReadDescription function http://msdn.microsoft.com/en-us/library/aa372739(v=vs.85).aspx                                                              */
	/************************************************************************/

	UCHAR readBuffer[4096];
	DWORD size = sizeof(readBuffer);  
	if ( ERROR_SUCCESS!=PowerReadDescription(NULL, &scheme, &NO_SUBGROUP_GUID, NULL, readBuffer, &size)) 
	{	
		description = NULL;
		return FALSE;
	}
	else 
	{ 
		wcscpy(description, (LPWSTR)readBuffer); 
		return TRUE;
	} 
}

/// <summary>
/// Sets the scheme description.
/// </summary>
/// <param name="schemeGuid">The scheme unique identifier.</param>
/// <param name="schemeDescription">The scheme description.</param>
/// <returns></returns>
BOOL PowerSchemes::SetSchemeDescription(GUID schemeGuid, LPWSTR  schemeDescription)
{
	/************************************************************************/
	/*   PowerWriteDescription function http://msdn.microsoft.com/en-us/library/aa372772(v=vs.85).aspx                                                                   */
	/************************************************************************/

	DWORD size = (wcslen(schemeDescription)+1) * sizeof(wchar_t); // +1 for NULL and because Buffer parameter is a Unicode string
	return PowerWriteDescription(NULL, &schemeGuid, NULL, NULL, (UCHAR *)schemeDescription, size) == ERROR_SUCCESS;
}


/// <summary>
/// Sets the name of the scheme.
/// </summary>
/// <param name="schemeGuid">The scheme unique identifier.</param>
/// <param name="schemeName">Name of the scheme.</param>
/// <returns></returns>
BOOL PowerSchemes::SetSchemeName(GUID &schemeGuid, LPWSTR &schemeName)
{
	/************************************************************************/
	/*  PowerWriteFriendlyName http://msdn.microsoft.com/en-us/library/aa372773.aspx                                                                    */
	/************************************************************************/ 
	DWORD size = (wcslen(schemeName)+1) * sizeof(wchar_t); // +1 for NULL and because Buffer parameter is a Unicode string
	return PowerWriteFriendlyName(NULL, &schemeGuid, NULL, NULL, (UCHAR *)schemeName, size) == ERROR_SUCCESS;
} 

/// <summary>
/// Gets power setting value.
/// </summary>
/// <param name="source">The power source AC or DC.</param>
/// <param name="scheme">The scheme.</param>
/// <param name="subGroup">The sub group.</param>
/// <param name="setting">The setting.</param>
/// <param name="Value">The value.</param>
/// <returns></returns>
BOOL PowerSchemes::GetValue(PowerSource source, GUID scheme, GUID subGroup, GUID setting, DWORD &Value) 
{
	if(source==Power_AC)
	{
		return GetAcValue(scheme, subGroup, setting, Value); 
	}
	else if(source==Power_DC)
	{
		return GetDcValue(scheme, subGroup, setting, Value); 
	}
	else
	{
		return FALSE;
	}
} 

/// <summary>
/// Gets the AC value of power setting.
/// </summary>
/// <param name="scheme">The scheme.</param>
/// <param name="subGroup">The sub group.</param>
/// <param name="setting">The setting.</param>
/// <param name="Value">The value.</param>
/// <returns></returns>
BOOL PowerSchemes::GetAcValue(GUID scheme, GUID subGroup, GUID setting, DWORD &Value) 
{
	/************************************************************************/
	/*  PowerReadACValueIndex  http://msdn.microsoft.com/en-us/library/aa372735.aspx                                                                    */
	/************************************************************************/

	return PowerReadACValueIndex(NULL, &scheme, &subGroup, &setting, &Value) == ERROR_SUCCESS; 
}

/// <summary>
/// Gets the DC value of power setting.
/// </summary>
/// <param name="scheme">The scheme.</param>
/// <param name="subGroup">The sub group.</param>
/// <param name="setting">The setting.</param>
/// <param name="Value">The value.</param>
/// <returns></returns>
BOOL PowerSchemes::GetDcValue(GUID scheme, GUID subGroup, GUID setting, DWORD &Value) 
{ 
	return PowerReadDCValueIndex(NULL, &scheme, &subGroup, &setting, &Value) == ERROR_SUCCESS; 
}

/// <summary>
/// Sets the setting value.
/// </summary>
/// <param name="source">The source.</param>
/// <param name="schemeGuid">The scheme unique identifier.</param>
/// <param name="subGroupGuid">The sub group unique identifier.</param>
/// <param name="settingGuid">The setting unique identifier.</param>
/// <param name="Value">The value.</param>
/// <returns></returns>
BOOL PowerSchemes::SetValue(PowerSource source, GUID schemeGuid, GUID subGroupGuid, GUID settingGuid, DWORD Value)
{
	if(source==Power_AC)
	{
		return SetAcValue(schemeGuid, subGroupGuid, settingGuid, Value) ; 
	}
	else if(source==Power_DC)
	{
		return SetDcValue(schemeGuid, subGroupGuid, settingGuid, Value) ; 
	}
	else
	{
		return FALSE;
	}
}

/// <summary>
/// Sets the AC value of power setting.
/// </summary>
/// <param name="schemeGuid">The scheme unique identifier.</param>
/// <param name="subGroupGuid">The sub group unique identifier.</param>
/// <param name="settingGuid">The setting unique identifier.</param>
/// <param name="Value">The value.</param>
/// <returns></returns>
BOOL PowerSchemes::SetAcValue(GUID schemeGuid, GUID subGroupGuid, GUID settingGuid, DWORD Value) 
{
	/************************************************************************/
	/* PowerWriteACValueIndex  http://msdn.microsoft.com/en-us/library/aa372735.aspx                                                                   */
	/************************************************************************/

	return PowerWriteACValueIndex(NULL, &schemeGuid, &subGroupGuid, &settingGuid, Value) == ERROR_SUCCESS;
}

/// <summary>
/// Sets the DC value of power setting.
/// </summary>
/// <param name="schemeGuid">The scheme unique identifier.</param>
/// <param name="subGroupGuid">The sub group unique identifier.</param>
/// <param name="settingGuid">The setting unique identifier.</param>
/// <param name="Value">The value.</param>
/// <returns></returns>
BOOL PowerSchemes::SetDcValue(GUID schemeGuid, GUID subGroupGuid, GUID settingGuid, DWORD Value) 
{ 
	return PowerWriteDCValueIndex(NULL, &schemeGuid, &subGroupGuid, &settingGuid, Value) == ERROR_SUCCESS;
}


/// <summary>
/// Compares two power scheme.
/// </summary>
/// <param name="schemeGuid1">The scheme guid1.</param>
/// <param name="schemeGuid2">The scheme guid2.</param>
/// <returns></returns>
BOOL PowerSchemes::CompareScheme(GUID schemeGuid1, GUID schemeGuid2)
{
	return IsEqualGUID(schemeGuid1, schemeGuid2);
}

/// <summary>
/// Applies the current power settings.
/// </summary>
/// <returns></returns>
BOOL PowerSchemes::ApplyCurrentPowerSettings()
{
	SetCurrentPowerSettings();
	return PowerSetActiveScheme(NULL, mySchemeGuid) == ERROR_SUCCESS;
}

/// <summary>
/// Applies the power scheme.
/// </summary>
/// <param name="schemeGuid">The scheme unique identifier.</param>
/// <returns></returns>
BOOL PowerSchemes::ApplyPowerScheme(GUID &schemeGuid)
{
	/************************************************************************/
	/*   PowerSetActiveScheme function http://msdn.microsoft.com/en-us/library/aa372758(v=vs.85).aspx                                                                   */
	/************************************************************************/

	return PowerSetActiveScheme(NULL, &schemeGuid) == ERROR_SUCCESS;
}


BOOL PowerSchemes::ApplyPowerSchemeXP(UINT & uiID)
{
	BOOL bResult = FALSE;
	if(GetActivePwrScheme(&uiID))
	{
		//printf("GetActivePwrScheme OK\n");

		//PROCESSOR_POWER_POLICY  ppp; 
		MACHINE_PROCESSOR_POWER_POLICY  mppp;
		/*
		ReadProcessorPwrScheme is available for use in the operating systems specified in the Requirements section. 
		Requirements: Windows XP [desktop apps only] | Windows Server 2003 [desktop apps only]  
		*/
		if(ReadProcessorPwrScheme(uiID, &mppp))
		{
			printf("ReadProcessorPwrScheme OK\n");

			mppp.ProcessorPolicyAc.Policy[0].AllowPromotion = 1; // C1
			mppp.ProcessorPolicyAc.Policy[1].AllowPromotion = 1; // C2
			mppp.ProcessorPolicyAc.Policy[2].AllowPromotion = 1; // C3
			if (WriteProcessorPwrScheme(uiID, &mppp))
				SetActivePwrScheme(uiID, 0, 0);

			bResult = TRUE;
		}
		else
		{
			//printf("ReadProcessorPwrScheme FAIL\n");
			bResult = FALSE;
		}
	}
	else {
		printf("GetActivePwrScheme FAIL\n"); 
		bResult = FALSE;;
	} 
	return bResult;
}

/// <summary>
/// Sets the current power scheme unique identifier.
/// </summary>
/// <param name="schemeGuid">The scheme unique identifier.</param>
VOID PowerSchemes::SetCurrentPowerSchemeGuid(GUID schemeGuid)
{
	*mySchemeGuid = schemeGuid;
}

/// <summary>
/// Gets the current power scheme unique identifier.
/// </summary>
/// <returns></returns>
GUID PowerSchemes::GetCurrentPowerSchemeGuid()
{
	return *mySchemeGuid;
}

/// <summary>
/// Gets the active scheme.
/// </summary>
/// <param name="schemeGuid">The scheme unique identifier.</param>
/// <returns></returns>
BOOL PowerSchemes::GetActiveScheme(GUID *&schemeGuid)
{
	/************************************************************************/
	/*  PowerGetActiveScheme function http://msdn.microsoft.com/en-us/library/aa372731(v=vs.85).aspx                                                               */
	/************************************************************************/

	return PowerGetActiveScheme(NULL, &schemeGuid) == ERROR_SUCCESS;
} 

/// <summary>
/// Deletes the scheme.
/// </summary>
/// <param name="schemeGuid">The scheme unique identifier.</param>
/// <returns></returns>
BOOL PowerSchemes::DeleteScheme(GUID schemeGuid)
{
	/************************************************************************/
	/* PowerDeleteScheme function http://msdn.microsoft.com/en-us/library/aa372758(v=vs.85).aspx                                                                  */
	/************************************************************************/

	return PowerDeleteScheme(NULL, &schemeGuid) == ERROR_SUCCESS;
}

/// <summary>
/// Gets the current power settings.
/// </summary>
/// <returns></returns>
BOOL PowerSchemes::GetCurrentPowerSettings()
{ 
	/* Extracting power settings */
	BOOL bSuccess = TRUE;

	GetFriendlyName(*mySchemeGuid, Settings.SchemeName);
	GetSchemeDescription(*mySchemeGuid, Settings.SchemeDescription);


	// Check current power source is on AC or DC
	PowerSource source = GetPowerSource();

	bSuccess &= GetValue(source, *mySchemeGuid, GUID_PROCESSOR_SETTINGS_SUBGROUP, GUID_PROCESSOR_THROTTLE_MINIMUM, Settings.MinThrottle);
	bSuccess &= GetValue(source, *mySchemeGuid, GUID_PROCESSOR_SETTINGS_SUBGROUP, GUID_PROCESSOR_THROTTLE_MAXIMUM, Settings.MaxThrottle);

	/************************************************************************/
	/*      Get throttle policy                                             */
	/************************************************************************/ 
	bSuccess &= GetValue(source, *mySchemeGuid, GUID_PROCESSOR_SETTINGS_SUBGROUP, GUID_PROCESSOR_ALLOW_THROTTLING, Settings.AllowThrottling);
	bSuccess &= GetValue(source, *mySchemeGuid, GUID_PROCESSOR_SETTINGS_SUBGROUP, GUID_PROCESSOR_THROTTLE_POLICY, Settings.ThrottlePolicyAc);

	/************************************************************************/
	/*  Get P-State settings                                                */
	/************************************************************************/
	//bSuccess &= GetAcValue(*mySchemeGuid, GUID_PROCESSOR_SETTINGS_SUBGROUP, GUID_PROCESSOR_PERFSTATE_POLICY, mySettings.PerfState.PerfStatePolicy);

	bSuccess &= GetValue(source, *mySchemeGuid, GUID_PROCESSOR_SETTINGS_SUBGROUP, GUID_PROCESSOR_PERF_INCREASE_THRESHOLD, Settings.PerfState.IncreaseThreshold);
	bSuccess &= GetValue(source, *mySchemeGuid, GUID_PROCESSOR_SETTINGS_SUBGROUP, GUID_PROCESSOR_PERF_DECREASE_THRESHOLD, Settings.PerfState.DecreaseThreshold);

	bSuccess &= GetValue(source, *mySchemeGuid, GUID_PROCESSOR_SETTINGS_SUBGROUP, GUID_PROCESSOR_PERF_INCREASE_POLICY, Settings.PerfState.IncreasePolicy);
	bSuccess &= GetValue(source, *mySchemeGuid, GUID_PROCESSOR_SETTINGS_SUBGROUP, GUID_PROCESSOR_PERF_DECREASE_POLICY, Settings.PerfState.DecreasePolicy);

	bSuccess &= GetValue(source, *mySchemeGuid, GUID_PROCESSOR_SETTINGS_SUBGROUP, GUID_PROCESSOR_PERF_TIME_CHECK, Settings.PerfState.TimeCheck);
	bSuccess &= GetValue(source, *mySchemeGuid, GUID_PROCESSOR_SETTINGS_SUBGROUP, GUID_PROCESSOR_PERF_INCREASE_TIME, Settings.PerfState.IncreaseTime);
	bSuccess &= GetValue(source, *mySchemeGuid, GUID_PROCESSOR_SETTINGS_SUBGROUP, GUID_PROCESSOR_PERF_DECREASE_TIME, Settings.PerfState.DecreaseTime);

	bSuccess &= GetValue(source, *mySchemeGuid, GUID_PROCESSOR_SETTINGS_SUBGROUP, GUID_PROCESSOR_PERF_BOOST_POLICY, Settings.PerfState.BoostPolicy);
	//bSuccess &= GetValue(source, *mySchemeGuid, GUID_PROCESSOR_SETTINGS_SUBGROUP, GUID_PROCESSOR_PERF_BOOST_MODE, mySettings.PerfState.BoostMode);

	/************************************************************************/
	/*  Get C-State settings                                                */
	/************************************************************************/
	//bSuccess &= GetValue(source, *mySchemeGuid, GUID_PROCESSOR_SETTINGS_SUBGROUP, GUID_PROCESSOR_IDLESTATE_POLICY, mySettings.IdleState.IdleStatePolicy);

	bSuccess &= GetValue(source, *mySchemeGuid, GUID_PROCESSOR_SETTINGS_SUBGROUP, GUID_PROCESSOR_IDLE_ALLOW_SCALING, Settings.IdleState.AllowScaling);
	bSuccess &= GetValue(source, *mySchemeGuid, GUID_PROCESSOR_SETTINGS_SUBGROUP, GUID_PROCESSOR_IDLE_DISABLE, Settings.IdleState.IdleDisable);

	//bSuccess &= GetValue(source, *mySchemeGuid, GUID_PROCESSOR_SETTINGS_SUBGROUP, GUID_PROCESSOR_IDLE_STATE_MAXIMUM, mySettings.IdleState.StateMaximum);
	bSuccess &= GetValue(source, *mySchemeGuid, GUID_PROCESSOR_SETTINGS_SUBGROUP, GUID_PROCESSOR_IDLE_TIME_CHECK, Settings.IdleState.TimeCheck);

	bSuccess &= GetValue(source, *mySchemeGuid, GUID_PROCESSOR_SETTINGS_SUBGROUP, GUID_PROCESSOR_IDLE_PROMOTE_THRESHOLD, Settings.IdleState.PromoteThreshold);
	bSuccess &= GetValue(source, *mySchemeGuid, GUID_PROCESSOR_SETTINGS_SUBGROUP, GUID_PROCESSOR_IDLE_DEMOTE_THRESHOLD, Settings.IdleState.DemoteThreshold);

	/************************************************************************/
	/* Disk Settings                                                        */
	/************************************************************************/ 
	bSuccess &= GetValue(source, *mySchemeGuid, GUID_DISK_SUBGROUP, GUID_DISK_POWERDOWN_TIMEOUT, Settings.Disk.PowerdownTimeout); 
	bSuccess &= GetValue(source, *mySchemeGuid, GUID_DISK_SUBGROUP, GUID_DISK_BURST_IGNORE_THRESHOLD, Settings.Disk.BurstIgnoreThreshold); 

	/************************************************************************/
	/* Video Settings                                                        */
	/************************************************************************/
	bSuccess &= GetValue(source, *mySchemeGuid, GUID_VIDEO_SUBGROUP, GUID_VIDEO_POWERDOWN_TIMEOUT, Settings.Video.PowerdownTimeout); 
	bSuccess &= GetValue(source, *mySchemeGuid, GUID_VIDEO_SUBGROUP, GUID_VIDEO_ANNOYANCE_TIMEOUT, Settings.Video.AnnoyanceTimeout); 
	bSuccess &= GetValue(source, *mySchemeGuid, GUID_VIDEO_SUBGROUP, GUID_VIDEO_ADAPTIVE_PERCENT_INCREASE, Settings.Video.AdaptivePercentIncrease); 
	bSuccess &= GetValue(source, *mySchemeGuid, GUID_VIDEO_SUBGROUP, GUID_VIDEO_DIM_TIMEOUT, Settings.Video.DimTimeout); 
	bSuccess &= GetValue(source, *mySchemeGuid, GUID_VIDEO_SUBGROUP, GUID_VIDEO_ADAPTIVE_POWERDOWN, Settings.Video.AdaptivePowerdown);  
	bSuccess &= GetValue(source, *mySchemeGuid, GUID_VIDEO_SUBGROUP, GUID_DEVICE_POWER_POLICY_VIDEO_BRIGHTNESS, Settings.Video.VideoBrightness); 
	bSuccess &= GetValue(source, *mySchemeGuid, GUID_VIDEO_SUBGROUP, GUID_DEVICE_POWER_POLICY_VIDEO_DIM_BRIGHTNESS, Settings.Video.VideoDimBrightness);   
	bSuccess &= GetValue(source, *mySchemeGuid, GUID_VIDEO_SUBGROUP, GUID_VIDEO_ADAPTIVE_DISPLAY_BRIGHTNESS, Settings.Video.AdaptiveDisplayBrightness);  

	/************************************************************************/
	/* Cooling Settings                                                     */
	/************************************************************************/
	bSuccess &= GetValue(source, *mySchemeGuid, GUID_PROCESSOR_SETTINGS_SUBGROUP, GUID_SYSTEM_COOLING_POLICY, Settings.SytemCoolingPolicy);

	return bSuccess;
}


/// <summary>
/// Sets the current power settings.
/// </summary>
/// <returns></returns>
BOOL PowerSchemes::SetCurrentPowerSettings()
{ 
	/* Set new power settings */
	BOOL bSuccess = TRUE;

	// Check current power source is on AC or DC
	PowerSource source = GetPowerSource();

	bSuccess &= SetValue(source, *mySchemeGuid, GUID_PROCESSOR_SETTINGS_SUBGROUP, GUID_PROCESSOR_THROTTLE_MINIMUM, Settings.MinThrottle );
	bSuccess &= SetValue(source, *mySchemeGuid, GUID_PROCESSOR_SETTINGS_SUBGROUP, GUID_PROCESSOR_THROTTLE_MAXIMUM, Settings.MaxThrottle );

	/************************************************************************/
	/*      Get throttle policy                                             */
	/************************************************************************/ 
	bSuccess &= SetValue(source, *mySchemeGuid, GUID_PROCESSOR_SETTINGS_SUBGROUP, GUID_PROCESSOR_ALLOW_THROTTLING, Settings.AllowThrottling);

	if (Settings.MinThrottle == 100 && Settings.MaxThrottle == 100)  
		Settings.ThrottlePolicyAc = PO_THROTTLE_NONE;   
	else if (Settings.MaxThrottle == 100)   
		Settings.ThrottlePolicyAc = PO_THROTTLE_ADAPTIVE; 
	else
		Settings.ThrottlePolicyAc = PO_THROTTLE_CONSTANT;  
	bSuccess &= SetValue(source, *mySchemeGuid, GUID_PROCESSOR_SETTINGS_SUBGROUP, GUID_PROCESSOR_THROTTLE_POLICY, Settings.ThrottlePolicyAc);

	/************************************************************************/
	/*  Set P-State settings                                                */
	/************************************************************************/
	//bSuccess &= SetValue(source, *mySchemeGuid, GUID_PROCESSOR_SETTINGS_SUBGROUP, GUID_PROCESSOR_PERFSTATE_POLICY, mySettings.PerfState.PerfStatePolicy);

	bSuccess &= SetValue(source, *mySchemeGuid, GUID_PROCESSOR_SETTINGS_SUBGROUP, GUID_PROCESSOR_PERF_INCREASE_THRESHOLD, Settings.PerfState.IncreaseThreshold);
	bSuccess &= SetValue(source, *mySchemeGuid, GUID_PROCESSOR_SETTINGS_SUBGROUP, GUID_PROCESSOR_PERF_DECREASE_THRESHOLD, Settings.PerfState.DecreaseThreshold);

	bSuccess &= SetValue(source, *mySchemeGuid, GUID_PROCESSOR_SETTINGS_SUBGROUP, GUID_PROCESSOR_PERF_INCREASE_POLICY, Settings.PerfState.IncreasePolicy);
	bSuccess &= SetValue(source, *mySchemeGuid, GUID_PROCESSOR_SETTINGS_SUBGROUP, GUID_PROCESSOR_PERF_DECREASE_POLICY, Settings.PerfState.DecreasePolicy);

	bSuccess &= SetValue(source, *mySchemeGuid, GUID_PROCESSOR_SETTINGS_SUBGROUP, GUID_PROCESSOR_PERF_TIME_CHECK, Settings.PerfState.TimeCheck);
	bSuccess &= SetValue(source, *mySchemeGuid, GUID_PROCESSOR_SETTINGS_SUBGROUP, GUID_PROCESSOR_PERF_INCREASE_TIME, Settings.PerfState.IncreaseTime);
	bSuccess &= SetValue(source, *mySchemeGuid, GUID_PROCESSOR_SETTINGS_SUBGROUP, GUID_PROCESSOR_PERF_DECREASE_TIME, Settings.PerfState.DecreaseTime);

	bSuccess &= SetValue(source, *mySchemeGuid, GUID_PROCESSOR_SETTINGS_SUBGROUP, GUID_PROCESSOR_PERF_BOOST_POLICY, Settings.PerfState.BoostPolicy);
	//bSuccess &= SetValue(source, *mySchemeGuid, GUID_PROCESSOR_SETTINGS_SUBGROUP, GUID_PROCESSOR_PERF_BOOST_MODE, mySettings.PerfState.BoostMode);

	/************************************************************************/
	/*  Set C-State settings                                                */
	/************************************************************************/
	//bSuccess &= SetValue(source, *mySchemeGuid, GUID_PROCESSOR_SETTINGS_SUBGROUP, GUID_PROCESSOR_IDLESTATE_POLICY, mySettings.IdleState.IdleStatePolicy);

	bSuccess &= SetValue(source, *mySchemeGuid, GUID_PROCESSOR_SETTINGS_SUBGROUP, GUID_PROCESSOR_IDLE_ALLOW_SCALING, Settings.IdleState.AllowScaling);
	bSuccess &= SetValue(source, *mySchemeGuid, GUID_PROCESSOR_SETTINGS_SUBGROUP, GUID_PROCESSOR_IDLE_DISABLE, Settings.IdleState.IdleDisable);

	//bSuccess &= SetAcValue(*mySchemeGuid, GUID_PROCESSOR_SETTINGS_SUBGROUP, GUID_PROCESSOR_IDLE_STATE_MAXIMUM, mySettings.IdleState.StateMaximum);
	bSuccess &= SetValue(source, *mySchemeGuid, GUID_PROCESSOR_SETTINGS_SUBGROUP, GUID_PROCESSOR_IDLE_TIME_CHECK, Settings.IdleState.TimeCheck);

	bSuccess &= SetValue(source, *mySchemeGuid, GUID_PROCESSOR_SETTINGS_SUBGROUP, GUID_PROCESSOR_IDLE_PROMOTE_THRESHOLD, Settings.IdleState.PromoteThreshold);
	bSuccess &= SetValue(source, *mySchemeGuid, GUID_PROCESSOR_SETTINGS_SUBGROUP, GUID_PROCESSOR_IDLE_DEMOTE_THRESHOLD, Settings.IdleState.DemoteThreshold);

	/************************************************************************/
	/* Cooling Settings                                                     */
	/************************************************************************/
	bSuccess &= SetValue(source, *mySchemeGuid, GUID_PROCESSOR_SETTINGS_SUBGROUP, GUID_SYSTEM_COOLING_POLICY, Settings.SytemCoolingPolicy);

	return bSuccess;
}

/// <summary>
/// Gets the index of the power setting.
/// </summary>
/// <param name="SchemeGuid">The scheme unique identifier.</param>
/// <param name="SubgroupOfPowerSettingsGuid">The subgroup of power settings unique identifier.</param>
/// <param name="powerSettingGuid">The power setting unique identifier.</param>
/// <param name="powerSettingIndex">Index of the power setting.</param>
/// <returns></returns>
BOOL PowerSchemes::GetPowerSettingIndex(GUID SchemeGuid, GUID SubgroupOfPowerSettingsGuid, GUID powerSettingGuid, ULONG &powerSettingIndex)
{
	/*
	Enumerate individual power settings under SchemeGuid\SubgroupOfPowerSettingsGuid. 
	To enumerate power settings directly under the SchemeGuid key, 
	use NO_SUBGROUP_GUID as the SubgroupOfPowerSettingsGuid parameter.

	schemeGuid:

	GUID_MIN_POWER_SAVINGS      = High performance
	GUID_MAX_POWER_SAVINGS      = Power saver
	GUID_TYPICAL_POWER_SAVINGS  = Balanced 

	schemeGuidSubGroup:

	NO_SUBGROUP_GUID
	GUID_DISK_SUBGROUP
	GUID_SYSTEM_BUTTON_SUBGROUP
	GUID_PROCESSOR_SETTINGS_SUBGROUP
	GUID_VIDEO_SUBGROUP
	GUID_BATTERY_SUBGROUP
	GUID_SLEEP_SUBGROUP
	GUID_PCIEXPRESS_SETTINGS_SUBGROUP

	powerSetting:
	Setting of each group
	*/

	GUID    buffer;
	DWORD   bufferSize = sizeof(buffer);  
	//wchar_t *schemeName = new wchar_t[255];
	ULONG   index=0;
	BOOL    bContinue = TRUE; 
	BOOL    bSuccess = TRUE;

	if(SubgroupOfPowerSettingsGuid==GUID_NULL)
	{
		SubgroupOfPowerSettingsGuid = NO_SUBGROUP_GUID;
	}

	while (bContinue)
	{
		ZeroMemory(&buffer, sizeof(buffer)); 
		DWORD errorCode = PowerEnumerate(
			NULL, /* Root Power Key, this MUST null */
			&SchemeGuid, /* Scheme Guid : default personality GUID */
			&SubgroupOfPowerSettingsGuid, /* SubGroup Of Power Settings Guid  */
			ACCESS_INDIVIDUAL_SETTING, /* AccessFlags: ACCESS_SCHEME | ACCESS_SUBGROUP | ACCESS_INDIVIDUAL_SETTING */
			index, /* The zero-based index of the scheme, subgroup, or setting that is being enumerated */
			(UCHAR*)&buffer, 
			&bufferSize);

		switch (errorCode)
		{
		case  ERROR_SUCCESS:
			if(IsEqualGUID(powerSettingGuid, buffer))
			{
				bContinue = FALSE;
				powerSettingIndex  = index;
				//GetFriendlyName(buffer, schemeName);
				//wprintf(L"Name: %s\n", schemeName); 
			}  
			else
			{
				index++;
			} 
			break;
		case ERROR_MORE_DATA:
			//printf("BufferSize parameter is too small, or the Buffer parameter is NULL\n");
			bContinue = FALSE;
			bSuccess = FALSE;
			break;
		case ERROR_NO_MORE_ITEMS:
			bContinue = FALSE;
			break; 
		default:
			//printf("GetPowerSettingIndex error, %d !!!\n", errorCode);
			//PrintErrorMessage(errorCode);
			bContinue = FALSE;
			break; 
		}  
	}

	return bSuccess;
}


/// <summary>
/// Determines whether is power scheme exist.
/// </summary>
/// <param name="checkSchemeGuid">The check scheme unique identifier.</param>
/// <param name="SchemeGuidIndex">Index of the scheme unique identifier.</param>
/// <returns></returns>
BOOL PowerSchemes::IsPowerSchemeExist(GUID checkSchemeGuid, ULONG &SchemeGuidIndex)
{
	/************************************************************************/
	/* Enumerate power schemes. The SchemeGuid and SubgroupOfPowerSettingsGuid parameters will be ignored.  */
	/* Get default scheme index                                                */
	/*   GUID_MIN_POWER_SAVINGS      = High performance                        */
	/*   GUID_MAX_POWER_SAVINGS      = Power saver                             */
	/*   GUID_TYPICAL_POWER_SAVINGS  = Balanced                                */
	/*   Custom plans.....
	/************************************************************************/

	GUID    buffer;
	DWORD   bufferSize = sizeof(buffer);   
	ULONG   index=0;

	BOOL bIsExist = FALSE;
	BOOL bContinue = TRUE;
	while (bContinue)
	{
		ZeroMemory(&buffer, sizeof(buffer)); 
		DWORD errorCode = PowerEnumerate(
			NULL, /* Root Power Key, this value MUST null */
			NULL, /* Scheme Guid  */
			NULL, /* SubGroup Of Power Settings Guid  */
			ACCESS_SCHEME, /* AccessFlags: ACCESS_SCHEME | ACCESS_SUBGROUP | ACCESS_INDIVIDUAL_SETTING */
			index, /* The zero-based index of the scheme, subgroup, or setting that is being enumerated */
			(UCHAR*)&buffer, 
			&bufferSize);

		switch (errorCode)
		{
		case  ERROR_SUCCESS:
			if( IsEqualGUID(checkSchemeGuid, buffer) )
			{
				SchemeGuidIndex = index;
				bIsExist = TRUE;
				bContinue = FALSE; 
			} 
			else
			{
				index++;
			}

			break;
		case ERROR_MORE_DATA:
			//printf("BufferSize parameter is too small, or the Buffer parameter is NULL\n");
			bContinue = FALSE;
			bIsExist = FALSE;
			break;
		case ERROR_NO_MORE_ITEMS:
			bContinue = FALSE;
			break; 
		default:
			//printf("IsPowerSchemeExist error, %d !!!\n", errorCode);
			//PrintErrorMessage(errorCode);
			bContinue = FALSE;
			break; 
		}  
	}

	return bIsExist;
}


BOOL PowerSchemes::GetSchemeSubgroupIndex(GUID SchemeGuid, GUID SubgroupOfPowerSettingsGuid, ULONG &SubgroupIndex)
{ 
	/************************************************************************/
	/*  Enumerate subgroups under SchemeGuid. The SubgroupOfPowerSettingsGuid parameter will be ignored.
	//
	// SchemeGuid:
	//
	// GUID_MIN_POWER_SAVINGS      = High performance
	// GUID_MAX_POWER_SAVINGS      = Power saver
	// GUID_TYPICAL_POWER_SAVINGS  = Balanced  */
	/************************************************************************/ 

	GUID    buffer;
	DWORD   bufferSize = sizeof(buffer);   
	ULONG   index=0;
	BOOL    bContinue = TRUE;
	BOOL    bSuccess = TRUE;

	while (bContinue)
	{
		ZeroMemory(&buffer, sizeof(buffer)); 
		DWORD errorCode = PowerEnumerate(
			NULL, /* Root Power Key, this value MUST null */
			&SchemeGuid, /* Scheme Guid  */
			NULL, /* SubGroup Of Power Settings Guid  */
			ACCESS_SUBGROUP, /* AccessFlags: ACCESS_SCHEME | ACCESS_SUBGROUP | ACCESS_INDIVIDUAL_SETTING */
			index, /* The zero-based index of the scheme, subgroup, or setting that is being enumerated */
			(UCHAR*)&buffer, 
			&bufferSize);

		switch (errorCode)
		{
		case  ERROR_SUCCESS:
			if(SchemeGuid != GUID_NULL )
			{ 	
				if(IsEqualGUID(SubgroupOfPowerSettingsGuid, buffer))
				{
					SubgroupIndex = index;
					bContinue = FALSE;
				} 
				else
				{
					index++;
				}
			}
			else
			{ 
				index++;
			}

			break;
		case ERROR_MORE_DATA:
			//printf("BufferSize parameter is too small, or the Buffer parameter is NULL\n");
			bContinue = FALSE;
			bSuccess = FALSE;
			break;
		case ERROR_NO_MORE_ITEMS:
			bContinue = FALSE;
			break; 
		default:
			//printf("PowerEnumerate error, %d !!!\n", errorCode);
			//PrintErrorMessage(errorCode);
			bContinue = FALSE;
			break; 
		}  
	}

	return bSuccess;
}

/// <summary>
/// Registers the power status. When power setting changed, OS will notify us
/// </summary>
/// <param name="hWnd">The h WND.</param>
VOID PowerSchemes::RegisterPowerStatus(HWND hWnd)
{  
	hNotify  = RegisterPowerSettingNotification(hWnd, &GUID_POWERSCHEME_PERSONALITY, DEVICE_NOTIFY_WINDOW_HANDLE);
	hNotify  = RegisterPowerSettingNotification(hWnd, &GUID_ACDC_POWER_SOURCE, DEVICE_NOTIFY_WINDOW_HANDLE);
	hNotify  = RegisterPowerSettingNotification(hWnd, &GUID_MONITOR_POWER_ON, DEVICE_NOTIFY_WINDOW_HANDLE);
	hNotify  = RegisterPowerSettingNotification(hWnd, &GUID_BATTERY_PERCENTAGE_REMAINING, DEVICE_NOTIFY_WINDOW_HANDLE);
} 

/// <summary>
/// Unregister power setting notification.
/// </summary>
/// <returns></returns>
BOOL PowerSchemes::UnRegisterPowerStatus( ) 
{
	return	UnregisterPowerSettingNotification(hNotify);
}

/// <summary>
/// Poll power source status.
/// </summary>
VOID PowerSchemes::Poll()
{
	if (!GetSystemPowerStatus(&pwrStat))
	{
		isPsValid = FALSE;
	}
	isPsValid = TRUE;
}


/// <summary>
/// Is laptop ?
/// </summary>
/// <returns></returns>
BOOL PowerSchemes::IsLaptop()
{
	BOOL bLaptop = FALSE;

	Poll();

	if(isPsValid)
	{
		// BatteryFlag
		//
		// The battery charge status. This member can contain one or more of the following flags.
		// 1: High！the battery capacity is at more than 66 percent
		// 2: Low！the battery capacity is at less than 33 percent
		// 4: Critical！the battery capacity is at less than five percent
		// 8: Charging
		// 128: No system battery
		// 255: Unknown status！unable to read the battery flag information
		int BatStat = (int)pwrStat.BatteryFlag;

		if (BatStat == 128)
		{
			bLaptop = FALSE;
		}
		else
		{
			bLaptop = TRUE;
		}
	}
	return bLaptop;
}

/// <summary>
/// Gets the power source.
/// </summary>
/// <returns></returns>
PowerSource PowerSchemes::GetPowerSource()
{
	PowerSource retVal = Power_Undefined;

	Poll();

	if (!isPsValid)
	{
		return retVal;
	}

	BYTE acLine = pwrStat.ACLineStatus;
	switch((int)acLine)
	{
	case 0: // Offline
		retVal = Power_DC;
		break;
	case 1: // Online
		retVal = Power_AC;
		break;
	case 255: // Unknown status
		retVal = Power_Undefined;
		break;
	default:
		retVal = Power_Undefined;
		break;
	}
	return retVal;
}

/// <summary>
/// Gets the power source as string.
/// </summary>
/// <returns></returns>
LPWSTR PowerSchemes::GetPowerSourceStr()
{
	PowerSource sourcePower = GetPowerSource();
	switch((int)sourcePower)
	{
	case 0:
		return L"DC";
	case 1:
		return L"AC";
	case 255:
		return L"N/A"; 
	default:
		return L"N/A"; 
	} 
}
 
/// <summary>
/// This functions returns percent battery life indicating how much
/// battery life is left. Common usage of this function is that it should
/// be called when running on battery only. This can be called when running
/// on AC power as well. 
/// </summary>
/// <returns>Returns -1 if function fails</returns>
INT PowerSchemes::GetPercentBatteryLife()
{
	Poll();

	if (!isPsValid)
	{
		return -1;
	}
	INT BatLife = (INT)pwrStat.BatteryLifePercent; // 0-100%

	return BatLife;
}

 
/// <summary>
/// This function returns battery life left in seconds.
/// Common usage of this function is that it should
/// be called when running on battery only 
/// </summary>
/// <returns>Returns -1 when function call fails</returns>
ULONG PowerSchemes::GetSecBatteryLifeTimeRemaining()
{
	Poll();

	if (!isPsValid)
	{
		return -1;
	}

	DWORD BatLifeTime = pwrStat.BatteryLifeTime;

	if ((BatLifeTime/3600) > 24)
	{
		return -1;
	}

	return BatLifeTime;
}