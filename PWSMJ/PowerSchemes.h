// 
// Name: PowerSchemes.cpp : implementation file 
// Author: hieunt
// Description:  Use for Windows Power Management 
//				 We can change power policy and CPU frequency by using WPM
//

#pragma once

#include <ntstatus.h>
#include <PowrProf.h> 
#pragma comment(lib, "PowrProf.lib")

#include "PowerSettings.h" 

using namespace std;

class PowerSchemes
{ 
private:
	GUID *mySchemeGuid; 
	LPWSTR mySchemeName;
	LPWSTR mySchemeDescription;  
	SYSTEM_INFO sysInfo; 
	HANDLE hPowerRequest; // Handle power request object
	REASON_CONTEXT *powerReasonContext; // Contains information about a power request. 


	SYSTEM_POWER_STATUS pwrStat; // store power status information
	BOOL isPsValid; 
	VOID Poll();
	HPOWERNOTIFY hNotify; // notify if powersetting is changed
public:
	PowerSettings Settings;  

	PowerSchemes();
	~PowerSchemes();

	BOOL Cleaning();
	BOOL ScheduleCpu(DWORD currentPower);
	BOOL GetCurrentSystemPowerPolicy();
	BOOL IsSupportOS();

	BOOL GetCurrentSettingForDisplay(); 

	BOOL GetCpuPowerInfo(DWORD &MaxMhz, DWORD &CurrentMhz, DWORD &MhzLimit); 

	BOOL GetPowerCapabilitites();

	LPWSTR GetThrottleName(DWORD throttle);
	LPWSTR GetPolicyName(DWORD policy);
	/************************************************************************/
	/*           Windows power management functions                         */
	/************************************************************************/
	LPWSTR GetFriendlyName(GUID schemeGuid);
	BOOL GetFriendlyName(GUID schemeGuid, LPWSTR friendlyName);
	LPWSTR GetActiveFriendlyName(); 
	BOOL GetActiveFriendlyName(GUID *schemeGuid, LPWSTR friendlyName);
	BOOL GetSchemeDescription(GUID scheme, LPWSTR description); 
	BOOL SetSchemeDescription(GUID schemeGuid, LPWSTR schemeDescription); 
	BOOL SetSchemeName(GUID &schemeGuid, LPWSTR &schemeName);
	BOOL GetValue(PowerSource source, GUID scheme, GUID subGroup, GUID setting, DWORD &Value);
	BOOL GetAcValue(GUID scheme, GUID subGroup, GUID setting, DWORD &Value);
	BOOL GetDcValue(GUID scheme, GUID subGroup, GUID setting, DWORD &Value); 
	BOOL SetValue(PowerSource source, GUID schemeGuid, GUID subGroupGuid, GUID settingGuid, DWORD Value);
	BOOL SetAcValue(GUID schemeGuid, GUID subGroupGuid, GUID settingGuid, DWORD Value);
	BOOL SetDcValue(GUID schemeGuid, GUID subGroupGuid, GUID settingGuid, DWORD Value);
	BOOL CompareScheme(GUID, GUID);
	BOOL ApplyCurrentPowerSettings();
	BOOL ApplyPowerScheme(GUID &schemeGuid);
	BOOL ApplyPowerSchemeXP(UINT & uiID);
	BOOL ApplyCustomPowerPlan();
	VOID ClearCustomPowerPlan();
	VOID SetCurrentPowerSchemeGuid(GUID schemeGuid);
	GUID GetCurrentPowerSchemeGuid();

	BOOL GetActiveScheme(GUID *&schemeGuid);
	BOOL DeleteScheme(GUID schemeGuid);

	BOOL RestoreIndividualDefaultPowerSchemeSettings();
	BOOL GetCurrentPowerSettings();
	BOOL SetCurrentPowerSettings();

	BOOL CreateCustomPowerPlan(); 

	BOOL GetPowerSettingIndex(GUID SchemeGuid, GUID SubgroupOfPowerSettingsGuid, GUID powerSettingGuid, ULONG &powerSettingIndex);  
	BOOL IsPowerSchemeExist(GUID checkSchemeGuid, ULONG &SchemeGuidIndex);  
	BOOL GetSchemeSubgroupIndex(GUID SchemeGuid, GUID SubgroupOfPowerSettingsGuid, ULONG &SubgroupIndex); 

	// POWER SOURCE
	////////////////////////////////////////////////////////////////////////// 
	BOOL        IsLaptop();
	PowerSource GetPowerSource();
	LPWSTR      GetPowerSourceStr();
	ULONG       GetSecBatteryLifeTimeRemaining();
	INT         GetPercentBatteryLife(); 
	VOID        RegisterPowerStatus(HWND hWnd);
	BOOL        UnRegisterPowerStatus( );
};
 