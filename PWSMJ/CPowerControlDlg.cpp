// 
// Name: CPowerControlDlg.cpp
// Author: hieunt
// Description: Windows Power management dialog
//

#include "stdafx.h"
#include "CPowerControlDlg.h" 

extern WMI_ProcessorStatus g_P_StatesTable;  

/// <summary>
/// Initializes a new instance of the <see cref="CPowerControlDlg"/> class.
/// </summary>
CPowerControlDlg::CPowerControlDlg() : CDialogEx(CPowerControlDlg::IDD)
{

}

/// <summary>
/// Finalizes an instance of the <see cref="CPowerControlDlg"/> class.
/// </summary>
CPowerControlDlg::~CPowerControlDlg()
{

}

void CPowerControlDlg::DoDataExchange(CDataExchange* pDX)
{
	CDialogEx::DoDataExchange(pDX);
	DDX_Control(pDX, IDC_CBB_PPM_POWER_SCHEME, m_cbbPowerScheme);
	DDX_Control(pDX, IDC_LB_POWER_SOURCE, m_lbPowerSource);
	DDX_Control(pDX, IDC_TB_PPM_THROTTLE_MODE, m_tbThrottleMode);
	DDX_Control(pDX, IDC_CHK_ALLOW_THROTTLE, m_chkAllowThrottle);
	DDX_Control(pDX, IDC_TB_PPM_MIN_THROTTLE, m_tbMinThrottle);
	DDX_Control(pDX, IDC_TB_PPM_MAX_THROTTLE, m_tbMaxThrottle);
	DDX_Control(pDX, IDC_CBB_SET_FREQUENCY, m_cbbSetCpuFrequency);
	DDX_Control(pDX, IDC_TB_PPM_P_INCREASE_THRESHOLD, m_P_tbIncreaseThreshold);
	DDX_Control(pDX, IDC_TB_PPM_P_DECREASE_THRESHOLD, m_P_tbDecreaseThreshold);
	DDX_Control(pDX, IDC_TB_PPM_P_TIME_CHECK, m_P_tbTimeCheck);
	DDX_Control(pDX, IDC_TB_PPM_P_INCREASE_TIME, m_P_tbIncreaseTime);
	DDX_Control(pDX, IDC_TB_PPM_P_DECREASE_TIME, m_P_tbDecreaseTime);
	DDX_Control(pDX, IDC_CBB_PPM_P_POLICY_INCREASE, m_P_cbbIncreasePolicy);
	DDX_Control(pDX, IDC_CBB_PPM_P_POLICY_DECREASE, m_P_cbbDecreasePolicy);
	DDX_Control(pDX, IDC_CHK_PPM_C_ALLOW_SCALE, m_C_chkAllowScalling);
	DDX_Control(pDX, IDC_CHK_PPM_C_IDLE_DISABLE, m_C_chkIdleDisable);
	DDX_Control(pDX, IDC_TB_PPM_C_TIME_CHECK, m_C_tbTimeCheck);
	DDX_Control(pDX, IDC_TB_PPM_C_PROMTE_THRESHOLD, m_C_tbPromote);
	DDX_Control(pDX, IDC_TB_PPM_C_DEMOTE_THRESHOLD, m_C_tbDemote);
}

BEGIN_MESSAGE_MAP(CPowerControlDlg, CDialogEx)
	ON_CBN_SELCHANGE(IDC_CBB_PPM_POWER_SCHEME, &CPowerControlDlg::OnCbbPowerScheme_SelectedIndexChanged)
	ON_CBN_SELCHANGE(IDC_CBB_SET_FREQUENCY, &CPowerControlDlg::OnCbbSetFrequency_SelectedIndexChanged)
	ON_BN_CLICKED(IDC_RESTORE_DEFAULT, &CPowerControlDlg::OnRestoreDefault_Clicked)
	ON_BN_CLICKED(IDC_APPLY, &CPowerControlDlg::OnApplySettings_Clicked)
	ON_WM_POWERBROADCAST()
	ON_BN_CLICKED(IDC_EXIT, &CPowerControlDlg::OnExit_Clicked)
END_MESSAGE_MAP() 

/// <summary>
/// Called when [initialize dialog].
/// </summary>
/// <returns></returns>
BOOL CPowerControlDlg::OnInitDialog()
{
	CDialog::OnInitDialog(); 

	// TODO: Add extra initialization here

	// Get current active scheme
	m_PowerPlan.GetCurrentSettingForDisplay();

	m_PowerPlan.RegisterPowerStatus(this->m_hWnd);
	m_lbPowerSource.SetWindowTextW(m_PowerPlan.GetPowerSourceStr());

	// Create Power scheme combobox items
	CreatePowerSchemeItems(); 

	CreateCpuFrequencyItems();

	CreatePolicyItems();

	UpdatePowerPlanInfos(); 
	 
	//Create the ToolTip control
	if( !m_ToolTip.Create(this))
	{
		TRACE0("Unable to create the ToolTip!");
	}
	else
	{
		// Add tool tips to the controls, either by hard coded string 
		// or using the string table resource 
		m_ToolTip.AddTool(&m_P_tbIncreaseThreshold,  L"Specifies the increase busy percentage threshold that must be met before increasing the processor performance state.\n\nValue: 0% - 100%");
		m_ToolTip.AddTool(&m_P_tbDecreaseThreshold,  L"Specifies the decrease busy percentage threshold that must be met before decreasing the processor performance state.\n\nValue: 0% - 100%");

		LPWSTR tipContent; 

		tipContent =L"Specifies a percentage (between 0 and 100) that the processor frequency should not drop below.  For example, if this value is set to 50, then the processor frequency will never be throttled below 50 percent of its maximum frequency by the system"; 
		m_ToolTip.AddTool(&m_tbMinThrottle, tipContent);

		tipContent =L"Specifies a percentage (between 0 and 100) that the processor frequency should never go above.  For example, if this value is set to 80, then the processor frequency will never be throttled above 80 percent of its maximum frequency by the system.";
		m_ToolTip.AddTool(&m_tbMaxThrottle, tipContent);

		tipContent = L"The processor performance control policy constants indicate the processor performance control algorithm applied to a power scheme."; 
		m_ToolTip.AddTool(&m_tbThrottleMode, tipContent);

		//tipContent = L"Throttle states can be used. However, the processor throttle state does not change adaptively. When enabled, the Minimum Processor State and Maximum Processor State settings can be used to lock the system processors into a specific processor throttle state.";
		//m_ToolTip.AddTool(IDC_CHK_ALLOW_THROTTLE, tipContent);

		tipContent = L"Specifies, either as ideal, single or rocket, how aggressive performance states should be selected when increasing the processor performance state.\n\n Value:\n Ideal:\n A target performance state is selected based on calculating which performance state decreases the processor utilization to just below the value of the Processor Performance Increase Threshold setting.\n Single:\n The next highest performance state (compared to the current performance state) is selected\n Rocket:\n The highest performance state is selected.";
		m_ToolTip.AddTool(&m_P_cbbIncreasePolicy, tipContent);

		tipContent =L"Specifies, either as ideal, single or rocket, how aggressive performance states should be selected when decreasing the processor performance state.\n\nValue:\n Ideal:\n A target performance state is selected based on calculating which performance state increases the processor utilization to just above the value of the Processor Performance Decrease Threshold setting.\nSingle:\n The next lowest performance state (compared to the current performance state) is selected.\nRocket:\n The lowest performance state is selected. ";
		m_ToolTip.AddTool(&m_P_cbbDecreasePolicy, tipContent);

		tipContent =L"Specifies the time, in milliseconds, that must expire before considering a change in the processor performance states or parked core set.\n\nValue: 1ms - 5000ms";
		m_ToolTip.AddTool(&m_P_tbTimeCheck, tipContent);

		tipContent = L"Specifies, in milliseconds, the minimum amount of time that must elapse after the last processor performance state change before increasing the processor performance state.\n\nValue: 1ms - 100ms";
		m_ToolTip.AddTool(&m_P_tbIncreaseTime, tipContent);

		tipContent = L"Specifies, in milliseconds, the minimum amount of time that must elapse after the last processor performance state change before increasing the processor performance state.\n\nValue: 1ms - 100ms";
		m_ToolTip.AddTool(&m_P_tbDecreaseTime, tipContent);

		tipContent = L"Specifies if idle state promotion and demotion values should be scaled based on the current peformance state.";
		m_ToolTip.AddTool(&m_C_chkAllowScalling, tipContent);

		tipContent = L"Specifies if idle states should be disabled.";
		m_ToolTip.AddTool(&m_C_chkIdleDisable, tipContent);

		tipContent = L"Specifies the time that elapsed since the last idle state promotion  or demotion before idle states may be promoted or demoted again (in microseconds).\n\nValue: 1 - 200000";
		m_ToolTip.AddTool(&m_C_tbTimeCheck, tipContent);

		tipContent = L"Specifies the lower busy threshold that must be met before promoting the processor to a deeper idle state (in percentage).\n\nValue: 0 - 100";
		m_ToolTip.AddTool(&m_C_tbPromote, tipContent);

		tipContent = L"Specifies the upper busy threshold that must be met before demoting the processor to a lighter idle state (in percentage).\n\nValue: 0 - 100";
		m_ToolTip.AddTool(&m_C_tbDemote, tipContent); 

		m_ToolTip.SetMaxTipWidth(300);
		//m_ToolTip.SetDelayTime(20*1000);
		m_ToolTip.Activate(TRUE);
	}


	return TRUE;  // return TRUE  unless you set the focus to a control
}

/// <summary>
/// Pre the translate message.
/// </summary>
/// <param name="pMsg">The p MSG.</param>
/// <returns></returns>
BOOL CPowerControlDlg::PreTranslateMessage(MSG* pMsg)
{
	//The call to RelayEvent() is necessary to pass a mouse message to the tool tip control for processing.
	m_ToolTip.RelayEvent(pMsg);

	return CDialog::PreTranslateMessage(pMsg);
}


VOID CPowerControlDlg::CreateCpuFrequencyItems()
{ 
	// Clear exist items
	m_CpuFrequencyItems.clear();

	LPWSTR displayName = new TCHAR[MAX_PATH];

	for(UINT i=0; i<g_P_StatesTable.PerfStates.Count; i++)
	{
		WMI_PerformanceState p_state = g_P_StatesTable.PerfStates.State[i];
		if(p_state.Flags==1)
		{ 
			// DOUBLE power = p_state.Power;
			if(i==0)
			{
				wsprintf(displayName, L"%d Mhz (%d %%) ~ %d mW TURBO", p_state.Frequency, p_state.PercentFrequency, p_state.Power);
			}
			else
			{
				wsprintf(displayName, L"%d Mhz (%d %%) ~ %d mW", p_state.Frequency, p_state.PercentFrequency, p_state.Power);
			}
		}
		else if(p_state.Flags==2)
		{
			wsprintf(displayName, L"%d Mhz (%d %%)", p_state.Frequency, p_state.PercentFrequency);
		}

		// Save 
		CpuFrequencyItem * state = new CpuFrequencyItem(displayName, g_P_StatesTable.PerfStates.State[i].PercentFrequency);
		m_CpuFrequencyItems.push_back(state); 

		// Add to combobox
		m_cbbSetCpuFrequency.AddString(displayName); 
	}

	DWORD currentPercentThrottle = m_PowerPlan.Settings.MaxThrottle;
	DWORD stateCbbCount = m_cbbSetCpuFrequency.GetCount();
	for(UINT i=0; i<stateCbbCount; i++)
	{
		if(currentPercentThrottle==m_CpuFrequencyItems[i]->Value)
		{
			m_cbbSetCpuFrequency.SetCurSel(i);
			break;
		}
	} 
} 

VOID CPowerControlDlg::UpdatePowerPlanInfos()
{  
	//// Select current active scheme
	LPWSTR activeSchemeName = m_PowerPlan.GetActiveFriendlyName();
	INT schemeCount = m_cbbPowerScheme.GetCount(); 
	for(UINT i = 0; i < schemeCount; i++)
	{
		if(wcscmp(ComboboxGetTextByIndex(m_cbbPowerScheme, i), activeSchemeName) == 0)
		{
			m_cbbPowerScheme.SetCurSel(i);
			break; 
		} 
	} 

	DWORD currentPercentThrottle = m_PowerPlan.Settings.MaxThrottle;
	DWORD stateCbbCount = m_cbbSetCpuFrequency.GetCount();
	for(UINT i=0; i<stateCbbCount; i++)
	{
		if(currentPercentThrottle==m_CpuFrequencyItems[i]->Value)
		{
			m_cbbSetCpuFrequency.SetCurSel(i);
			break;
		}
	} 

	m_tbThrottleMode.SetWindowTextW(m_PowerPlan.GetThrottleName(m_PowerPlan.Settings.ThrottlePolicyAc));
	m_tbMinThrottle.SetWindowTextW(LongToString(m_PowerPlan.Settings.MinThrottle, L"%d"));
	m_tbMaxThrottle.SetWindowTextW(LongToString(m_PowerPlan.Settings.MaxThrottle, L"%d")); 
	m_P_tbIncreaseThreshold.SetWindowTextW(LongToString(m_PowerPlan.Settings.PerfState.IncreaseThreshold, L"%d")); 
	m_P_tbDecreaseThreshold.SetWindowTextW(LongToString(m_PowerPlan.Settings.PerfState.DecreaseThreshold, L"%d"));

	m_P_tbTimeCheck.SetWindowTextW(LongToString(m_PowerPlan.Settings.PerfState.TimeCheck, L"%d"));
	m_P_tbIncreaseTime.SetWindowTextW(LongToString(m_PowerPlan.Settings.PerfState.IncreaseTime, L"%d"));
	m_P_tbDecreaseTime.SetWindowTextW(LongToString(m_PowerPlan.Settings.PerfState.DecreaseTime, L"%d"));

	ComboboxSetSelectedIndexByText(m_P_cbbIncreasePolicy, m_PowerPlan.GetPolicyName(m_PowerPlan.Settings.PerfState.IncreasePolicy));
	ComboboxSetSelectedIndexByText(m_P_cbbDecreasePolicy, m_PowerPlan.GetPolicyName(m_PowerPlan.Settings.PerfState.DecreasePolicy));

	m_C_tbTimeCheck.SetWindowTextW(LongToString(m_PowerPlan.Settings.IdleState.TimeCheck, L"%d"));
	m_C_tbPromote.SetWindowTextW(LongToString(m_PowerPlan.Settings.IdleState.PromoteThreshold, L"%d"));
	m_C_tbDemote.SetWindowTextW(LongToString(m_PowerPlan.Settings.IdleState.DemoteThreshold, L"%d")); 


	m_C_chkAllowScalling.SetCheck(m_PowerPlan.Settings.IdleState.AllowScaling);
	m_C_chkIdleDisable.SetCheck(m_PowerPlan.Settings.IdleState.IdleDisable);

	m_C_tbPromote.SetReadOnly(!m_PowerPlan.Settings.IdleState.AllowScaling);
	m_C_tbDemote.SetReadOnly(!m_PowerPlan.Settings.IdleState.AllowScaling); 
}

void CPowerControlDlg::OnCbbPowerScheme_SelectedIndexChanged()
{ 
	GUID selectedSchemeValue = GUID_NULL;  
	LPCWSTR selectedSchemeText = ComboboxGetTextByIndex(m_cbbPowerScheme, m_cbbPowerScheme.GetCurSel());

	for(UINT i = 0; i < m_PowerSchemeItems.size(); i++)
	{
		if(wcscmp(m_PowerSchemeItems[i]->DisplayName, selectedSchemeText) == 0)
		{
			selectedSchemeValue = m_PowerSchemeItems[i]->Value;
			break;
		} 
	}

	if(selectedSchemeValue!=GUID_NULL)
	{
		// Set class GUID
		m_PowerPlan.SetCurrentPowerSchemeGuid(selectedSchemeValue);

		// Active select power scheme
		m_PowerPlan.ApplyPowerScheme(selectedSchemeValue); 

		// Update class settings
		m_PowerPlan.GetCurrentPowerSettings();
		// Update UI
		UpdatePowerPlanInfos(); 
	}
}


void CPowerControlDlg::OnCbbSetFrequency_SelectedIndexChanged()
{
	// TODO: Add your control notification handler code here
	INT selectedIndex = m_cbbSetCpuFrequency.GetCurSel();
	if(selectedIndex>=0)
	{
		for(UINT i = 0; i < m_CpuFrequencyItems.size(); i++)
		{
			if(selectedIndex==i)
			{
				m_PowerPlan.Settings.AllowThrottling = TRUE;

				m_PowerPlan.Settings.MinThrottle = m_CpuFrequencyItems[i]->Value;
				m_PowerPlan.Settings.MaxThrottle = m_CpuFrequencyItems[i]->Value; 

				/************************************************************************/
				/*      Get throttle policy                                             */
				/************************************************************************/ 

				if (m_PowerPlan.Settings.MinThrottle == 100 && m_PowerPlan.Settings.MaxThrottle == 100)  
					m_PowerPlan.Settings.ThrottlePolicyAc = PO_THROTTLE_NONE;   
				else if (m_PowerPlan.Settings.MaxThrottle == 100)   
					m_PowerPlan.Settings.ThrottlePolicyAc = PO_THROTTLE_ADAPTIVE; 
				else
					m_PowerPlan.Settings.ThrottlePolicyAc = PO_THROTTLE_CONSTANT;  

				m_PowerPlan.ApplyCurrentPowerSettings();

				m_PowerPlan.GetCurrentPowerSettings();

				// Update GUI
				UpdatePowerPlanInfos(); 

				break;
			} 
		}

	} 
}


void CPowerControlDlg::OnRestoreDefault_Clicked()
{
	// TODO: Add your control notification handler code here

	if(m_PowerPlan.RestoreIndividualDefaultPowerSchemeSettings())
	{
		AfxMessageBox(L"Fail!", MB_OK);
	} 
	// Set to High Performance
	m_PowerPlan.SetCurrentPowerSchemeGuid(GUID_MIN_POWER_SAVINGS);
	//myPowerScheme.ApplyCurrentPowerSettings();

	// Get class settings
	m_PowerPlan.GetCurrentPowerSettings(); 

	// Update UI
	UpdatePowerPlanInfos(); 


	AfxMessageBox(L"Restore success !", MB_OK); 
}


void CPowerControlDlg::OnApplySettings_Clicked()
{ 
	DWORD ThrottlePolicyAc, MinThrottle, MaxThrottle;
	DWORD P_IncreaseThreshold, P_DecreaseThreshold, P_TimeCheck, P_IncreaseTime, P_DecreaseTime;
	DWORD P_IncreasePolicy, P_DecreasePolicy;
	DWORD C_TimeCheck, C_PromoteThreshold, C_DemoteThreshold;
	BOOL C_AllowScaling, C_IdleDisable;

	MinThrottle = GetDlgItemInt(IDC_TB_PPM_MIN_THROTTLE, FALSE, FALSE);
	MaxThrottle = GetDlgItemInt(IDC_TB_PPM_MAX_THROTTLE, FALSE, FALSE);

	if (MinThrottle == 100 && MaxThrottle == 100)  
		ThrottlePolicyAc = PO_THROTTLE_NONE;   
	else if (MaxThrottle == 100)   
		ThrottlePolicyAc = PO_THROTTLE_ADAPTIVE; 
	else
		ThrottlePolicyAc = PO_THROTTLE_CONSTANT; 

	P_IncreaseThreshold = GetDlgItemInt(IDC_TB_PPM_P_INCREASE_THRESHOLD, FALSE, FALSE);
	P_DecreaseThreshold = GetDlgItemInt(IDC_TB_PPM_P_DECREASE_THRESHOLD, FALSE, FALSE); 
	P_TimeCheck = GetDlgItemInt(IDC_TB_PPM_P_TIME_CHECK, FALSE, FALSE);
	P_IncreaseTime = GetDlgItemInt(IDC_TB_PPM_P_INCREASE_TIME, FALSE, FALSE);
	P_DecreaseTime = GetDlgItemInt(IDC_TB_PPM_P_DECREASE_TIME, FALSE, FALSE);


	INT selectedIndex = -1;
	selectedIndex = m_P_cbbIncreasePolicy.GetCurSel();
	for(INT i=0; i<m_PolicyItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			P_IncreasePolicy = m_PolicyItems[i]->Value;
			break;
		}
	}

	selectedIndex = m_P_cbbDecreasePolicy.GetCurSel();
	for(INT i=0; i<m_PolicyItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			P_DecreasePolicy = m_PolicyItems[i]->Value;
			break;
		}
	}

	C_TimeCheck  = GetDlgItemInt(IDC_TB_PPM_C_TIME_CHECK, FALSE, FALSE);
	C_PromoteThreshold = GetDlgItemInt(IDC_TB_PPM_C_PROMTE_THRESHOLD, FALSE, FALSE);
	C_DemoteThreshold = GetDlgItemInt(IDC_TB_PPM_C_DEMOTE_THRESHOLD, FALSE, FALSE);
	C_AllowScaling = m_C_chkAllowScalling.GetCheck();
	C_IdleDisable = m_C_chkIdleDisable.GetCheck();

	// Validate input
	if(MinThrottle > 100 || MaxThrottle > 100 || MinThrottle < 0 || MaxThrottle < 0)
	{
		AfxMessageBox(L"Cpu Min Throttle & Max Throttle must in range 0 - 100", MB_OK);
		return;
	}

	if(P_IncreaseThreshold > 100 || P_DecreaseThreshold > 100 || P_IncreaseThreshold < 0 || P_DecreaseThreshold < 0)
	{
		AfxMessageBox(L"P state increase threshold and decrease threshold must in range 0 - 100", MB_OK);
		return;
	}

	if(P_TimeCheck > 5000 || P_TimeCheck < 1)
	{
		AfxMessageBox(L"P state time check must in range 1 - 5000", MB_OK);
		return;
	}

	if(P_IncreaseTime > 100 || P_DecreaseTime > 100 || P_IncreaseTime < 0 || P_DecreaseTime < 0)
	{
		AfxMessageBox(L"P state increase time and decrease time must in range 0 - 100", MB_OK);
		return;
	}

	if(C_TimeCheck > 200000 || C_TimeCheck < 1)
	{
		AfxMessageBox(L"C state time check must in range 1 - 200000", MB_OK);
		return;
	}

	if(C_PromoteThreshold > 100 || C_DemoteThreshold > 100 || C_PromoteThreshold < 0 || C_DemoteThreshold < 0)
	{
		AfxMessageBox(L"C state promote threshold and demote threshold must in range 0 - 100", MB_OK);
		return;
	}

	m_PowerPlan.Settings.MinThrottle = MinThrottle;
	m_PowerPlan.Settings.MaxThrottle = MaxThrottle; 
	m_PowerPlan.Settings.ThrottlePolicyAc = ThrottlePolicyAc;

	m_PowerPlan.Settings.PerfState.IncreaseThreshold = P_IncreaseThreshold;
	m_PowerPlan.Settings.PerfState.DecreaseThreshold = P_DecreaseThreshold;
	m_PowerPlan.Settings.PerfState.TimeCheck = P_TimeCheck;
	m_PowerPlan.Settings.PerfState.IncreaseTime = P_IncreaseTime;
	m_PowerPlan.Settings.PerfState.DecreaseTime = P_DecreaseTime;
	m_PowerPlan.Settings.PerfState.IncreasePolicy = P_IncreasePolicy;
	m_PowerPlan.Settings.PerfState.DecreasePolicy = P_DecreasePolicy;

	m_PowerPlan.Settings.IdleState.TimeCheck = C_TimeCheck;
	m_PowerPlan.Settings.IdleState.PromoteThreshold = C_PromoteThreshold;
	m_PowerPlan.Settings.IdleState.DemoteThreshold = C_DemoteThreshold;
	m_PowerPlan.Settings.IdleState.AllowScaling = C_AllowScaling;
	m_PowerPlan.Settings.IdleState.IdleDisable = C_IdleDisable;

	m_PowerPlan.ApplyCurrentPowerSettings();

	AfxMessageBox(L"Apply power settings OK\r\n", MB_OK); 
}


UINT CPowerControlDlg::OnPowerBroadcast(UINT nPowerEvent, UINT nEventData)
{
	if(nPowerEvent==PBT_POWERSETTINGCHANGE)
	{ 
		//AfxMessageBox(L"Setting change\r\n", MB_OK); 

		GUID *activeScheme = new GUID;
		m_PowerPlan.GetActiveScheme(activeScheme);

		// Set class GUID
		m_PowerPlan.SetCurrentPowerSchemeGuid(*activeScheme);

		// Update class settings
		m_PowerPlan.GetCurrentPowerSettings();

		// Update UI
		UpdatePowerPlanInfos();  
	}  

	if(nPowerEvent==PBT_APMPOWERSTATUSCHANGE)
	{
		m_lbPowerSource.SetWindowTextW(m_PowerPlan.GetPowerSourceStr()); 
	}


	return CDialogEx::OnPowerBroadcast(nPowerEvent, nEventData);
}


void CPowerControlDlg::OnExit_Clicked()
{
	//this->CloseWindow();
	this->ShowWindow(SW_HIDE);
}


VOID CPowerControlDlg::CreatePolicyItems()
{ 
	// Clear exist items
	m_PolicyItems.clear();

	PStatePolicyItem *item;
	item = new PStatePolicyItem (m_PowerPlan.GetPolicyName(PERFSTATE_POLICY_CHANGE_IDEAL) , PERFSTATE_POLICY_CHANGE_IDEAL); m_PolicyItems.push_back(item); 
	item = new PStatePolicyItem (m_PowerPlan.GetPolicyName(PERFSTATE_POLICY_CHANGE_SINGLE) , PERFSTATE_POLICY_CHANGE_SINGLE); m_PolicyItems.push_back(item); 
	item = new PStatePolicyItem (m_PowerPlan.GetPolicyName(PERFSTATE_POLICY_CHANGE_ROCKET) , PERFSTATE_POLICY_CHANGE_ROCKET); m_PolicyItems.push_back(item);  

	for(UINT i = 0; i<m_PolicyItems.size(); i++)
	{
		m_P_cbbIncreasePolicy.AddString(m_PolicyItems[i]->DisplayName);
		m_P_cbbDecreasePolicy.AddString(m_PolicyItems[i]->DisplayName);
	} 
}

VOID CPowerControlDlg::CreatePowerSchemeItems()
{
	m_PowerSchemeItems.clear(); // delete exist items in vector
	m_cbbPowerScheme.Clear(); // delete exist items in combobox

	PowerSchemeItem *item;
	/*   GUID_MIN_POWER_SAVINGS      = High performance                        */
	/*   GUID_MAX_POWER_SAVINGS      = Power saver                             */
	/*   GUID_TYPICAL_POWER_SAVINGS  = Balanced                                */
	item = new PowerSchemeItem (m_PowerPlan.GetFriendlyName(GUID_MIN_POWER_SAVINGS) , GUID_MIN_POWER_SAVINGS); m_PowerSchemeItems.push_back(item); 
	item = new PowerSchemeItem (m_PowerPlan.GetFriendlyName(GUID_MAX_POWER_SAVINGS) , GUID_MAX_POWER_SAVINGS); m_PowerSchemeItems.push_back(item); 
	item = new PowerSchemeItem (m_PowerPlan.GetFriendlyName(GUID_TYPICAL_POWER_SAVINGS) , GUID_TYPICAL_POWER_SAVINGS); m_PowerSchemeItems.push_back(item); 

	ULONG temp;
	if(m_PowerPlan.IsPowerSchemeExist(GUID_POWER_AWARE_SMJ, temp))
	{
		item = new PowerSchemeItem (m_PowerPlan.GetFriendlyName(GUID_POWER_AWARE_SMJ) , GUID_POWER_AWARE_SMJ); m_PowerSchemeItems.push_back(item); 
	}

	// Add power scheme into combobox
	for(UINT i = 0; i<m_PowerSchemeItems.size(); i++)
	{
		m_cbbPowerScheme.AddString(m_PowerSchemeItems[i]->DisplayName);
	} 
} 