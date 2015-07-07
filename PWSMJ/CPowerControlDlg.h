// 
// Name: CPowerControlDlg.h
// Author: hieunt
// Description: Windows Power management dialog
//

#pragma once
 
#include "afxwin.h"
#include "PowerSchemes.h"
//#include "WMI.h"
 
  
/// <summary>
/// CPU freequency combobox items
/// </summary>
typedef struct CpuFrequencyItem
{
	LPTSTR DisplayName;
	DWORD Value; 
	CpuFrequencyItem() {}
	CpuFrequencyItem(LPTSTR vName, DWORD vValue) { DisplayName = vName; Value = vValue; }
}CpuFrequencyItem; 

/// <summary>
/// P-State Policy combobox items
/// </summary>
typedef struct PStatePolicyItem
{
	LPTSTR DisplayName;
	DWORD Value; 
	PStatePolicyItem() {}
	PStatePolicyItem(LPTSTR vName, DWORD vValue) { DisplayName = vName; Value = vValue; }
}PStatePolicyItem; 

/// <summary>
/// Power Scheme combobox items
/// </summary>
typedef struct PowerSchemeItem
{
	LPTSTR DisplayName;
	GUID Value; 
	PowerSchemeItem() {}
	PowerSchemeItem(LPTSTR vName, GUID vValue) 
	{
		DisplayName = vName; 
		Value = vValue; 
	}
}PowerSchemeItem;

class CPowerControlDlg : public CDialogEx
{
public:
	CPowerControlDlg();

	// Dialog Data
	enum { IDD = IDD_POWER_CONTROL_DIALOG };

	~CPowerControlDlg();
protected:
	HICON m_hIcon;
	CToolTipCtrl m_ToolTip;

	virtual void DoDataExchange(CDataExchange* pDX);    // DDX/DDV support
	virtual BOOL OnInitDialog();
	virtual BOOL PreTranslateMessage(MSG* pMsg);

	VOID UpdatePowerPlanInfos(); 
	 
	VOID CreateCpuFrequencyItems(); 
	VOID CreatePolicyItems();
	VOID CreatePowerSchemeItems();
	 
	

	// Implementation
protected:
	DECLARE_MESSAGE_MAP()
public:
	PowerSchemes m_PowerPlan;
	std::vector<PowerSchemeItem *> m_PowerSchemeItems;
	std::vector<PStatePolicyItem *> m_PolicyItems;
	std::vector<CpuFrequencyItem *> m_CpuFrequencyItems;
	

	CStatic m_lbPowerSource;
	CComboBox m_cbbPowerScheme;
	CComboBox m_cbbSetCpuFrequency;
	CComboBox m_P_cbbIncreasePolicy;
	CComboBox m_P_cbbDecreasePolicy; 

	CEdit m_tbThrottleMode; 
	CEdit m_tbMinThrottle;
	CEdit m_tbMaxThrottle; 
	CEdit m_P_tbDecreaseThreshold;
	CEdit m_P_tbIncreaseThreshold;
	CEdit m_P_tbTimeCheck;
	CEdit m_P_tbIncreaseTime;
	CEdit m_P_tbDecreaseTime; 
	CEdit m_C_tbTimeCheck;
	CEdit m_C_tbPromote;
	CEdit m_C_tbDemote; 

	CButton m_chkAllowThrottle;
	CButton m_C_chkAllowScalling;
	CButton m_C_chkIdleDisable;
	afx_msg void OnCbbPowerScheme_SelectedIndexChanged();
	afx_msg void OnCbbSetFrequency_SelectedIndexChanged();
	afx_msg void OnRestoreDefault_Clicked();
	afx_msg void OnApplySettings_Clicked();
	afx_msg UINT OnPowerBroadcast(UINT nPowerEvent, UINT nEventData);
	afx_msg void OnExit_Clicked();
};