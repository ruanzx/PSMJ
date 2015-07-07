
// 
// Name: CProcessorInfoDlg.h
// Author: hieunt
// Description: Processor information, power capping, frequency,.. dialog
//

#pragma once 
#include "afxwin.h"
#include "CPU.h" 
#include "CpuWorkload.h" 
#include "Loggers.h"
#include "DiskMonitor.h"
#include "CpuUsage.h" 

class CProcessorInfoDlg : public CDialogEx
{
public:
	CProcessorInfoDlg();
	// Dialog Data
	enum { IDD = IDD_CPU_INFO_DIALOG };

	~CProcessorInfoDlg();

protected:
	HICON m_hIcon;
	CpuWorkload m_WorkLoad;

	virtual void DoDataExchange(CDataExchange* pDX);    // DDX/DDV support
	virtual BOOL OnInitDialog();

	typedef struct ThreadParams				//structure for passing to the controlling function
	{
		CProcessorInfoDlg*	_this;
	} ThreadParams;

	// Implementation 
	DWORD WINAPI UpdateCpuStatus(LPVOID lpParam);
	static DWORD WINAPI UpdateCpuStatusEx(LPVOID lpParam);   
	
protected:
	DECLARE_MESSAGE_MAP() 
	CPU m_CPU; 
	DiskMonitor m_Disk;
	HANDLE m_hCpuStatusThread;
	volatile BOOL m_Quit;
public:
	CEdit m_CpuModel;
	CEdit m_PhysicalCoreNum;
	CEdit m_LogicalCoreNum;
	CEdit m_Multiplier;
	CEdit m_PackageMinPower;
	CEdit m_PackageMaxPower;
	CEdit m_PackagePower;
	CEdit m_TDPPower;
	CEdit m_Core_0_Power;
	CEdit m_Core_1_Power;
	CEdit m_Voltage;
	CEdit m_FreqMsr;
	CEdit m_BaseFreq;
	CEdit m_CurrentMultiplier;
	CEdit m_FreqCycles;
	CEdit m_FreqCyclesBusyPercent;
	CEdit m_FreqRdTSC;
	CEdit m_PackageTemp;
	CEdit m_TjMax;
	CEdit m_Core0Temp;
	CEdit m_Core1Temp;
	CEdit m_RefreshInterval;
	CEdit m_PowerCap;
	CButton m_StressWorkLoad;
	CEdit m_DiskReadRate;
	CEdit m_DiskWriteRate;

	afx_msg void OnBnSetInterval_Clicked();
	afx_msg void OnDestroy();
	afx_msg void OnBnOk_Clicked();
	afx_msg void OnBnStress_Clicked();

	CEdit m_tbProcessPower;
	CEdit m_tbProcessUsage;
	CButton m_btSetPowerCap;
	afx_msg void OnBtSetPowerConstrain_Clicked();
	CEdit m_tbGuiInterval;
	afx_msg void OnBnClickedBtSetGuiInterval();
	afx_msg void OnBnClickedBtSetPowerTimebase();
};

