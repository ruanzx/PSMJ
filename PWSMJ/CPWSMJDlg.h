
// PWSMJDlg.h : header file
//

// MAIN dialog

#pragma once

#include "CPowerControlDlg.h" // power control dialog
#include "CProcessorInfoDlg.h" // CPU information dialog
#include "CAboutDlg.h"
#include "CBnljDlg.h" // Block Nested Loop Join dialog
#include "CSmjDlg.h" // Sort Merge Join dialog
#include "CPemsDlg.h" // Parallel External Merge Sort dialog
#include "CNsmDlg.h"  // create NSM dialog
#include "CLargeFileDlg.h" // debug dialog
#include "CHhjDlg.h" // Hybrid hash join dialog
#include "CGhjDlg.h" // Grace hash join dialog
#include "CRpDlg.h" // Replacement Selection
#include "CPsmjDlg.h" // Parallel Sort Merge Join
#include "CSmj2Dlg.h" // Replacement Selection + Join
#include "CCemsDlg.h" // Cache External merge sort

// CPWSMJDlg dialog
class CPWSMJDlg : public CDialog
{
	// Construction
public:
	CPWSMJDlg(CWnd* pParent = NULL);	// standard constructor

	// Dialog Data
	enum { IDD = IDD_PWSMJ_DIALOG };

protected:
	virtual void DoDataExchange(CDataExchange* pDX);	// DDX/DDV support


	// Implementation
protected:
	HICON m_hIcon;
	 CPowerControlDlg *powerControlDlg;
	 CProcessorInfoDlg *processorInfoDlg;

	// Generated message map functions
	virtual BOOL OnInitDialog();
	afx_msg void OnSysCommand(UINT nID, LPARAM lParam);
	afx_msg void OnPaint();
	afx_msg HCURSOR OnQueryDragIcon();
	DECLARE_MESSAGE_MAP()
public:
	afx_msg void OnBnPowerControl_Clicked();
	afx_msg void OnBnBNLJ_Clicked();
	afx_msg void OnBnCpuInfo_Clicked();
	afx_msg void OnBnSmj_Clicked();
	afx_msg void OnBnPems_Clicked();
	afx_msg void OnBnNsm_Clicked();
	afx_msg void OnBnReadPage_Clicked();
	afx_msg void OnBnHhj_Clicked();
	afx_msg void OnBnGhj_Clicked();
	afx_msg void OnBnRp_Clicked();
	afx_msg void OnBnPsmj_Clicked();
	afx_msg void OnBnSmj2_Clicked();
	afx_msg void OnBnCems_Clicked();
};
