// 
// Name: CPsmjDlg.cpp
// Author: hieunt
// Description: Parallel sort-merge join dialog
//

#pragma once 
#include "afxwin.h" 
#include "PSMJ.h"
#include "PSMJRP.h"
 
class CPsmjDlg : public CDialogEx
{
public:
	CPsmjDlg();

	enum { IDD = IDD_PSMJ_DIALOG };

	~CPsmjDlg();

protected:
	HICON m_hIcon;
	HANDLE m_hPsmjThread;
	WaitInfo m_Requestor;
	std::vector<ComboBoxItem *> joinPlanItems;

	virtual void DoDataExchange(CDataExchange* pDX);    // DDX/DDV support
	virtual BOOL OnInitDialog();
	LRESULT OnThreadTerminated(WPARAM wParam, LPARAM lParam);

	typedef struct ThreadParams				//structure for passing to the controlling function
	{
		CPsmjDlg*	_this;
		PSMJ_PARAMS psmjParams; // input parameters
	} ThreadParams;

	typedef struct ThreadParams2				//structure for passing to the controlling function
	{
		CPsmjDlg*	_this;
		PSMJRP_PARAMS psmjrpParams; // input parameters
	} ThreadParams2;

	// Implementation 
	DWORD WINAPI Run(LPVOID lpParam);
	static DWORD WINAPI RunEx(LPVOID lpParam);  

	DWORD WINAPI Run2(LPVOID lpParam);
	static DWORD WINAPI RunEx2(LPVOID lpParam);  

protected:
	DECLARE_MESSAGE_MAP() 
public:
	CEdit m_tbRelationPath_S;
	CEdit m_tbRelationPath_R;
	CEdit m_tbKeyPos_R;
	CEdit m_tbKeyPos_S;
	CEdit m_tbWorkSpace;
	CComboBox m_cbbSortReadBufferSize;
	CComboBox m_cbbSortWriteBufferSize;
	CButton m_btExecute;
	CComboBox m_cbbThreadNum;
	CButton m_btSelectS;
	CButton m_btSelectR;
	CEdit m_tbPoolSize;

	afx_msg void OnBnSelectWorkspace_Clicked();
	afx_msg void OnBnSelectFile_Clicked();
	afx_msg void OnBnSelectFileS_Clicked();
	afx_msg void OnBnExecute_Clicked();
	  
	CButton m_chkUsePowerCap;
	CComboBox m_cbbJoinPlan;
	CButton m_btExecuteRP;
	afx_msg void OnBnClickedBtPsmjExecute2();
};

