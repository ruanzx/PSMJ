// 
// Name: CSmj2Dlg.cpp : implementation file 
// Author: hieunt
// Description: Clasic Sort merge join dialog
//

#pragma once 
#include "afxwin.h" 
#include "SMJ2.h"
#include "TSMJ.h"
 
class CSmj2Dlg : public CDialogEx
{
public:
	CSmj2Dlg();

	enum { IDD = IDD_SMJ2_DIALOG };

	~CSmj2Dlg();

protected:
	HICON m_hIcon;
	HANDLE m_hSmjThread;
	WaitInfo m_Requestor;

	virtual void DoDataExchange(CDataExchange* pDX);    // DDX/DDV support
	virtual BOOL OnInitDialog();
	LRESULT OnThreadTerminated(WPARAM wParam, LPARAM lParam);

	typedef struct ThreadParams				//structure for passing to the controlling function
	{
		CSmj2Dlg*	_this;
		DWORD		_what; // for determine which version of SMJ to run
		SMJ2_PARAMS smjParams;
	} ThreadParams;

	// Implementation 
	DWORD WINAPI Run(LPVOID lpParam);
	static DWORD WINAPI RunEx(LPVOID lpParam);  

protected:
	DECLARE_MESSAGE_MAP() 
public:
	CButton m_btSelectWorkSpace;
	CButton m_btSelectFileR;
	CButton m_btSelectFileS;
	CEdit m_tbFileS;
	CEdit m_tbFileR;
	CEdit m_tbWorkSpace;
	CEdit m_tbKeyR;
	CEdit m_tbKeyS;
	CEdit m_tbPoolSize;
	CComboBox m_cbbSortReadSize;
	CComboBox m_cbbSortWriteSize;
	CComboBox m_cbbMergeReadSize;
	CComboBox m_cbbMergeWriteSize;
	CComboBox m_cbbJoinReadSize;
	CComboBox m_cbbJoinWriteSize;
	CButton m_btExecute;
	CButton m_btExecuteNoDb;
	afx_msg void OnBnExecute_Clicked();
	afx_msg void OnBnSelectWorkspace_Clicked();
	afx_msg void OnBnSelectFileR_Clicked();
	afx_msg void OnBnSelectFileS_Clicked();
	
	afx_msg void OnBnExecuteNoDb_Clicked();
};

