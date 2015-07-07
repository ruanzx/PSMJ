// 
// Name: CSmjDlg.cpp : implementation file 
// Author: hieunt
// Description: Clasic Sort merge join dialog
//

#pragma once 
#include "afxwin.h" 
#include "SMJ.h" 

class CSmjDlg : public CDialogEx
{
public:
	CSmjDlg();

	enum { IDD = IDD_SMJ_DIALOG };

	~CSmjDlg();

protected:
	HICON m_hIcon;
	HANDLE m_hSmjThread;
	WaitInfo m_Requestor;

	virtual void DoDataExchange(CDataExchange* pDX);    // DDX/DDV support
	virtual BOOL OnInitDialog();

	typedef struct ThreadParams				//structure for passing to the controlling function
	{
		CSmjDlg*	_this;
		SMJ_PARAMS  smjParams;
	} ThreadParams;

	// Implementation 
	DWORD WINAPI Run(LPVOID lpParam);
	static DWORD WINAPI RunEx(LPVOID lpParam);  

protected:
	DECLARE_MESSAGE_MAP() 
public:
	LRESULT OnThreadTerminated(WPARAM wParam, LPARAM lParam);
	afx_msg void OnSelectWorkspace_Clicked();
	afx_msg void OnSmjSelectFile1_Clicked();
	afx_msg void OnSmjSelectFile2_Clicked();
	afx_msg void OnSmjExecute_Clicked();

	CEdit m_WorkSpace;
	CEdit m_R_Path;
	CEdit m_R_Key;
	CEdit m_S_Path;
	CEdit m_S_Key;
	CEdit m_PoolSize;
	CComboBox m_cbbReadSize;
	CComboBox m_cbbWriteSize;

	CButton m_btExecute;
};

