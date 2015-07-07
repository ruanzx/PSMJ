// 
// Name: CHhjDlg.h : dialog header file 
// Author: hieunt
// Description: Hybrid hash join implementation
//

#pragma once  
#include "afxwin.h" 
#include "HHJ.h"

// Hybrid Hash Join Dialog 

class CHhjDlg : public CDialogEx
{
public:
	CHhjDlg();

	enum { IDD = IDD_HHJ_DIALOG };

	~CHhjDlg();

protected:
	HICON m_hIcon;
	HANDLE m_hHhjThread;
	WaitInfo m_Requestor;

	virtual void DoDataExchange(CDataExchange* pDX);    // DDX/DDV support
	virtual BOOL OnInitDialog();
	LRESULT OnThreadTerminated(WPARAM wParam, LPARAM lParam);

	typedef struct ThreadParams				//structure for passing to the controlling function
	{
		CHhjDlg*	_this;
		HHJ_PARAMS   hhjParams; // Input parameters from GUI
	} ThreadParams;

	// Implementation 
	DWORD WINAPI Run(LPVOID lpParam);
	static DWORD WINAPI RunEx(LPVOID lpParam);  

protected:
	DECLARE_MESSAGE_MAP() 
public:
	CEdit m_tbWorkSpace;
	CButton m_btWorkSpace;
	CButton m_btSelectR;
	CButton m_btSelectS;
	CEdit m_tbFileR;
	CEdit m_tbKeyPosR;
	CEdit m_tbFileS;
	CEdit m_tbKeyPosS;
	CEdit m_tbPoolSize;
	CComboBox m_cbbReadBufferSize;
	CComboBox m_cbbWriteBufferSize;
	CButton m_btExecute;
	afx_msg void OnBnSelectWorkspace_Clicked();
	afx_msg void OnBnSelectFileR_Clicked();
	afx_msg void OnBnSelectFileS_Clicked();
	afx_msg void OnBnExecute_Clicked();
};

