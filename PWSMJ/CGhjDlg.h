// 
// Name: CGhjDlg.h : dialog header file 
// Author: hieunt
// Description: Grace hash join implementation
//


#pragma once 

#include "afxwin.h" 
#include "GHJ.h"

// Grace Hash Join dialog

class CGhjDlg : public CDialogEx
{
public:
	CGhjDlg();

	enum { IDD = IDD_GHJ_DIALOG };

	~CGhjDlg();

protected:
	HICON m_hIcon;
	HANDLE m_hGhjThread;
	WaitInfo m_Requestor;

	virtual void DoDataExchange(CDataExchange* pDX);    // DDX/DDV support
	virtual BOOL OnInitDialog();
	LRESULT OnThreadTerminated(WPARAM wParam, LPARAM lParam);

	typedef struct ThreadParams				//structure for passing to the controlling function
	{
		CGhjDlg*	_this;
		GHJ_PARAMS   ghjParams; // input parameters from GUI
	} ThreadParams;

	// Implementation 
	DWORD WINAPI Run(LPVOID lpParam);
	static DWORD WINAPI RunEx(LPVOID lpParam);  

protected:
	DECLARE_MESSAGE_MAP() 
public:
	CComboBox m_cbbWriteBufferSize;
	CComboBox m_cbbReadBufferSize;
	CEdit m_tbPoolSize;
	CEdit m_tbKeyPosS;
	CEdit m_tbKeyPosR;
	CEdit m_tbFileR;
	CEdit m_tbFileS;
	CEdit m_tbWorkSpace;
	CButton m_btSelectWorkSpace;
	CButton m_btSelectR;
	CButton m_btSelectS;
	CButton m_btExecute;
	afx_msg void OnBnSelectWorkspace_Clicked();
	afx_msg void OnBnSelectFileR_Clicked();
	afx_msg void OnBnSelectFileS_Clicked();
	afx_msg void OnBnExecute_Clicked();

};

