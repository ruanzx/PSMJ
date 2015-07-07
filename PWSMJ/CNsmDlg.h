// 
// Name: CNsmDlg.cpp : dialog implementation file 
// Author: hieunt
// Description: Create Nary page structure file
//

#pragma once 
#include "afxwin.h" 
#include "NSM.h"

// Making TCP-H table file with NSM structure

class CNsmDlg : public CDialogEx
{
public:
	CNsmDlg();

	enum { IDD = IDD_NSM_DIALOG };

	~CNsmDlg();

protected:
	HICON m_hIcon;
	HANDLE m_hNsmThread;
	WaitInfo m_Requestor;

	virtual void DoDataExchange(CDataExchange* pDX);    // DDX/DDV support
	virtual BOOL OnInitDialog();
	LRESULT OnThreadTerminated(WPARAM wParam, LPARAM lParam);

	typedef struct ThreadParams				//structure for passing to the controlling function
	{
		CNsmDlg*	_this;
		NSM_PARAMS nsmParams; // input parameters
	} ThreadParams;

	// Implementation 
	DWORD WINAPI Run(LPVOID lpParam);
	static DWORD WINAPI RunEx(LPVOID lpParam);  

protected:
	DECLARE_MESSAGE_MAP() 
public:
	CButton m_btExecute;
	CEdit m_WorkSpace;
	CEdit m_FilePath;
	afx_msg void OnBnExecute_Clicked();
	afx_msg void OnBnSelectFile_Clicked();
	afx_msg void OnBnSelectWorkspace_Clicked();
};

