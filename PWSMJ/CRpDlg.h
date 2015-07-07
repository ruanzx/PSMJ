// 
// Name: CRPDlg.cpp : implementation file 
// Author: hieunt
// Description: Replacement selection dialog
//

#pragma once 
#include "afxwin.h" 
#include "ReplacementSelection.h"
 
class CRpDlg : public CDialogEx
{
public:
	CRpDlg();

	enum { IDD = IDD_RP_DIALOG };

	~CRpDlg();

protected:
	HICON m_hIcon;
	HANDLE m_hRpThread;
	WaitInfo m_Requestor;

	virtual void DoDataExchange(CDataExchange* pDX);    // DDX/DDV support
	virtual BOOL OnInitDialog();
	LRESULT OnThreadTerminated(WPARAM wParam, LPARAM lParam);

	typedef struct ThreadParams				//structure for passing to the controlling function
	{
		CRpDlg*	_this;
		RP_PARAMS rpParams; // input parameters
	} ThreadParams;

	// Implementation 
	DWORD WINAPI Run(LPVOID lpParam);
	static DWORD WINAPI RunEx(LPVOID lpParam);  

protected:
	DECLARE_MESSAGE_MAP() 
public:
	CButton m_btExecute;
	CEdit m_tbFile;
	CEdit m_tbKeyPos;
	CEdit m_tbWorkSpace;
	CButton m_btWorkSpace;
	CButton m_btSelectFile;
	CEdit m_tbPoolSize;
	CComboBox m_cbbSortReadBufferSize;
	CComboBox m_cbbSortWriteBufferSize;
	CComboBox m_cbbMergeReadBufferSize;
	CComboBox m_cbbMergeWriteBufferSize;
	afx_msg void OnBnExecute_Clicked();
	afx_msg void OnBnSelectWorkspace_Clicked();
	afx_msg void OnBnSelectFile_Clicked();
};

