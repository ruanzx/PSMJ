// 
// Name: CCemsDlg.h : dialog implementation file 
// Author: hieunt
// Description: Cache External Merge Sort dialog implementation
//

#pragma once 
#include "afxwin.h" 
#include "CEMS.h"
 

class CCemsDlg : public CDialogEx
{
public:
	CCemsDlg();

	enum { IDD = IDD_CEMS_DIALOG };

	~CCemsDlg();

protected:
	HICON m_hIcon;
	HANDLE m_hCemsThread;
	WaitInfo m_Requestor;

	virtual void DoDataExchange(CDataExchange* pDX);    // DDX/DDV support
	virtual BOOL OnInitDialog();
	LRESULT OnThreadTerminated(WPARAM wParam, LPARAM lParam);

	typedef struct ThreadParams				//structure for passing to the controlling function
	{
		CCemsDlg*	_this;
		CEMS_PARAMS cemsParams;
	} ThreadParams;

	// Implementation 
	DWORD WINAPI Run(LPVOID lpParam);
	static DWORD WINAPI RunEx(LPVOID lpParam);  
	
protected:
	DECLARE_MESSAGE_MAP() 
public:
	CComboBox m_cbbSSDReadSize;
	CComboBox m_cbbSSDWriteSize;
	CComboBox m_cbbHDDReadSize;
	CComboBox m_cbbHDDWriteSize;
	CComboBox m_cbbHeapSortMemorySize;
	CButton m_btExecute;
	CButton m_btSelectFile;
	CEdit m_tbSsdSize;
	CEdit m_tbKeyPos;
	CEdit m_tbFilePath;
	CEdit m_tbHddWorkSpace;
	CEdit m_tbSsdWorkSpace;
	CButton m_chkDeleteAfterUse;
	CEdit m_tbPoolSize; 
	CButton m_btSsdWorkSpace;
	CButton m_btHddWorkSpace;
	afx_msg void OnBnSelectSsdWorkspace_Clicked();
	afx_msg void OnBnSelectHddWorkspace_Clicked();
	afx_msg void OnBnSelectFile_Clicked();
	afx_msg void OnBnExecute_Clicked(); 
};

