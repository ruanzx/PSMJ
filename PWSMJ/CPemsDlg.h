// 
// Name: CPemsDlg.h
// Author: hieunt
// Description: Parallel external merge sort dialog
//

#pragma once 
#include "afxwin.h" 
#include "ExsPartitionPhase.h"
#include "ExsMergePhase.h"
#include "PEMS.h"
 

class CPemsDlg : public CDialogEx
{
public:
	CPemsDlg();

	enum { IDD = IDD_PEMS_DIALOG };

	~CPemsDlg();

protected:
	HICON m_hIcon;
	HANDLE m_hPemsThread;
	WaitInfo m_Requestor;

	virtual void DoDataExchange(CDataExchange* pDX);    // DDX/DDV support
	virtual BOOL OnInitDialog();
	LRESULT OnThreadTerminated(WPARAM wParam, LPARAM lParam);
		
	typedef struct ThreadParams				//structure for passing to the controlling function
	{
		CPemsDlg*	_this;
		PEMS_PARAMS pemsParams; // input parameters
	} ThreadParams;

	// Implementation 
	DWORD WINAPI Run(LPVOID lpParam); // execute with PageHelpers   (No support Buffer Pool)
	static DWORD WINAPI RunEx(LPVOID lpParam);  

	DWORD WINAPI RunV2(LPVOID lpParam); // execute with PaggeHelpers2  (Support Buffer Pool version)
	static DWORD WINAPI RunV2Ex(LPVOID lpParam);
	 
protected:
	DECLARE_MESSAGE_MAP() 
public:
	CEdit m_WorkSpace;
	CEdit m_FilePath;
	CEdit m_KeyPos;
	CEdit m_PoolSize;
	CComboBox m_cbbThreadNum;
	CComboBox m_cbbSortReadSize;
	CComboBox m_cbbSortWriteSize;
	CComboBox m_cbbMergeReadSize;
	CComboBox m_cbbMergeWriteSize;
	CButton m_chkDeleteAfterUse;
	CButton m_chkParallelMerge;
	CButton m_btExecute;
	CButton m_btExecute_v2;

	afx_msg void OnSelectFile_Clicked();
	afx_msg void OnBnExecute_Clicked();
	afx_msg void OnBnSelectWorkspace_Clicked(); 
	afx_msg void OnBnExecute2_Clicked();
	CStatic m_lbInfos;
	afx_msg void OnStnClickedLbPemsInfo();
};

