#pragma once 
#include "afxwin.h"
#include "BNLJ.h" 
  
/// <summary>
/// Block Nested loop join Dialog header
/// </summary>
class CBnljDlg : public CDialogEx
{
public:
	CBnljDlg();

	// Dialog Data
	enum { IDD = IDD_BNLJ_DIALOG };

	virtual void DoDataExchange(CDataExchange* pDX);    // DDX/DDV support
	virtual BOOL OnInitDialog();

	typedef struct ThreadParams				//structure for passing to the controlling function
	{
		CBnljDlg*	_this;
		BNLJ_PARAMS bnljParams;
	} ThreadParams; 

	// Implementation 
	DWORD WINAPI Run(LPVOID lpParam);
	static DWORD WINAPI RunEx(LPVOID lpParam);  
	
	~CBnljDlg();

protected:
	DECLARE_MESSAGE_MAP() 
public:
	HANDLE	m_hBNLJThread;
	WaitInfo m_Requestor; // wait for worker thread done then notify dialog

	CEdit m_WorkSpace;
	CEdit m_R_Path;
	CEdit m_S_Path;
	CEdit m_R_Key;
	CEdit m_S_Key;
	CEdit m_PoolSize;
	CComboBox m_cbbReadBufferSize;
	CComboBox m_cbbWriteBufferSize;
	afx_msg void OnExecuteBnlj_Clicked(); 
	afx_msg void OnSelectWorkspace_Clicked();
	afx_msg void OnSelectFile1_Clicked();
	afx_msg void OnSelectFile2_Clicked(); 
	LRESULT OnThreadTerminated(WPARAM wParam, LPARAM lParam);
	CButton m_btExecute;
};

