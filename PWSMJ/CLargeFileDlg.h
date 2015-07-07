// 
// Name: CLargeFileDlg.h : dialog implementation file 
// Author: hieunt
// Description: Split very large file to small piece, calculate tuple count in file
//

#pragma once 
#include "afxwin.h"  
#include "PageHelpers2.h"

// Debug Dialog, when write very large file, it's imposible for normal text editor read file content
// use this function to read piece of file

class CLargeFileDlg : public CDialogEx
{
public:
	CLargeFileDlg();

	enum { IDD = IDD_DEBUG_DIALOG };

	~CLargeFileDlg();

protected:
	HICON m_hIcon;  

	virtual void DoDataExchange(CDataExchange* pDX);    // DDX/DDV support
	virtual BOOL OnInitDialog();
 
	// Implementation 
 

protected:
	DECLARE_MESSAGE_MAP() 
public:
	 
	CEdit m_WorkSpace;
	CEdit m_FilePath;
	CEdit m_TotalPage;
	afx_msg void OnBnOk_Clicked();
	afx_msg void OnBnSelectWorkspace_Clicked();
	afx_msg void OnBnSelectFile_Clicked();
	CButton m_btExecute;
	CEdit m_FileSize;
	CEdit m_FromPage;
	CEdit m_ToPage;
	CEdit m_TupleCount;
	afx_msg void OnBnClickedDebugTupleCount();
	CButton m_btTupleCount;
};

