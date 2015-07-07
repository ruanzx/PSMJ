// 
// Name: CBnljDlg.cpp : dialog implementation file 
// Author: hieunt
// Description: Block nested loop join implementation
//

#include "stdafx.h"
#include "CBnljDlg.h"


extern std::vector<ComboBoxItem *> g_ComboBoxBufferItems;

CBnljDlg::CBnljDlg()
{

}

CBnljDlg::~CBnljDlg()
{
} 

void CBnljDlg::DoDataExchange(CDataExchange* pDX)
{
	CDialogEx::DoDataExchange(pDX);
	DDX_Control(pDX, IDC_TB_BNLJ_WORKSPACE, m_WorkSpace);
	DDX_Control(pDX, IDC_TB_BNLJ_FILE_1, m_R_Path);
	DDX_Control(pDX, IDC_TB_BNLJ_FILE_2, m_S_Path);
	DDX_Control(pDX, IDC_TB_BNLJ_KEY_POS_1, m_R_Key);
	DDX_Control(pDX, IDC_TB_BNLJ_KEY_POS_2, m_S_Key);
	DDX_Control(pDX, IDC_TB_BNLJ_BUFFER_POOL_SIZE, m_PoolSize);
	DDX_Control(pDX, IDC_CBB_BNLJ_READ_BUFFER_SIZE, m_cbbReadBufferSize);
	DDX_Control(pDX, IDC_CBB_BNLJ_WRITE_BUFFER_SIZE, m_cbbWriteBufferSize);
	DDX_Control(pDX, IDC_BNLJ_EXECUTE, m_btExecute);
}

BEGIN_MESSAGE_MAP(CBnljDlg, CDialogEx) 
	ON_BN_CLICKED(IDC_BNLJ_EXECUTE, &CBnljDlg::OnExecuteBnlj_Clicked) 
	ON_BN_CLICKED(IDC_BT_BNLJ_SELECT_WORKSPACE, &CBnljDlg::OnSelectWorkspace_Clicked)
	ON_BN_CLICKED(IDC_BT_BNLJ_SELECT_FILE_1, &CBnljDlg::OnSelectFile1_Clicked)
	ON_BN_CLICKED(IDC_BT_BNLJ_SELECT_FILE_2, &CBnljDlg::OnSelectFile2_Clicked) 
	ON_REGISTERED_MESSAGE(WaitInfo::UWM_THREAD_TERMINATED, &CBnljDlg::OnThreadTerminated)
END_MESSAGE_MAP()


LRESULT CBnljDlg::OnThreadTerminated(WPARAM wParam, LPARAM lParam)
{
	//m_Notify->PostMessage(UWM_THREAD_TERMINATED, (WPARAM)m_Operation, (LPARAM)m_Handle); 
	HANDLE h = (HANDLE)lParam;
	OPERATIONS op = (OPERATIONS)wParam;
	if(op==OP_BNLJ)
	{
		AfxMessageBox(L"Join done", MB_OK);
		m_btExecute.EnableWindow(TRUE);
	}

	CloseHandle(h);

	return 0;
} 

BOOL CBnljDlg::OnInitDialog()
{
	CDialog::OnInitDialog();  

	// TODO: Add extra initialization here

	for (UINT i=0; i<g_ComboBoxBufferItems.size(); i++)
	{
		m_cbbReadBufferSize.AddString(g_ComboBoxBufferItems[i]->Name);
		m_cbbWriteBufferSize.AddString(g_ComboBoxBufferItems[i]->Name);
	}

	// Set combobox default set to some value
	ComboboxSetSelectedIndexByText(m_cbbReadBufferSize, L"4MB");
	ComboboxSetSelectedIndexByText(m_cbbWriteBufferSize, L"32MB");

	SetDlgItemInt(IDC_TB_BNLJ_BUFFER_POOL_SIZE, DEFAULT_BUFFER_POOL_SIZE_MB, FALSE);

	m_WorkSpace.SetWindowTextW(DEFAULT_WORK_SPACE);
	m_S_Path.EnableWindow(FALSE);
	m_S_Key.EnableWindow(FALSE);  
	m_btExecute.EnableWindow(FALSE);  

	return TRUE; 
}

DWORD WINAPI CBnljDlg::Run(LPVOID lpParam)
{ 
	ThreadParams *p =(ThreadParams *)lpParam;

	BNLJ bnlj(p->bnljParams);
	RC rc = bnlj.Execute();
	if(SUCCESS!=rc)
	{ 
		AfxMessageBox(PrintErrorRC(rc), MB_OK);
	}

	return 0;
}

DWORD WINAPI CBnljDlg::RunEx(LPVOID lpParam)
{
	ThreadParams* p = (ThreadParams*)(lpParam);
	p->_this->Run((LPVOID)(p));
	return 0;
}

void CBnljDlg::OnExecuteBnlj_Clicked()
{   
	DWORD textLength = m_WorkSpace.GetWindowTextLengthW();
	if(textLength==0)
	{ 
		AfxMessageBox(L"Please enter workspace path", MB_OK);
		return;
	}

	textLength = m_R_Path.GetWindowTextLengthW();
	if(textLength==0)
	{ 
		AfxMessageBox(L"Please enter path for R relation", MB_OK);
		return;
	}

	textLength = m_R_Key.GetWindowTextLengthW();
	if(textLength==0)
	{ 
		AfxMessageBox(L"Please enter key attribute for R relation", MB_OK);
		return;
	}

	textLength =  m_S_Path.GetWindowTextLengthW();
	if(textLength==0)
	{ 
		AfxMessageBox(L"Please enter path for S relation", MB_OK);
		return;
	}

	textLength = m_S_Key.GetWindowTextLengthW(); 
	if(textLength==0)
	{ 
		AfxMessageBox(L"Please enter key attribute for S relation", MB_OK);
		return;
	}

	textLength =  m_PoolSize.GetWindowTextLengthW();
	if(textLength==0)
	{ 
		AfxMessageBox(L"Please enter buffer pool size", MB_OK);
		return;
	}

	BNLJ_PARAMS bnljParams;
	m_WorkSpace.GetWindowTextW(bnljParams.WORK_SPACE_PATH, MAX_PATH);

	UINT poolSize = GetDlgItemInt(IDC_TB_BNLJ_BUFFER_POOL_SIZE, FALSE, FALSE);
	bnljParams.BUFFER_POOL_SIZE = poolSize * SSD_PAGE_SIZE * 256;

	m_R_Path.GetWindowTextW(bnljParams.RELATION_R_PATH, MAX_PATH);
	_wsplitpath( bnljParams.RELATION_R_PATH, NULL, NULL, bnljParams.RELATION_R_NO_EXT, NULL); 
	bnljParams.R_KEY_POS = GetDlgItemInt(IDC_TB_BNLJ_KEY_POS_1, FALSE, FALSE);

	m_S_Path.GetWindowTextW(bnljParams.RELATION_S_PATH, MAX_PATH);
	_wsplitpath( bnljParams.RELATION_S_PATH, NULL, NULL, bnljParams.RELATION_S_NO_EXT, NULL);
	bnljParams.S_KEY_POS = GetDlgItemInt(IDC_TB_BNLJ_KEY_POS_2, FALSE, FALSE);

	INT selectedIndex =-1;
	selectedIndex = m_cbbReadBufferSize.GetCurSel();

	for(UINT i = 0; i < g_ComboBoxBufferItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			bnljParams.READ_BUFFER_SIZE = g_ComboBoxBufferItems[i]->Value;
			break;
		} 
	}

	selectedIndex = m_cbbWriteBufferSize.GetCurSel();

	for(UINT i = 0; i < g_ComboBoxBufferItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			bnljParams.WRITE_BUFFER_SIZE = g_ComboBoxBufferItems[i]->Value;
			break;
		} 
	}

	m_btExecute.EnableWindow(FALSE);

	ThreadParams *myParams = new ThreadParams;
	myParams->_this = this;
	myParams->bnljParams = bnljParams;

	m_hBNLJThread = CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)RunEx, myParams, CREATE_SUSPENDED, NULL); 
	SetThreadPriority(m_hBNLJThread, THREAD_PRIORITY_NORMAL);
	m_Requestor.RequestNotification(m_hBNLJThread, this, OP_BNLJ);
	ResumeThread(m_hBNLJThread);  
}

void CBnljDlg::OnSelectWorkspace_Clicked()
{
	TCHAR szWorkspacePath[MAX_PATH] = {0};
	BROWSEINFO bi;
	ZeroMemory(&bi,sizeof(BROWSEINFO));
	bi.hwndOwner = NULL;
	bi.pszDisplayName = szWorkspacePath;
	bi.lpszTitle = _T("Selected folder");
	bi.ulFlags = BIF_RETURNFSANCESTORS;
	LPITEMIDLIST idl = SHBrowseForFolder(&bi); 

	SHGetPathFromIDList(idl, szWorkspacePath);
	lstrcat(szWorkspacePath, L"\\"); 
	m_WorkSpace.SetWindowTextW(szWorkspacePath);  
}

void CBnljDlg::OnSelectFile1_Clicked()
{ 
	TCHAR szSortFilePath[_MAX_PATH];
	OPENFILENAME ofn = { OPENFILENAME_SIZE_VERSION_400 };
	ofn.hwndOwner = this->m_hWnd;
	ofn.lpstrFilter = TEXT("*.*\0");
	lstrcpy(szSortFilePath, TEXT("*.*"));
	ofn.lpstrFile = szSortFilePath;
	ofn.nMaxFile = _countof(szSortFilePath);
	ofn.lpstrTitle = TEXT("Select file");
	ofn.lpstrDefExt = L"dat";
	ofn.lpstrFilter = L"Data File (*.dat)\0*.dat\0"; 
	ofn.Flags = OFN_EXPLORER | OFN_FILEMUSTEXIST;
	BOOL bOk = GetOpenFileName(&ofn);
	if (bOk)
	{ 
		m_R_Path.SetWindowTextW(szSortFilePath); 
	}

	m_S_Path.EnableWindow(bOk);
	m_S_Key.EnableWindow(bOk);  
}


void CBnljDlg::OnSelectFile2_Clicked()
{ 
	TCHAR szSortFilePath[_MAX_PATH];
	OPENFILENAME ofn = { OPENFILENAME_SIZE_VERSION_400 };
	ofn.hwndOwner = this->m_hWnd;
	ofn.lpstrFilter = TEXT("*.*\0");
	lstrcpy(szSortFilePath, TEXT("*.*"));
	ofn.lpstrFile = szSortFilePath;
	ofn.nMaxFile = _countof(szSortFilePath);
	ofn.lpstrTitle = TEXT("Select file");
	ofn.lpstrDefExt = L"dat";
	ofn.lpstrFilter = L"Data File (*.dat)\0*.dat\0"; 
	ofn.Flags = OFN_EXPLORER | OFN_FILEMUSTEXIST; 
	BOOL bOk = GetOpenFileName(&ofn);
	if (bOk)
	{ 
		m_S_Path.SetWindowTextW(szSortFilePath); 
	}

	m_btExecute.EnableWindow(bOk); 
}

