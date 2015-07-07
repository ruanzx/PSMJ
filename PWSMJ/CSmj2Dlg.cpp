// 
// Name: CSmj2Dlg.cpp : implementation file 
// Author: hieunt
// Description: Clasic Sort merge join dialog
//

#include "stdafx.h"
#include "CSmj2Dlg.h"

extern std::vector<ComboBoxItem *> g_ComboBoxBufferItems;

CSmj2Dlg::CSmj2Dlg()
{

}


CSmj2Dlg::~CSmj2Dlg()
{
}


void CSmj2Dlg::DoDataExchange(CDataExchange* pDX)
{
	CDialogEx::DoDataExchange(pDX);

	DDX_Control(pDX, IDC_BT_SMJ2_SELECT_WORKSPACE, m_btSelectWorkSpace);
	DDX_Control(pDX, IDC_BT_SMJ2_SELECT_FILE_1, m_btSelectFileR);
	DDX_Control(pDX, IDC_BT_SMJ2_SELECT_FILE_2, m_btSelectFileS);
	DDX_Control(pDX, IDC_TB_SMJ2_FILE_2, m_tbFileS);
	DDX_Control(pDX, IDC_TB_SMJ2_FILE_1, m_tbFileR);
	DDX_Control(pDX, IDC_TB_SMJ2_WORKSPACE, m_tbWorkSpace);
	DDX_Control(pDX, IDC_TB_SMJ2_KEY_POS_1, m_tbKeyR);
	DDX_Control(pDX, IDC_TB_SMJ2_KEY_POS_2, m_tbKeyS);
	DDX_Control(pDX, IDC_TB_SMJ2_BUFFER_POOL_SIZE, m_tbPoolSize);
	DDX_Control(pDX, IDC_CBB_SMJ2_SORT_READ_BUFFER_SIZE, m_cbbSortReadSize);
	DDX_Control(pDX, IDC_CBB_SMJ2_SORT_WRITE_BUFFER_SIZE, m_cbbSortWriteSize);
	DDX_Control(pDX, IDC_CBB_SMJ2_MERGE_READ_BUFFER_SIZE, m_cbbMergeReadSize);
	DDX_Control(pDX, IDC_CBB_SMJ2_MERGE_WRITE_BUFFER_SIZE, m_cbbMergeWriteSize);
	DDX_Control(pDX, IDC_CBB_SMJ2_JOIN_READ_BUFFER_SIZE, m_cbbJoinReadSize);
	DDX_Control(pDX, IDC_CBB_SMJ2_JOIN_WRITE_BUFFER_SIZE, m_cbbJoinWriteSize);
	DDX_Control(pDX, IDC_BT_SMJ2_EXECUTE, m_btExecute);
	DDX_Control(pDX, IDC_BT_SMJ2_EXECUTE_NO_DB, m_btExecuteNoDb);
}

BEGIN_MESSAGE_MAP(CSmj2Dlg, CDialogEx) 
	ON_REGISTERED_MESSAGE(WaitInfo::UWM_THREAD_TERMINATED, &CSmj2Dlg::OnThreadTerminated)
	ON_BN_CLICKED(IDC_BT_SMJ2_EXECUTE, &CSmj2Dlg::OnBnExecute_Clicked)
	ON_BN_CLICKED(IDC_BT_SMJ2_SELECT_WORKSPACE, &CSmj2Dlg::OnBnSelectWorkspace_Clicked)
	ON_BN_CLICKED(IDC_BT_SMJ2_SELECT_FILE_1, &CSmj2Dlg::OnBnSelectFileR_Clicked)
	ON_BN_CLICKED(IDC_BT_SMJ2_SELECT_FILE_2, &CSmj2Dlg::OnBnSelectFileS_Clicked)
	ON_BN_CLICKED(IDC_BT_SMJ2_EXECUTE_NO_DB, &CSmj2Dlg::OnBnExecuteNoDb_Clicked)
END_MESSAGE_MAP()


LRESULT CSmj2Dlg::OnThreadTerminated(WPARAM wParam, LPARAM lParam)
{
	//m_Notify->PostMessage(UWM_THREAD_TERMINATED, (WPARAM)m_Operation, (LPARAM)m_Handle); 
	HANDLE h = (HANDLE)lParam;
	OPERATIONS op = (OPERATIONS)wParam;
	if(op==OP_SMJ2)
	{
		AfxMessageBox(L"Done", MB_OK);
		m_btExecute.EnableWindow(TRUE);
	}

	CloseHandle(h);

	return 0;
} 


BOOL CSmj2Dlg::OnInitDialog()
{
	CDialog::OnInitDialog();  

	// TODO: Add extra initialization here
	for (UINT i=0; i<g_ComboBoxBufferItems.size(); i++)
	{
		m_cbbSortReadSize.AddString(g_ComboBoxBufferItems[i]->Name);
		m_cbbSortWriteSize.AddString(g_ComboBoxBufferItems[i]->Name);
		m_cbbMergeReadSize.AddString(g_ComboBoxBufferItems[i]->Name);
		m_cbbMergeWriteSize.AddString(g_ComboBoxBufferItems[i]->Name);
		m_cbbJoinReadSize.AddString(g_ComboBoxBufferItems[i]->Name);
		m_cbbJoinWriteSize.AddString(g_ComboBoxBufferItems[i]->Name);
	}

	ComboboxSetSelectedIndexByText(m_cbbSortReadSize, L"2MB");
	ComboboxSetSelectedIndexByText(m_cbbSortWriteSize, L"2MB");
	ComboboxSetSelectedIndexByText(m_cbbMergeReadSize, L"128KB");
	ComboboxSetSelectedIndexByText(m_cbbMergeWriteSize, L"2MB");
	ComboboxSetSelectedIndexByText(m_cbbJoinReadSize, L"2MB");
	ComboboxSetSelectedIndexByText(m_cbbJoinWriteSize, L"2MB");

	SetDlgItemInt(IDC_TB_SMJ2_BUFFER_POOL_SIZE, DEFAULT_BUFFER_POOL_SIZE_MB, FALSE);

	m_tbWorkSpace.SetWindowTextW(DEFAULT_WORK_SPACE);
	m_btSelectFileS.EnableWindow(FALSE);
	m_tbFileS.EnableWindow(FALSE);
	m_tbKeyS.EnableWindow(FALSE);  
	m_btExecute.EnableWindow(FALSE);  
	m_btExecuteNoDb.EnableWindow(FALSE); 
	return TRUE;  // return TRUE  unless you set the focus to a control
}


DWORD WINAPI CSmj2Dlg::RunEx(LPVOID lpParam)
{
	ThreadParams* p = (ThreadParams*)(lpParam);
	p->_this->Run((LPVOID)(p));
	return 0;
}

DWORD WINAPI CSmj2Dlg::Run(LPVOID lpParam)
{ 
	ThreadParams *p =(ThreadParams *)lpParam;
	if(p->_what==1) // smj version with double buffer
	{
		//AfxMessageBox(L"1", MB_OK);
		//return 0;
		SMJ2 smj(p->smjParams);
		RC rc = smj.Execute();
		if(SUCCESS!=rc)
		{ 
			AfxMessageBox(PrintErrorRC(rc), MB_OK);
		}
	}
	else if(p->_what==2) // smj version without double buffer
	{
		//AfxMessageBox(L"2", MB_OK);
		//return 0;
		TSMJ smj(p->smjParams);
		RC rc = smj.Execute();
		if(SUCCESS!=rc)
		{ 
			AfxMessageBox(PrintErrorRC(rc), MB_OK);
		}
	}
	return 0;
}

void CSmj2Dlg::OnBnExecute_Clicked()
{
	DWORD textLength = m_tbWorkSpace.GetWindowTextLengthW();
	if(textLength==0)
	{ 
		AfxMessageBox(L"Please enter workspace path", MB_OK);
		return;
	}

	textLength = m_tbFileR.GetWindowTextLengthW();
	if(textLength==0)
	{ 
		AfxMessageBox(L"Please enter path for R relation", MB_OK);
		return;
	}

	textLength = m_tbKeyR.GetWindowTextLengthW();
	if(textLength==0)
	{ 
		AfxMessageBox(L"Please enter key attribute for R relation", MB_OK);
		return;
	}

	textLength =  m_tbFileS.GetWindowTextLengthW();
	if(textLength==0)
	{ 
		AfxMessageBox(L"Please enter path for S relation", MB_OK);
		return;
	}

	textLength = m_tbKeyS.GetWindowTextLengthW(); 
	if(textLength==0)
	{ 
		AfxMessageBox(L"Please enter key attribute for S relation", MB_OK);
		return;
	}

	textLength =  m_tbPoolSize.GetWindowTextLengthW();
	if(textLength==0)
	{ 
		AfxMessageBox(L"Please enter buffer pool size", MB_OK);
		return;
	}

	SMJ2_PARAMS smjParams;
	m_tbWorkSpace.GetWindowTextW(smjParams.WORK_SPACE_PATH, MAX_PATH);

	UINT poolSize = GetDlgItemInt(IDC_TB_SMJ2_BUFFER_POOL_SIZE, FALSE, FALSE);
	smjParams.BUFFER_POOL_SIZE = poolSize * SSD_PAGE_SIZE * 256;

	m_tbFileR.GetWindowTextW(smjParams.RELATION_R_PATH, MAX_PATH);
	_wsplitpath( smjParams.RELATION_R_PATH, NULL, NULL, smjParams.RELATION_R_NO_EXT, NULL); 
	smjParams.R_KEY_POS = GetDlgItemInt(IDC_TB_SMJ2_KEY_POS_1, FALSE, FALSE);

	m_tbFileS.GetWindowTextW(smjParams.RELATION_S_PATH, MAX_PATH);
	_wsplitpath( smjParams.RELATION_S_PATH, NULL, NULL, smjParams.RELATION_S_NO_EXT, NULL);
	smjParams.S_KEY_POS = GetDlgItemInt(IDC_TB_SMJ2_KEY_POS_2, FALSE, FALSE);

	INT selectedIndex =-1;
	selectedIndex = m_cbbSortReadSize.GetCurSel(); 
	for(int i = 0; i < g_ComboBoxBufferItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			smjParams.SORT_READ_BUFFER_SIZE = g_ComboBoxBufferItems[i]->Value;
			break;
		} 
	}

	selectedIndex = m_cbbSortWriteSize.GetCurSel(); 
	for(int i = 0; i < g_ComboBoxBufferItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			smjParams.SORT_WRITE_BUFFER_SIZE = g_ComboBoxBufferItems[i]->Value;
			break;
		} 
	}

	//////////////////////////////////////////////////////////////////////////
	selectedIndex = m_cbbMergeReadSize.GetCurSel(); 
	for(int i = 0; i < g_ComboBoxBufferItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			smjParams.MERGE_READ_BUFFER_SIZE  = g_ComboBoxBufferItems[i]->Value;
			break;
		} 
	}

	selectedIndex = m_cbbMergeWriteSize.GetCurSel(); 
	for(int i = 0; i < g_ComboBoxBufferItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			smjParams.MERGE_WRITE_BUFFER_SIZE = g_ComboBoxBufferItems[i]->Value;
			break;
		} 
	}
	//////////////////////////////////////////////////////////////////////////
	selectedIndex = m_cbbJoinReadSize.GetCurSel(); 
	for(int i = 0; i < g_ComboBoxBufferItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			smjParams.JOIN_READ_BUFFER_SIZE   = g_ComboBoxBufferItems[i]->Value;
			break;
		} 
	}

	selectedIndex = m_cbbJoinWriteSize.GetCurSel(); 
	for(int i = 0; i < g_ComboBoxBufferItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			smjParams.JOIN_WRITE_BUFFER_SIZE = g_ComboBoxBufferItems[i]->Value;
			break;
		} 
	}

	m_btExecute.EnableWindow(FALSE);

	ThreadParams *myParams = new ThreadParams;
	myParams->_this = this;
	myParams->_what = 1;
	myParams->smjParams = smjParams;

	m_hSmjThread = CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)RunEx, myParams, CREATE_SUSPENDED, NULL); 
	SetThreadPriority(m_hSmjThread, THREAD_PRIORITY_NORMAL);
	m_Requestor.RequestNotification(m_hSmjThread, this, OP_SMJ2);
	ResumeThread(m_hSmjThread);  
}


void CSmj2Dlg::OnBnSelectWorkspace_Clicked()
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
	m_tbWorkSpace.SetWindowTextW(szWorkspacePath); 
}

void CSmj2Dlg::OnBnSelectFileR_Clicked()
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
		m_tbFileR.SetWindowTextW(szSortFilePath); 
	}
	m_btSelectFileS.EnableWindow(bOk);
	m_tbFileS.EnableWindow(bOk);
	m_tbKeyS.EnableWindow(bOk);  
}


void CSmj2Dlg::OnBnSelectFileS_Clicked()
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
		m_tbFileS.SetWindowTextW(szSortFilePath); 
	}

	m_btExecute.EnableWindow(bOk); 
	m_btExecuteNoDb.EnableWindow(bOk); 
}


void CSmj2Dlg::OnBnExecuteNoDb_Clicked()
{
	// SMJ version without Double buffer

	DWORD textLength = m_tbWorkSpace.GetWindowTextLengthW();
	if(textLength==0)
	{ 
		AfxMessageBox(L"Please enter workspace path", MB_OK);
		return;
	}

	textLength = m_tbFileR.GetWindowTextLengthW();
	if(textLength==0)
	{ 
		AfxMessageBox(L"Please enter path for R relation", MB_OK);
		return;
	}

	textLength = m_tbKeyR.GetWindowTextLengthW();
	if(textLength==0)
	{ 
		AfxMessageBox(L"Please enter key attribute for R relation", MB_OK);
		return;
	}

	textLength =  m_tbFileS.GetWindowTextLengthW();
	if(textLength==0)
	{ 
		AfxMessageBox(L"Please enter path for S relation", MB_OK);
		return;
	}

	textLength = m_tbKeyS.GetWindowTextLengthW(); 
	if(textLength==0)
	{ 
		AfxMessageBox(L"Please enter key attribute for S relation", MB_OK);
		return;
	}

	textLength =  m_tbPoolSize.GetWindowTextLengthW();
	if(textLength==0)
	{ 
		AfxMessageBox(L"Please enter buffer pool size", MB_OK);
		return;
	}

	SMJ2_PARAMS smjParams;
	m_tbWorkSpace.GetWindowTextW(smjParams.WORK_SPACE_PATH, MAX_PATH);

	UINT poolSize = GetDlgItemInt(IDC_TB_SMJ2_BUFFER_POOL_SIZE, FALSE, FALSE);
	smjParams.BUFFER_POOL_SIZE = poolSize * SSD_PAGE_SIZE * 256;

	m_tbFileR.GetWindowTextW(smjParams.RELATION_R_PATH, MAX_PATH);
	_wsplitpath( smjParams.RELATION_R_PATH, NULL, NULL, smjParams.RELATION_R_NO_EXT, NULL); 
	smjParams.R_KEY_POS = GetDlgItemInt(IDC_TB_SMJ2_KEY_POS_1, FALSE, FALSE);

	m_tbFileS.GetWindowTextW(smjParams.RELATION_S_PATH, MAX_PATH);
	_wsplitpath( smjParams.RELATION_S_PATH, NULL, NULL, smjParams.RELATION_S_NO_EXT, NULL);
	smjParams.S_KEY_POS = GetDlgItemInt(IDC_TB_SMJ2_KEY_POS_2, FALSE, FALSE);

	INT selectedIndex =-1;
	selectedIndex = m_cbbSortReadSize.GetCurSel(); 
	for(int i = 0; i < g_ComboBoxBufferItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			smjParams.SORT_READ_BUFFER_SIZE = g_ComboBoxBufferItems[i]->Value;
			break;
		} 
	}

	selectedIndex = m_cbbSortWriteSize.GetCurSel(); 
	for(int i = 0; i < g_ComboBoxBufferItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			smjParams.SORT_WRITE_BUFFER_SIZE = g_ComboBoxBufferItems[i]->Value;
			break;
		} 
	}

	//////////////////////////////////////////////////////////////////////////
	selectedIndex = m_cbbMergeReadSize.GetCurSel(); 
	for(int i = 0; i < g_ComboBoxBufferItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			smjParams.MERGE_READ_BUFFER_SIZE  = g_ComboBoxBufferItems[i]->Value;
			break;
		} 
	}

	selectedIndex = m_cbbMergeWriteSize.GetCurSel(); 
	for(int i = 0; i < g_ComboBoxBufferItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			smjParams.MERGE_WRITE_BUFFER_SIZE = g_ComboBoxBufferItems[i]->Value;
			break;
		} 
	}
	//////////////////////////////////////////////////////////////////////////
	selectedIndex = m_cbbJoinReadSize.GetCurSel(); 
	for(int i = 0; i < g_ComboBoxBufferItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			smjParams.JOIN_READ_BUFFER_SIZE   = g_ComboBoxBufferItems[i]->Value;
			break;
		} 
	}

	selectedIndex = m_cbbJoinWriteSize.GetCurSel(); 
	for(int i = 0; i < g_ComboBoxBufferItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			smjParams.JOIN_WRITE_BUFFER_SIZE = g_ComboBoxBufferItems[i]->Value;
			break;
		} 
	}

	m_btExecute.EnableWindow(FALSE);

	ThreadParams *myParams = new ThreadParams;
	myParams->_this = this;
	myParams->_what = 2;
	myParams->smjParams = smjParams;

	m_hSmjThread = CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)RunEx, myParams, CREATE_SUSPENDED, NULL); 
	SetThreadPriority(m_hSmjThread, THREAD_PRIORITY_NORMAL);
	m_Requestor.RequestNotification(m_hSmjThread, this, OP_SMJ2);
	ResumeThread(m_hSmjThread);  
}
