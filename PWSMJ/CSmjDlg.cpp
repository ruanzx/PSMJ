// 
// Name: CSmjDlg.cpp : implementation file 
// Author: hieunt
// Description: Clasic Sort merge join dialog
//

#include "stdafx.h"
#include "CSmjDlg.h"

// Global variables
extern std::vector<ComboBoxItem *> g_ComboBoxBufferItems;

/// <summary>
/// Initializes a new instance of the <see cref="CSmjDlg"/> class.
/// </summary>
CSmjDlg::CSmjDlg()
{
}


/// <summary>
/// Finalizes an instance of the <see cref="CSmjDlg"/> class.
/// </summary>
CSmjDlg::~CSmjDlg()
{
}


void CSmjDlg::DoDataExchange(CDataExchange* pDX)
{
	CDialogEx::DoDataExchange(pDX);

	DDX_Control(pDX, IDC_TB_SMJ_WORKSPACE, m_WorkSpace);
	DDX_Control(pDX, IDC_TB_SMJ_FILE_1, m_R_Path);
	DDX_Control(pDX, IDC_TB_SMJ_KEY_POS_1, m_R_Key);
	DDX_Control(pDX, IDC_TB_SMJ_FILE_2, m_S_Path);
	DDX_Control(pDX, IDC_TB_SMJ_KEY_POS_2, m_S_Key);
	DDX_Control(pDX, IDC_TB_SMJ_BUFFER_POOL_SIZE, m_PoolSize);
	DDX_Control(pDX, IDC_CBB_SMJ_READ_BUFFER_SIZE, m_cbbReadSize);
	DDX_Control(pDX, IDC_CBB_SMJ_WRITE_BUFFER_SIZE, m_cbbWriteSize);
	DDX_Control(pDX, IDC_BT_SMJ_EXECUTE, m_btExecute);
}

BEGIN_MESSAGE_MAP(CSmjDlg, CDialogEx) 
	ON_BN_CLICKED(IDC_BT_SMJ_EXECUTE, &CSmjDlg::OnSmjExecute_Clicked)
	ON_BN_CLICKED(IDC_BT_SMJ_SELECT_WORKSPACE, &CSmjDlg::OnSelectWorkspace_Clicked)
	ON_BN_CLICKED(IDC_BT_SMJ_SELECT_FILE_1, &CSmjDlg::OnSmjSelectFile1_Clicked)
	ON_BN_CLICKED(IDC_BT_SMJ_SELECT_FILE_2, &CSmjDlg::OnSmjSelectFile2_Clicked)
	ON_REGISTERED_MESSAGE(WaitInfo::UWM_THREAD_TERMINATED, &CSmjDlg::OnThreadTerminated)
END_MESSAGE_MAP()

/// <summary>
/// Called when [thread terminated].
/// </summary>
/// <param name="wParam">The w parameter.</param>
/// <param name="lParam">The l parameter.</param>
/// <returns></returns>
LRESULT CSmjDlg::OnThreadTerminated(WPARAM wParam, LPARAM lParam)
{
	//m_Notify->PostMessage(UWM_THREAD_TERMINATED, (WPARAM)m_Operation, (LPARAM)m_Handle); 
	HANDLE h = (HANDLE)lParam;
	OPERATIONS op = (OPERATIONS)wParam;
	if(op==OP_SMJ)
	{
		AfxMessageBox(L"Join done", MB_OK);
		m_btExecute.EnableWindow(TRUE);
	}

	CloseHandle(h);

	return 0;
} 

BOOL CSmjDlg::OnInitDialog()
{
	CDialog::OnInitDialog();  

	// TODO: Add extra initialization here
	for (UINT i=0; i<g_ComboBoxBufferItems.size(); i++)
	{
		m_cbbReadSize.AddString(g_ComboBoxBufferItems[i]->Name);
		m_cbbWriteSize.AddString(g_ComboBoxBufferItems[i]->Name);
	}

	ComboboxSetSelectedIndexByText(m_cbbReadSize, L"2MB");
	ComboboxSetSelectedIndexByText(m_cbbWriteSize, L"2MB");

	SetDlgItemInt(IDC_TB_SMJ_BUFFER_POOL_SIZE, DEFAULT_BUFFER_POOL_SIZE_MB, FALSE);

	m_WorkSpace.SetWindowTextW(DEFAULT_WORK_SPACE);
	m_S_Path.EnableWindow(FALSE);
	m_S_Key.EnableWindow(FALSE);  
	m_btExecute.EnableWindow(FALSE);  

	return TRUE;  // return TRUE  unless you set the focus to a control
}


DWORD WINAPI CSmjDlg::RunEx(LPVOID lpParam)
{
	ThreadParams* p = (ThreadParams*)(lpParam);
	p->_this->Run((LPVOID)(p));
	return 0;
}

DWORD WINAPI CSmjDlg::Run(LPVOID lpParam)
{ 
	ThreadParams *p =(ThreadParams *)lpParam;
	 
	SMJ smj(p->smjParams);
	RC rc = smj.Execute();
	if(SUCCESS!=rc)
	{ 
		AfxMessageBox(PrintErrorRC(rc), MB_OK);
	} 

	return 0;
}

void CSmjDlg::OnSmjExecute_Clicked()
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

	SMJ_PARAMS smjParams;
	m_WorkSpace.GetWindowTextW(smjParams.WORK_SPACE_PATH, MAX_PATH);

	UINT poolSize = GetDlgItemInt(IDC_TB_SMJ_BUFFER_POOL_SIZE, FALSE, FALSE);
	smjParams.BUFFER_POOL_SIZE = poolSize * SSD_PAGE_SIZE * 256;

	m_R_Path.GetWindowTextW(smjParams.RELATION_R_PATH, MAX_PATH);
	_wsplitpath( smjParams.RELATION_R_PATH, NULL, NULL, smjParams.RELATION_R_NO_EXT, NULL); 
	smjParams.R_KEY_POS = GetDlgItemInt(IDC_TB_SMJ_KEY_POS_1, FALSE, FALSE);

	m_S_Path.GetWindowTextW(smjParams.RELATION_S_PATH, MAX_PATH);
	_wsplitpath( smjParams.RELATION_S_PATH, NULL, NULL, smjParams.RELATION_S_NO_EXT, NULL);
	smjParams.S_KEY_POS = GetDlgItemInt(IDC_TB_SMJ_KEY_POS_2, FALSE, FALSE);

	INT selectedIndex =-1;
	selectedIndex = m_cbbReadSize.GetCurSel();

	for(int i = 0; i < g_ComboBoxBufferItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			smjParams.READ_BUFFER_SIZE = g_ComboBoxBufferItems[i]->Value;
			break;
		} 
	}

	selectedIndex = m_cbbWriteSize.GetCurSel();

	for(int i = 0; i < g_ComboBoxBufferItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			smjParams.WRITE_BUFFER_SIZE = g_ComboBoxBufferItems[i]->Value;
			break;
		} 
	}

	m_btExecute.EnableWindow(FALSE);

	ThreadParams *myParams = new ThreadParams;
	myParams->_this = this;
	myParams->smjParams = smjParams;

	m_hSmjThread = CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)RunEx, myParams, CREATE_SUSPENDED, NULL); 
	SetThreadPriority(m_hSmjThread, THREAD_PRIORITY_NORMAL);
	m_Requestor.RequestNotification(m_hSmjThread, this, OP_SMJ);
	ResumeThread(m_hSmjThread);  
}


void CSmjDlg::OnSelectWorkspace_Clicked()
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
 
void CSmjDlg::OnSmjSelectFile1_Clicked()
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


void CSmjDlg::OnSmjSelectFile2_Clicked()
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
