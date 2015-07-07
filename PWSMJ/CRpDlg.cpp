// 
// Name: CRPDlg.cpp : implementation file 
// Author: hieunt
// Description: Replacement selection dialog
//

#include "stdafx.h"
#include "CRpDlg.h"

// Global variables
extern std::vector<ComboBoxItem *> g_ComboBoxBufferItems;  

/// <summary>
/// Initializes a new instance of the <see cref="CRpDlg"/> class.
/// </summary>
CRpDlg::CRpDlg()
{
}


/// <summary>
/// Finalizes an instance of the <see cref="CRpDlg"/> class.
/// </summary>
CRpDlg::~CRpDlg()
{
}


void CRpDlg::DoDataExchange(CDataExchange* pDX)
{
	CDialogEx::DoDataExchange(pDX);

	DDX_Control(pDX, IDC_BT_RP_EXECUTE, m_btExecute);
	DDX_Control(pDX, IDC_TB_RP_FILE, m_tbFile);
	DDX_Control(pDX, IDC_TB_RP_KEY_POS, m_tbKeyPos);
	DDX_Control(pDX, IDC_TB_RP_WORKSPACE, m_tbWorkSpace);
	DDX_Control(pDX, IDC_BT_RP_SELECT_WORKSPACE, m_btWorkSpace);
	DDX_Control(pDX, IDC_BT_RP_SELECT_FILE, m_btSelectFile);
	DDX_Control(pDX, IDC_TB_RP_BUFFER_POOL_SIZE, m_tbPoolSize);
	DDX_Control(pDX, IDC_CBB_RP_SORT_READ_BUFFER_SIZE, m_cbbSortReadBufferSize);
	DDX_Control(pDX, IDC_CBB_RP_SORT_WRITE_BUFFER_SIZE, m_cbbSortWriteBufferSize);
	DDX_Control(pDX, IDC_CBB_RP_MERGE_READ_BUFFER_SIZE, m_cbbMergeReadBufferSize);
	DDX_Control(pDX, IDC_CBB_RP_MERGE_WRITE_BUFFER_SIZE, m_cbbMergeWriteBufferSize);
}

BEGIN_MESSAGE_MAP(CRpDlg, CDialogEx) 
	ON_REGISTERED_MESSAGE(WaitInfo::UWM_THREAD_TERMINATED, &CRpDlg::OnThreadTerminated)
	ON_BN_CLICKED(IDC_BT_RP_EXECUTE, &CRpDlg::OnBnExecute_Clicked)
	ON_BN_CLICKED(IDC_BT_RP_SELECT_WORKSPACE, &CRpDlg::OnBnSelectWorkspace_Clicked)
	ON_BN_CLICKED(IDC_BT_RP_SELECT_FILE, &CRpDlg::OnBnSelectFile_Clicked)
END_MESSAGE_MAP()


/// <summary>
/// Called when [thread terminated].
/// </summary>
/// <param name="wParam">The w parameter.</param>
/// <param name="lParam">The l parameter.</param>
/// <returns></returns>
LRESULT CRpDlg::OnThreadTerminated(WPARAM wParam, LPARAM lParam)
{
	//m_Notify->PostMessage(UWM_THREAD_TERMINATED, (WPARAM)m_Operation, (LPARAM)m_Handle); 
	HANDLE h = (HANDLE)lParam;
	OPERATIONS op = (OPERATIONS)wParam;
	if(op==OP_RP)
	{
		AfxMessageBox(L"Sort done", MB_OK);
		m_btExecute.EnableWindow(TRUE);
	}

	CloseHandle(h);

	return 0;
} 


BOOL CRpDlg::OnInitDialog()
{
	CDialog::OnInitDialog();  

	// TODO: Add extra initialization here
	for (UINT i=0; i<g_ComboBoxBufferItems.size(); i++)
	{
		m_cbbSortReadBufferSize.AddString(g_ComboBoxBufferItems[i]->Name);
		m_cbbSortWriteBufferSize.AddString(g_ComboBoxBufferItems[i]->Name);
		m_cbbMergeReadBufferSize.AddString(g_ComboBoxBufferItems[i]->Name);
		m_cbbMergeWriteBufferSize.AddString(g_ComboBoxBufferItems[i]->Name);
	}

	ComboboxSetSelectedIndexByText(m_cbbSortReadBufferSize, L"1MB");
	ComboboxSetSelectedIndexByText(m_cbbSortWriteBufferSize, L"4MB");

	ComboboxSetSelectedIndexByText(m_cbbMergeReadBufferSize, L"128KB");
	ComboboxSetSelectedIndexByText(m_cbbMergeWriteBufferSize, L"4MB");

	SetDlgItemInt(IDC_TB_RP_BUFFER_POOL_SIZE, 40, FALSE);//DEFAULT_BUFFER_POOL_SIZE_MB

	m_tbWorkSpace.SetWindowTextW(DEFAULT_WORK_SPACE);

	m_btExecute.EnableWindow(FALSE);  
	return TRUE;  // return TRUE  unless you set the focus to a control
}


DWORD WINAPI CRpDlg::RunEx(LPVOID lpParam)
{
	ThreadParams* p = (ThreadParams*)(lpParam);
	p->_this->Run((LPVOID)(p));
	return 0;
}

DWORD WINAPI CRpDlg::Run(LPVOID lpParam)
{ 
	ThreadParams* p = (ThreadParams*)(lpParam);
	RC rc;
	ReplacementSelection rp(p->rpParams);
	rc = rp.RP_CheckEnoughMemory();
	if(rc!=SUCCESS)
	{
		AfxMessageBox(PrintErrorRC(rc), MB_OK);
		return 1;
	}

	rc = rp.RP_Execute();
	if(rc!=SUCCESS)
	{
		AfxMessageBox(PrintErrorRC(rc), MB_OK);
		return 1;
	}

	return 0;
}

void CRpDlg::OnBnExecute_Clicked()
{
	DWORD textLength = m_tbWorkSpace.GetWindowTextLengthW();
	if(textLength==0)
	{ 
		AfxMessageBox(L"Please enter workspace path", MB_OK);
		return;
	}

	textLength = m_tbFile.GetWindowTextLengthW();
	if(textLength==0)
	{ 
		AfxMessageBox(L"Please enter path for sort file", MB_OK);
		return;
	}

	textLength = m_tbKeyPos.GetWindowTextLengthW();
	if(textLength==0)
	{ 
		AfxMessageBox(L"Please enter key attribute for sort file", MB_OK);
		return;
	}

	textLength =  m_tbPoolSize.GetWindowTextLengthW();
	if(textLength==0)
	{ 
		AfxMessageBox(L"Please enter buffer pool size", MB_OK);
		return;
	}

	RP_PARAMS rpParams;
	m_tbWorkSpace.GetWindowTextW(rpParams.WORK_SPACE_PATH, MAX_PATH);

	UINT poolSize = GetDlgItemInt(IDC_TB_RP_BUFFER_POOL_SIZE, FALSE, FALSE);
	rpParams.BUFFER_POOL_SIZE = poolSize * SSD_PAGE_SIZE * 256;

	m_tbFile.GetWindowTextW(rpParams.SORT_FILE_PATH, MAX_PATH);
	_wsplitpath( rpParams.SORT_FILE_PATH, NULL, NULL, rpParams.FILE_NAME_NO_EXT, NULL); 
	rpParams.KEY_POS = GetDlgItemInt(IDC_TB_RP_KEY_POS, FALSE, FALSE);

	INT selectedIndex =-1;
	selectedIndex = m_cbbSortReadBufferSize.GetCurSel(); 
	for(UINT i = 0; i < g_ComboBoxBufferItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			rpParams.SORT_READ_BUFFER_SIZE = g_ComboBoxBufferItems[i]->Value;
			break;
		} 
	}

	selectedIndex = m_cbbSortWriteBufferSize.GetCurSel(); 
	for(UINT i = 0; i < g_ComboBoxBufferItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			rpParams.SORT_WRITE_BUFFER_SIZE = g_ComboBoxBufferItems[i]->Value;
			break;
		} 
	}

	selectedIndex = m_cbbMergeReadBufferSize.GetCurSel(); 
	for(UINT i = 0; i < g_ComboBoxBufferItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			rpParams.MERGE_READ_BUFFER_SIZE = g_ComboBoxBufferItems[i]->Value;
			break;
		} 
	}

	selectedIndex = m_cbbMergeWriteBufferSize.GetCurSel(); 
	for(UINT i = 0; i < g_ComboBoxBufferItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			rpParams.MERGE_WRITE_BUFFER_SIZE = g_ComboBoxBufferItems[i]->Value;
			break;
		} 
	} 

	rpParams.USE_DELETE_AFTER_OPERATION = FALSE;
	rpParams.USE_PARALLEL_MERGE = FALSE;
	rpParams.USE_LOG_TO_FILE = FALSE;
	rpParams.USE_SYNC_READ_WRITE_MODE = FALSE;

	m_btExecute.EnableWindow(FALSE);

	ThreadParams *myParams = new ThreadParams;
	myParams->_this = this;
	myParams->rpParams = rpParams;

	m_hRpThread = CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)RunEx, myParams, CREATE_SUSPENDED, NULL); 
	SetThreadPriority(m_hRpThread, THREAD_PRIORITY_NORMAL);
	m_Requestor.RequestNotification(m_hRpThread, this, OP_RP);
	ResumeThread(m_hRpThread); 
}


void CRpDlg::OnBnSelectWorkspace_Clicked()
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


void CRpDlg::OnBnSelectFile_Clicked()
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
		m_tbFile.SetWindowTextW(szSortFilePath); 
	}

	m_btExecute.EnableWindow(bOk);
}
