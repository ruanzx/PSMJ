// 
// Name: CCemsDlg.cpp : dialog implementation file 
// Author: hieunt
// Description: Cache external merge sort dialog implementation
//


#include "stdafx.h"
#include "CCemsDlg.h"

extern std::vector<ComboBoxItem *> g_ComboBoxBufferItems;

CCemsDlg::CCemsDlg()
{
}


CCemsDlg::~CCemsDlg()
{

}


void CCemsDlg::DoDataExchange(CDataExchange* pDX)
{
	CDialogEx::DoDataExchange(pDX);

	DDX_Control(pDX, IDC_CBB_CEMS_SORT_READ_BUFFER_SIZE, m_cbbSSDReadSize);
	DDX_Control(pDX, IDC_CBB_CEMS_SORT_WRITE_BUFFER_SIZE, m_cbbSSDWriteSize);
	DDX_Control(pDX, IDC_CBB_CEMS_MERGE_READ_BUFFER_SIZE, m_cbbHDDReadSize);
	DDX_Control(pDX, IDC_CBB_CEMS_MERGE_WRITE_BUFFER_SIZE, m_cbbHDDWriteSize);
	DDX_Control(pDX, IDC_BT_CEMS_EXECUTE, m_btExecute);
	DDX_Control(pDX, IDC_BT_CEMS_SELECT_FILE, m_btSelectFile);
	DDX_Control(pDX, IDC_TB_CEMS_SSD_STORAGE_SIZE, m_tbSsdSize);
	DDX_Control(pDX, IDC_TB_CEMS_KEY_POS, m_tbKeyPos);
	DDX_Control(pDX, IDC_TB_CEMS_FILE, m_tbFilePath);
	DDX_Control(pDX, IDC_TB_CEMS_WORKSPACE_HDD, m_tbHddWorkSpace);
	DDX_Control(pDX, IDC_TB_CEMS_WORKSPACE, m_tbSsdWorkSpace);
	DDX_Control(pDX, IDC_CHK_CEMS_DELETE_AFTER_USE, m_chkDeleteAfterUse);
	DDX_Control(pDX, IDC_TB_CEMS_BUFFER_POOL_SIZE, m_tbPoolSize);
	DDX_Control(pDX, IDC_BT_CEMS_SELECT_WORKSPACE, m_btSsdWorkSpace);
	DDX_Control(pDX, IDC_BT_CEMS_SELECT_WORKSPACE2, m_btHddWorkSpace);
	DDX_Control(pDX, IDC_CBB_CEMS_HEAP_SORT_MEMORY_SIZE, m_cbbHeapSortMemorySize);
}

BEGIN_MESSAGE_MAP(CCemsDlg, CDialogEx) 
	ON_REGISTERED_MESSAGE(WaitInfo::UWM_THREAD_TERMINATED, &CCemsDlg::OnThreadTerminated)
	ON_BN_CLICKED(IDC_BT_CEMS_EXECUTE, &CCemsDlg::OnBnExecute_Clicked)
	ON_BN_CLICKED(IDC_BT_CEMS_SELECT_WORKSPACE, &CCemsDlg::OnBnSelectSsdWorkspace_Clicked)
	ON_BN_CLICKED(IDC_BT_CEMS_SELECT_WORKSPACE2, &CCemsDlg::OnBnSelectHddWorkspace_Clicked)
	ON_BN_CLICKED(IDC_BT_CEMS_SELECT_FILE, &CCemsDlg::OnBnSelectFile_Clicked)
END_MESSAGE_MAP()


LRESULT CCemsDlg::OnThreadTerminated(WPARAM wParam, LPARAM lParam)
{
	//m_Notify->PostMessage(UWM_THREAD_TERMINATED, (WPARAM)m_Operation, (LPARAM)m_Handle); 
	HANDLE h = (HANDLE)lParam;
	OPERATIONS op = (OPERATIONS)wParam;
	if(op==OP_CEMS)
	{
		AfxMessageBox(L"Done", MB_OK);
		m_btExecute.EnableWindow(TRUE);
	}

	CloseHandle(h);

	return 0;
} 


BOOL CCemsDlg::OnInitDialog()
{
	CDialog::OnInitDialog();  

	for (UINT i=0; i<g_ComboBoxBufferItems.size(); i++)
	{
		m_cbbSSDReadSize.AddString(g_ComboBoxBufferItems[i]->Name);
		m_cbbSSDWriteSize.AddString(g_ComboBoxBufferItems[i]->Name);
		m_cbbHDDReadSize.AddString(g_ComboBoxBufferItems[i]->Name);
		m_cbbHDDWriteSize.AddString(g_ComboBoxBufferItems[i]->Name);
		m_cbbHeapSortMemorySize.AddString(g_ComboBoxBufferItems[i]->Name);
	}

	ComboboxSetSelectedIndexByText(m_cbbSSDReadSize, L"128KB"); // merge buffer
	ComboboxSetSelectedIndexByText(m_cbbSSDWriteSize, L"1MB"); // write buffer
	ComboboxSetSelectedIndexByText(m_cbbHDDReadSize, L"1MB");
	ComboboxSetSelectedIndexByText(m_cbbHDDWriteSize, L"1MB");
	ComboboxSetSelectedIndexByText(m_cbbHeapSortMemorySize, L"4MB");

	SetDlgItemInt(IDC_TB_CEMS_BUFFER_POOL_SIZE, 10, FALSE); //DEFAULT_BUFFER_POOL_SIZE_MB
	SetDlgItemInt(IDC_TB_CEMS_SSD_STORAGE_SIZE, 50, FALSE); //DEFAULT_SSD_STORAGE_SIZE_MB

	m_tbSsdWorkSpace.SetWindowTextW(DEFAULT_WORK_SPACE);
	m_tbHddWorkSpace.SetWindowTextW(DEFAULT_WORK_SPACE_HDD); 
	m_btExecute.EnableWindow(FALSE); 

	return TRUE;  // return TRUE  unless you set the focus to a control
}


/// <summary>
/// Wrapper function, exec Run in class.
/// </summary>
/// <param name="lpParam">The lp parameter.</param>
/// <returns></returns>
DWORD WINAPI CCemsDlg::RunEx(LPVOID lpParam)
{
	ThreadParams* p = (ThreadParams*)(lpParam);
	p->_this->Run((LPVOID)(p));
	return 0;
}

/// <summary>
/// Do work.
/// </summary>
/// <param name="lpParam">The lp parameter.</param>
/// <returns></returns>
DWORD WINAPI CCemsDlg::Run(LPVOID lpParam)
{ 
	ThreadParams* p = (ThreadParams*)(lpParam);
	RC rc;
	CEMS cems(p->cemsParams);
	rc = cems.Execute();
	if(rc!=SUCCESS)
	{
		AfxMessageBox(PrintErrorRC(rc), MB_OK);
		return 1;
	}

	return 0;
}

/// <summary>
/// Called when [bn execute_ clicked].
/// </summary>
void CCemsDlg::OnBnExecute_Clicked()
{
	DWORD textLength = m_tbSsdWorkSpace.GetWindowTextLengthW();
	if(textLength==0)
	{ 
		AfxMessageBox(L"Please enter ssd workspace path", MB_OK);
		return;
	}

	textLength = m_tbHddWorkSpace.GetWindowTextLengthW();
	if(textLength==0)
	{ 
		AfxMessageBox(L"Please enter hdd workspace path", MB_OK);
		return;
	}

	textLength = m_tbFilePath.GetWindowTextLengthW();
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

	CEMS_PARAMS cemsParams;
	m_tbSsdWorkSpace.GetWindowTextW(cemsParams.SSD_WORK_SPACE_PATH, MAX_PATH);
	m_tbHddWorkSpace.GetWindowTextW(cemsParams.HDD_WORK_SPACE_PATH, MAX_PATH);

	UINT poolSize = GetDlgItemInt(IDC_TB_CEMS_BUFFER_POOL_SIZE, FALSE, FALSE);
	cemsParams.BUFFER_POOL_SIZE = poolSize * SSD_PAGE_SIZE * 256;

	UINT ssdSize = GetDlgItemInt(IDC_TB_CEMS_SSD_STORAGE_SIZE, FALSE, FALSE);
	cemsParams.SSD_STORAGE_SIZE = ssdSize * SSD_PAGE_SIZE * 256;

	m_tbFilePath.GetWindowTextW(cemsParams.SORT_FILE_PATH, MAX_PATH);
	_wsplitpath( cemsParams.SORT_FILE_PATH, NULL, NULL, cemsParams.FILE_NAME_NO_EXT, NULL); 
	cemsParams.KEY_POS = GetDlgItemInt(IDC_TB_CEMS_KEY_POS, FALSE, FALSE);

	INT selectedIndex =-1;
	selectedIndex = m_cbbSSDReadSize.GetCurSel(); 
	for(UINT i = 0; i < g_ComboBoxBufferItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			cemsParams.SSD_READ_BUFFER_SIZE = g_ComboBoxBufferItems[i]->Value;
			break;
		} 
	}

	selectedIndex = m_cbbSSDWriteSize.GetCurSel(); 
	for(UINT i = 0; i < g_ComboBoxBufferItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			cemsParams.SSD_WRITE_BUFFER_SIZE = g_ComboBoxBufferItems[i]->Value;
			break;
		} 
	}

	selectedIndex = m_cbbHDDReadSize.GetCurSel(); 
	for(UINT i = 0; i < g_ComboBoxBufferItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			cemsParams.HDD_READ_BUFFER_SIZE = g_ComboBoxBufferItems[i]->Value;
			break;
		} 
	}

	selectedIndex = m_cbbHDDWriteSize.GetCurSel(); 
	for(UINT i = 0; i < g_ComboBoxBufferItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			cemsParams.HDD_WRITE_BUFFER_SIZE = g_ComboBoxBufferItems[i]->Value;
			break;
		} 
	}

	selectedIndex = m_cbbHeapSortMemorySize.GetCurSel(); 
	for(UINT i = 0; i < g_ComboBoxBufferItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			cemsParams.HEAP_SORT_MEMORY_SIZE = g_ComboBoxBufferItems[i]->Value;
			break;
		} 
	}

	cemsParams.USE_DELETE_AFTER_OPERATION = m_chkDeleteAfterUse.GetCheck();   

	m_btExecute.EnableWindow(FALSE); 

	ThreadParams *myParams = new ThreadParams;
	myParams->_this = this;
	myParams->cemsParams = cemsParams;

	m_hCemsThread = CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)RunEx, myParams, CREATE_SUSPENDED, NULL); 
	SetThreadPriority(m_hCemsThread, THREAD_PRIORITY_NORMAL);
	m_Requestor.RequestNotification(m_hCemsThread, this, OP_CEMS);
	ResumeThread(m_hCemsThread); 
} 

/// <summary>
/// Called when [bn select SSD workspace_ clicked].
/// </summary>
void CCemsDlg::OnBnSelectSsdWorkspace_Clicked()
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
	m_tbSsdWorkSpace.SetWindowTextW(szWorkspacePath); 
} 

/// <summary>
/// Called when [bn select HDD workspace_ clicked].
/// </summary>
void CCemsDlg::OnBnSelectHddWorkspace_Clicked()
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
	m_tbHddWorkSpace.SetWindowTextW(szWorkspacePath); 
} 

/// <summary>
/// Called when [bn select file_ clicked].
/// </summary>
void CCemsDlg::OnBnSelectFile_Clicked()
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
		m_tbFilePath.SetWindowTextW(szSortFilePath); 
	}

	m_btExecute.EnableWindow(bOk);  
} 
