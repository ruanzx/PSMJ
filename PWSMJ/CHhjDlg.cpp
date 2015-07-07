// 
// Name: CHhjDlg.cpp : dialog  implementation file 
// Author: hieunt
// Description: Hybrid hash join implementation
//


#include "stdafx.h"
#include "CHhjDlg.h"
 
extern std::vector<ComboBoxItem *> g_ComboBoxBufferItems;

/// <summary>
/// Initializes a new instance of the <see cref="CHhjDlg"/> class.
/// </summary>
CHhjDlg::CHhjDlg()
{

}
 
/// <summary>
/// Finalizes an instance of the <see cref="CHhjDlg"/> class.
/// </summary>
CHhjDlg::~CHhjDlg()
{

}
 
void CHhjDlg::DoDataExchange(CDataExchange* pDX)
{
	CDialogEx::DoDataExchange(pDX);

	DDX_Control(pDX, IDC_TB_HHJ_WORKSPACE, m_tbWorkSpace);
	DDX_Control(pDX, IDC_BT_HHJ_SELECT_WORKSPACE, m_btWorkSpace);
	DDX_Control(pDX, IDC_BT_HHJ_SELECT_FILE_1, m_btSelectR);
	DDX_Control(pDX, IDC_BT_HHJ_SELECT_FILE_2, m_btSelectS);
	DDX_Control(pDX, IDC_TB_HHJ_FILE_1, m_tbFileR);
	DDX_Control(pDX, IDC_TB_HHJ_KEY_POS_1, m_tbKeyPosR);
	DDX_Control(pDX, IDC_TB_HHJ_FILE_2, m_tbFileS);
	DDX_Control(pDX, IDC_TB_HHJ_KEY_POS_2, m_tbKeyPosS);
	DDX_Control(pDX, IDC_TB_HHJ_BUFFER_POOL_SIZE, m_tbPoolSize);
	DDX_Control(pDX, IDC_CBB_HHJ_READ_BUFFER_SIZE, m_cbbReadBufferSize);
	DDX_Control(pDX, IDC_CBB_HHJ_WRITE_BUFFER_SIZE, m_cbbWriteBufferSize);
	DDX_Control(pDX, IDC_BT_HHJ_EXECUTE, m_btExecute);
}

BEGIN_MESSAGE_MAP(CHhjDlg, CDialogEx) 
	ON_REGISTERED_MESSAGE(WaitInfo::UWM_THREAD_TERMINATED, &CHhjDlg::OnThreadTerminated)
	ON_BN_CLICKED(IDC_BT_HHJ_SELECT_WORKSPACE, &CHhjDlg::OnBnSelectWorkspace_Clicked)
	ON_BN_CLICKED(IDC_BT_HHJ_SELECT_FILE_1, &CHhjDlg::OnBnSelectFileR_Clicked)
	ON_BN_CLICKED(IDC_BT_HHJ_SELECT_FILE_2, &CHhjDlg::OnBnSelectFileS_Clicked)
	ON_BN_CLICKED(IDC_BT_HHJ_EXECUTE, &CHhjDlg::OnBnExecute_Clicked)
END_MESSAGE_MAP()


/// <summary>
/// Called when [thread terminated].
/// </summary>
/// <param name="wParam">The w parameter.</param>
/// <param name="lParam">The l parameter.</param>
/// <returns></returns>
LRESULT CHhjDlg::OnThreadTerminated(WPARAM wParam, LPARAM lParam)
{
	//m_Notify->PostMessage(UWM_THREAD_TERMINATED, (WPARAM)m_Operation, (LPARAM)m_Handle); 
	HANDLE h = (HANDLE)lParam;
	OPERATIONS op = (OPERATIONS)wParam;
	if(op==OP_HHJ)
	{
		AfxMessageBox(L"HHJ done", MB_OK);
		//m_btExecute.EnableWindow(TRUE);
	}

	CloseHandle(h);

	return 0;
} 


/// <summary>
/// Called when [initialize dialog].
/// </summary>
/// <returns></returns>
BOOL CHhjDlg::OnInitDialog()
{
	CDialog::OnInitDialog();  

	// TODO: Add extra initialization here
	for (UINT i=0; i<g_ComboBoxBufferItems.size(); i++)
	{
		m_cbbReadBufferSize.AddString(g_ComboBoxBufferItems[i]->Name);
		m_cbbWriteBufferSize.AddString(g_ComboBoxBufferItems[i]->Name);
	}

	ComboboxSetSelectedIndexByText(m_cbbReadBufferSize, L"4MB");
	ComboboxSetSelectedIndexByText(m_cbbWriteBufferSize, L"32MB");

	SetDlgItemInt(IDC_TB_HHJ_BUFFER_POOL_SIZE, DEFAULT_BUFFER_POOL_SIZE_MB, FALSE);

	m_tbWorkSpace.SetWindowTextW(DEFAULT_WORK_SPACE);
	m_tbFileS.EnableWindow(FALSE);
	m_tbKeyPosS.EnableWindow(FALSE);  
	m_btExecute.EnableWindow(FALSE);  

	return TRUE;  // return TRUE  unless you set the focus to a control
}
 
DWORD WINAPI CHhjDlg::RunEx(LPVOID lpParam)
{
	ThreadParams* p = (ThreadParams*)(lpParam);
	p->_this->Run((LPVOID)(p));
	return 0;
}

DWORD WINAPI CHhjDlg::Run(LPVOID lpParam)
{ 
	ThreadParams *p =(ThreadParams *)lpParam;

	HHJ hhj(p->hhjParams);
	RC rc = hhj.Execute();
	if(SUCCESS!=rc)
	{ 
		AfxMessageBox(PrintErrorRC(rc), MB_OK);
	}  

	return 0;
}

/// <summary>
/// Called when [bn select workspace_ clicked].
/// </summary>
void CHhjDlg::OnBnSelectWorkspace_Clicked()
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


/// <summary>
/// Called when [bn select file R clicked].
/// </summary>
void CHhjDlg::OnBnSelectFileR_Clicked()
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

	m_tbFileS.EnableWindow(bOk);
	m_tbKeyPosS.EnableWindow(bOk); 
}


/// <summary>
/// Called when [bn select file S clicked].
/// </summary>
void CHhjDlg::OnBnSelectFileS_Clicked()
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
}


/// <summary>
/// Called when [bn execute clicked].
/// </summary>
void CHhjDlg::OnBnExecute_Clicked()
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

	textLength = m_tbKeyPosR.GetWindowTextLengthW();
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

	textLength = m_tbKeyPosS.GetWindowTextLengthW(); 
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

	HHJ_PARAMS hhjParams;
	m_tbWorkSpace.GetWindowTextW(hhjParams.WORK_SPACE_PATH, MAX_PATH);

	UINT poolSize = GetDlgItemInt(IDC_TB_HHJ_BUFFER_POOL_SIZE, FALSE, FALSE);
	hhjParams.BUFFER_POOL_SIZE = poolSize * SSD_PAGE_SIZE * 256;

	m_tbFileR.GetWindowTextW(hhjParams.RELATION_R_PATH, MAX_PATH);
	_wsplitpath(hhjParams.RELATION_R_PATH, NULL, NULL, hhjParams.RELATION_R_NO_EXT, NULL); 
	hhjParams.R_KEY_POS = GetDlgItemInt(IDC_TB_HHJ_KEY_POS_1, FALSE, FALSE);

	m_tbFileS.GetWindowTextW(hhjParams.RELATION_S_PATH, MAX_PATH);
	_wsplitpath( hhjParams.RELATION_S_PATH, NULL, NULL, hhjParams.RELATION_S_NO_EXT, NULL);
	hhjParams.S_KEY_POS = GetDlgItemInt(IDC_TB_HHJ_KEY_POS_2, FALSE, FALSE);

	INT selectedIndex =-1;
	selectedIndex = m_cbbReadBufferSize.GetCurSel();

	for(int i = 0; i < g_ComboBoxBufferItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			hhjParams.READ_BUFFER_SIZE = g_ComboBoxBufferItems[i]->Value;
			break;
		} 
	}

	selectedIndex = m_cbbWriteBufferSize.GetCurSel();

	for(UINT i = 0; i < g_ComboBoxBufferItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			hhjParams.WRITE_BUFFER_SIZE = g_ComboBoxBufferItems[i]->Value;
			break;
		} 
	}

	hhjParams.FUDGE_FACTOR = 1.2;  
	hhjParams.BUCKET_SIZE = 4096 * 64; // 256 KB 
	m_btExecute.EnableWindow(FALSE);

	ThreadParams *myParams = new ThreadParams;
	myParams->_this = this;
	myParams->hhjParams = hhjParams;

	m_hHhjThread = CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)RunEx, myParams, CREATE_SUSPENDED, NULL); 
	SetThreadPriority(m_hHhjThread, THREAD_PRIORITY_NORMAL);
	m_Requestor.RequestNotification(m_hHhjThread, this, OP_HHJ);
	ResumeThread(m_hHhjThread); 
}
