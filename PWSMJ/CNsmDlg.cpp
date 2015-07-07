// 
// Name: CNsmDlg.cpp : dialog implementation file 
// Author: hieunt
// Description: Create Nary page structure file
//

#include "stdafx.h"
#include "CNsmDlg.h"

/// <summary>
/// Initializes a new instance of the <see cref="CNsmDlg"/> class.
/// </summary>
CNsmDlg::CNsmDlg()
{
}


/// <summary>
/// Finalizes an instance of the <see cref="CNsmDlg"/> class.
/// </summary>
CNsmDlg::~CNsmDlg()
{
}


void CNsmDlg::DoDataExchange(CDataExchange* pDX)
{
	CDialogEx::DoDataExchange(pDX);

	DDX_Control(pDX, IDC_BT_NSM_EXECUTE, m_btExecute);
	DDX_Control(pDX, IDC_TB_NSM_WORKSPACE, m_WorkSpace);
	DDX_Control(pDX, IDC_TB_NSM_FILE, m_FilePath);
}

BEGIN_MESSAGE_MAP(CNsmDlg, CDialogEx) 
	ON_REGISTERED_MESSAGE(WaitInfo::UWM_THREAD_TERMINATED, &CNsmDlg::OnThreadTerminated)
	ON_BN_CLICKED(IDC_BT_NSM_EXECUTE, &CNsmDlg::OnBnExecute_Clicked)
	ON_BN_CLICKED(IDC_BT_NSM_SELECT_FILE, &CNsmDlg::OnBnSelectFile_Clicked)
	ON_BN_CLICKED(IDC_BT_NSM_SELECT_WORKSPACE, &CNsmDlg::OnBnSelectWorkspace_Clicked)
END_MESSAGE_MAP()


/// <summary>
/// Called when [thread terminated].
/// </summary>
/// <param name="wParam">The w parameter.</param>
/// <param name="lParam">The l parameter.</param>
/// <returns></returns>
LRESULT CNsmDlg::OnThreadTerminated(WPARAM wParam, LPARAM lParam)
{
	//m_Notify->PostMessage(UWM_THREAD_TERMINATED, (WPARAM)m_Operation, (LPARAM)m_Handle); 
	HANDLE h = (HANDLE)lParam;
	OPERATIONS op = (OPERATIONS)wParam;
	if(op==OP_NSM)
	{
		AfxMessageBox(L"Create page done", MB_OK);
		m_btExecute.EnableWindow(TRUE);
	}

	CloseHandle(h);

	return 0;
} 


/// <summary>
/// Called when [initialize dialog].
/// </summary>
/// <returns></returns>
BOOL CNsmDlg::OnInitDialog()
{
	CDialog::OnInitDialog();  

	// TODO: Add extra initialization here
	m_WorkSpace.SetWindowTextW(DEFAULT_WORK_SPACE);
	m_btExecute.EnableWindow(FALSE); 
	return TRUE;  // return TRUE  unless you set the focus to a control
}


DWORD WINAPI CNsmDlg::RunEx(LPVOID lpParam)
{
	ThreadParams* p = (ThreadParams*)(lpParam);
	p->_this->Run((LPVOID)(p));
	return 0;
}

DWORD WINAPI CNsmDlg::Run(LPVOID lpParam)
{   
	ThreadParams* p = (ThreadParams*)(lpParam);

	NSM nsm(p->nsmParams);
	nsm.Create();
	return 0;
}

/// <summary>
/// Called when [bn execute_ clicked].
/// </summary>
void CNsmDlg::OnBnExecute_Clicked()
{
	DWORD textLength = m_WorkSpace.GetWindowTextLengthW();
	if(textLength==0)
	{ 
		AfxMessageBox(L"Please enter workspace path", MB_OK);
		return;
	}

	textLength = m_FilePath.GetWindowTextLengthW();
	if(textLength==0)
	{ 
		AfxMessageBox(L"Please enter path for file", MB_OK);
		return;
	}


	NSM_PARAMS nsmParams;
	m_WorkSpace.GetWindowTextW(nsmParams.WORK_SPACE_PATH, MAX_PATH);
	m_FilePath.GetWindowTextW(nsmParams.SORT_FILE_PATH, MAX_PATH);
	_wsplitpath(nsmParams.SORT_FILE_PATH, NULL, NULL, nsmParams.FILE_NAME_NO_EXT, NULL); 

	ThreadParams *myParams = new ThreadParams;
	myParams->_this = this;
	myParams->nsmParams = nsmParams;

	m_btExecute.EnableWindow(FALSE);

	m_hNsmThread = CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)RunEx, myParams, NULL, NULL);  
	SetThreadPriority(m_hNsmThread, THREAD_PRIORITY_NORMAL);
	m_Requestor.RequestNotification(m_hNsmThread, this, OP_NSM);
	ResumeThread(m_hNsmThread);  
}


void CNsmDlg::OnBnSelectFile_Clicked()
{
	TCHAR szSortFilePath[_MAX_PATH];
	OPENFILENAME ofn = { OPENFILENAME_SIZE_VERSION_400 };
	ofn.hwndOwner = this->m_hWnd;
	ofn.lpstrFilter = TEXT("*.tbl\0");
	lstrcpy(szSortFilePath, TEXT("*.tbl"));
	ofn.lpstrFile = szSortFilePath;
	ofn.nMaxFile = _countof(szSortFilePath);
	ofn.lpstrTitle = TEXT("Select file");
	ofn.lpstrDefExt = L"dat";
	ofn.lpstrFilter = L"Tbl File (*.tbl)\0*.tbl\0"; 
	ofn.Flags = OFN_EXPLORER | OFN_FILEMUSTEXIST;
	BOOL bOk = GetOpenFileName(&ofn);
	if (bOk)
	{ 
		m_FilePath.SetWindowTextW(szSortFilePath); 
	}

	m_btExecute.EnableWindow(bOk); 
}


void CNsmDlg::OnBnSelectWorkspace_Clicked()
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
