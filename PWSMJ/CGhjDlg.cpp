// 
// Name: CGhjDlg.cpp : dialog  implementation file 
// Author: hieunt
// Description: Grace hash join implementation
//


#include "stdafx.h"
#include "CGhjDlg.h"


extern std::vector<ComboBoxItem *> g_ComboBoxBufferItems;

/// <summary>
/// Initializes a new instance of the <see cref="CGhjDlg"/> class.
/// </summary>
CGhjDlg::CGhjDlg()
{
}
 
/// <summary>
/// Finalizes an instance of the <see cref="CGhjDlg"/> class.
/// </summary>
CGhjDlg::~CGhjDlg()
{
}
 
void CGhjDlg::DoDataExchange(CDataExchange* pDX)
{
	CDialogEx::DoDataExchange(pDX);

	DDX_Control(pDX, IDC_CBB_GHJ_WRITE_BUFFER_SIZE, m_cbbWriteBufferSize);
	DDX_Control(pDX, IDC_CBB_GHJ_READ_BUFFER_SIZE, m_cbbReadBufferSize);
	DDX_Control(pDX, IDC_TB_GHJ_BUFFER_POOL_SIZE, m_tbPoolSize);
	DDX_Control(pDX, IDC_TB_GHJ_KEY_POS_2, m_tbKeyPosS);
	DDX_Control(pDX, IDC_TB_GHJ_KEY_POS_1, m_tbKeyPosR);
	DDX_Control(pDX, IDC_TB_GHJ_FILE_1, m_tbFileR);
	DDX_Control(pDX, IDC_TB_GHJ_FILE_2, m_tbFileS);
	DDX_Control(pDX, IDC_TB_GHJ_WORKSPACE, m_tbWorkSpace);
	DDX_Control(pDX, IDC_BT_GHJ_SELECT_WORKSPACE, m_btSelectWorkSpace);
	DDX_Control(pDX, IDC_BT_GHJ_SELECT_FILE_1, m_btSelectR);
	DDX_Control(pDX, IDC_BT_GHJ_SELECT_FILE_2, m_btSelectS);
	DDX_Control(pDX, IDC_BT_GHJ_EXECUTE, m_btExecute);
}

BEGIN_MESSAGE_MAP(CGhjDlg, CDialogEx) 
	ON_REGISTERED_MESSAGE(WaitInfo::UWM_THREAD_TERMINATED, &CGhjDlg::OnThreadTerminated)
	ON_BN_CLICKED(IDC_BT_GHJ_SELECT_WORKSPACE, &CGhjDlg::OnBnSelectWorkspace_Clicked)
	ON_BN_CLICKED(IDC_BT_GHJ_SELECT_FILE_1, &CGhjDlg::OnBnSelectFileR_Clicked)
	ON_BN_CLICKED(IDC_BT_GHJ_SELECT_FILE_2, &CGhjDlg::OnBnSelectFileS_Clicked)
	ON_BN_CLICKED(IDC_BT_GHJ_EXECUTE, &CGhjDlg::OnBnExecute_Clicked)
END_MESSAGE_MAP()


/// <summary>
/// Called when [thread terminated].
/// </summary>
/// <param name="wParam">The w parameter.</param>
/// <param name="lParam">The l parameter.</param>
/// <returns></returns>
LRESULT CGhjDlg::OnThreadTerminated(WPARAM wParam, LPARAM lParam)
{
	//m_Notify->PostMessage(UWM_THREAD_TERMINATED, (WPARAM)m_Operation, (LPARAM)m_Handle); 
	HANDLE h = (HANDLE)lParam;
	OPERATIONS op = (OPERATIONS)wParam;

	if(op==OP_GHJ)
	{
		AfxMessageBox(L"Grace Hash Join done", MB_OK);
		m_btExecute.EnableWindow(TRUE);
	}

	CloseHandle(h);

	return 0;
} 


/// <summary>
/// Called when [initialize dialog].
/// </summary>
/// <returns></returns>
BOOL CGhjDlg::OnInitDialog()
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

	SetDlgItemInt(IDC_TB_GHJ_BUFFER_POOL_SIZE, DEFAULT_BUFFER_POOL_SIZE_MB, FALSE);

	m_tbWorkSpace.SetWindowTextW(DEFAULT_WORK_SPACE);
	m_tbFileS.EnableWindow(FALSE);
	m_tbKeyPosS.EnableWindow(FALSE);  
	m_btExecute.EnableWindow(FALSE);  

	return TRUE;  // return TRUE  unless you set the focus to a control
}


DWORD WINAPI CGhjDlg::RunEx(LPVOID lpParam)
{
	ThreadParams* p = (ThreadParams*)(lpParam);
	p->_this->Run((LPVOID)(p));
	return 0;
}

DWORD WINAPI CGhjDlg::Run(LPVOID lpParam)
{  
	ThreadParams *p =(ThreadParams *)lpParam;

	GHJ ghj(p->ghjParams);
	RC rc = ghj.Execute();
	if(SUCCESS!=rc)
	{ 
		AfxMessageBox(PrintErrorRC(rc), MB_OK);
	}  

	return 0;
}

/// <summary>
/// Called when [bn select workspace_ clicked].
/// </summary>
void CGhjDlg::OnBnSelectWorkspace_Clicked()
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


void CGhjDlg::OnBnSelectFileR_Clicked()
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
/// Called when [bn select file s_ clicked].
/// </summary>
void CGhjDlg::OnBnSelectFileS_Clicked()
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
/// Called when [bn execute_ clicked].
/// </summary>
void CGhjDlg::OnBnExecute_Clicked()
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

	GHJ_PARAMS ghjParams;
	m_tbWorkSpace.GetWindowTextW(ghjParams.WORK_SPACE_PATH, MAX_PATH);

	UINT poolSize = GetDlgItemInt(IDC_TB_GHJ_BUFFER_POOL_SIZE, FALSE, FALSE);
	ghjParams.BUFFER_POOL_SIZE = poolSize * SSD_PAGE_SIZE * 256;

	m_tbFileR.GetWindowTextW(ghjParams.RELATION_R_PATH, MAX_PATH);
	_wsplitpath(ghjParams.RELATION_R_PATH, NULL, NULL, ghjParams.RELATION_R_NO_EXT, NULL); 
	ghjParams.R_KEY_POS = GetDlgItemInt(IDC_TB_GHJ_KEY_POS_1, FALSE, FALSE);

	m_tbFileS.GetWindowTextW(ghjParams.RELATION_S_PATH, MAX_PATH);
	_wsplitpath( ghjParams.RELATION_S_PATH, NULL, NULL, ghjParams.RELATION_S_NO_EXT, NULL);
	ghjParams.S_KEY_POS = GetDlgItemInt(IDC_TB_GHJ_KEY_POS_2, FALSE, FALSE);

	INT selectedIndex =-1;
	selectedIndex = m_cbbReadBufferSize.GetCurSel();

	for(UINT i = 0; i < g_ComboBoxBufferItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			ghjParams.READ_BUFFER_SIZE = g_ComboBoxBufferItems[i]->Value;
			break;
		} 
	}

	selectedIndex = m_cbbWriteBufferSize.GetCurSel();

	for(UINT i = 0; i < g_ComboBoxBufferItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			ghjParams.WRITE_BUFFER_SIZE = g_ComboBoxBufferItems[i]->Value;
			break;
		} 
	}

	ghjParams.FUDGE_FACTOR = 1.2;  
	ghjParams.BUCKET_SIZE = 4096 * 64; // 256 KB
	 

	m_btExecute.EnableWindow(FALSE);

	ThreadParams *myParams = new ThreadParams;
	myParams->_this = this;
	myParams->ghjParams = ghjParams;

	m_hGhjThread = CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)RunEx, myParams, CREATE_SUSPENDED, NULL); 
	SetThreadPriority(m_hGhjThread, THREAD_PRIORITY_NORMAL);
	m_Requestor.RequestNotification(m_hGhjThread, this, OP_GHJ);
	ResumeThread(m_hGhjThread); 
}
