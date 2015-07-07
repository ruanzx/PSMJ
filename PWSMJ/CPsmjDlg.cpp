// 
// Name: CPsmjDlg.cpp
// Author: hieunt
// Description: Parallel sort-merge join dialog
//

#include "stdafx.h"
#include "CPsmjDlg.h"

extern std::vector<ComboBoxItem *> g_ComboBoxBufferItems;  
extern UINT g_ThreadNum[];

/// <summary>
/// Initializes a new instance of the <see cref="CPsmjDlg"/> class.
/// </summary>
CPsmjDlg::CPsmjDlg()
{
}


/// <summary>
/// Finalizes an instance of the <see cref="CPsmjDlg"/> class.
/// </summary>
CPsmjDlg::~CPsmjDlg()
{

}


void CPsmjDlg::DoDataExchange(CDataExchange* pDX)
{
	CDialogEx::DoDataExchange(pDX);

	DDX_Control(pDX, IDC_TB_PSMJ_FILE_S, m_tbRelationPath_S);
	DDX_Control(pDX, IDC_TB_PEMS_FILE, m_tbRelationPath_R);
	DDX_Control(pDX, IDC_TB_PSMJ_KEY_POS_R, m_tbKeyPos_R);
	DDX_Control(pDX, IDC_TB_PSMJ_KEY_POS_S, m_tbKeyPos_S);
	DDX_Control(pDX, IDC_TB_PSMJ_WORKSPACE, m_tbWorkSpace);
	DDX_Control(pDX, IDC_CBB_PSMJ_SORT_READ_BUFFER_SIZE, m_cbbSortReadBufferSize);
	DDX_Control(pDX, IDC_CBB_PSMJ_SORT_WRITE_BUFFER_SIZE, m_cbbSortWriteBufferSize);
	DDX_Control(pDX, IDC_BT_PSMJ_EXECUTE, m_btExecute);
	DDX_Control(pDX, IDC_CBB_PSMJ_THREAD_NUM, m_cbbThreadNum);
	DDX_Control(pDX, IDC_BT_PSMJ_SELECT_FILE_S, m_btSelectS);
	DDX_Control(pDX, IDC_BT_PEMS_SELECT_FILE, m_btSelectR);
	DDX_Control(pDX, IDC_TB_PSMJ_BUFFER_POOL_SIZE, m_tbPoolSize);  
	DDX_Control(pDX, IDC_CHK_PSMJ_USE_POWER_CAP, m_chkUsePowerCap);
	DDX_Control(pDX, IDC_CBB_PSMJ_JOIN_PLAN, m_cbbJoinPlan);
	DDX_Control(pDX, IDC_BT_PSMJ_EXECUTE2, m_btExecuteRP);
}

BEGIN_MESSAGE_MAP(CPsmjDlg, CDialogEx) 
	ON_REGISTERED_MESSAGE(WaitInfo::UWM_THREAD_TERMINATED, &CPsmjDlg::OnThreadTerminated)
	ON_BN_CLICKED(IDC_BT_PSMJ_SELECT_WORKSPACE, &CPsmjDlg::OnBnSelectWorkspace_Clicked)
	ON_BN_CLICKED(IDC_BT_PEMS_SELECT_FILE, &CPsmjDlg::OnBnSelectFile_Clicked)
	ON_BN_CLICKED(IDC_BT_PSMJ_SELECT_FILE_S, &CPsmjDlg::OnBnSelectFileS_Clicked)
	ON_BN_CLICKED(IDC_BT_PSMJ_EXECUTE, &CPsmjDlg::OnBnExecute_Clicked)
	ON_BN_CLICKED(IDC_BT_PSMJ_EXECUTE2, &CPsmjDlg::OnBnClickedBtPsmjExecute2)
END_MESSAGE_MAP()


/// <summary>
/// Called when [thread terminated].
/// </summary>
/// <param name="wParam">The w parameter.</param>
/// <param name="lParam">The l parameter.</param>
/// <returns></returns>
LRESULT CPsmjDlg::OnThreadTerminated(WPARAM wParam, LPARAM lParam)
{
	//m_Notify->PostMessage(UWM_THREAD_TERMINATED, (WPARAM)m_Operation, (LPARAM)m_Handle); 
	HANDLE h = (HANDLE)lParam;
	OPERATIONS op = (OPERATIONS)wParam;

	if(op==OP_PSMJ)
	{
		AfxMessageBox(L"Join done", MB_OK);
		m_btExecute.EnableWindow(TRUE);
		m_btExecuteRP.EnableWindow(TRUE);
	}

	CloseHandle(h);

	return 0;
} 


/// <summary>
/// Called when [initialize dialog].
/// </summary>
/// <returns></returns>
BOOL CPsmjDlg::OnInitDialog()
{
	CDialog::OnInitDialog();  

	// TODO: Add extra initialization here

	for (UINT i=0; i<g_ComboBoxBufferItems.size(); i++)
	{
		m_cbbSortReadBufferSize.AddString(g_ComboBoxBufferItems[i]->Name);
		m_cbbSortWriteBufferSize.AddString(g_ComboBoxBufferItems[i]->Name);
		//m_cbbMergeReadSize.AddString(g_ComboBoxBufferItems[i]->Name);
		//m_cbbMergeWriteSize.AddString(g_ComboBoxBufferItems[i]->Name);
	}


	ComboboxSetSelectedIndexByText(m_cbbSortReadBufferSize, L"2MB");
	ComboboxSetSelectedIndexByText(m_cbbSortWriteBufferSize, L"2MB");

	//ComboboxSetSelectedIndexByText(m_cbbMergeReadSize, L"128KB");
	//ComboboxSetSelectedIndexByText(m_cbbMergeWriteSize, L"8MB");

	for(int i = 0; i < 7; i++)
	{
		m_cbbThreadNum.AddString(LongToString(g_ThreadNum[i], L"%d")); 
	} 

	ComboboxSetSelectedIndexByText(m_cbbThreadNum, L"0");

	SetDlgItemInt(IDC_TB_PSMJ_BUFFER_POOL_SIZE, DEFAULT_BUFFER_POOL_SIZE_MB, FALSE);

	joinPlanItems.clear();
	ComboBoxItem *item;
	item = new ComboBoxItem (L"1" , PLAN_1);joinPlanItems.push_back(item); 
	item = new ComboBoxItem (L"2" , PLAN_2);joinPlanItems.push_back(item); 
	item = new ComboBoxItem (L"3" , PLAN_3);joinPlanItems.push_back(item); 
	item = new ComboBoxItem (L"4" , PLAN_4);joinPlanItems.push_back(item); 

	for (UINT i=0; i<joinPlanItems.size(); i++)
	{
		m_cbbJoinPlan.AddString(joinPlanItems[i]->Name); 
	}
	ComboboxSetSelectedIndexByText(m_cbbJoinPlan, L"3"); 


	m_chkUsePowerCap.SetCheck(FALSE);
	//m_chkUseBarrier.SetCheck(FALSE);

	m_tbWorkSpace.SetWindowTextW(DEFAULT_WORK_SPACE);

	m_btSelectS.EnableWindow(FALSE);  
	m_tbRelationPath_S.EnableWindow(FALSE); 
	m_tbKeyPos_S.EnableWindow(FALSE); 

	m_btExecute.EnableWindow(FALSE);   
	m_btExecuteRP.EnableWindow(FALSE);  

	//////////////////////////////////////////////////////////////////////////


	//////////////////////////////////////////////////////////////////////////


	return TRUE;  // return TRUE  unless you set the focus to a control
}


DWORD WINAPI CPsmjDlg::RunEx(LPVOID lpParam)
{
	ThreadParams* p = (ThreadParams*)(lpParam);
	p->_this->Run((LPVOID)(p));
	return 0;
}


DWORD WINAPI CPsmjDlg::RunEx2(LPVOID lpParam)
{
	ThreadParams2* p = (ThreadParams2*)(lpParam);
	p->_this->Run2((LPVOID)(p));
	return 0;
}

DWORD WINAPI CPsmjDlg::Run(LPVOID lpParam)
{  
	ThreadParams* p = (ThreadParams*)(lpParam);
	RC rc;
	PSMJ psmj(p->psmjParams);
	rc = psmj.Execute();
	if(SUCCESS!=rc)
	{ 
		AfxMessageBox(PrintErrorRC(rc), MB_OK);
	}
	return 0;
}


DWORD WINAPI CPsmjDlg::Run2(LPVOID lpParam)
{  
	ThreadParams2* p = (ThreadParams2*)(lpParam);
	RC rc;
	PSMJRP psmjrp(p->psmjrpParams);
	rc = psmjrp.Execute();
	if(SUCCESS!=rc)
	{ 
		AfxMessageBox(PrintErrorRC(rc), MB_OK);
	}
	return 0;
}


void CPsmjDlg::OnBnSelectWorkspace_Clicked()
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

void CPsmjDlg::OnBnSelectFile_Clicked()
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
		m_tbRelationPath_R.SetWindowTextW(szSortFilePath); 
	} 

	m_btSelectS.EnableWindow(bOk); 
	m_tbRelationPath_S.EnableWindow(bOk); 
	m_tbKeyPos_S.EnableWindow(bOk); 
}


void CPsmjDlg::OnBnSelectFileS_Clicked()
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
		m_tbRelationPath_S.SetWindowTextW(szSortFilePath); 
	}

	m_btExecute.EnableWindow(bOk); 
	m_btExecuteRP.EnableWindow(bOk); 
}


void CPsmjDlg::OnBnExecute_Clicked()
{

	DWORD textLength = m_tbWorkSpace.GetWindowTextLengthW();
	if(textLength==0)
	{ 
		AfxMessageBox(L"Please enter workspace path", MB_OK);
		return;
	}

	textLength = m_tbRelationPath_R.GetWindowTextLengthW();
	if(textLength==0)
	{ 
		AfxMessageBox(L"Please enter path for R file", MB_OK);
		return;
	}

	textLength = m_tbKeyPos_R.GetWindowTextLengthW();
	if(textLength==0)
	{ 
		AfxMessageBox(L"Please enter key attribute for R file", MB_OK);
		return;
	}

	textLength = m_tbRelationPath_S.GetWindowTextLengthW();
	if(textLength==0)
	{ 
		AfxMessageBox(L"Please enter path for S file", MB_OK);
		return;
	}

	textLength = m_tbKeyPos_S.GetWindowTextLengthW();
	if(textLength==0)
	{ 
		AfxMessageBox(L"Please enter key attribute for S file", MB_OK);
		return;
	} 

	textLength =  m_tbPoolSize.GetWindowTextLengthW();
	if(textLength==0)
	{ 
		AfxMessageBox(L"Please enter buffer pool size", MB_OK);
		return;
	}

	PSMJ_PARAMS psmjParams;
	m_tbWorkSpace.GetWindowTextW(psmjParams.WORK_SPACE_PATH, MAX_PATH);

	UINT poolSize = GetDlgItemInt(IDC_TB_PSMJ_BUFFER_POOL_SIZE, FALSE, FALSE);
	psmjParams.BUFFER_POOL_SIZE = poolSize * SSD_PAGE_SIZE * 256;

	m_tbRelationPath_R.GetWindowTextW(psmjParams.RELATION_R_PATH, MAX_PATH);
	_wsplitpath( psmjParams.RELATION_R_PATH, NULL, NULL, psmjParams.RELATION_R_NO_EXT, NULL); 
	psmjParams.R_KEY_POS = GetDlgItemInt(IDC_TB_PSMJ_KEY_POS_R, FALSE, FALSE);


	m_tbRelationPath_S.GetWindowTextW(psmjParams.RELATION_S_PATH, MAX_PATH);
	_wsplitpath( psmjParams.RELATION_S_PATH, NULL, NULL, psmjParams.RELATION_S_NO_EXT, NULL); 
	psmjParams.S_KEY_POS = GetDlgItemInt(IDC_TB_PSMJ_KEY_POS_S, FALSE, FALSE);

	INT selectedIndex =-1;
	selectedIndex = m_cbbSortReadBufferSize.GetCurSel(); 
	for(UINT i = 0; i < g_ComboBoxBufferItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			psmjParams.SORT_READ_BUFFER_SIZE = g_ComboBoxBufferItems[i]->Value;
			break;
		} 
	}

	selectedIndex = m_cbbSortWriteBufferSize.GetCurSel(); 
	for(UINT i = 0; i < g_ComboBoxBufferItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			psmjParams.SORT_WRITE_BUFFER_SIZE = g_ComboBoxBufferItems[i]->Value;
			break;
		} 
	}

	selectedIndex = m_cbbThreadNum.GetCurSel(); 
	for(UINT i = 0; i < 7; i++)
	{
		if(selectedIndex==i)
		{
			psmjParams.THREAD_NUM = g_ThreadNum[i];
			break;
		} 
	}

	selectedIndex = m_cbbJoinPlan.GetCurSel(); 
	for(UINT i = 0; i < joinPlanItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			psmjParams.PLAN_FOR_JOIN = joinPlanItems[i]->Value;
			break;
		} 
	}

	psmjParams.USE_POWER_CAP = m_chkUsePowerCap.GetCheck();

	//psmjParams.USE_DELETE_AFTER_OPERATION = m_chkDeleteAfterUse.GetCheck();
	//psmjParams.USE_PARALLEL_MERGE = m_chkParallelMerge.GetCheck();
	//psmjParams.USE_LOG_TO_FILE = FALSE;
	//psmjParams.USE_SYNC_READ_WRITE_MODE = FALSE;

	m_btExecute.EnableWindow(FALSE); 
	m_btExecuteRP.EnableWindow(FALSE); 
	ThreadParams *myParams = new ThreadParams;
	myParams->_this = this;
	myParams->psmjParams = psmjParams;

	m_hPsmjThread = CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)RunEx, myParams, CREATE_SUSPENDED, NULL); 
	SetThreadPriority(m_hPsmjThread, THREAD_PRIORITY_NORMAL);
	m_Requestor.RequestNotification(m_hPsmjThread, this, OP_PSMJ);
	ResumeThread(m_hPsmjThread); 
}


void CPsmjDlg::OnBnClickedBtPsmjExecute2()
{
	// TODO: Add your control notification handler code here

	DWORD textLength = m_tbWorkSpace.GetWindowTextLengthW();
	if(textLength==0)
	{ 
		AfxMessageBox(L"Please enter workspace path", MB_OK);
		return;
	}

	textLength = m_tbRelationPath_R.GetWindowTextLengthW();
	if(textLength==0)
	{ 
		AfxMessageBox(L"Please enter path for R file", MB_OK);
		return;
	}

	textLength = m_tbKeyPos_R.GetWindowTextLengthW();
	if(textLength==0)
	{ 
		AfxMessageBox(L"Please enter key attribute for R file", MB_OK);
		return;
	}

	textLength = m_tbRelationPath_S.GetWindowTextLengthW();
	if(textLength==0)
	{ 
		AfxMessageBox(L"Please enter path for S file", MB_OK);
		return;
	}

	textLength = m_tbKeyPos_S.GetWindowTextLengthW();
	if(textLength==0)
	{ 
		AfxMessageBox(L"Please enter key attribute for S file", MB_OK);
		return;
	} 

	textLength =  m_tbPoolSize.GetWindowTextLengthW();
	if(textLength==0)
	{ 
		AfxMessageBox(L"Please enter buffer pool size", MB_OK);
		return;
	}

	PSMJRP_PARAMS psmjrpParams;
	m_tbWorkSpace.GetWindowTextW(psmjrpParams.WORK_SPACE_PATH, MAX_PATH);

	UINT poolSize = GetDlgItemInt(IDC_TB_PSMJ_BUFFER_POOL_SIZE, FALSE, FALSE);
	psmjrpParams.BUFFER_POOL_SIZE = poolSize * SSD_PAGE_SIZE * 256;

	m_tbRelationPath_R.GetWindowTextW(psmjrpParams.RELATION_R_PATH, MAX_PATH);
	_wsplitpath( psmjrpParams.RELATION_R_PATH, NULL, NULL, psmjrpParams.RELATION_R_NO_EXT, NULL); 
	psmjrpParams.R_KEY_POS = GetDlgItemInt(IDC_TB_PSMJ_KEY_POS_R, FALSE, FALSE);


	m_tbRelationPath_S.GetWindowTextW(psmjrpParams.RELATION_S_PATH, MAX_PATH);
	_wsplitpath( psmjrpParams.RELATION_S_PATH, NULL, NULL, psmjrpParams.RELATION_S_NO_EXT, NULL); 
	psmjrpParams.S_KEY_POS = GetDlgItemInt(IDC_TB_PSMJ_KEY_POS_S, FALSE, FALSE);

	INT selectedIndex =-1;
	selectedIndex = m_cbbSortReadBufferSize.GetCurSel(); 
	for(UINT i = 0; i < g_ComboBoxBufferItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			psmjrpParams.SORT_READ_BUFFER_SIZE = g_ComboBoxBufferItems[i]->Value;
			break;
		} 
	}

	selectedIndex = m_cbbSortWriteBufferSize.GetCurSel(); 
	for(UINT i = 0; i < g_ComboBoxBufferItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			psmjrpParams.SORT_WRITE_BUFFER_SIZE = g_ComboBoxBufferItems[i]->Value;
			break;
		} 
	}

	selectedIndex = m_cbbThreadNum.GetCurSel(); 
	for(UINT i = 0; i < 7; i++)
	{
		if(selectedIndex==i)
		{
			psmjrpParams.THREAD_NUM = g_ThreadNum[i];
			break;
		} 
	}

	//selectedIndex = m_cbbJoinPlan.GetCurSel(); 
	//for(UINT i = 0; i < joinPlanItems.size(); i++)
	//{
	//	if(selectedIndex==i)
	//	{
	//		psmjrpParams.PLAN_FOR_JOIN = joinPlanItems[i]->Value;
	//		break;
	//	} 
	//}

	//psmjrpParams.USE_POWER_CAP = m_chkUsePowerCap.GetCheck();

	//psmjParams.USE_DELETE_AFTER_OPERATION = m_chkDeleteAfterUse.GetCheck();
	//psmjParams.USE_PARALLEL_MERGE = m_chkParallelMerge.GetCheck();
	//psmjParams.USE_LOG_TO_FILE = FALSE;
	//psmjParams.USE_SYNC_READ_WRITE_MODE = FALSE;

	m_btExecute.EnableWindow(FALSE); 
	m_btExecuteRP.EnableWindow(FALSE); 

	ThreadParams2 *myParams = new ThreadParams2;
	myParams->_this = this;
	myParams->psmjrpParams = psmjrpParams;

	m_hPsmjThread = CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)RunEx2, myParams, CREATE_SUSPENDED, NULL); 
	SetThreadPriority(m_hPsmjThread, THREAD_PRIORITY_NORMAL);
	m_Requestor.RequestNotification(m_hPsmjThread, this, OP_PSMJ);
	ResumeThread(m_hPsmjThread);  
}
