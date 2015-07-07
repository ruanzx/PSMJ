// 
// Name: CPemsDlg.cpp
// Author: hieunt
// Description: Parallel external merge sort dialog
//


#include "stdafx.h"
#include "CPemsDlg.h"

extern std::vector<ComboBoxItem *> g_ComboBoxBufferItems;  
extern UINT g_ThreadNum[];


/// <summary>
/// Initializes a new instance of the <see cref="CPemsDlg"/> class.
/// </summary>
CPemsDlg::CPemsDlg()
{
}

/// <summary>
/// Finalizes an instance of the <see cref="CPemsDlg"/> class.
/// </summary>
CPemsDlg::~CPemsDlg()
{
}

void CPemsDlg::DoDataExchange(CDataExchange* pDX)
{
	CDialogEx::DoDataExchange(pDX);

	DDX_Control(pDX, IDC_TB_PEMS_WORKSPACE, m_WorkSpace);
	DDX_Control(pDX, IDC_TB_PEMS_FILE, m_FilePath);
	DDX_Control(pDX, IDC_TB_PEMS_KEY_POS, m_KeyPos);
	DDX_Control(pDX, IDC_TB_PEMS_BUFFER_POOL_SIZE, m_PoolSize);
	DDX_Control(pDX, IDC_CBB_PEMS_THREAD_NUM, m_cbbThreadNum);
	DDX_Control(pDX, IDC_CBB_PEMS_SORT_READ_BUFFER_SIZE, m_cbbSortReadSize);
	DDX_Control(pDX, IDC_CBB_PEMS_SORT_WRITE_BUFFER_SIZE, m_cbbSortWriteSize);
	DDX_Control(pDX, IDC_CBB_PEMS_MERGE_READ_BUFFER_SIZE, m_cbbMergeReadSize);
	DDX_Control(pDX, IDC_CBB_PEMS_MERGE_WRITE_BUFFER_SIZE, m_cbbMergeWriteSize);
	DDX_Control(pDX, IDC_CHK_PEMS_DELETE_AFTER_USE, m_chkDeleteAfterUse);
	DDX_Control(pDX, IDC_CHK_PEMS_PARALLEL_MERGE, m_chkParallelMerge);
	DDX_Control(pDX, IDC_BT_PEMS_EXECUTE, m_btExecute);
	DDX_Control(pDX, IDC_BT_PEMS_EXECUTE_2, m_btExecute_v2);
	DDX_Control(pDX, IDC_LB_PEMS_INFO, m_lbInfos);
}

BEGIN_MESSAGE_MAP(CPemsDlg, CDialogEx) 
	ON_REGISTERED_MESSAGE(WaitInfo::UWM_THREAD_TERMINATED, &CPemsDlg::OnThreadTerminated)
	ON_BN_CLICKED(IDC_BT_PEMS_SELECT_FILE, &CPemsDlg::OnSelectFile_Clicked)
	ON_BN_CLICKED(IDC_BT_PEMS_EXECUTE, &CPemsDlg::OnBnExecute_Clicked)
	ON_BN_CLICKED(IDC_BT_PEMS_SELECT_WORKSPACE, &CPemsDlg::OnBnSelectWorkspace_Clicked)
	ON_BN_CLICKED(IDC_BT_PEMS_EXECUTE_2, &CPemsDlg::OnBnExecute2_Clicked)
	ON_STN_CLICKED(IDC_LB_PEMS_INFO, &CPemsDlg::OnStnClickedLbPemsInfo)
END_MESSAGE_MAP()


LRESULT CPemsDlg::OnThreadTerminated(WPARAM wParam, LPARAM lParam)
{
	//m_Notify->PostMessage(UWM_THREAD_TERMINATED, (WPARAM)m_Operation, (LPARAM)m_Handle); 
	HANDLE h = (HANDLE)lParam;
	OPERATIONS op = (OPERATIONS)wParam;
	if(op==OP_PEMS)
	{
		AfxMessageBox(L"Sort done", MB_OK); 
		m_btExecute.EnableWindow(TRUE);
		m_btExecute_v2.EnableWindow(TRUE);
	}

	CloseHandle(h);

	return 0;
} 


/// <summary>
/// Called when [initialize dialog].
/// </summary>
/// <returns></returns>
BOOL CPemsDlg::OnInitDialog()
{
	CDialog::OnInitDialog();  

	// TODO: Add extra initialization here
	for (UINT i=0; i<g_ComboBoxBufferItems.size(); i++)
	{
		m_cbbSortReadSize.AddString(g_ComboBoxBufferItems[i]->Name);
		m_cbbSortWriteSize.AddString(g_ComboBoxBufferItems[i]->Name);
		m_cbbMergeReadSize.AddString(g_ComboBoxBufferItems[i]->Name);
		m_cbbMergeWriteSize.AddString(g_ComboBoxBufferItems[i]->Name);
	}

	ComboboxSetSelectedIndexByText(m_cbbSortReadSize, L"4MB");
	ComboboxSetSelectedIndexByText(m_cbbSortWriteSize, L"8MB");

	ComboboxSetSelectedIndexByText(m_cbbMergeReadSize, L"128KB");
	ComboboxSetSelectedIndexByText(m_cbbMergeWriteSize, L"8MB");

	for(int i = 0; i < 7; i++)
	{
		m_cbbThreadNum.AddString(LongToString(g_ThreadNum[i], L"%d")); 
	} 

	ComboboxSetSelectedIndexByText(m_cbbThreadNum, L"0");

	SetDlgItemInt(IDC_TB_PEMS_BUFFER_POOL_SIZE, DEFAULT_BUFFER_POOL_SIZE_MB, FALSE);

	m_WorkSpace.SetWindowTextW(DEFAULT_WORK_SPACE);

	m_btExecute.EnableWindow(FALSE);  
	m_btExecute_v2.EnableWindow(FALSE);  

	return TRUE;  // return TRUE  unless you set the focus to a control
}


/// <summary>
/// Runs the v2 ex.
/// </summary>
/// <param name="lpParam">The lp parameter.</param>
/// <returns></returns>
DWORD WINAPI CPemsDlg::RunV2Ex(LPVOID lpParam)
{
	ThreadParams* p = (ThreadParams*)(lpParam);
	p->_this->RunV2((LPVOID)(p));
	return 0;
}


DWORD WINAPI CPemsDlg::RunV2(LPVOID lpParam)
{ 
	ThreadParams* p = (ThreadParams*)(lpParam);
	RC rc;

	PEMS pems(p->pemsParams);
	rc = pems.Execute();

	if(rc!=SUCCESS)
	{
		AfxMessageBox(PrintErrorRC(rc), MB_OK);
		return 1;
	}

	return 0;
}


DWORD WINAPI CPemsDlg::RunEx(LPVOID lpParam)
{
	ThreadParams* p = (ThreadParams*)(lpParam);
	p->_this->Run((LPVOID)(p));
	return 0;
}

DWORD WINAPI CPemsDlg::Run(LPVOID lpParam)
{ 
	ThreadParams* p = (ThreadParams*)(lpParam);
	RC rc;
	ExsPartitionPhase partitionPhase(p->pemsParams);
	rc = partitionPhase.PartitionPhase_CheckEnoughMemory();
	if(rc!=SUCCESS)
	{
		AfxMessageBox(PrintErrorRC(rc), MB_OK);
		return 1;
	}

	rc = partitionPhase.PartitionPhase_Execute();
	if(rc!=SUCCESS)
	{
		AfxMessageBox(PrintErrorRC(rc), MB_OK);
		return 1;
	}

	std::queue<FANS*> fanIns = partitionPhase.GetFanIns(); 
	ExsMergePhase mergePhase(p->pemsParams, fanIns, 0); 
	rc = mergePhase.MergePhase_CheckEnoughMemory();
	if(rc!=SUCCESS)
	{
		AfxMessageBox(PrintErrorRC(rc), MB_OK);
		return 1;
	}

	rc = mergePhase.MergePhase_Execute();
	if(rc!=SUCCESS)
	{
		AfxMessageBox(PrintErrorRC(rc), MB_OK);
		return 1;
	}

	//myMessage.Post(L"Wait for merge operation complete\r\n");
	//myMessage.Post(L"Merge operation complete, fanOut %s\r\n", finalFan->fileName);
	FANS*  finalFan = mergePhase.GetFinalFanOut(); 

	return 0;
}

void CPemsDlg::OnSelectFile_Clicked()
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
		m_FilePath.SetWindowTextW(szSortFilePath); 
	}

	m_btExecute.EnableWindow(bOk); 
	m_btExecute_v2.EnableWindow(bOk);  
}


void CPemsDlg::OnBnExecute_Clicked()
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
		AfxMessageBox(L"Please enter path for sort file", MB_OK);
		return;
	}

	textLength = m_KeyPos.GetWindowTextLengthW();
	if(textLength==0)
	{ 
		AfxMessageBox(L"Please enter key attribute for sort file", MB_OK);
		return;
	}

	textLength =  m_PoolSize.GetWindowTextLengthW();
	if(textLength==0)
	{ 
		AfxMessageBox(L"Please enter buffer pool size", MB_OK);
		return;
	}

	PEMS_PARAMS pemsParams;
	m_WorkSpace.GetWindowTextW(pemsParams.WORK_SPACE_PATH, MAX_PATH);

	UINT poolSize = GetDlgItemInt(IDC_TB_PEMS_BUFFER_POOL_SIZE, FALSE, FALSE);
	pemsParams.BUFFER_POOL_SIZE = poolSize * SSD_PAGE_SIZE * 256;

	m_FilePath.GetWindowTextW(pemsParams.SORT_FILE_PATH, MAX_PATH);
	_wsplitpath( pemsParams.SORT_FILE_PATH, NULL, NULL, pemsParams.FILE_NAME_NO_EXT, NULL); 
	pemsParams.KEY_POS = GetDlgItemInt(IDC_TB_PEMS_KEY_POS, FALSE, FALSE);

	INT selectedIndex =-1;
	selectedIndex = m_cbbSortReadSize.GetCurSel(); 
	for(UINT i = 0; i < g_ComboBoxBufferItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			pemsParams.SORT_READ_BUFFER_SIZE = g_ComboBoxBufferItems[i]->Value;
			break;
		} 
	}

	selectedIndex = m_cbbSortWriteSize.GetCurSel(); 
	for(UINT i = 0; i < g_ComboBoxBufferItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			pemsParams.SORT_WRITE_BUFFER_SIZE = g_ComboBoxBufferItems[i]->Value;
			break;
		} 
	}

	selectedIndex = m_cbbMergeReadSize.GetCurSel(); 
	for(UINT i = 0; i < g_ComboBoxBufferItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			pemsParams.MERGE_READ_BUFFER_SIZE = g_ComboBoxBufferItems[i]->Value;
			break;
		} 
	}

	selectedIndex = m_cbbMergeWriteSize.GetCurSel(); 
	for(UINT i = 0; i < g_ComboBoxBufferItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			pemsParams.MERGE_WRITE_BUFFER_SIZE = g_ComboBoxBufferItems[i]->Value;
			break;
		} 
	}

	selectedIndex = m_cbbThreadNum.GetCurSel(); 
	for(UINT i = 0; i < 7; i++)
	{
		if(selectedIndex==i)
		{
			pemsParams.THREAD_NUM = g_ThreadNum[i];
			break;
		} 
	}

	pemsParams.USE_DELETE_AFTER_OPERATION = m_chkDeleteAfterUse.GetCheck();
	pemsParams.USE_PARALLEL_MERGE = m_chkParallelMerge.GetCheck();
	pemsParams.USE_LOG_TO_FILE = FALSE;
	pemsParams.USE_SYNC_READ_WRITE_MODE = FALSE;

	m_btExecute.EnableWindow(FALSE);
	m_btExecute_v2.EnableWindow(FALSE);

	ThreadParams *myParams = new ThreadParams;
	myParams->_this = this;
	myParams->pemsParams = pemsParams;

	m_hPemsThread = CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)RunEx, myParams, CREATE_SUSPENDED, NULL); 
	SetThreadPriority(m_hPemsThread, THREAD_PRIORITY_NORMAL);
	m_Requestor.RequestNotification(m_hPemsThread, this, OP_PEMS);
	ResumeThread(m_hPemsThread); 
}

void CPemsDlg::OnBnSelectWorkspace_Clicked()
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

void CPemsDlg::OnBnExecute2_Clicked()
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
		AfxMessageBox(L"Please enter path for sort file", MB_OK);
		return;
	}

	textLength = m_KeyPos.GetWindowTextLengthW();
	if(textLength==0)
	{ 
		AfxMessageBox(L"Please enter key attribute for sort file", MB_OK);
		return;
	}

	textLength =  m_PoolSize.GetWindowTextLengthW();
	if(textLength==0)
	{ 
		AfxMessageBox(L"Please enter buffer pool size", MB_OK);
		return;
	}

	PEMS_PARAMS pemsParams;
	m_WorkSpace.GetWindowTextW(pemsParams.WORK_SPACE_PATH, MAX_PATH);

	UINT poolSize = GetDlgItemInt(IDC_TB_PEMS_BUFFER_POOL_SIZE, FALSE, FALSE);
	pemsParams.BUFFER_POOL_SIZE = poolSize * SSD_PAGE_SIZE * 256;

	m_FilePath.GetWindowTextW(pemsParams.SORT_FILE_PATH, MAX_PATH);
	_wsplitpath( pemsParams.SORT_FILE_PATH, NULL, NULL, pemsParams.FILE_NAME_NO_EXT, NULL); 
	pemsParams.KEY_POS = GetDlgItemInt(IDC_TB_PEMS_KEY_POS, FALSE, FALSE);

	INT selectedIndex =-1;
	selectedIndex = m_cbbSortReadSize.GetCurSel(); 
	for(UINT i = 0; i < g_ComboBoxBufferItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			pemsParams.SORT_READ_BUFFER_SIZE = g_ComboBoxBufferItems[i]->Value;
			break;
		} 
	}

	selectedIndex = m_cbbSortWriteSize.GetCurSel(); 
	for(UINT i = 0; i < g_ComboBoxBufferItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			pemsParams.SORT_WRITE_BUFFER_SIZE = g_ComboBoxBufferItems[i]->Value;
			break;
		} 
	}

	selectedIndex = m_cbbMergeReadSize.GetCurSel(); 
	for(UINT i = 0; i < g_ComboBoxBufferItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			pemsParams.MERGE_READ_BUFFER_SIZE = g_ComboBoxBufferItems[i]->Value;
			break;
		} 
	}

	selectedIndex = m_cbbMergeWriteSize.GetCurSel(); 
	for(UINT i = 0; i < g_ComboBoxBufferItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			pemsParams.MERGE_WRITE_BUFFER_SIZE = g_ComboBoxBufferItems[i]->Value;
			break;
		} 
	}

	selectedIndex = m_cbbThreadNum.GetCurSel(); 
	for(UINT i = 0; i < 7; i++)
	{
		if(selectedIndex==i)
		{
			pemsParams.THREAD_NUM = g_ThreadNum[i];
			break;
		} 
	}

	pemsParams.USE_DELETE_AFTER_OPERATION = m_chkDeleteAfterUse.GetCheck();
	pemsParams.USE_PARALLEL_MERGE = m_chkParallelMerge.GetCheck();
	pemsParams.USE_LOG_TO_FILE = FALSE;
	pemsParams.USE_SYNC_READ_WRITE_MODE = FALSE;

	m_btExecute.EnableWindow(FALSE);
	m_btExecute_v2.EnableWindow(FALSE);

	ThreadParams *myParams = new ThreadParams;
	myParams->_this = this;
	myParams->pemsParams = pemsParams;

	m_hPemsThread = CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)RunV2Ex, myParams, CREATE_SUSPENDED, NULL); 
	SetThreadPriority(m_hPemsThread, THREAD_PRIORITY_NORMAL);
	m_Requestor.RequestNotification(m_hPemsThread, this, OP_PEMS);
	ResumeThread(m_hPemsThread); 
}


void CPemsDlg::OnStnClickedLbPemsInfo()
{
	LPWSTR text = L"[?]";
	LPWSTR info = new TCHAR[1024];

	DWORD textLength = m_WorkSpace.GetWindowTextLengthW();
	if(textLength==0)
	{ 
		m_lbInfos.SetWindowTextW(text);  
		return;
	}

	textLength = m_FilePath.GetWindowTextLengthW();
	if(textLength==0)
	{ 
		m_lbInfos.SetWindowTextW(text); 
		return;
	}

	textLength = m_KeyPos.GetWindowTextLengthW();
	if(textLength==0)
	{ 
		m_lbInfos.SetWindowTextW(text); 
		return;
	}

	textLength =  m_PoolSize.GetWindowTextLengthW();
	if(textLength==0)
	{ 
		m_lbInfos.SetWindowTextW(text); 
		return;
	}

	PEMS_PARAMS pemsParams;
	m_WorkSpace.GetWindowTextW(pemsParams.WORK_SPACE_PATH, MAX_PATH);

	UINT poolSize = GetDlgItemInt(IDC_TB_PEMS_BUFFER_POOL_SIZE, FALSE, FALSE);
	pemsParams.BUFFER_POOL_SIZE = poolSize * SSD_PAGE_SIZE * 256;

	m_FilePath.GetWindowTextW(pemsParams.SORT_FILE_PATH, MAX_PATH);
	_wsplitpath( pemsParams.SORT_FILE_PATH, NULL, NULL, pemsParams.FILE_NAME_NO_EXT, NULL); 
	pemsParams.KEY_POS = GetDlgItemInt(IDC_TB_PEMS_KEY_POS, FALSE, FALSE);

	INT selectedIndex =-1;
	selectedIndex = m_cbbSortReadSize.GetCurSel(); 
	for(UINT i = 0; i < g_ComboBoxBufferItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			pemsParams.SORT_READ_BUFFER_SIZE = g_ComboBoxBufferItems[i]->Value;
			break;
		} 
	}

	selectedIndex = m_cbbSortWriteSize.GetCurSel(); 
	for(UINT i = 0; i < g_ComboBoxBufferItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			pemsParams.SORT_WRITE_BUFFER_SIZE = g_ComboBoxBufferItems[i]->Value;
			break;
		} 
	}

	selectedIndex = m_cbbMergeReadSize.GetCurSel(); 
	for(UINT i = 0; i < g_ComboBoxBufferItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			pemsParams.MERGE_READ_BUFFER_SIZE = g_ComboBoxBufferItems[i]->Value;
			break;
		} 
	}

	selectedIndex = m_cbbMergeWriteSize.GetCurSel(); 
	for(UINT i = 0; i < g_ComboBoxBufferItems.size(); i++)
	{
		if(selectedIndex==i)
		{
			pemsParams.MERGE_WRITE_BUFFER_SIZE = g_ComboBoxBufferItems[i]->Value;
			break;
		} 
	}

	selectedIndex = m_cbbThreadNum.GetCurSel(); 
	for(UINT i = 0; i < 7; i++)
	{
		if(selectedIndex==i)
		{
			pemsParams.THREAD_NUM = g_ThreadNum[i];
			break;
		} 
	}

	DWORD threadNum = 0;
	DWORD runSize = 0;
	SYSTEM_INFO sysinfo;
	GetSystemInfo(&sysinfo);
	if(pemsParams.THREAD_NUM==0)
		threadNum = sysinfo.dwNumberOfProcessors; 
	else 
		threadNum = pemsParams.THREAD_NUM;

	// Estimate memory  
	DOUBLE totalMemory = pemsParams.BUFFER_POOL_SIZE; 
	DOUBLE memoryEachThread = totalMemory / threadNum; 
	DOUBLE memoryNeed = 0;
	DOUBLE memoryForRead = 0;
	DOUBLE memoryForWrite = 0;
	DOUBLE memoryForQuickSort = 0;
	DWORD maxQuickSortItem = (pemsParams.SORT_READ_BUFFER_SIZE / SSD_PAGE_SIZE) * MAXIMUM_TUPLE_IN_ONE_PAGE; // The maximum tuple for quick sort 
	DOUBLE memoryForMerge = 0;
	DWORD readBufferNum = 0;

	// Caculate ouput sort buffer
	memoryForWrite = (pemsParams.SORT_WRITE_BUFFER_SIZE * 2);
	memoryForRead = pemsParams.SORT_READ_BUFFER_SIZE * 2;
	memoryForMerge = (sizeof(UINT64) + (TUPLE_SIZE * sizeof(CHAR)) + SSD_PAGE_SIZE);// (LoserKey + LoserTreeData + MergeBuffer) * NumberOfThread 
	memoryForQuickSort = (maxQuickSortItem * (sizeof(UINT64) + (TUPLE_SIZE * sizeof(CHAR))) + SSD_PAGE_SIZE);

	memoryEachThread = memoryEachThread - (memoryForWrite + memoryForQuickSort);

	if( memoryEachThread <= 0)
	{ 
		swprintf(info, 1024,L"[?] Thread num: %d, (WBx2 + QuickSort) x ThreadNum =  %d MB\nNeed larger memory for all thread", 
			threadNum,
			DWORD(memoryForWrite + memoryForQuickSort) / (1024 * 1024));
		m_lbInfos.SetWindowTextW(info); 
		return; 
	}

	// Calculate buffer num K
	readBufferNum = (DWORD)chROUNDDOWN((DWORD)memoryEachThread, memoryForMerge+memoryForRead) / (memoryForMerge+memoryForRead);
	runSize = pemsParams.SORT_READ_BUFFER_SIZE * readBufferNum;
	memoryEachThread = memoryEachThread - readBufferNum *(memoryForMerge+memoryForRead);
	memoryNeed = (memoryForWrite + memoryForQuickSort + (memoryForMerge+memoryForRead)*readBufferNum) * threadNum; 

	if( readBufferNum <= 1 )
	{  
		swprintf(info, 1024,L"[?] Thread num: %d, K = %d, Run size: %d MB,\nExecute not efficient. Need larger memory size", 
			threadNum, 
			readBufferNum, 
			runSize/(1024 * 1024));

		m_lbInfos.SetWindowTextW(info);
		return; 
	}   
	     
	swprintf(info, 1024,L"[?] Thread num: %d, Memory need: %d MB, K = %d, Run size: %d MB", 
		threadNum,
		(DWORD)memoryNeed/(1024 * 1024), 
		readBufferNum, 
		runSize/(1024*1024));

	m_lbInfos.SetWindowTextW(info); 

	delete info; 
}
