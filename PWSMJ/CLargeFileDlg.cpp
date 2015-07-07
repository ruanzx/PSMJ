// 
// Name: CLargeFileDlg.cpp : dialog implementation file 
// Author: hieunt
// Description: Split very large file to small piece, calculate tuple count in file
//

#include "stdafx.h"
#include "CLargeFileDlg.h"


/// <summary>
/// Initializes a new instance of the <see cref="CLargeFileDlg"/> class.
/// </summary>
CLargeFileDlg::CLargeFileDlg()
{
}


/// <summary>
/// Finalizes an instance of the <see cref="CLargeFileDlg"/> class.
/// </summary>
CLargeFileDlg::~CLargeFileDlg()
{
}


void CLargeFileDlg::DoDataExchange(CDataExchange* pDX)
{
	CDialogEx::DoDataExchange(pDX);

	DDX_Control(pDX, IDC_TB_DEBUG_WORKSPACE, m_WorkSpace);
	DDX_Control(pDX, IDC_TB_DEBUG_FILE, m_FilePath);
	DDX_Control(pDX, IDC_TB_DEBUG_PAGE_TOTAL, m_TotalPage);
	DDX_Control(pDX, IDC_DEBUG_OK, m_btExecute);
	DDX_Control(pDX, IDC_TB_DEBUG_FILESIZE, m_FileSize);
	DDX_Control(pDX, IDC_TB_DEBUG_PAGE_FROM, m_FromPage);
	DDX_Control(pDX, IDC_TB_DEBUG_PAGE_TO, m_ToPage);
	DDX_Control(pDX, IDC_TB_DEBUG_TUPLE_TOTAL, m_TupleCount);
	DDX_Control(pDX, IDC_DEBUG_TUPLE_COUNT, m_btTupleCount);
}

BEGIN_MESSAGE_MAP(CLargeFileDlg, CDialogEx)  
	ON_BN_CLICKED(IDC_DEBUG_OK, &CLargeFileDlg::OnBnOk_Clicked)
	ON_BN_CLICKED(IDC_BT_DEBUG_SELECT_WORKSPACE, &CLargeFileDlg::OnBnSelectWorkspace_Clicked)
	ON_BN_CLICKED(IDC_BT_DEBUG_SELECT_FILE, &CLargeFileDlg::OnBnSelectFile_Clicked)
	ON_BN_CLICKED(IDC_DEBUG_TUPLE_COUNT, &CLargeFileDlg::OnBnClickedDebugTupleCount)
END_MESSAGE_MAP()

BOOL CLargeFileDlg::OnInitDialog()
{
	CDialog::OnInitDialog();  

	// TODO: Add extra initialization here
	m_WorkSpace.SetWindowTextW(DEFAULT_WORK_SPACE); 
	SetDlgItemInt(IDC_TB_DEBUG_PAGE_FROM, 0, FALSE);
	SetDlgItemInt(IDC_TB_DEBUG_PAGE_TO, 1024, FALSE);

	m_btExecute.EnableWindow(FALSE);
	m_btTupleCount.EnableWindow(FALSE);

	return TRUE;  // return TRUE  unless you set the focus to a control
}


void CLargeFileDlg::OnBnOk_Clicked()
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
		AfxMessageBox(L"Please enter file path", MB_OK);
		return;
	}

	textLength = m_FromPage.GetWindowTextLengthW();
	if(textLength==0)
	{ 
		AfxMessageBox(L"Please enter <from> page to get", MB_OK);
		return;
	}

	textLength = m_ToPage.GetWindowTextLengthW();
	if(textLength==0)
	{ 
		AfxMessageBox(L"Please enter <to> page to get", MB_OK);
		return;
	}

	DWORD fromPage = 0, toPage = 0, pageToRead = 0, byteToRead = 0;
	fromPage = GetDlgItemInt(IDC_TB_DEBUG_PAGE_FROM, FALSE, FALSE);
	toPage = GetDlgItemInt(IDC_TB_DEBUG_PAGE_TO, FALSE, FALSE);
	if(fromPage > toPage)
	{
		AfxMessageBox(L"<from page> must smaller than <to page>", MB_OK);
		return;
	}

	m_btExecute.EnableWindow(FALSE);

	LPWSTR workSpace = new TCHAR[MAX_PATH];
	LPWSTR filePathRead = new TCHAR[MAX_PATH];
	LPWSTR fileNameNoExt = new TCHAR[MAX_PATH];
	LPWSTR filePathWrite = new TCHAR[MAX_PATH];
	LPWSTR msg = new TCHAR[MAX_PATH];

	m_WorkSpace.GetWindowTextW(workSpace, MAX_PATH);
	m_FilePath.GetWindowTextW(filePathRead, MAX_PATH);

	_wsplitpath(filePathRead, NULL, NULL, fileNameNoExt, NULL);
	swprintf(filePathWrite, MAX_PATH, L"%s%s_%d_%d.txt", workSpace, fileNameNoExt, fromPage, toPage);


	pageToRead = toPage - fromPage;
	byteToRead = pageToRead * 4096;

	CHAR *buff = new CHAR[byteToRead];
	HANDLE hFileRead;
	HANDLE hFileWrite;

	// Create file handle
	hFileRead = CreateFile(
		(LPCWSTR)filePathRead, // file to open
		GENERIC_READ,			// open for reading
		FILE_SHARE_READ,        // share for reading
		NULL,					// default security
		OPEN_EXISTING,			// existing file only
		FILE_FLAG_NO_BUFFERING,	// overlapped operation //| FILE_FLAG_NO_BUFFERING
		NULL);					// no attr. template 

	if (INVALID_HANDLE_VALUE==hFileRead) 
	{ 
		swprintf(msg,MAX_PATH, L"Cannot create handle of file %s\r\n", filePathRead);
		AfxMessageBox(msg, MB_OK);
		m_btExecute.EnableWindow(TRUE);
		return;
	} 

	hFileWrite = CreateFile(
		(LPCWSTR)filePathWrite,		// file to write
		GENERIC_WRITE,			// open for writing
		0,						// Do not share
		NULL,					// default security
		CREATE_ALWAYS,			// Overwrite existing
		FILE_FLAG_NO_BUFFERING,	// overlapped operation //| FILE_FLAG_OVERLAPPED
		NULL);					// no attr. template

	if (INVALID_HANDLE_VALUE==hFileWrite) 
	{  
		swprintf(msg,MAX_PATH, L"Cannot create handle of file %s\r\n", filePathWrite);
		AfxMessageBox(msg, MB_OK);
		m_btExecute.EnableWindow(TRUE);
		return;
	}   

	LARGE_INTEGER fileSize;
	fileSize.QuadPart = fromPage * 4096;
	OVERLAPPED overlap;
	overlap.hEvent = NULL;
	overlap.Offset = fileSize.LowPart;
	overlap.OffsetHigh = fileSize.HighPart;

	DWORD dwBytesRead = 0;
	ReadFile(hFileRead, 
		buff, 
		byteToRead, 
		&dwBytesRead, 
		&overlap); 

	DWORD dwBytesWritten = 0; 

	WriteFile(hFileWrite, 
		buff, 
		dwBytesRead,  
		&dwBytesWritten, 
		NULL); 		

	CloseHandle(hFileRead);
	CloseHandle(hFileWrite);

	swprintf(msg,MAX_PATH, L"Write complete, file %s created", filePathWrite);
	AfxMessageBox(msg, MB_OK); 

	delete[] buff;
	delete[] workSpace;
	delete[] filePathRead;
	delete[] fileNameNoExt;
	delete[] filePathWrite;
	delete[] msg;

	m_btExecute.EnableWindow(TRUE);
}

void CLargeFileDlg::OnBnSelectWorkspace_Clicked()
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

void CLargeFileDlg::OnBnSelectFile_Clicked()
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
	ofn.lpstrFilter = L"*.* File (*.*)\0*.*\0"; 
	ofn.Flags = OFN_EXPLORER | OFN_FILEMUSTEXIST;
	BOOL bOk = GetOpenFileName(&ofn);
	if (bOk)
	{ 
		m_FilePath.SetWindowTextW(szSortFilePath); 
		LARGE_INTEGER *liFileSize = new LARGE_INTEGER(); 

		HANDLE hFile = CreateFile(
			(LPCWSTR)szSortFilePath, // file to open
			GENERIC_READ,			// open for reading
			FILE_SHARE_READ,        // share for reading
			NULL,					// default security
			OPEN_EXISTING,			// existing file only
			FILE_ATTRIBUTE_NORMAL,	// overlapped operation //| FILE_FLAG_NO_BUFFERING
			NULL);					// no attr. template

		if (INVALID_HANDLE_VALUE==hFile) 
		{   
			ShowMB(L"Cannot create Handle file %s", szSortFilePath); 
			return; 
		} 

		if (!GetFileSizeEx(hFile, liFileSize))
		{      
			ShowMB(L"Cannot get size of file %s", szSortFilePath); 
			return; 
		} 

		m_FileSize.SetWindowTextW(LongToString(liFileSize->QuadPart, L"%d"));

		DWORD totalPage = chROUNDUP(liFileSize->QuadPart, 4096) / 4096;
		m_TotalPage.SetWindowTextW(LongToString(totalPage, L"%d")); 

		CloseHandle(hFile); 
	}

	m_btExecute.EnableWindow(bOk);
	m_btTupleCount.EnableWindow(bOk);
}


void CLargeFileDlg::OnBnClickedDebugTupleCount()
{ 
	LPWSTR filePathRead = new TCHAR[MAX_PATH];
	m_FilePath.GetWindowTextW(filePathRead, MAX_PATH);

	HANDLE hFile = CreateFile(
		(LPCWSTR)filePathRead, // file to open
		GENERIC_READ,			// open for reading
		FILE_SHARE_READ,        // share for reading
		NULL,					// default security
		OPEN_EXISTING,			// existing file only
		FILE_ATTRIBUTE_NORMAL,	// overlapped operation //| FILE_FLAG_NO_BUFFERING
		NULL);					// no attr. template

	if (INVALID_HANDLE_VALUE==hFile) 
	{   
		ShowMB(L"Cannot create Handle file %s", filePathRead); 
		return; 
	} 

	BufferPool bufferPool;
	bufferPool.size = SSD_PAGE_SIZE * 256 * 64; // 64MB
	bufferPool.currentSize = 0;
	bufferPool.data = new CHAR[bufferPool.size];

	PageHelpers2 *utl = new PageHelpers2();
	Buffer pageBuffer;
	utl->InitBuffer(pageBuffer, SSD_PAGE_SIZE, &bufferPool); 

	PagePtr *pagePtr; 
	utl->InitRunPage(pagePtr, pageBuffer);

	Buffer readBuffer;
	utl->InitBuffer(readBuffer, bufferPool.size-bufferPool.currentSize, &bufferPool); 

	DWORD pageCount = 0;
	DWORD tupleCount = 0;
	while (TRUE)
	{
		DWORD dwBytesRead = 0;
		ReadFile(hFile, readBuffer.data, readBuffer.size, &dwBytesRead, NULL);
		if(dwBytesRead==0) { break; }

		if(dwBytesRead%SSD_PAGE_SIZE==0)
			pageCount = dwBytesRead / SSD_PAGE_SIZE;
		else
			pageCount = dwBytesRead / SSD_PAGE_SIZE + 1;

		for(UINT i=0; i<pageCount; i++)
		{
			utl->GetPageInfo(readBuffer.data, pagePtr, pageBuffer, i, SSD_PAGE_SIZE);
			PageHeader *pageHeader = (PageHeader *)(pagePtr->page); 
			tupleCount+=pageHeader->totalTuple;
		}
	}

	m_TupleCount.SetWindowTextW(LongToString(tupleCount, L"%d"));

	CloseHandle(hFile); 

	delete filePathRead;
	delete bufferPool.data;
}
