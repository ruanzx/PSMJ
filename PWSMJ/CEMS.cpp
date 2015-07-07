// 
// Name: CEMS.cpp :  implementation file 
// Author: hieunt
// Description: Cache external merge sort implementation
//


#include "stdafx.h"
#include "CEMS.h"

/// <summary>
/// Initializes a new instance of the <see cref="CEMS"/> class.
/// </summary>
/// <param name="vParams">The v parameters.</param>
CEMS::CEMS(CEMS_PARAMS vParams) : m_CemsParams(vParams)
{     
	m_FanIndex = 0;
	m_CurrentMark = 0;  
	m_MaxFanInInOneStep = 0;
	m_HeapSize = 0;
	m_SsdFanInPath = new TCHAR[MAX_PATH];
	m_HddFanInPath = new TCHAR[MAX_PATH]; 
	isGetFanInSize = FALSE;
	utl = new PageHelpers2();  
}

/// <summary>
/// Finalizes an instance of the <see cref="CEMS"/> class.
/// </summary>
CEMS::~CEMS() 
{  
	delete m_SsdFanInPath;
	delete m_HddFanInPath;
	delete utl;
} 

/// <summary>
/// Initialize sort phase params.
/// </summary>
/// <returns></returns>
RC CEMS::Phase1_Initialize()
{
	RC rc; 

	/* Create handle for source table */ 
	hTableFile=CreateFile(
		(LPCWSTR)m_CemsParams.SORT_FILE_PATH,
		GENERIC_READ,
		FILE_SHARE_READ,
		NULL,
		OPEN_EXISTING,
		FILE_FLAG_OVERLAPPED | FILE_FLAG_NO_BUFFERING,
		NULL); 

	if(INVALID_HANDLE_VALUE==hTableFile)
	{  
		ShowMB(L"Cannot open input file %s", m_CemsParams.SORT_FILE_PATH);
		return ERR_CANNOT_CREATE_HANDLE; 
	}

	// Init buffer to read from HDD
	rc = utl->InitBuffer(hddReadBuffer, m_CemsParams.HDD_READ_BUFFER_SIZE, &bufferPool); 
	if(rc!=SUCCESS) {return rc;}

	// Init buffer to write to SSD
	rc = utl->InitBuffer(ssdWriteBuffer, m_CemsParams.SSD_WRITE_BUFFER_SIZE, &bufferPool); 
	if(rc!=SUCCESS) {return rc;} 

	// Init buffer to write to HDD
	rc = utl->InitBuffer(hddWriteBuffer, m_CemsParams.HDD_WRITE_BUFFER_SIZE, &bufferPool); 
	if(rc!=SUCCESS) {return rc;}

	// Init ssd page & ssd page buffer
	rc = utl->InitBuffer(ssdPageBuffer, SSD_PAGE_SIZE, &bufferPool);
	if(rc!=SUCCESS) {  return rc; }

	rc= utl->InitRunPage(ssdPage, ssdPageBuffer);
	if(rc!=SUCCESS) {  return rc; }
	//////////////////////////////////////////////////////////////////////////

	// Init hdd page & hdd page buffer
	rc = utl->InitBuffer(hddPageBuffer, SSD_PAGE_SIZE, &bufferPool);
	if(rc!=SUCCESS) {  return rc; }

	rc= utl->InitRunPage(hddPage, hddPageBuffer);
	if(rc!=SUCCESS) {  return rc; }

	//////////////////////////////////////////////////////////////////////////

	/* Get source table size */
	LARGE_INTEGER  liFileSize;  
	if (!GetFileSizeEx(hTableFile, &liFileSize))
	{     
		ShowMB(L"Cannot get the size of file %s", m_CemsParams.SORT_FILE_PATH);
		return ERR_CANNOT_GET_FILE_SIZE;
	} 

	// Save table size info
	rp.TableSize = liFileSize.QuadPart;

	/* Set up overlap structure to READ source table on HDD */
	DWORD64 chunkSize = m_CemsParams.HDD_READ_BUFFER_SIZE;
	DWORD64 totalChunk = chROUNDUP(liFileSize.QuadPart, chunkSize) / chunkSize; 
	overlapReadHdd = new OVERLAPPEDEX();
	overlapReadHdd->dwBytesToReadWrite = m_CemsParams.HDD_READ_BUFFER_SIZE;   
	overlapReadHdd->fileSize.QuadPart = 0;
	overlapReadHdd->overlap.Offset = 0;
	overlapReadHdd->overlap.OffsetHigh = 0;
	overlapReadHdd->chunkIndex = 0;
	overlapReadHdd->startChunk = 0;
	overlapReadHdd->endChunk = totalChunk;
	overlapReadHdd->totalChunk = totalChunk;

	overlapWriteHdd = new OVERLAPPEDEX(); 
	overlapWriteHdd->dwBytesToReadWrite = m_CemsParams.HDD_WRITE_BUFFER_SIZE; 
	overlapWriteHdd->fileSize.QuadPart = 0;
	overlapWriteHdd->overlap.Offset = 0;
	overlapWriteHdd->overlap.OffsetHigh = 0;
	overlapWriteHdd->chunkIndex = 0;
	overlapWriteHdd->startChunk = 0;
	overlapWriteHdd->endChunk = 0;
	overlapWriteHdd->totalChunk = 0;

	//////////////////////////////////////////////////////////////////////////

	m_HeapSize = chROUNDUP(m_CemsParams.HEAP_SORT_MEMORY_SIZE, TUPLE_SIZE) / TUPLE_SIZE;
	// Init heap buffer
	DWORD heapBufferSize = m_HeapSize * TUPLE_SIZE; 
	rc = utl->InitBuffer(heapBuffer, heapBufferSize, &bufferPool);
	if(rc!=SUCCESS) { return rc;} 

	DWORD memSize = bufferPool.size - bufferPool.currentSize; 

	// Add SSD_PAGE_SIZE for page buffer
	m_MaxFanInInOneStep = chROUNDDOWN(memSize,  m_CemsParams.SSD_READ_BUFFER_SIZE + SSD_PAGE_SIZE) / ( m_CemsParams.SSD_READ_BUFFER_SIZE + SSD_PAGE_SIZE);
	if(m_MaxFanInInOneStep==1)
	{
		ShowMB(L"Not enough memory for kWay merge on SSD");
		return rc;
	}

	// Init merge buffer, read buffer on SSD
	ssdReadBuffer = new Buffer[m_MaxFanInInOneStep]; 
	ssdMergePageBuffer = new Buffer[m_MaxFanInInOneStep]; // buffer for merge page on SSD
	ssdMergePage = new PagePtr*[m_MaxFanInInOneStep];/* merge page on SSD */

	for(int i=0;i < m_MaxFanInInOneStep;i++)
	{
		rc = utl->InitBuffer(ssdReadBuffer[i], m_CemsParams.SSD_READ_BUFFER_SIZE, &bufferPool);
		if(rc!=SUCCESS) {  return rc; }

		rc = utl->InitBuffer(ssdMergePageBuffer[i], SSD_PAGE_SIZE, &bufferPool);
		if(rc!=SUCCESS) {  return rc; }

		rc= utl->InitRunPage(ssdMergePage[i], ssdMergePageBuffer[i]);
		if(rc!=SUCCESS) {  return rc; }
	} 
	return SUCCESS;
}

RC CEMS::Estimate()
{
	RC rc;
	HANDLE hFile = CreateFile(
		(LPCWSTR)m_CemsParams.SORT_FILE_PATH, // file to open
		GENERIC_READ,			// open for reading
		FILE_SHARE_READ,        // share for reading
		NULL,					// default security
		OPEN_EXISTING,			// existing file only
		FILE_ATTRIBUTE_NORMAL,	// overlapped operation //| FILE_FLAG_NO_BUFFERING
		NULL);					// no attr. template

	if (INVALID_HANDLE_VALUE==hFile) 
	{   
		ShowMB(L"Cannot create Handle file %s", m_CemsParams.SORT_FILE_PATH); 
		return ERR_CANNOT_CREATE_HANDLE; 
	} 
	LARGE_INTEGER liFileSize; 
	if (!GetFileSizeEx(hFile, &liFileSize))
	{      
		ShowMB(L"Cannot get size of file %s", m_CemsParams.SORT_FILE_PATH); 
		return ERR_CANNOT_GET_FILE_SIZE; 
	} 

	DWORD totalPage = chROUNDUP(liFileSize.QuadPart, SSD_PAGE_SIZE) / SSD_PAGE_SIZE; 

	BufferPool _pool;
	DWORD pageToReadNum = 1000;
	_pool.size = SSD_PAGE_SIZE * pageToReadNum + 5; 
	_pool.currentSize = 0;
	_pool.data = new CHAR[_pool.size];

	Buffer _pageBuffer;
	rc = utl->InitBuffer(_pageBuffer, SSD_PAGE_SIZE, &_pool); 
	if(rc!=SUCCESS) {return rc; }

	PagePtr *_pagePtr; 
	rc = utl->InitRunPage(_pagePtr, _pageBuffer);
	if(rc!=SUCCESS) {return rc; }

	Buffer _readBuffer; // read 100 page
	rc = utl->InitBuffer(_readBuffer, _pool.size-_pool.currentSize, &_pool); 
	if(rc!=SUCCESS) {return rc; }

	DWORD _pageCount = 0;
	DWORD _tupleCount = 0;

	DWORD dwBytesRead = 0;
	ReadFile(hFile, _readBuffer.data, _readBuffer.size, &dwBytesRead, NULL);
	CloseHandle(hFile);

	if(dwBytesRead%SSD_PAGE_SIZE==0)
		_pageCount = dwBytesRead / SSD_PAGE_SIZE;
	else
		_pageCount = dwBytesRead / SSD_PAGE_SIZE + 1;

	if(_pageCount <= pageToReadNum)
		pageToReadNum = _pageCount;

	PageHeader *_pageHeader;
	for(UINT i=0; i < pageToReadNum; i++)
	{
		utl->ResetRunPage(_pagePtr, _pageBuffer);
		utl->GetPageInfo(_readBuffer.data, _pagePtr, _pageBuffer, i, SSD_PAGE_SIZE);

		_pageHeader = (PageHeader *)(_pagePtr->page); 
		_tupleCount+=_pageHeader->totalTuple; 
	} 

	DWORD avgTupleNum = _tupleCount / pageToReadNum;


	delete _pool.data;

	return SUCCESS;
}


RC CEMS::Phase1_GetHeapSize(DWORD &maxFanInNum, DWORD &heapSize)
{
	//RC rc;
	// Need to know: HEAPSIZE, K
	// K = number of FanIn
	// R = MergeReadBufferSize
	// H = HeapSize in bytes = HEAPSIZE * TUPLE_SIZE
	// S = SSD storage size --> S = K * H * 2 (because runsize in best case is about 2*H)
	// M = Current memory Size = H + K * R
	// (1) S = K * H *2 --> K = S / (2H) -->K Should max to merge in one step
	// (2) M = H + K*R = H + (S*R)/(2H) 
	//     M = (2(H^2) + SR) / (2H) 


	DWORD _heapSize = chROUNDUP(m_CemsParams.HEAP_SORT_MEMORY_SIZE, TUPLE_SIZE) / TUPLE_SIZE;



	//DWORD K = 3; 
	//DWORD mergeSize = m_CemsParams.SSD_READ_BUFFER_SIZE;
	//DWORD ssdSize = m_CemsParams.SSD_STORAGE_SIZE;
	//DWORD _heapSize = 0;
	//DWORD temp = 0;


	//while (true)
	//{ 
	//	_heapSize = ssdSize / (2*K*TUPLE_SIZE); 
	//	temp = (_heapSize * TUPLE_SIZE) + (ssdSize * mergeSize) / (2 * _heapSize * TUPLE_SIZE);
	//	temp = (_heapSize * TUPLE_SIZE) + (K * mergeSize);
	//
	//	if(temp <= memorySize) 
	//	{
	//		break;
	//	}  
	//}

	//maxFanInNum = K;
	//heapSize = _heapSize;

	//if(maxFanInNum==1)
	//	return ERR_NOT_ENOUGH_MEMORY;

	return SUCCESS;
}


/// <summary>
/// Executes this instance.
/// </summary>
/// <returns></returns>
RC CEMS::Execute()
{ 
	RC rc;
	//rc = Estimate();
	stwTotalTime.Start();
	rp.TotalTime[0] = stwTotalTime.NowInMilliseconds();
	rp.CpuTime[0] = GetCpuTime();
	// Init buffer pool
	//////////////////////////////////////////////////////////////////////////
	bufferPool.size = m_CemsParams.BUFFER_POOL_SIZE; 
	bufferPool.currentSize = 0;  
	// Creates a private heap object that can be used by the calling process. 
	// The function reserves space in the virtual address space of the process and allocates physical storage 
	// for a specified initial portion of this block.
	// C++ cannot allocate more than 400MB with new[] in MFC
	// http://msdn.microsoft.com/en-us/library/aa366599.aspx
	HANDLE hHeap = HeapCreate(0, 0, bufferPool.size);
	bufferPool.data  = (CHAR*)HeapAlloc(hHeap, 0, bufferPool.size);

	if(NOERROR != GetLastError())
	{
		ReportLastError();
		HeapDestroy(hHeap);
		return ERR_CANNOT_INITIAL_BUFFER_POOL;
	}
	//////////////////////////////////////////////////////////////////////////
	rc = Phase1_Initialize();
	if(rc!=SUCCESS) {return rc;}
	//////////////////////////////////////////////////////////////////////////


	/* Init heap tree*/  //TODO: compute heapsize based on memory size
	//////////////////////////////////////////////////////////////////////////
	//
	// HeapSize = H --> RUN size approximate about 2 * H * TUPLESIZE

	DWORD64 maxSSDpageCount = m_CemsParams.SSD_STORAGE_SIZE / SSD_PAGE_SIZE;   

	const DWORD	HEAP_SIZE = m_HeapSize; 

	RECORD **heap = new RECORD *[HEAP_SIZE];
	for (int i = 0; i < HEAP_SIZE; i++)    
	{
		heap[i] = new RECORD(0, heapBuffer.data + i * TUPLE_SIZE, TUPLE_SIZE);  
	} 

	//////////////////////////////////////////////////////////////////////////
	/* 
	Parition phase 
	1. Read from HDD table into memory
	2. Use replacement selection to create runs on SSD, if the ssd is full, then merge all run on ssd to create new run on HDD
	3. Repeat until read source table on HDD finish
	*/

	RECORD *pTempRecord =  new RECORD(); /* Working record */
	RECORD *pOutRecord = new RECORD(); /* Output record from heap */
	RECORD *pInRecord = new RECORD(); /* Addition record fill in heap */

	//////////////////////////////////////////////////////////////////////////
	// Report infomation
	rp.BufferPoolSize = m_CemsParams.BUFFER_POOL_SIZE; 
	rp.HeapSize = HEAP_SIZE;
	rp.HeapBufferSize = m_CemsParams.HEAP_SORT_MEMORY_SIZE;
	rp.SsdReadBufferSize = m_CemsParams.SSD_READ_BUFFER_SIZE;
	rp.SsdWriteBufferSize = m_CemsParams.SSD_WRITE_BUFFER_SIZE;
	rp.HddReadBufferSize = m_CemsParams.HDD_READ_BUFFER_SIZE;
	rp.HddWriteBufferSize = m_CemsParams.HDD_WRITE_BUFFER_SIZE;
	rp.SsdStorageSize  = m_CemsParams.SSD_STORAGE_SIZE;
	//////////////////////////////////////////////////////////////////////////

	// First read from HDD 
	////////////////////////////////////////////////////////////////////////// 
	Phase1_ReadFromHdd();
	// Get page from buffer
	utl->GetPageInfo(hddReadBuffer.data, hddPageBuffer, 0, HDD_PAGE_SIZE);
	hddPageBuffer.currentTupleIndex=1;
	//////////////////////////////////////////////////////////////////////////

	// Add tuple to heap
	for (int i = 0; i < HEAP_SIZE; i++)     
	{ 
		Phase1_GetNextTupleFromHDD(pTempRecord); 
		pTempRecord->mark = m_CurrentMark;
		CopyRecord(heap[i], pTempRecord); 
	}

	/* Create first run handle and name */
	CreateSsdRun();

	/* Init fanIn counter */

	DWORD64 currentSSDpageCount = 0;  
	DWORD64 lastKey = 0; 
	DWORD64 totalPage = 0;
	DWORD64 totalTuple = 0;
	DWORD64 lowestKey, highestKey;  /* the min and max key in run file */
	BOOL lowestKeyIsGet = FALSE;
	DWORD lastNode = HEAP_SIZE-1;
	BOOL isDone = FALSE; 

	while(!isDone)
	{        
		/* Heapify heap */
		BuildMinHeap(heap, HEAP_SIZE-1);  

		/* Reset heap sort */
		lastNode = HEAP_SIZE-1;

		/* Sorting */
		while(lastNode > 0) 
		{   
			/* Get new record from HDD */
			Phase1_GetNextTupleFromHDD(pInRecord);  

			/* Read input file EOF */
			if(pInRecord->key==MAX) { isDone= TRUE; break; } 

			std::swap( heap[0], heap[lastNode] );  //heap[LastNode] is minimum item 

			/* Check new run or not ? */
			if (heap[1]->mark != m_CurrentMark) 
			{
				/* All items in heap are throw out, terminate current run and start a new run*/  
				Phase1_TerminateRun(totalPage, currentSSDpageCount);

				/* Save current run info */
				SaveSsdFanIn(lowestKey, highestKey, totalTuple, totalPage, overlapWriteHdd->fileSize.QuadPart);

				/* Close current run */
				CloseHandle(hSsdRunFile);

				//////////////////////////////////////////////////////////////////////////
				/* check current ssd size, if ssd is full, then we merge all fanin on SSD write to HDD */ 
				if(currentSSDpageCount >= maxSSDpageCount)
				{
					Phase1_MergeRunsOnSsdWriteToHdd(); 
					currentSSDpageCount = 0;
				}
				//////////////////////////////////////////////////////////////////////////

				CreateSsdRun(); // Get name, begin a new run 

				/* Reset counter for next run */
				overlapWriteHdd->fileSize.QuadPart = 0;
				totalPage = 0;
				totalTuple = 0;
				lowestKey = 0;
				highestKey = 0;
				lowestKeyIsGet = FALSE;

				/* Reverse the mark so that all the elements in the heap will be available for the next run */ 
				m_CurrentMark = heap[1]->mark;
				break;
			}

			/* Get element has minimum key */   
			pOutRecord = heap[lastNode]; 

			/* Tracking the lowest key, highest key */
			lastKey = pOutRecord->key;  

			if(lowestKeyIsGet==FALSE) {
				lowestKeyIsGet = TRUE; lowestKey = lastKey;
			}

			highestKey = lastKey;

			/* Send record has minimum key to buffer*/
			Phase1_SentOutput(pOutRecord->data, totalTuple, totalPage, currentSSDpageCount);

			/* Determine if the newly read record belongs in the current run  or the next run. */  
			if(pInRecord->key >= lastKey)
			{ 
				pInRecord->mark = m_CurrentMark;  
				CopyRecord(heap[lastNode], pInRecord);
			}
			else
			{
				/* New record cannot belong to current run */ 
				pInRecord->mark = m_CurrentMark ^ 1;  

				CopyRecord( heap[lastNode], pInRecord );

				/* Continue heap sort */
				lastNode--; 
			}  

			MinHeapify(heap, 0,  lastNode); 
		}    
	} 

	/* Push all records still in memory to disk */
	while(lastNode > 0)
	{ 
		std::swap(heap[0], heap[lastNode]);  //heap[LastNode] is minimum item 

		/* Determine record has key is minimum  */   
		pOutRecord = heap[lastNode];  
		lastKey = pOutRecord->key;  
		if(lowestKeyIsGet==FALSE)
		{
			lowestKeyIsGet = TRUE;
			lowestKey = lastKey;
		} 
		highestKey = lastKey;

		/* Sent record has key minimum to disk*/
		Phase1_SentOutput(pOutRecord->data, totalTuple, totalPage, currentSSDpageCount);

		lastNode--; 
		MinHeapify(heap, 0,  lastNode); 
	}

	/* Terminate final run */   
	Phase1_TerminateRun(totalPage, currentSSDpageCount);

	/* Save final run info */
	SaveSsdFanIn(lowestKey, highestKey, totalTuple, totalPage, overlapWriteHdd->fileSize.QuadPart);

	/* Close final run */
	CloseHandle(hSsdRunFile); 

	/* If still have fanIns on SSD then merge them */
	if(currentSSDpageCount > 0)
	{ 
		rc = Phase1_MergeRunsOnSsdWriteToHdd();
		if(rc!=SUCCESS) {return rc;}
	}

	/* Start merge run on HDD */
	//////////////////////////////////////////////////////////////////////////
	bufferPool.currentSize = 0;

	//////////////////////////////////////////////////////////////////////////

	rc =  Phase2_MergeRunsOnHdd();
	if(rc!=SUCCESS) {return rc;}
	//////////////////////////////////////////////////////////////////////////
	rp.TotalTime[1] = stwTotalTime.NowInMilliseconds();
	rp.CpuTime[1] = GetCpuTime();

	//////////////////////////////////////////////////////////////////////////
	WriteReport();

	//////////////////////////////////////////////////////////////////////////
	CloseHandle(hTableFile);

	delete pTempRecord;
	delete pOutRecord;
	delete pInRecord;

	//////////////////////////////////////////////////////////////////////////
	BOOL bRet = HeapFree(hHeap, 0, bufferPool.data);
	bRet = HeapDestroy(hHeap);
	//////////////////////////////////////////////////////////////////////////
	return SUCCESS;
}

/// <summary>
/// Writes the report to disk.
/// </summary>
/// <returns></returns>
RC CEMS::WriteReport()
{
	FILE *fp;   
	CHAR *reportFilePath = new CHAR[MAX_PATH];   
	LPWSTR tempReportPath = new TCHAR[MAX_PATH];
	swprintf(tempReportPath, MAX_PATH, L"%s%s", m_CemsParams.SSD_WORK_SPACE_PATH, L"CEMS_Report.csv" ); 
	// convert file path to char  
	size_t  count = wcstombs(reportFilePath, tempReportPath, MAX_PATH); 

	CHAR *reportTitle = "Relation Size,Memory Size,Run Size,Heap Size,Heap Buffer Size,SSD Read Buffer Size,SSD Write Buffer Size,HDD Read Buffer Size,HDD Write Buffer Size,Total Execute Time(ms),CPU Time\n";
	CHAR *reportContent = new CHAR[1024];
	sprintf(reportContent, "%lld,%d,%lld,%d,%d,%d,%d,%d,%d,%lld,%.f", 
		rp.TableSize, 
		rp.BufferPoolSize, 
		rp.FanInSize,
		rp.HeapSize, 
		rp.HeapBufferSize, 
		rp.SsdReadBufferSize,
		rp.SsdWriteBufferSize, 
		rp.HddReadBufferSize,
		rp.HddWriteBufferSize, 
		rp.TotalTime[1] - rp.TotalTime[0],  
		rp.CpuTime[1] - rp.CpuTime[0]);

	fp=fopen(reportFilePath, "w+b"); 
	fprintf(fp, reportTitle);
	fprintf(fp, reportContent);
	fclose(fp);

	delete reportFilePath;
	delete tempReportPath; 
	delete reportContent;


	return SUCCESS;
}

/// <summary>
/// Saves the SSD fan-in to a queue.
/// </summary>
/// <param name="lowestKey">The lowest key.</param>
/// <param name="highestKey">The highest key.</param>
/// <param name="totalTuple">The total tuple.</param>
/// <param name="totalPage">The total page.</param>
/// <param name="fileSize">Size of the file.</param>
/// <returns></returns>
RC CEMS::SaveSsdFanIn(DWORD64 lowestKey, DWORD64 highestKey, DWORD64 totalTuple, DWORD64 totalPage, LONGLONG fileSize)
{
	/* Tracking fanIn on SSD */
	/* Save current run info */
	FANS *_fan = new FANS();  
	wcscpy(_fan->fileName,  m_SsdFanInPath); 
	_fan->threadID = 99999;
	_fan->pageCount = totalPage;
	_fan->tupleCount = totalTuple;
	_fan->fileSize.QuadPart = fileSize;

	_fan->lowestKey = lowestKey;
	_fan->highestKey = highestKey; 

	m_FanInSsd.push(_fan);   

	if(isGetFanInSize==FALSE)
	{
		rp.FanInSize = _fan->fileSize.QuadPart;
		isGetFanInSize = TRUE;
	}

	return SUCCESS;
}

/// <summary>
/// Saves the HDD fan-in to a queue.
/// </summary>
/// <param name="lowestKey">The lowest key.</param>
/// <param name="highestKey">The highest key.</param>
/// <param name="totalTuple">The total tuple.</param>
/// <param name="totalPage">The total page.</param>
/// <param name="fileSize">Size of the file.</param>
/// <returns></returns>
RC CEMS::SaveHddFanIn(DWORD64 lowestKey, DWORD64 highestKey, DWORD64 totalTuple, DWORD64 totalPage, LONGLONG fileSize)
{
	/* Tracking fanIn on HDD */
	/* Save current run info */
	FANS *_fan = new FANS();  
	wcscpy(_fan->fileName,  m_HddFanInPath); 
	_fan->threadID = 99999;
	_fan->pageCount = totalPage;
	_fan->tupleCount = totalTuple;
	_fan->fileSize.QuadPart = fileSize;

	_fan->lowestKey = lowestKey;
	_fan->highestKey = highestKey; 

	m_FanInHdd.push(_fan);   

	return SUCCESS;
}

/// <summary>
/// Terminate run in sort phase.
/// </summary>
/// <param name="totalPage">The total page.</param>
/// <param name="currentSsdRunPageCount">The current SSD run page count.</param>
/// <returns></returns>
RC CEMS::Phase1_TerminateRun(DWORD64 &totalPage, DWORD64 &currentSsdRunPageCount)
{ 
	if(ssdWriteBuffer.currentSize > 0)
	{
		Phase1_WriteToSsd();  
	}

	/* The last page is not write to disk */
	//if(ssdPage->bConsomed == FALSE)
	//{
	//	utl->AddPageToBuffer(ssdWriteBuffer, ssdPage->page, SSD_PAGE_SIZE);
	//	ssdPage->bConsomed = TRUE;
	//	currentSsdRunPageCount++;  // neeed to check SSD storage is full or not -> if true--> flush buffer to ssd  
	//	totalPage++;
	//	WriteToSsd();  
	//}

	utl->ResetRunPage(ssdPage, ssdPageBuffer); 

	return SUCCESS;
}

/// <summary>
/// Write the output tuple in sort phase to buffer, if buffer is full then write to disk.
/// </summary>
/// <param name="tupleData">The tuple data.</param>
/// <param name="totalTuple">The total tuple.</param>
/// <param name="totalPage">The total page.</param>
/// <param name="currentSsdRunPageCount">The current SSD run page count.</param>
/// <returns></returns>
RC CEMS::Phase1_SentOutput(CHAR *tupleData, DWORD64 &totalTuple, DWORD64 &totalPage, DWORD64 &currentSsdRunPageCount)
{ 
	/* Sent tuple data from heap tree to output buffer, if buffer is full then write buffer to ssd */
	/* Write out minimum item */ 
	ssdPage->consumed = FALSE;
	if(utl->IsPageFull(ssdPage))
	{ 
		if(!utl->IsBufferFull(ssdWriteBuffer))
		{ 
			utl->AddPageToBuffer(ssdWriteBuffer, ssdPage->page, SSD_PAGE_SIZE);
			ssdPage->consumed = TRUE;
			ssdWriteBuffer.pageCount++;
			currentSsdRunPageCount++;
			totalPage++;
		}   

		utl->ResetRunPage(ssdPage, ssdPageBuffer);   
	}

	/* Output buffer is full -> write buffer to ssd */
	if(utl->IsBufferFull(ssdWriteBuffer))
	{ 
		Phase1_WriteToSsd();  
	}

	utl->AddTupleToPage(ssdPage, tupleData, ssdPageBuffer);
	ssdWriteBuffer.tupleCount++;
	totalTuple++;

	return SUCCESS;
}

/// <summary>
/// Creates the SSD fan-in path and assign handler.
/// </summary>
/// <returns></returns>
RC CEMS::CreateSsdRun()
{
	/* Get fanIn name for heap tree */ 
	GetSsdFanInPath(m_SsdFanInPath);  
	hSsdRunFile=CreateFile((LPCWSTR)m_SsdFanInPath,		// file to write
		GENERIC_WRITE,			// open for writing
		0,						// Do not share
		NULL,					// default security
		CREATE_ALWAYS,			// Overwrite existing
		FILE_FLAG_OVERLAPPED,	// overlapped operation
		NULL);					// no attr. template

	if (INVALID_HANDLE_VALUE==hSsdRunFile) 
	{ 
		ShowMB(L"Cannot create SSD run file %s", m_SsdFanInPath);    
		return ERR_CANNOT_CREATE_HANDLE; 
	}  

	return SUCCESS;
}

/// <summary>
/// Creates the HDD fan-in path and assign handler.
/// </summary>
/// <returns></returns>
RC CEMS::CreateHddRun()
{
	GetHddFanInPath(m_HddFanInPath);  
	hHddRunFile=CreateFile((LPCWSTR)m_HddFanInPath,		// file to write
		GENERIC_WRITE,			// open for writing
		0,						// Do not share
		NULL,					// default security
		CREATE_ALWAYS,			// Overwrite existing
		FILE_FLAG_OVERLAPPED,	// overlapped operation
		NULL);					// no attr. template

	if (INVALID_HANDLE_VALUE==hHddRunFile) 
	{ 
		ShowMB(L"Cannot create HDD run file %s", m_HddFanInPath);    
		return ERR_CANNOT_CREATE_HANDLE; 
	}  

	return SUCCESS;
}

/// <summary>
/// Copies the record.
/// </summary>
/// <param name="desRecordPtr">The record pointer will copy to.</param>
/// <param name="srcRecordPtr">The input record pointer.</param>
/// <returns></returns>
RC CEMS::CopyRecord(RECORD *desRecordPtr, RECORD *&srcRecordPtr)
{
	desRecordPtr->key = srcRecordPtr->key;
	strcpy(desRecordPtr->data, srcRecordPtr->data);
	desRecordPtr->length= srcRecordPtr->length;
	desRecordPtr->mark = srcRecordPtr->mark;

	return SUCCESS;
}

/// <summary>
/// Write to SSD functionm in sort phase.
/// </summary>
/// <returns></returns>
RC CEMS::Phase1_WriteToSsd()
{  	   
	/* Write fanIn from heap to SSD */ 
	overlapWriteHdd->overlap.Offset = overlapWriteHdd->fileSize.LowPart;
	overlapWriteHdd->overlap.OffsetHigh = overlapWriteHdd->fileSize.HighPart;

	WriteFile(hSsdRunFile, ssdWriteBuffer.data, ssdWriteBuffer.currentSize, &overlapWriteHdd->dwBytesReadWritten, &overlapWriteHdd->overlap); 	
	GetOverlappedResult(hSsdRunFile, &overlapWriteHdd->overlap, &overlapWriteHdd->dwBytesReadWritten, TRUE);   

	overlapWriteHdd->fileSize.QuadPart+=overlapWriteHdd->dwBytesReadWritten;

	utl->ResetBuffer(ssdWriteBuffer);
	return SUCCESS;
}

/// <summary>
/// Get next tuple from HDD in sort phase.
/// </summary>
/// <param name="recordPtr">The record PTR.</param>
/// <returns></returns>
RC CEMS::Phase1_GetNextTupleFromHDD(RECORD *&recordPtr)
{  
	if(hddPageBuffer.currentTupleIndex > hddPageBuffer.tupleCount )
	{
		// Reset hdd page buffer
		utl->ResetBuffer(hddPageBuffer); 

		// read next page from buffer
		hddReadBuffer.currentPageIndex++; 

		// check read buffer
		if(hddReadBuffer.currentPageIndex >= hddReadBuffer.pageCount)
		{   
			Phase1_ReadFromHdd(); 
		}   

		// Get page from buffer
		utl->GetPageInfo(hddReadBuffer.data, hddPageBuffer, hddReadBuffer.currentPageIndex, HDD_PAGE_SIZE);
		hddPageBuffer.currentTupleIndex=1; 
	}

	// Get page info to page buffer
	utl->GetTupleInfo(recordPtr, hddPageBuffer.currentTupleIndex, hddPageBuffer.data, HDD_PAGE_SIZE, m_CemsParams.KEY_POS);
	hddPageBuffer.currentTupleIndex++;

	return SUCCESS;
}

/// <summary>
/// Read next chunk from HDD.
/// </summary>
/// <returns></returns>
RC CEMS::Phase1_ReadFromHdd()
{  
	/* Read source table to buffer */ 
	utl->ResetBuffer(hddReadBuffer);  
	if(overlapReadHdd->chunkIndex >= overlapReadHdd->totalChunk)
	{ 
		utl->AddPageMAXToBuffer(hddReadBuffer, HDD_PAGE_SIZE); 
		utl->GetPageInfo(hddReadBuffer.data, hddPageBuffer, 0, HDD_PAGE_SIZE);
		hddPageBuffer.isFullMaxValue = TRUE; 

		utl->GetPageInfo(hddReadBuffer.data, hddPageBuffer, 0, HDD_PAGE_SIZE);
		hddPageBuffer.currentTupleIndex = 1; 

		return ERR_END_OF_FILE; 
	}

	overlapReadHdd->fileSize.QuadPart = overlapReadHdd->chunkIndex * overlapReadHdd->dwBytesToReadWrite;
	overlapReadHdd->overlap.Offset = overlapReadHdd->fileSize.LowPart;
	overlapReadHdd->overlap.OffsetHigh = overlapReadHdd->fileSize.HighPart;

	ReadFile(hTableFile, hddReadBuffer.data, overlapReadHdd->dwBytesToReadWrite,  &overlapReadHdd->dwBytesReadWritten, &overlapReadHdd->overlap);

	// Wait for Read complete 
	GetOverlappedResult(hTableFile, &overlapReadHdd->overlap, &overlapReadHdd->dwBytesReadWritten, TRUE);   
	utl->ComputeBuffer(hddReadBuffer, overlapReadHdd->dwBytesReadWritten, HDD_PAGE_SIZE);  

	overlapReadHdd->chunkIndex++;
	hddReadBuffer.currentPageIndex = 0;  

	return SUCCESS;
}

/// <summary>
/// Copy file from HDD to SSD.
/// </summary>
/// <param name="hHddFile">The handle of HDD file.</param>
/// <param name="newSsdFanIn">The new SSD fan-in.</param>
/// <param name="overlapRead">The overlap read.</param>
/// <param name="copyBuffer">The copy buffer.</param>
/// <param name="maxRunSize">Maximum size of the run.</param>
/// <returns></returns>
RC CEMS::Phase2_CopyFileFromHddToSsd(HANDLE hHddFile, FANS *&newSsdFanIn, OVERLAPPEDEX *overlapRead, Buffer &copyBuffer, DWORD64 maxRunSize)
{ 

	/* FanIn on HDD read done, dont need to copy */
	if(overlapRead->chunkIndex>=overlapRead->totalChunk)
	{ 
		newSsdFanIn = NULL;
		return ERR_END_OF_FILE;
	} 

	/* reset copy buffer */
	utl->ResetBuffer(copyBuffer);

	GetSsdFanInPath(newSsdFanIn->fileName);  

	HANDLE hFile = CreateFile(
		(LPCWSTR)newSsdFanIn->fileName, // file to open
		GENERIC_WRITE,			// open for writing
		0,						 // share for reading
		NULL,					// default security
		CREATE_ALWAYS,			// always create file  
		FILE_FLAG_NO_BUFFERING | FILE_FLAG_OVERLAPPED,	// overlapped operation  
		NULL);					// no attr. template 

	if (INVALID_HANDLE_VALUE==hFile) 
	{   
		ShowMB(L"Cannot create handle for SSD run: %s \n", newSsdFanIn->fileName );
		return ERR_CANNOT_CREATE_HANDLE; 
	}   

	/* Set up overlap structure for write to SSD */ 
	OVERLAPPEDEX *overlapSsdWrite = new OVERLAPPEDEX();
	overlapSsdWrite->dwBytesToReadWrite = copyBuffer.size;
	overlapSsdWrite->dwBytesReadWritten = 0;
	overlapSsdWrite->fileSize.QuadPart = 0;

	INT64 runSize = maxRunSize;
	DWORD64 totalPage = 0;

	while( runSize > 0 )
	{
		/* Compute offset to read */
		overlapRead->fileSize.QuadPart = overlapRead->chunkIndex * overlapRead->dwBytesToReadWrite;
		overlapRead->overlap.Offset = overlapRead->fileSize.LowPart;
		overlapRead->overlap.OffsetHigh = overlapRead->fileSize.HighPart;

		/* Execute read from HDD */
		ReadFile(hHddFile, copyBuffer.data, overlapRead->dwBytesToReadWrite, &overlapRead->dwBytesReadWritten, &overlapRead->overlap);  

		/* Wait until read operation done*/
		GetOverlappedResult(hHddFile, &overlapRead->overlap, &overlapRead->dwBytesReadWritten, TRUE);  
		overlapRead->chunkIndex++;

		/* Update new offset for read */
		overlapRead->fileSize.QuadPart+=overlapRead->dwBytesReadWritten;
		utl->ComputeBuffer(copyBuffer, overlapRead->dwBytesReadWritten, HDD_PAGE_SIZE);
		totalPage+=copyBuffer.pageCount;

		if(overlapRead->dwBytesReadWritten > 0)
		{
			overlapSsdWrite->overlap.Offset = overlapSsdWrite->fileSize.LowPart;
			overlapSsdWrite->overlap.OffsetHigh = overlapSsdWrite->fileSize.HighPart;

			/* Execute write to SSD */
			WriteFile(hFile, copyBuffer.data, overlapSsdWrite->dwBytesToReadWrite, &overlapSsdWrite->dwBytesReadWritten, &overlapSsdWrite->overlap); 

			/* Wait until write operation done*/
			GetOverlappedResult(hFile, &overlapSsdWrite->overlap, &overlapSsdWrite->dwBytesReadWritten, TRUE); 

			/* Update new offset for write */
			overlapSsdWrite->fileSize.QuadPart+=overlapSsdWrite->dwBytesReadWritten;  
		}

		utl->ResetBuffer(copyBuffer);
		/* Decrease run size */
		runSize-=overlapRead->dwBytesReadWritten;

		if(overlapRead->chunkIndex>=overlapRead->totalChunk)
		{
			newSsdFanIn->isFinal = TRUE; /* Mark this ssd fanIn is the finally fanIn*/
			break;
		}  
	}

	/* Create file done, close handle */
	CloseHandle(hFile);

	/* Update ssd fanIn info */   
	newSsdFanIn->pageCount = totalPage;
	newSsdFanIn->tupleCount = 0;
	newSsdFanIn->fileSize.QuadPart = overlapSsdWrite->fileSize.QuadPart; 
	newSsdFanIn->lowestKey = 0;
	newSsdFanIn->highestKey = 0; 
	newSsdFanIn->isDeleted = FALSE; /* Mark this run is not deleted */

	//wprintf(L"New SSD run: %s create success \n", newSsdFanIn->fileName );

	/* Clean */
	delete overlapSsdWrite; 

	return SUCCESS; 
}


/// <summary>
/// Merge runs on HDD.
/// </summary>
/// <returns></returns>
RC CEMS::Phase2_MergeRunsOnHdd()
{ 
	RC rc;
	/* Merge the runs on HDD to final runs 
	1. Copy run in HDD to SSD, use sequence read from HDD, create run on SSD with maximum ssdRunsize
	2. Merge all ssd run, use random read from SSD
	if all the run on ssd is exhaused, continue read from HDD to create ssd run
	when all the run on HDD is read-> finish work
	*/

	std::queue<LPWSTR> runWillDelete;

	//////////////////////////////////////////////////////////////////////////

	/* Init buffer for write out FanOut */ 
	rc = utl->InitBuffer(hddWriteBuffer, m_CemsParams.HDD_WRITE_BUFFER_SIZE, &bufferPool);
	if(rc!=SUCCESS) {return rc;}

	/* Init buffer for copy file */
	/* Buffer may be read large block to speed up */
	rc = utl->InitBuffer(hddReadBuffer, m_CemsParams.HDD_READ_BUFFER_SIZE, &bufferPool); 
	if(rc!=SUCCESS) {return rc;}

	// buffer for write copy fanin from Hdd to ssd
	rc = utl->InitBuffer(ssdWriteBuffer, m_CemsParams.SSD_WRITE_BUFFER_SIZE, &bufferPool); 
	if(rc!=SUCCESS) {return rc;}

	/* Init hdd run page */ 
	rc= utl->InitBuffer(hddPageBuffer, HDD_PAGE_SIZE,  &bufferPool);
	if(rc!=SUCCESS) {return rc;}

	rc = utl->InitRunPage(hddPage, hddPageBuffer);
	if(rc!=SUCCESS) {return rc;}

	//////////////////////////////////////////////////////////////////////////
	// Calculate how many SSD FanIn fit in memory
	DWORD currentMemSize = bufferPool.size - bufferPool.currentSize;

	DWORD MaxSsdFanInInOneStep = 0;
	// Plus SSD_PAGE_SIZE for mergePageBuffer
	MaxSsdFanInInOneStep = chROUNDDOWN(currentMemSize, m_CemsParams.SSD_READ_BUFFER_SIZE + SSD_PAGE_SIZE) /  (m_CemsParams.SSD_READ_BUFFER_SIZE + SSD_PAGE_SIZE);
	if(MaxSsdFanInInOneStep<=1) 
	{
		ShowMB(L"Not enough memory for kWay merge");
		return ERR_NOT_ENOUGH_MEMORY;
	}

	/* Init ssd read buffer and probeBuffer and overlap structure to read */
	ssdReadBuffer= new Buffer[MaxSsdFanInInOneStep];
	ssdMergePageBuffer= new Buffer[MaxSsdFanInInOneStep];
	ssdMergePage = new PagePtr *[MaxSsdFanInInOneStep]; 
	overlapReadSsd = new OVERLAPPEDEX*[MaxSsdFanInInOneStep];

	for(INT i=0; i < MaxSsdFanInInOneStep; i++)
	{
		rc = utl->InitBuffer(ssdReadBuffer[i], m_CemsParams.SSD_READ_BUFFER_SIZE, &bufferPool);
		if(rc!=SUCCESS) {return rc;}

		rc = utl->InitBuffer(ssdMergePageBuffer[i], SSD_PAGE_SIZE,  &bufferPool);
		if(rc!=SUCCESS) {return rc;}

		rc = utl->InitRunPage(ssdMergePage[i], ssdMergePageBuffer[i]);
		if(rc!=SUCCESS) {return rc;} 

		overlapReadSsd[i] = new OVERLAPPEDEX(); 
		overlapReadSsd[i]->dwBytesToReadWrite = m_CemsParams.SSD_READ_BUFFER_SIZE;
		overlapReadSsd[i]->dwBytesReadWritten = 0;
		overlapReadSsd[i]->chunkIndex = 0;
		overlapReadSsd[i]->startChunk = 0;
		overlapReadSsd[i]->endChunk = 0;
		overlapReadSsd[i]->totalChunk = 0;
	}

	//////////////////////////////////////////////////////////////////////////
	overlapWriteHdd = new OVERLAPPEDEX();
	overlapWriteHdd->dwBytesToReadWrite = m_CemsParams.HDD_WRITE_BUFFER_SIZE;
	overlapWriteHdd->fileSize.QuadPart = 0;
	overlapWriteHdd->chunkIndex = 0;
	overlapWriteHdd->totalChunk = 0;
	overlapWriteHdd->endChunk = 0;
	overlapWriteHdd->startChunk  = 0;
	overlapWriteHdd->overlap.Offset = 0;
	overlapWriteHdd->overlap.OffsetHigh = 0;
	//////////////////////////////////////////////////////////////////////////
	RECORD *recordPtr = new RECORD(); /* working record */

	//////////////////////////////////////////////////////////////////////////

	INT hddFanInNum = 0;
	// Multiple merge step HHD run
	//////////////////////////////////////////////////////////////////////////
	while (true)
	{ 
		hddFanInNum = m_FanInHdd.size();
		// Check HDD fanIn num to fit in memory
		if(hddFanInNum > MaxSsdFanInInOneStep)
		{
			hddFanInNum = MaxSsdFanInInOneStep;
		}
		//////////////////////////////////////////////////////////////////////////

		/* Compute size of run on SSD, copy run from HDD to SSD */ 
		DWORD ssdRunSize = m_CemsParams.SSD_STORAGE_SIZE / hddFanInNum;  
		ssdRunSize = chROUNDUP(ssdRunSize, SSD_PAGE_SIZE);

		/* Setup handle hdd run file and overlap structure to read from HDD */ 
		HANDLE *hHddFile = new HANDLE[hddFanInNum];
		HANDLE *hSsdFile = new HANDLE[hddFanInNum];
		FANS **ssdFanInFile = new FANS*[hddFanInNum];
		FANS **hddFanInFile = new FANS*[hddFanInNum];
		overlapCopyHddToSsd = new OVERLAPPEDEX*[hddFanInNum];

		//////////////////////////////////////////////////////////////////////////
		/* Create fan out name to write to HDD */ 
		GetHddFanInPath(m_HddFanInPath);
		HANDLE hWriteToHdd = CreateFile(
			(LPCWSTR)m_HddFanInPath, // file to open
			GENERIC_WRITE,			// open for reading
			0,        // share for reading
			NULL,					// default security
			CREATE_ALWAYS,			// existing file only
			FILE_FLAG_NO_BUFFERING | FILE_FLAG_OVERLAPPED,	// overlapped operation  
			NULL);					// no attr. template

		if (INVALID_HANDLE_VALUE==hWriteToHdd) 
		{ 
			ShowMB(L"Cannot create handle for file %s", m_HddFanInPath);    
			return ERR_CANNOT_CREATE_HANDLE; 
		}   

		//////////////////////////////////////////////////////////////////////////

		DWORD64 hddChunkSize = m_CemsParams.HDD_READ_BUFFER_SIZE;
		for(int i=0; i < hddFanInNum; i++)
		{
			/* Get the first item in queue */
			FANS *fanIn = m_FanInHdd.front();

			/* Save hdd FanIn */
			hddFanInFile[i] = fanIn;
			hddFanInFile[i]->isDeleted = FALSE;

			/* Remove first item from queue*/
			m_FanInHdd.pop();  

			/* Save file name and path for later delete */
			runWillDelete.push(fanIn->fileName);

			hHddFile[i]=CreateFile((LPCWSTR)fanIn->fileName,		// file to write
				GENERIC_READ,			// open for writing
				0,						// Do not share
				NULL,					// default security
				OPEN_EXISTING,			// Overwrite existing
				FILE_FLAG_NO_BUFFERING | FILE_FLAG_OVERLAPPED,	// overlapped operation 
				NULL);					// no attr. template

			if (INVALID_HANDLE_VALUE==hHddFile[i]) 
			{ 
				ShowMB(L"Cannot create handle for file %s", fanIn->fileName);    
				return ERR_CANNOT_CREATE_HANDLE; 
			}   

			/* Compute chunk part */
			DWORD64 totalChunk = chROUNDUP(fanIn->fileSize.QuadPart, hddChunkSize) / hddChunkSize ;

			overlapCopyHddToSsd[i] = new OVERLAPPEDEX(); 
			overlapCopyHddToSsd[i]->dwBytesToReadWrite = hddChunkSize;
			overlapCopyHddToSsd[i]->dwBytesReadWritten = 0;
			overlapCopyHddToSsd[i]->chunkIndex = 0;
			overlapCopyHddToSsd[i]->startChunk = 0;
			overlapCopyHddToSsd[i]->endChunk = totalChunk;
			overlapCopyHddToSsd[i]->totalChunk = totalChunk; 

			/* Set up handle for SSD fanIns */ 
			ssdFanInFile[i] = new FANS();  
		}

		//////////////////////////////////////////////////////////////////////////

		/* Copy file from HDD to SSD */
		for(int i=0; i < hddFanInNum; i++)
		{
			// fan in size in SSD must not over ssdRunSize
			Phase2_CopyFileFromHddToSsd( hHddFile[i], ssdFanInFile[i], overlapCopyHddToSsd[i], ssdWriteBuffer, ssdRunSize); 
		}

		//////////////////////////////////////////////////////////////////////////


		/* Now SSD is full, open SSD fanIn for merge */
		for(int i=0; i < hddFanInNum; i++)
		{
			/* Create current ssd fanIn handle */  
			hSsdFile[i]=CreateFile((LPCWSTR)ssdFanInFile[i]->fileName,		// file to write
				GENERIC_READ,			// open for writing
				0,						// Do not share
				NULL,					// default security
				OPEN_EXISTING,			// Overwrite existing
				FILE_FLAG_NO_BUFFERING | FILE_FLAG_OVERLAPPED,	// overlapped operation 
				NULL);					// no attr. template

			if (INVALID_HANDLE_VALUE==hSsdFile[i]) 
			{   
				ShowMB(L"Cannot open file: %s for READ \n", ssdFanInFile[i]->fileName);
				return ERR_CANNOT_CREATE_HANDLE;
			}   

			// Reset overlap structure & buffer
			UINT64 fanInSize = GetFanSize(hSsdFile[i]);
			DWORD64 totalChunk = chROUNDUP(fanInSize, m_CemsParams.SSD_READ_BUFFER_SIZE) / m_CemsParams.SSD_READ_BUFFER_SIZE;

			overlapReadSsd[i]->dwBytesToReadWrite = m_CemsParams.SSD_READ_BUFFER_SIZE;
			overlapReadSsd[i]->dwBytesReadWritten = 0;
			overlapReadSsd[i]->chunkIndex = 0;
			overlapReadSsd[i]->startChunk = 0;
			overlapReadSsd[i]->endChunk = totalChunk;
			overlapReadSsd[i]->totalChunk = totalChunk;

			utl->ResetBuffer(ssdReadBuffer[i]);
			utl->ResetRunPage(ssdMergePage[i], ssdMergePageBuffer[i]); 
		}

		//////////////////////////////////////////////////////////////////////////

		/* Create loser tree */
		LoserTree *lsTree = new LoserTree(hddFanInNum); 

		//////////////////////////////////////////////////////////////////////////

		/* Start to read ssd fanIn, fill first record of each FanIn to loser tree */

		for(INT i=0; i < hddFanInNum; i++)
		{
			ReadSsdFanIn(hSsdFile[i], ssdReadBuffer[i], overlapReadSsd[i]); 

			/* Get the FIRST page in buffer */
			utl->GetPageInfo(ssdReadBuffer[i].data, ssdMergePageBuffer[i], 0, SSD_PAGE_SIZE); 

			/* Get the FIRST record */
			utl->GetTupleInfo(recordPtr, 1, ssdMergePageBuffer[i].data, SSD_PAGE_SIZE, m_CemsParams.KEY_POS);  

			/* Add this record to loser tree */
			lsTree->AddNewNode(recordPtr, i);

			ssdMergePageBuffer[i].currentTupleIndex++;
		}

		//////////////////////////////////////////////////////////////////////////
		/* Create loser tree */
		lsTree->CreateLoserTree();  

		//////////////////////////////////////////////////////////////////////////
		/* Merge SSD fanIn then write to HDD */

		utl->ResetBuffer(hddPageBuffer); 
		utl->ResetRunPage(hddPage, hddPageBuffer);
		utl->ResetBuffer(hddWriteBuffer); 

		INT treeIndex;
		while(TRUE) 
		{ 
			lsTree->GetMinRecord( recordPtr, treeIndex ); // index = loserTree[0]

			/* Loser tree finish job */
			if(recordPtr->key==MAX)  { break; }

			/* Check output buffer */
			if(utl->IsBufferFull(hddWriteBuffer)) // write buffer is full
			{ 
				WriteSsdFanInToHdd(hWriteToHdd); 
			}

			/* Check run page */
			hddPage->consumed = FALSE;
			if(utl->IsPageFull(hddPage))
			{  
				utl->AddPageToBuffer(hddWriteBuffer, hddPage->page, HDD_PAGE_SIZE);  
				hddWriteBuffer.pageCount++;
				hddPage->consumed = TRUE;
				// Reset run page, begin new page
				utl->ResetRunPage(hddPage, hddPageBuffer);  
			} 

			/* Add output tuple to run page */
			utl->AddTupleToPage(hddPage, recordPtr->data, hddPageBuffer);  
			hddWriteBuffer.tupleCount++;

			//////////////////////////////////////////////////////////////////////////

			/* Get next tuple from buffer, 
			if buffer exhaused then read from ssd, 
			if ssd fanIn is exhaused, delete current fanIn, copy new file from hdd to ssd
			*/  
			Phase2_GetNextTuple(hHddFile[treeIndex], hSsdFile[treeIndex], ssdFanInFile[treeIndex], treeIndex, recordPtr, ssdRunSize);

			/* Add new tuple to loser tree */
			lsTree->AddNewNode(recordPtr, treeIndex); 

			/* Shrink the tree */
			lsTree->Adjust( treeIndex ); 
		}

		if( (hddPage->consumed==FALSE) && (utl->IsBufferFull(hddWriteBuffer)==FALSE) )
		{ 
			if(!utl->IsEmptyPage(hddPage))
			{
				utl->AddPageToBuffer(hddWriteBuffer, hddPage->page, HDD_PAGE_SIZE);  
				hddWriteBuffer.pageCount++; 
				hddPage->consumed = TRUE;  
			}
		}

		if(hddWriteBuffer.currentSize > 0)
		{ 
			WriteSsdFanInToHdd(hWriteToHdd); 
		}

		/* Cleaning */ 

		for(int i=0; i < hddFanInNum; i++)
		{
			if(hddFanInFile[i]->isDeleted==FALSE)
			{
				CloseHandle(hHddFile[i]);
				if(!DeleteFile(hddFanInFile[i]->fileName)) // delete exhaused fanIn
				{
					ShowMB(L"Cannot delete file: %s ", hddFanInFile[i]->fileName); 
				}
				else
				{
					hddFanInFile[treeIndex]->isDeleted = TRUE;
				} 
			} 
		}

		//////////////////////////////////////////////////////////////////////////
		//  push fanIn to queue
		FANS *_fanOut = new FANS();
		wcscpy( _fanOut->fileName, m_HddFanInPath);
		_fanOut->fileSize.QuadPart = GetFanSize(hWriteToHdd); 
		_fanOut->threadID = 99999;
		_fanOut->lowestKey = 0;
		_fanOut->highestKey = 0;
		_fanOut->tupleCount = 0;
		_fanOut->pageCount = 0;
		_fanOut->isFinal = FALSE;
		_fanOut->isDeleted = FALSE;
		m_FanInHdd.push(_fanOut);


		/* Close the final fanOut */
		CloseHandle(hWriteToHdd);
		//////////////////////////////////////////////////////////////////////////

		if(m_FanInHdd.size()==1)
		{
			// this is the final run  
			break;
		}  

		//////////////////////////////////////////////////////////////////////////
	} // end while


	return SUCCESS;
}


// Get next tuple from SSD fanIn, if return MAX then check for HDD fanIn is still have data or not
// If HDD fanIn have data then copy them to SSD and continue merge
RC  CEMS::Phase2_GetNextTuple(HANDLE hHddFile, HANDLE &hSsdFile, FANS *&SsdFanInFile, const DWORD treeIndex, RECORD *recordPtr, DWORD SsdRunSize)
{   
	/* Get tuple from runs on SSD */  
	if(ssdMergePageBuffer[treeIndex].currentTupleIndex > ssdMergePageBuffer[treeIndex].tupleCount)
	{  
		// Reset merge buffer index 
		utl->ResetBuffer(ssdMergePageBuffer[treeIndex]); // probeBuffer.currentTupleIndex = 1

		/* Current page is exhaused, get new page from input buffer */
		ssdReadBuffer[treeIndex].currentPageIndex++;  

		if(ssdReadBuffer[treeIndex].currentPageIndex >= ssdReadBuffer[treeIndex].pageCount)
		{       
			utl->ResetBuffer(ssdReadBuffer[treeIndex]);
			/* Input buffer exhaused, read next from SSD */
			ReadSsdFanIn(hSsdFile, ssdReadBuffer[treeIndex], overlapReadSsd[treeIndex]);  
			ssdReadBuffer[treeIndex].currentPageIndex = 0;
		}   

		utl->GetPageInfo(ssdReadBuffer[treeIndex].data, ssdMergePageBuffer[treeIndex], ssdReadBuffer[treeIndex].currentPageIndex, SSD_PAGE_SIZE); 
		ssdMergePageBuffer[treeIndex].currentTupleIndex=1;
	}  

	utl->GetTupleInfo(recordPtr, ssdMergePageBuffer[treeIndex].currentTupleIndex, ssdMergePageBuffer[treeIndex].data, SSD_PAGE_SIZE, m_CemsParams.KEY_POS);  
	ssdMergePageBuffer[treeIndex].currentTupleIndex++;

	//////////////////////////////////////////////////////////////////////////
	// Check current SSD fanIn is empty or not
	if(recordPtr->key==MAX)
	{
		// At this point, current SSD FanIn is empty
		// Delete empty file to take free space on SSD
		CloseHandle(hSsdFile);
		if(!DeleteFile(SsdFanInFile->fileName))  
		{
			ShowMB(L"Cannot delete file: %s", SsdFanInFile->fileName);
		}

		// Reset overlap struc
		overlapReadSsd[treeIndex]->dwBytesReadWritten = 0;
		overlapReadSsd[treeIndex]->chunkIndex = 0;
		overlapReadSsd[treeIndex]->startChunk = 0;
		overlapReadSsd[treeIndex]->endChunk = 0;
		overlapReadSsd[treeIndex]->totalChunk = 0;
		overlapReadSsd[treeIndex]->overlap.Offset = 0;
		overlapReadSsd[treeIndex]->overlap.OffsetHigh = 0;

		/* FanIn on SSD is over, copy new part from Hdd */ 
		utl->ResetBuffer(ssdReadBuffer[treeIndex] );
		utl->ResetBuffer(ssdMergePageBuffer[treeIndex]);
		//////////////////////////////////////////////////////////////////////////

		FANS *newSsdFanIn = new FANS();
		Phase2_CopyFileFromHddToSsd(hHddFile, newSsdFanIn, overlapCopyHddToSsd[treeIndex], ssdWriteBuffer, SsdRunSize);

		if(newSsdFanIn==NULL)
		{
			/* read from HDD done, SSD also done */ 
			utl->AddPageMAXToBuffer(ssdReadBuffer[treeIndex], SSD_PAGE_SIZE);  
			ssdReadBuffer[treeIndex].currentPageIndex = 0; 
			ssdReadBuffer[treeIndex].isFullMaxValue = TRUE;  /* Mark this buffer is end */
			ssdMergePageBuffer[treeIndex].currentTupleIndex = 1;
			utl->SetMaxTuple(recordPtr);   
		}
		else
		{
			/* Read new ssd run */ 
			/* Copy new fanIn info */
			wcscpy(SsdFanInFile->fileName, newSsdFanIn->fileName);
			SsdFanInFile->fileSize = newSsdFanIn->fileSize; //INPORTANCE
			SsdFanInFile->pageCount = newSsdFanIn->pageCount;
			SsdFanInFile->isDeleted = FALSE;
			SsdFanInFile->isFinal = newSsdFanIn->isFinal;

			/* Open ssd file for read */ 
			hSsdFile=CreateFile((LPCWSTR)SsdFanInFile->fileName,		// file to write
				GENERIC_READ,			// open for writing
				0,						// Do not share
				NULL,					// default security
				OPEN_EXISTING,			// Overwrite existing
				FILE_FLAG_NO_BUFFERING | FILE_FLAG_OVERLAPPED,	// overlapped operation 
				NULL);					// no attr. template

			if (INVALID_HANDLE_VALUE==hSsdFile) 
			{   
				ShowMB(L"Cannot open file: %s for READ", SsdFanInFile->fileName);
				return ERR_CANNOT_CREATE_HANDLE;
			}   

			/* Reset overlap structure to read from SSD fanIn*/  
			DWORD64 totalChunk = chROUNDUP(GetFanSize(hSsdFile), m_CemsParams.SSD_READ_BUFFER_SIZE) / m_CemsParams.SSD_READ_BUFFER_SIZE;
			overlapReadSsd[treeIndex]->chunkIndex = 0;
			overlapReadSsd[treeIndex]->startChunk = 0;
			overlapReadSsd[treeIndex]->endChunk = totalChunk;
			overlapReadSsd[treeIndex]->totalChunk = totalChunk; 

			/* Read FIRST record of new FanIn */ 
			ReadSsdFanIn(hSsdFile, ssdReadBuffer[treeIndex], overlapReadSsd[treeIndex]);

			/* Get the FIRST page in buffer */
			ssdReadBuffer[treeIndex].currentPageIndex = 0;
			ssdMergePageBuffer[treeIndex].currentTupleIndex = 1;
			utl->GetPageInfo(ssdReadBuffer[treeIndex].data, ssdMergePageBuffer[treeIndex], ssdReadBuffer[treeIndex].currentPageIndex, SSD_PAGE_SIZE); 

			/* Get the FIRST record */
			utl->GetTupleInfo(recordPtr, ssdMergePageBuffer[treeIndex].currentTupleIndex, ssdMergePageBuffer[treeIndex].data, SSD_PAGE_SIZE, m_CemsParams.KEY_POS);   
			ssdMergePageBuffer[treeIndex].currentTupleIndex++; /* Increase tuple index */ 
		}  
		////////////////////////////////////////////////////////////////////////// 
	} //if(recordPtr->key==MAX) 

	return SUCCESS;
}


/// <summary>
/// Gets the size of the fan-in.
/// </summary>
/// <param name="hFile">The h file.</param>
/// <returns></returns>
UINT64 CEMS::GetFanSize(HANDLE hFile)  
{
	LARGE_INTEGER liFileSize; 
	if (!GetFileSizeEx(hFile, &liFileSize))
	{      
		return 0;
	} 

	return liFileSize.QuadPart;
}


/// <summary>
/// Writes the SSD fan-in to HDD.
/// </summary>
/// <param name="hFile">The handle of file.</param>
/// <returns></returns>
RC CEMS::WriteSsdFanInToHdd(HANDLE hFile)
{   
	/* Flush buffer in memory to create ssd fanIns */
	/* Write from new offset */
	overlapWriteHdd->overlap.Offset = overlapWriteHdd->fileSize.LowPart;
	overlapWriteHdd->overlap.OffsetHigh = overlapWriteHdd->fileSize.HighPart;

	WriteFile(hFile, hddWriteBuffer.data, hddWriteBuffer.currentSize, &overlapWriteHdd->dwBytesReadWritten, &overlapWriteHdd->overlap); 	 

	/* Wait until write operation done*/
	GetOverlappedResult(hFile, &overlapWriteHdd->overlap, &overlapWriteHdd->dwBytesReadWritten, TRUE) ;  

	/* Update new position*/
	overlapWriteHdd->fileSize.QuadPart += overlapWriteHdd->dwBytesReadWritten;

	/* Reset buffer for next write */ 
	utl->ResetBuffer(hddWriteBuffer); 

	return SUCCESS;
}

/// <summary>
/// Reads the SSD fan in.
/// </summary>
/// <param name="hFile">The file handle.</param>
/// <param name="SsdReadBuffer">The SSD read buffer.</param>
/// <param name="overlapEx">The overlap ex.</param>
/// <returns></returns>
RC CEMS::ReadSsdFanIn(HANDLE hFile, Buffer &SsdReadBuffer, OVERLAPPEDEX *overlapEx)
{ 
	/* Read fanIn on SSD */ 
	utl->ResetBuffer(SsdReadBuffer);  
	if(overlapEx->chunkIndex >= overlapEx->totalChunk)
	{  
		utl->AddPageMAXToBuffer(SsdReadBuffer, SSD_PAGE_SIZE);  
		SsdReadBuffer.isFullMaxValue = TRUE;  /* Mark this buffer is end */
		return ERR_END_OF_FILE; 
	}

	/* Read next chunk */
	overlapEx->fileSize.QuadPart = overlapEx->chunkIndex * overlapEx->dwBytesToReadWrite;   
	overlapEx->overlap.Offset = overlapEx->fileSize.LowPart;
	overlapEx->overlap.OffsetHigh = overlapEx->fileSize.HighPart;  

	/* Execute async read */
	ReadFile(hFile, SsdReadBuffer.data, overlapEx->dwBytesToReadWrite, &overlapEx->dwBytesReadWritten, &overlapEx->overlap);    

	/* Wait until read operation done*/
	GetOverlappedResult(hFile, &overlapEx->overlap, &overlapEx->dwBytesReadWritten, TRUE) ;  

	/* Compute number page, tuples*/
	utl->ComputeBuffer(SsdReadBuffer, overlapEx->dwBytesReadWritten, SSD_PAGE_SIZE);

	overlapEx->chunkIndex++; 

	return SUCCESS;
}

/// <summary>
/// Get next tuple from buffer.
/// </summary>
/// <param name="hFile">The h file.</param>
/// <param name="overlapEx">The overlap ex.</param>
/// <param name="SsdReadBuffer">The SSD read buffer.</param>
/// <param name="SsdMergePageBuffer">The SSD merge page buffer.</param>
/// <param name="recordPtr">The record PTR.</param>
/// <returns></returns>
RC  CEMS::Phase1_GetNextTuple(HANDLE hFile, OVERLAPPEDEX *overlapEx, Buffer &SsdReadBuffer, Buffer &SsdMergePageBuffer, RECORD *recordPtr)
{   
	/* Get tuple from runs on SSD */  
	if(SsdMergePageBuffer.currentTupleIndex > SsdMergePageBuffer.tupleCount)
	{  
		// Reset merge buffer index 
		utl->ResetBuffer(SsdMergePageBuffer); // probeBuffer.currentTupleIndex = 1

		/* Current page is exhaused, get new page from input buffer */
		SsdReadBuffer.currentPageIndex++;  

		if(SsdReadBuffer.currentPageIndex >= SsdReadBuffer.pageCount)
		{       
			utl->ResetBuffer(SsdReadBuffer);
			/* Input buffer exhaused, read next from SSD */
			ReadSsdFanIn(hFile, SsdReadBuffer, overlapEx);  
			SsdReadBuffer.currentPageIndex = 0;
		}   

		utl->GetPageInfo(SsdReadBuffer.data, SsdMergePageBuffer, SsdReadBuffer.currentPageIndex, SSD_PAGE_SIZE); 
		SsdMergePageBuffer.currentTupleIndex=1;
	}  

	utl->GetTupleInfo(recordPtr, SsdMergePageBuffer.currentTupleIndex, SsdMergePageBuffer.data, SSD_PAGE_SIZE, m_CemsParams.KEY_POS);  
	SsdMergePageBuffer.currentTupleIndex++;

	return SUCCESS;
}

/// <summary>
/// Merge runs on SSD write to HDD.
/// </summary>
/// <returns></returns>
RC CEMS::Phase1_MergeRunsOnSsdWriteToHdd()
{  
	RC rc;
	/* Mearging all the runs on SSD to create new run on HDD */
	std::queue<LPWSTR> runWaitingDelete;

	/* Determine k-way merge */
	INT kWay = m_FanInSsd.size();
	if(kWay==1)
	{
		// This mean in queue has only one RUN, don't need to merge
		// Happen when the file to sort is already sorted or almost sorted
		// TODO: COPY file content to HDD
		ShowMB(L"Merge run on SSD error, only 1 run");	
		return SUCCESS;
	}

	RECORD *recordPtr = new RECORD();

	while (true) // begin multiple merge step
	{
		kWay = m_FanInSsd.size();
		// Must check for how many fan in can be merge in one step
		// If too many fanIn we need to merge in multiple step
		if(kWay > m_MaxFanInInOneStep)
		{
			kWay = m_MaxFanInInOneStep;
		}

		// Init variables
		//////////////////////////////////////////////////////////////////////////

		/* Init array files handle and overlap structure */ 
		HANDLE *hFanIn = new HANDLE[kWay];
		OVERLAPPEDEX **overlapFanInRead = new OVERLAPPEDEX*[kWay];
		DWORD chunkSize = m_CemsParams.SSD_READ_BUFFER_SIZE;

		// Create FanIn handle
		for (INT i=0; i<kWay; i++) 
		{ 
			/* Get the first item in queue */
			FANS *fanIn = m_FanInSsd.front();

			/* Remove first item from queue*/
			m_FanInSsd.pop();  

			/* Save file name and path for later delete */
			runWaitingDelete.push(fanIn->fileName);

			hFanIn[i]=CreateFile((LPCWSTR)fanIn->fileName,		// file to write
				GENERIC_READ,			// open for writing
				0,						// Do not share
				NULL,					// default security
				OPEN_EXISTING,			// Overwrite existing
				FILE_FLAG_NO_BUFFERING | FILE_FLAG_OVERLAPPED,	// overlapped operation 
				NULL);					// no attr. template

			if (INVALID_HANDLE_VALUE==hFanIn[i]) 
			{ 
				ShowMB(L"Cannot create output file %s", fanIn->fileName);  
				return ERR_CANNOT_CREATE_HANDLE; 
			}   

			/* Compute chunk part */
			DWORD64 totalChunk = chROUNDUP(fanIn->fileSize.QuadPart, chunkSize) / chunkSize ;

			overlapFanInRead[i] = new OVERLAPPEDEX(); 
			overlapFanInRead[i]->dwBytesToReadWrite = chunkSize;
			overlapFanInRead[i]->dwBytesReadWritten = 0;
			overlapFanInRead[i]->chunkIndex = 0;
			overlapFanInRead[i]->startChunk = 0;
			overlapFanInRead[i]->endChunk = totalChunk - 1;
			overlapFanInRead[i]->totalChunk = totalChunk;

			// Reset read buffer
			utl->ResetBuffer(ssdReadBuffer[i]);

			// Reset merge page buffer & merge page
			utl->ResetBuffer(ssdMergePageBuffer[i]);
			utl->ResetRunPage(ssdMergePage[i], ssdMergePageBuffer[i]);
		} // end FanIn handle

		//////////////////////////////////////////////////////////////////////////
		// Create OutPut variables
		//////////////////////////////////////////////////////////////////////////

		/* Set up overlap structure for write fanOut to HDD */
		overlapWriteHdd = new OVERLAPPEDEX();
		overlapWriteHdd->dwBytesToReadWrite = m_CemsParams.HDD_WRITE_BUFFER_SIZE;
		overlapWriteHdd->dwBytesReadWritten = 0;
		overlapWriteHdd->fileSize.QuadPart = 0;
		overlapWriteHdd->overlap.Offset = 0;
		overlapWriteHdd->overlap.OffsetHigh = 0;

		/* Init fanout name and handle for write to HDD */ 
		rc = CreateHddRun(); 

		/* Init HDD runpage*/
		utl->ResetBuffer(hddPageBuffer);
		utl->ResetRunPage(hddPage, hddPageBuffer);

		//////////////////////////////////////////////////////////////////////////

		/* Init loser tree kWay merge */
		LoserTree *lsTree = new LoserTree(kWay);  

		/* First read k files, then add to loser tree the 1st record */ 

		for (INT i=0; i<kWay; i++)
		{  
			/* Read fanIn from ssd to buffer and compute buffer */
			ReadSsdFanIn(hFanIn[i], ssdReadBuffer[i], overlapFanInRead[i]); 

			/* Reset probe buffer*/ 
			utl->ResetBuffer(ssdMergePageBuffer[i]); 

			/* Add the first page in read buffer to probe buffer*/
			utl->GetPageInfo(ssdReadBuffer[i].data, ssdMergePageBuffer[i], 0, SSD_PAGE_SIZE); //pageIndex = 0  

			Phase1_GetNextTuple(hFanIn[i], overlapFanInRead[i], ssdReadBuffer[i], ssdMergePageBuffer[i], recordPtr); 

			/* Add first tuple to loser tree */
			lsTree->AddNewNode(recordPtr, i);    
		}
		//////////////////////////////////////////////////////////////////////////

		/* Create loser tree */
		lsTree->CreateLoserTree();   

		//////////////////////////////////////////////////////////////////////////


		/* Init counter of fanIn on HDD  */
		DWORD64 totalPage = 0;
		DWORD64 totalTuple = 0;
		DWORD64 lowestKey = 0;
		DWORD64 highestKey = 0; 
		BOOL lowestIsGet = FALSE;
		INT treeIndex; /* index in loser tree, point the min record */
		hddPage->consumed = FALSE;
		utl->ResetBuffer(hddWriteBuffer);

		while( TRUE ) 
		{
			/* Get record has key is minimum, and index in tree of that record */
			lsTree->GetMinRecord( recordPtr, treeIndex );

			/* Finish loser tree */
			if(recordPtr->key==MAX)  { break; } 

			/* Tracking fanOut */
			if(lowestIsGet==FALSE)
			{
				lowestKey = recordPtr->key;
				lowestIsGet = TRUE;
			} 
			highestKey = recordPtr->key;

			/* Output minimum record to buffer */ 
			if(utl->IsBufferFull( hddWriteBuffer )) // write buffer is full
			{    
				WriteSsdFanInToHdd(hHddRunFile);  
			} 

			hddPage->consumed = FALSE; 

			if(utl->IsPageFull(hddPage))
			{  
				utl->AddPageToBuffer(hddWriteBuffer,  hddPage->page, HDD_PAGE_SIZE);   
				hddPage->consumed = TRUE; /* Mark is page is used */
				hddWriteBuffer.pageCount++;
				totalPage++;
				// Reset page về ban đầu, bắt đầu ghép 1 page mới  
				utl->ResetRunPage(hddPage, hddPageBuffer);  
			}  

			/* Add output tuple to buffer */
			utl->AddTupleToPage(hddPage, recordPtr->data, hddPageBuffer);  
			hddWriteBuffer.tupleCount++;
			totalTuple++;

			/* Get next record from buffer */
			Phase1_GetNextTuple(hFanIn[treeIndex], overlapFanInRead[treeIndex],  ssdReadBuffer[treeIndex], ssdMergePageBuffer[treeIndex], recordPtr);  

			/* Add new record to loser tree at treeNode[index] */
			lsTree->AddNewNode( recordPtr, treeIndex );  

			/* Shrink loser tree */
			lsTree->Adjust( treeIndex ); 
		} // end loser tree

		// If the last page has not consumed
		if((hddPage->consumed==FALSE) && (utl->IsBufferFull(hddWriteBuffer)==FALSE) )
		{ 
			if(!utl->IsEmptyPage(hddPage))
			{
				utl->AddPageToBuffer( hddWriteBuffer, hddPage->page, HDD_PAGE_SIZE ); 
				hddPage->consumed = TRUE;   
				hddWriteBuffer.pageCount++; 
				totalPage++;
			}
		}

		/* Push all data in memory to disk */
		if(hddWriteBuffer.currentSize > 0)
		{
			rc = WriteSsdFanInToHdd(hHddRunFile);   
		}

		//////////////////////////////////////////////////////////////////////////

		/* Save current HDD fanIn */ 
		SaveHddFanIn(lowestKey, highestKey, totalTuple, totalPage, overlapWriteHdd->fileSize.QuadPart);

		/* Cleanning */
		CloseHandle(hHddRunFile);   // Close output file

		for(INT i = 0; i < kWay; ++i)  // Close input file
		{   
			CloseHandle(hFanIn[i]);   
		} 

		/* delete fanIn on SSD, get storage back */
		while(runWaitingDelete.size() > 0)
		{ 
			DeleteFile(runWaitingDelete.front());
			runWaitingDelete.pop(); 
		}

		delete lsTree;

		//////////////////////////////////////////////////////////////////////////
		if(m_FanInSsd.size()==0)
		{
			break; // no fanIn in queue
		}
		//////////////////////////////////////////////////////////////////////////
	} // end multiple merge step

	delete recordPtr;


	//////////////////////////////////////////////////////////////////////////
	return SUCCESS;
}

/// <summary>
/// Gets the SSD fan-in path.
/// </summary>
/// <param name="fanInPath">The fan in path.</param>
/// <returns></returns>
RC CEMS::GetSsdFanInPath(LPWSTR &fanInPath)
{      
	swprintf(fanInPath, 255, L"%s%d_%s.dat", m_CemsParams.SSD_WORK_SPACE_PATH, m_FanIndex, m_CemsParams.FILE_NAME_NO_EXT);   
	InterlockedExchangeAdd(&m_FanIndex, 1);   
	return SUCCESS; 
} 

/// <summary>
/// Gets the HDD fan-in path.
/// </summary>
/// <param name="fanInPath">The fan in path.</param>
/// <returns></returns>
RC CEMS::GetHddFanInPath(LPWSTR &fanInPath)
{      
	swprintf(fanInPath, 255, L"%s%d_%s.dat", m_CemsParams.HDD_WORK_SPACE_PATH, m_FanIndex, m_CemsParams.FILE_NAME_NO_EXT);    
	InterlockedExchangeAdd(&m_FanIndex, 1);   
	return SUCCESS; 
} 

INT CEMS::TreeParent(INT i)
{
	return floor(i/2); 
}

INT  CEMS::TreeLeft(INT i)
{
	return   2 * i + 1; // 2 * i;  
} 

INT CEMS::TreeRight (INT i)
{
	return  2 * i + 2;  
} 

RC CEMS::MinHeapify(RECORD **rec, INT i, INT heapSize)
{		
	int left=TreeLeft(i);
	int right=TreeRight(i);
	int minimum=i;

	if((left<=heapSize) &&  (rec[left]->key < rec[i]->key) )
		minimum=left; 

	if((right<=heapSize) && (rec[right]->key < rec[minimum]->key) )
		minimum=right; 

	if(minimum!=i)
	{
		std::swap(rec[i], rec[minimum]);  
		MinHeapify(rec, minimum, heapSize); 
	} 

	return SUCCESS;
}

RC CEMS::MaxHeapify(RECORD **rec, INT i, INT heapSize)
{		
	int left=TreeLeft(i);
	int right=TreeRight(i);
	int largest=i;

	if(left<=heapSize && rec[left]->key > rec[i]->key)
		largest=left; 

	if(right<=heapSize && rec[right]->key > rec[left]->key)
		largest=right;

	if(largest!=i)
	{
		std::swap(rec[i], rec[largest]);  
		MaxHeapify(rec, largest, heapSize); 
	} 

	return SUCCESS;
}

RC CEMS::BuildMinHeap(RECORD **rec, INT heapSize)
{  
	for(int i=heapSize/2; i>=0; i--)
		MinHeapify(rec, i, heapSize);	// array[0] is the largest item 

	return SUCCESS;
}

RC CEMS::BuildMaxHeap(RECORD **rec, INT heapSize)
{  
	for(int i=heapSize/2; i>=0; i--)
		MaxHeapify(rec, i, heapSize); // array[0] is the smallest item

	return SUCCESS;
}