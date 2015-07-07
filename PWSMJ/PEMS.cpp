// 
// Name: PEMS.cpp : implementation file 
// Author: hieunt
// Description: Parallel external merge-sort implementation
//              In-memory sorting use quick sort to create mini runs in memory, then 
//              use replacement selection to merge these in memory minirun,
//              run size is equal NumberOfReadBuffer * ReadBufferSize
//              Merge Phase is merge all Runs created, 
//              if not enough memory, multiple merge step is needed
//              I/O and CPU computation all are overlapped
//

#include "stdafx.h"
#include "PEMS.h"

// Global variables
extern Loggers g_Logger;
 
/// <summary>
/// Initializes a new instance of the <see cref="PEMS"/> class.
/// </summary>
/// <param name="vParams">The PEMS parameters.</param>
PEMS::PEMS(const PEMS_PARAMS vParams) : m_Params(vParams) 
{
	m_PartitionThreadNum = vParams.THREAD_NUM; 
	// If thread num is zero, set thread num equal max logical core
	SYSTEM_INFO sysinfo;
	GetSystemInfo(&sysinfo);
	if(vParams.THREAD_NUM==0)
		m_PartitionThreadNum = sysinfo.dwNumberOfProcessors; 

	utl = new PageHelpers2(); 

	m_FanInIndex = 0;    
	m_FanOutIndex = 0;  
	m_MaxFanIn = 0; 
}

/// <summary>
/// Finalizes an instance of the <see cref="PEMS"/> class.
/// </summary>
PEMS::~PEMS()
{ 

}

/// <summary>
/// Executes this PEMS instance.
/// </summary>
/// <returns></returns>
RC PEMS::Execute()
{
	RC rc;
	//////////////////////////////////////////////////////////////////////////

	StopWatch stwTotalTime, stwMergeTime, stwPartitionTime;
	rp.BufferPoolSize = m_Params.BUFFER_POOL_SIZE;
	rp.SortReadBufferSize = m_Params.SORT_READ_BUFFER_SIZE;
	rp.SortWriteBufferSize = m_Params.SORT_WRITE_BUFFER_SIZE;
	rp.MergeReadBufferSize = m_Params.MERGE_READ_BUFFER_SIZE;
	rp.MergeWriteBufferSize = m_Params.MERGE_WRITE_BUFFER_SIZE;
	rp.MergePass = 1;
	stwTotalTime.Start();
	stwPartitionTime.Start();
	rp.CpuTime[0] = GetCpuTime(); 

	//////////////////////////////////////////////////////////////////////////
	rc = PartitionPhase_CheckEnoughMemory();
	if(rc != SUCCESS) {return rc; }

	g_Logger.Write("PEMS Start...\n");

	bufferPool.size = m_Params.BUFFER_POOL_SIZE;
	bufferPool.currentSize = 0;

	// Creates a private heap object that can be used by the calling process. 
	// The function reserves space in the virtual address space of the process and allocates physical storage 
	// for a specified initial portion of this block.
	// C++ cannot allocate more than 400MB with new[] in MFC
	// http://msdn.microsoft.com/en-us/library/aa366599.aspx
	HANDLE hHeap = HeapCreate(0, 0, 0);
	bufferPool.data  = (CHAR*)HeapAlloc(hHeap, 0, bufferPool.size);

	if(NOERROR != GetLastError())
	{
		ReportLastError();
		HeapDestroy(hHeap);
		return ERR_CANNOT_INITIAL_BUFFER_POOL;
	}

	rc = PartitionPhase_Execute();
	if(rc != SUCCESS) {return rc; }
	//////////////////////////////////////////////////////////////////////////

	rc = MergePhase_CheckEnoughMemory();
	if(rc != SUCCESS) {return rc; }

	rp.PartitionNum = m_FanIns.size();
	rp.PartitionTime = stwPartitionTime.NowInMilliseconds();
	stwMergeTime.Start();

	bufferPool.currentSize = 0;

	rc = MergePhase_Execute();
	if(rc != SUCCESS) {return rc; }

	//////////////////////////////////////////////////////////////////////////
	rp.CpuTime[1] = GetCpuTime(); 
	rp.TotalTime = stwTotalTime.NowInMilliseconds();
	rp.MergeTime = stwMergeTime.NowInMilliseconds();

	FILE *fp;   
	CHAR *reportFilePath = new CHAR[MAX_PATH];   
	LPWSTR tempReportPath = new TCHAR[MAX_PATH];
	swprintf(tempReportPath, MAX_PATH, L"%s%s", m_Params.WORK_SPACE_PATH, L"PEMS_Report.csv" ); 
	// convert file path to char  
	size_t  count = wcstombs(reportFilePath, tempReportPath, MAX_PATH); 

	CHAR *reportTitle = "Relation Size,Memory Size,Run Size,Partition Num,Merge Pass,Read Buffer Size(sort),Write Buffer Size(sort),Read Buffer Size(merge),Write Buffer Size(merge),Total Execute Time(ms),Partition Time(ms),Merge Time(ms),CPU Time\n";
	CHAR *reportContent = new CHAR[1024];
	sprintf(reportContent, "%lld,%d,%d,%d,%d,%d,%d,%d,%d,%lld,%lld,%lld,%.f", 
		rp.SourceTableSize, 
		rp.BufferPoolSize, 
		rp.RunSize,
		rp.PartitionNum, 
		rp.MergePass, 
		rp.SortReadBufferSize,
		rp.SortWriteBufferSize, 
		rp.MergeReadBufferSize,
		rp.MergeWriteBufferSize, 
		rp.TotalTime, 
		rp.PartitionTime, 
		rp.MergeTime, 
		rp.CpuTime[1] - rp.CpuTime[0]);

	fp=fopen(reportFilePath, "w+b"); 
	fprintf(fp, reportTitle);
	fprintf(fp, reportContent);
	fclose(fp);

	delete reportFilePath;
	delete tempReportPath; 
	delete reportContent;

	//////////////////////////////////////////////////////////////////////////
	CloseHandle(m_hTableFile);

	BOOL bRet = HeapFree(hHeap, 0, bufferPool.data);
	bRet = HeapDestroy(hHeap);

	delete utl;
	//////////////////////////////////////////////////////////////////////////
	g_Logger.Write("PEMS Stop\n");
	//////////////////////////////////////////////////////////////////////////
	return SUCCESS;
}

/// <summary>
/// Check enough memory for sort phase.
/// </summary>
/// <returns></returns>
RC PEMS::PartitionPhase_CheckEnoughMemory()
{
	// Estimate memory 
	DOUBLE m_TotalMemory = m_Params.BUFFER_POOL_SIZE; 
	m_MemoryEachThreadSize = m_TotalMemory / m_PartitionThreadNum;

	// Caculate ouput sort buffer
	DOUBLE memoryForWrite = 0;
	DOUBLE memoryForRead = 0;
	DOUBLE memoryForMerge = 0;
	DOUBLE memoryForQuickSort = 0;
	DWORD maxQuickSortItem = (m_Params.SORT_READ_BUFFER_SIZE / SSD_PAGE_SIZE) * MAXIMUM_TUPLE_IN_ONE_PAGE; // The maximum tuple for quick sort 
	memoryForQuickSort = (maxQuickSortItem * (sizeof(UINT64) + TUPLE_SIZE * sizeof(CHAR))) + SSD_PAGE_SIZE;
	memoryForWrite = m_Params.SORT_WRITE_BUFFER_SIZE * 2;
	memoryForRead = m_Params.SORT_READ_BUFFER_SIZE * 2;

	m_MemoryEachThreadSize = m_MemoryEachThreadSize - (memoryForWrite + memoryForQuickSort);

	if(m_MemoryEachThreadSize <= 0)
	{
		ShowMB(L"Not enough memory\nSort Write Buffer is too big");
		return ERR_NOT_ENOUGH_MEMORY; 
	}

	// (LoserKey + LoserTreeData + MergeBuffer) 
	memoryForMerge = sizeof(UINT64) + (TUPLE_SIZE * sizeof(CHAR)) + SSD_PAGE_SIZE;
	// Calculate inputBuffer num with current size   
	m_InputBufferNum =chROUNDDOWN(m_MemoryEachThreadSize,  (memoryForRead + memoryForMerge))/(memoryForRead + memoryForMerge) ;
	rp.RunSize = m_Params.SORT_READ_BUFFER_SIZE * m_InputBufferNum;

	if( m_InputBufferNum <= 1)
	{
		ShowMB(L"Number of miniruns in memory too small, Execute will not efficient. Add more memory.");
		return ERR_NOT_ENOUGH_MEMORY; 
	}

	m_MemoryEachThreadSize = m_MemoryEachThreadSize - (m_InputBufferNum * (memoryForRead + memoryForMerge));

	if( m_MemoryEachThreadSize <= 0 )
	{ 
		ShowMB(L"Not enough memory for %d thread\nIncrease pool size or choose other buffer size");
		return ERR_NOT_ENOUGH_MEMORY; 
	}   
	 
	return SUCCESS; 
}

/// <summary>
/// Wrapper for partitions thread.
/// </summary>
/// <param name="lpParam">The thread parameter.</param>
/// <returns></returns>
DWORD WINAPI PEMS::PartitionPhaseEx(LPVOID lpParam)
{
	PartitionThreadParams* p = (PartitionThreadParams*)(lpParam);
	p->_this->PartitionPhase((void *)(p));
	return 0;
}


/// <summary>
/// Partitions thread function.
/// </summary>
/// <param name="lpParam">The thread parameter.</param>
/// <returns></returns>
DWORD WINAPI PEMS::PartitionPhase(LPVOID lpParam)
{ 
	PartitionThreadParams *p = (PartitionThreadParams *)lpParam;
	DoubleBuffer *dbcWrite = p->dbcWrite;    

	RC rc;

	DWORD inputBufferCount = p->inputBufferCount; 

	p->inputBufferIndex = 0;

	// 1st read into BACK input buffer, then swap BACK to FRONT
	for(DWORD i=0; i<inputBufferCount; i++)
	{
		p->dbcRead[i]->LockProducer();

		rc = PartitionPhase_Read(p, i);   

		// Wait for Read complete  
		if(rc == SUCCESS)
		{
			GetOverlappedResult(p->hInputFile, &p->overlapRead.overlap, &p->overlapRead.dwBytesReadWritten, TRUE);   
			utl->ComputeBuffer(BACK_BUFFER(p->dbcRead[i]), p->overlapRead.dwBytesReadWritten);
		}
		p->inputBufferIndex++;  
		p->overlapRead.chunkIndex++;

		p->dbcRead[i]->UnLockProducer(); 
		p->dbcRead[i]->SwapBuffers();   

		if(p->dbcRead[i]->bFirstProduce==TRUE)
			p->dbcRead[i]->bFirstProduce=FALSE;
	}

	// Reset buffer index
	p->inputBufferIndex = 0; 

	BOOL bContinue  = TRUE;

	DWORD fanIn = inputBufferCount; 
	DWORD chunkMerged = 0;

	// Produce into BACK output buffer
	while(bContinue)
	{   
		// prefetch read, sort FRONT input buffer
		for(DWORD i=0; i<inputBufferCount; i++)
		{ 
			p->dbcRead[i]->LockProducer();
			rc = PartitionPhase_Read(p, i); // read + sort FRONT, current BACK is not sort  

			PartitionPhase_Sort(p, i);

			if(rc==SUCCESS) // Wait for Read complete 
			{  
				GetOverlappedResult(p->hInputFile, &p->overlapRead.overlap, &p->overlapRead.dwBytesReadWritten, TRUE);   
				utl->ComputeBuffer(BACK_BUFFER(p->dbcRead[i]),  p->overlapRead.dwBytesReadWritten); 
			}
			p->inputBufferIndex++;
			p->overlapRead.chunkIndex++;
			p->dbcRead[i]->UnLockProducer(); 
		}  

		PartitionPhase_Merge(p); // in-memory merg, merge k input-buffer

		chunkMerged += fanIn; 

		for(DWORD i = 0; i < inputBufferCount; i++) 
		{ 
			p->dbcRead[i]->SwapBuffers();   
		}  

		p->inputBufferIndex = 0; 

		if(chunkMerged > p->overlapRead.totalChunk)
		{
			bContinue = FALSE; 
		} 
	}    //  while bContinue  

	//if(m_Params.USE_SYNC_READ_WRITE_MODE==TRUE)
	//{
	//	sync.bIsDone[p->threadID] = TRUE; 
	//	SetEvent(sync.hReady[p->threadID]);
	//} 

	//if(m_Params.USE_PARALLEL_MERGE==TRUE)
	//{ 
	//	std::queue<FANS*> fanIns;
	//	for(UINT i=0; i<p->fanIns.size();i++)
	//	{
	//		FANS *fan = new FANS();
	//		fan = p->fanIns[i];
	//		fanIns.push(fan);
	//	}
	//	//
	//	ExsMergePhase mergePhase(m_Params, fanIns, m_MemoryEachThreadSize); /// bo nho cap phat la bao nhiu???  
	//	if(SUCCESS==mergePhase.MergePhase_CheckEnoughMemory())
	//	{ 
	//		mergePhase.MergePhase_Execute();

	//		FANS*  finalFan = mergePhase.GetFinalFanOut();
	//		if(finalFan!=NULL)
	//		{ 
	//			m_Mutex.Lock();
	//			m_ReturnFanIns.push(finalFan);
	//			m_Mutex.UnLock(); 
	//		}
	//	} 
	//	return 0;
	//}

	//// Cleaning, release memory 
	m_Mutex.Lock();
	while(p->fanIns.size() > 0)
	{
		FANS *fan = new FANS();
		fan = p->fanIns.back();
		p->fanIns.pop_back();

		m_FanIns.push(fan);  
	} 
	m_Mutex.UnLock(); 

	return 0;
}


/// <summary>
/// Initialize sort phase.
/// </summary>
/// <returns></returns>
RC PEMS::PartitionPhase_Initialize()
{    
	RC rc;  
	 
	m_hPartitionThread = new HANDLE[m_PartitionThreadNum];
	m_PartitionParams = new PartitionThreadParams[m_PartitionThreadNum]; 


	//if(m_Params.USE_SYNC_READ_WRITE_MODE)
	//{
	//	m_MonitorParams = new MonitorParams();
	//	m_MonitorParams->ClassPointer = this; /// IMPORTANCE

	//	sync.bQuit = FALSE;
	//	sync.dwThreadNum = m_PartitionThreadNum;
	//	sync.hReady = new HANDLE[m_PartitionThreadNum];
	//	sync.bIsDone = new BOOL[m_PartitionThreadNum]; // which thread is done their work?
	//	sync.hWaitAllOk = CreateEvent( NULL, TRUE, FALSE, NULL); // Manual reset, nonsignaled 
	//	for(DWORD i=0; i < m_PartitionThreadNum; i++)
	//	{ 
	//		sync.hReady[i] = CreateEvent( NULL, FALSE, FALSE, NULL); // Auto reset, nonsignaled
	//		sync.bIsDone[i] = FALSE;
	//	}
	//}

	m_hTableFile=CreateFile(
		(LPCWSTR)m_Params.SORT_FILE_PATH, // file to open
		GENERIC_READ,			// open for reading
		FILE_SHARE_READ,        // share for reading
		NULL,					// default security
		OPEN_EXISTING,			// existing file only
		FILE_FLAG_OVERLAPPED | FILE_FLAG_NO_BUFFERING,	// overlapped operation //| FILE_FLAG_NO_BUFFERING
		NULL);					// no attr. template

	if (INVALID_HANDLE_VALUE==m_hTableFile) 
	{   
		ShowMB(L"Cannot open file %s\r\n", m_Params.SORT_FILE_PATH);
		return ERR_CANNOT_CREATE_HANDLE; 
	} 

	LARGE_INTEGER  liSourceFileSize = {0}; 

	if (!GetFileSizeEx(m_hTableFile, &liSourceFileSize))
	{       
		ShowMB(L"Cannot get size of file %s\r\n", m_Params.SORT_FILE_PATH);
		return ERR_CANNOT_GET_FILE_SIZE;
	} 

	rp.SourceTableSize = liSourceFileSize.QuadPart;

	DWORD chunkSize = m_Params.SORT_READ_BUFFER_SIZE; 
	UINT64 totalChunk = chROUNDUP(liSourceFileSize.QuadPart, chunkSize) / chunkSize;

	// Init default value
	for (DWORD i = 0; i < m_PartitionThreadNum; i++)
	{
		m_PartitionParams[i]._this = this; ///////////// Importance
		m_PartitionParams[i].threadID = i; 
		m_PartitionParams[i].keyPosition = m_Params.KEY_POS;  
		m_PartitionParams[i].tempFanIn = new FANS(); 
		m_PartitionParams[i].hInputFile = m_hTableFile; 

		m_PartitionParams[i].inputBufferCount = m_InputBufferNum;

		// để xem đang đọc từ cái buffer nào
		m_PartitionParams[i].inputBufferIndex = 0;     

		m_PartitionParams[i].dbcRead = new DoubleBuffer*[m_InputBufferNum];  
		for (DWORD j=0; j<m_InputBufferNum; j++)
		{
			// Front input buffer
			m_PartitionParams[i].dbcRead[j] = new DoubleBuffer(m_Params.SORT_READ_BUFFER_SIZE); 
			rc = utl->InitBuffer(m_PartitionParams[i].dbcRead[j]->buffer[0], m_Params.SORT_READ_BUFFER_SIZE, &bufferPool);
			if(rc!=SUCCESS) {return rc;}
			rc = utl->InitBuffer(m_PartitionParams[i].dbcRead[j]->buffer[1], m_Params.SORT_READ_BUFFER_SIZE, &bufferPool);  
			if(rc!=SUCCESS) {return rc;}
		} 

		////////////////////////////////////////////////////////////////////////// 
		m_PartitionParams[i].dbcWrite = new DoubleBuffer(m_Params.SORT_WRITE_BUFFER_SIZE);    
		rc = utl->InitBuffer(m_PartitionParams[i].dbcWrite->buffer[0], m_Params.SORT_WRITE_BUFFER_SIZE, &bufferPool);
		if(rc!=SUCCESS) {return rc;}

		rc = utl->InitBuffer(m_PartitionParams[i].dbcWrite->buffer[1], m_Params.SORT_WRITE_BUFFER_SIZE, &bufferPool); 
		if(rc!=SUCCESS) {return rc;}
		//////////////////////////////////////////////////////////////////////////

		// Init run buffer
		rc = utl->InitBuffer(m_PartitionParams[i].runPageBuffer, SSD_PAGE_SIZE, &bufferPool); 
		if(rc!=SUCCESS) {return rc;}

		//////////////////////////////////////////////////////////////////////////
		// Init runpage 
		rc = utl->InitRunPage(m_PartitionParams[i].runPage, m_PartitionParams[i].runPageBuffer); 
		if(rc!=SUCCESS) {return rc;}
		//////////////////////////////////////////////////////////////////////////
		//Working space buffer 
		//////////////////////////////////////////////////////////////////////////

		DWORD maxQuickSortItem = (m_Params.SORT_READ_BUFFER_SIZE / 4096) * 40;
		DWORD sortBufferSize = maxQuickSortItem * sizeof(UINT64);

		// Quick sort page buffer
		rc = utl->InitBuffer(m_PartitionParams[i].quickSortPageBuffer, SSD_PAGE_SIZE, &bufferPool);  
		if(rc!=SUCCESS) {return rc;}

		rc = utl->InitBuffer(m_PartitionParams[i].quickSortDataBuffer, maxQuickSortItem * TUPLE_SIZE * sizeof(CHAR), &bufferPool);
		if(rc!=SUCCESS) {return rc;}
		// Quick sort data buffer  
		m_PartitionParams[i].quickSort = new QuickSort(maxQuickSortItem, m_PartitionParams[i].quickSortDataBuffer);

		// Init in memory merge buffer
		m_PartitionParams[i].memoryMergeBuffer = new Buffer[m_InputBufferNum];
		for (DWORD j = 0; j < m_InputBufferNum; j++)
		{
			rc = utl->InitBuffer(m_PartitionParams[i].memoryMergeBuffer[j], SSD_PAGE_SIZE, &bufferPool); 
			if(rc!=SUCCESS) {return rc;}
		}

		m_PartitionParams[i].overlapRead.dwBytesToReadWrite = m_Params.SORT_READ_BUFFER_SIZE;  
		m_PartitionParams[i].overlapRead.dwBytesReadWritten = 0;  
		m_PartitionParams[i].overlapRead.startChunk = 0;  
		m_PartitionParams[i].overlapRead.chunkIndex = 0;  
		m_PartitionParams[i].overlapRead.endChunk = 0;  
		m_PartitionParams[i].overlapRead.totalChunk = 0; 
		m_PartitionParams[i].overlapRead.overlap.Offset = 0; 
		m_PartitionParams[i].overlapRead.overlap.OffsetHigh = 0; 

		m_PartitionParams[i].overlapWrite.dwBytesToReadWrite = m_Params.SORT_WRITE_BUFFER_SIZE; 
		m_PartitionParams[i].overlapWrite.dwBytesReadWritten = 0; 
		m_PartitionParams[i].overlapWrite.overlap.Offset = 0;
		m_PartitionParams[i].overlapWrite.overlap.OffsetHigh = 0;
	} 

	UINT64 temp = totalChunk;
	while (temp > 0)
	{
		for (DWORD i = 0; i < m_PartitionThreadNum; i++)
		{
			m_PartitionParams[i].overlapRead.totalChunk++;
			temp--;
			if(temp==0) { break; }
		}  
	}
	temp = totalChunk;
	DWORD startChunk = 0;
	while (temp > 0)
	{
		for (DWORD i = 0; i < m_PartitionThreadNum; i++)
		{ 
			m_PartitionParams[i].overlapRead.startChunk = startChunk; 
			m_PartitionParams[i].overlapRead.chunkIndex = startChunk; 
			startChunk += m_PartitionParams[i].overlapRead.totalChunk;
			m_PartitionParams[i].overlapRead.endChunk = startChunk - 1;  
			temp -= m_PartitionParams[i].overlapRead.totalChunk;
			if(temp==0) { break; }
		}  	
	}

	return SUCCESS;
}


/// <summary>
/// Execute sort phase.
/// </summary>
/// <returns></returns>
RC PEMS::PartitionPhase_Execute()
{ 
	if(SUCCESS!=PartitionPhase_Initialize())
	{ 
		ShowMB(L"Cannot Initialize ExsPartitionPhase");
		return ERR_CANNOT_INITIAL_MEMORY;
	}

	//if(m_Params.USE_SYNC_READ_WRITE_MODE==TRUE)
	//{ 
	//	m_hMonitorThread = CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)MonitorEx, (LPVOID)&(m_MonitorParams[0]), CREATE_SUSPENDED, NULL);
	//	//
	//	SetThreadPriority(m_hMonitorThread, THREAD_PRIORITY_NORMAL);
	//	ResumeThread(m_hMonitorThread);  
	//}

	for(DWORD i = 0; i < m_PartitionThreadNum; i++ )
	{  
		m_hPartitionThread[i] = CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)PartitionPhaseEx, (LPVOID)&(m_PartitionParams[i]), CREATE_SUSPENDED, NULL);

		SetThreadPriority(m_hPartitionThread[i], THREAD_PRIORITY_NORMAL);
		ResumeThread(m_hPartitionThread[i]); 
	}

	// Must wait for all worker finish their jobs
	WaitForMultipleObjects(m_PartitionThreadNum, m_hPartitionThread, TRUE, INFINITE); 

	//if(m_Params.USE_SYNC_READ_WRITE_MODE==TRUE)
	//{
	//	// Wait for monitor thread exit
	//	for(DWORD i = 0; i < m_PartitionThreadNum; i++ )
	//	{
	//		sync.bIsDone[i] = TRUE;
	//		SetEvent(sync.hReady[i]);
	//	}  
	//	//
	//	WaitForSingleObject(m_hMonitorThread, INFINITE);
	//}

	for(DWORD i = 0; i < m_PartitionThreadNum; i++ )
	{
		CloseHandle(m_hPartitionThread[i]);
	}

	//if(m_Params.USE_SYNC_READ_WRITE_MODE==TRUE)
	//{ 
	//	CloseHandle(m_hMonitorThread);
	//}

	return SUCCESS;
}

/// <summary>
/// Get fan-out path.
/// </summary>
/// <param name="fanOutName">Name of the fan-out.</param>
/// <param name="threadID">The thread identifier.</param>
/// <returns></returns>
RC PEMS::MergePhase_GetFanOutPath(LPWSTR &fanOutName, INT threadID)  
{    
	swprintf(fanOutName, MAX_PATH, L"%s%d_%d_%s_merge.dat", m_Params.WORK_SPACE_PATH, threadID, m_FanOutIndex, m_Params.FILE_NAME_NO_EXT);  
	InterlockedExchangeAdd(&m_FanOutIndex, 1);
	return SUCCESS;
} 


/// <summary>
/// Read from disk to buffer.
/// </summary>
/// <param name="lpParam">The thread parameter.</param>
/// <param name="bufferIndex">Index of the buffer.</param>
/// <returns></returns>
RC PEMS::PartitionPhase_Read(LPVOID lpParam, DWORD bufferIndex)
{
	PartitionThreadParams *p = (PartitionThreadParams *)lpParam;

	if ( p->overlapRead.chunkIndex > p->overlapRead.endChunk )  
	{   
		utl->ResetBuffer(BACK_BUFFER(p->dbcRead[bufferIndex]));
		// Add max page to buffer
		utl->AddPageToBuffer(BACK_BUFFER(p->dbcRead[bufferIndex]), NULL, SSD_PAGE_SIZE); 

		BACK_BUFFER(p->dbcRead[bufferIndex]).currentPageIndex = 0;
		BACK_BUFFER(p->dbcRead[bufferIndex]).currentSize = SSD_PAGE_SIZE;
		BACK_BUFFER(p->dbcRead[bufferIndex]).pageCount = 1;
		BACK_BUFFER(p->dbcRead[bufferIndex]).tupleCount = utl->GetTupleNumInMaxPage(); 
		BACK_BUFFER(p->dbcRead[bufferIndex]).isSort = TRUE;
		BACK_BUFFER(p->dbcRead[bufferIndex]).isFullMaxValue = TRUE;
		return ERR_END_OF_FILE;
	}  

	p->overlapRead.fileSize.QuadPart = p->overlapRead.chunkIndex * m_Params.SORT_READ_BUFFER_SIZE; 
	p->overlapRead.overlap.Offset = p->overlapRead.fileSize.LowPart;
	p->overlapRead.overlap.OffsetHigh = p->overlapRead.fileSize.HighPart;  

	// Attempt an asynchronous read operation. 
	ReadFile(p->hInputFile, BACK_BUFFER(p->dbcRead[bufferIndex]).data, p->overlapRead.dwBytesToReadWrite, &p->overlapRead.dwBytesReadWritten, &p->overlapRead.overlap);   

	return SUCCESS;
}

 
/// <summary>
/// Merge mini-runs in memory.
/// </summary>
/// <param name="lpParam">The thread parameter.</param>
/// <returns></returns>
RC PEMS::PartitionPhase_Merge(LPVOID lpParam)
{ 
	PartitionThreadParams *p = (PartitionThreadParams *)lpParam;
	DoubleBuffer *dbcWrite = p->dbcWrite;
	DoubleBuffer **dbcRead = p->dbcRead;

	DWORD k = p->inputBufferCount; 
	LoserTree ls(k);
	RECORD *record = new RECORD();  

	for(DWORD i = 0; i < k; i++) 
	{     
		FRONT_BUFFER(dbcRead[i]).currentPageIndex = 0;    
		utl->ResetBuffer(p->memoryMergeBuffer[i]); 
		utl->GetPageInfo(FRONT_BUFFER(dbcRead[i]).data, p->memoryMergeBuffer[i], 0, SSD_PAGE_SIZE); //pageIndex = 0  
		PartitionPhase_GetNextTuple(p, record, i); 
		ls.AddNewNode(record, i);    
	}    

	ls.CreateLoserTree();  

	dbcWrite->bFirstProduce = TRUE;
	utl->ResetBuffer(BACK_BUFFER(dbcWrite));
	utl->ResetBuffer(FRONT_BUFFER(dbcWrite));

	utl->ResetRunPage(p->runPage, p->runPageBuffer);  

	dbcWrite->LockProducer();

	DWORD tupleCount = 0;
	INT lsIndex = 0; // index in loser tree point to minimum record 
	BOOL bFirstWrite = TRUE;
	BOOL bLowestIsGet = FALSE;  

	PartitionPhase_CreateNewRun(p);

	while(TRUE)  
	{   
		ls.GetMinRecord(record, lsIndex); // index = loserTree[0]

		if(record->key==MAX) { break; }

		if(bLowestIsGet==FALSE)
		{
			p->tempFanIn->lowestKey = record->key;
			bLowestIsGet = TRUE;
		}
		p->tempFanIn->highestKey = record->key;

		p->runPage->consumed = FALSE;

		if(utl->IsPageFull(p->runPage))
		{  
			utl->AddPageToBuffer(BACK_BUFFER(dbcWrite),  p->runPage->page, SSD_PAGE_SIZE); 
			p->runPage->consumed = TRUE;

			// Reset page về ban đầu, bắt đầu ghép 1 page mới  
			utl->ResetRunPage(p->runPage, p->runPageBuffer);

			BACK_BUFFER(dbcWrite).currentPageIndex++;
			BACK_BUFFER(dbcWrite).pageCount++;
		} 

		if(utl->IsBufferFull(BACK_BUFFER(dbcWrite))) // write buffer is full
		{    
			dbcWrite->UnLockProducer();   

			if(dbcWrite->bFirstProduce) // Back Buffer is full, check is it the first product
			{ 
				dbcWrite->bFirstProduce=FALSE;  
				dbcWrite->SwapBuffers();   
				dbcWrite->LockConsumer();    
				PartitionPhase_Write(p); // Execute async write FRONT, continue merge in to BACK 
			}
			else
			{    
				//Wait for write complete   
				GetOverlappedResult(p->hFanOut, &p->overlapWrite.overlap, &p->overlapWrite.dwBytesReadWritten, TRUE) ;  

				p->overlapWrite.fileSize.QuadPart += p->overlapWrite.dwBytesReadWritten;  
				p->overlapWrite.overlap.Offset = p->overlapWrite.fileSize.LowPart;
				p->overlapWrite.overlap.OffsetHigh = p->overlapWrite.fileSize.HighPart;  
				p->overlapWrite.dwBytesReadWritten = 0;

				p->tempFanIn->pageCount += FRONT_BUFFER(dbcWrite).pageCount;
				p->tempFanIn->tupleCount += FRONT_BUFFER(dbcWrite).tupleCount;

				utl->ResetBuffer(FRONT_BUFFER(dbcWrite));  
				dbcWrite->UnLockConsumer(); 

				dbcWrite->SwapBuffers();  

				dbcWrite->LockConsumer();  
				PartitionPhase_Write(p);  // Execute async write FRONT, continue merge in to BACK
			}  
			dbcWrite->LockProducer();  
		} 

		utl->AddTupleToPage(p->runPage, record->data, p->runPageBuffer);  
		BACK_BUFFER(dbcWrite).tupleCount++;
		tupleCount++;

		PartitionPhase_GetNextTuple(p, record, lsIndex); 

		ls.AddNewNode(record, lsIndex);// Add this tuple to loser tree  
		ls.Adjust(lsIndex); //Shrink loser tree 
	}   // end while

	//Wait until current write process complete 
	GetOverlappedResult(p->hFanOut, &p->overlapWrite.overlap, &p->overlapWrite.dwBytesReadWritten, TRUE); 
	if(p->overlapWrite.dwBytesReadWritten > 0)
	{
		if(FRONT_BUFFER(dbcWrite).currentSize > 0)
		{
			p->overlapWrite.fileSize.QuadPart += p->overlapWrite.dwBytesReadWritten;  
			p->overlapWrite.overlap.Offset = p->overlapWrite.fileSize.LowPart;
			p->overlapWrite.overlap.OffsetHigh = p->overlapWrite.fileSize.HighPart;  

			p->tempFanIn->pageCount += FRONT_BUFFER(dbcWrite).pageCount;
			p->tempFanIn->tupleCount += FRONT_BUFFER(dbcWrite).tupleCount;

			p->overlapWrite.dwBytesReadWritten = 0;

			utl->ResetBuffer(FRONT_BUFFER(dbcWrite));  
		}
	}

	//////////////////////////////////////////////////////////////////////////

	// If the last page has not consumed
	if((p->runPage->consumed==FALSE) && (utl->IsBufferFull(BACK_BUFFER(dbcWrite))==FALSE))
	{ 
		if(!utl->IsEmptyPage(p->runPage))
		{
			utl->AddPageToBuffer(BACK_BUFFER(dbcWrite),  p->runPage->page, SSD_PAGE_SIZE);  
			p->runPage->consumed = TRUE; 
			BACK_BUFFER(dbcWrite).currentPageIndex++;
			BACK_BUFFER(dbcWrite).pageCount++;    
		}
		else
		{
			int fuck=0;
		}
	}

	dbcWrite->UnLockProducer();

	dbcWrite->UnLockConsumer();

	if(BACK_BUFFER(dbcWrite).currentSize > 0)
	{    
		//Ghi xong rồi thì hoán đổi
		dbcWrite->SwapBuffers();   
		// Write the last run to disk  
		dbcWrite->LockConsumer(); 

		PartitionPhase_Write(p);

		GetOverlappedResult(p->hFanOut, &p->overlapWrite.overlap, &p->overlapWrite.dwBytesReadWritten, TRUE);

		p->overlapWrite.fileSize.QuadPart += p->overlapWrite.dwBytesReadWritten;  
		p->overlapWrite.overlap.Offset = p->overlapWrite.fileSize.LowPart;
		p->overlapWrite.overlap.OffsetHigh = p->overlapWrite.fileSize.HighPart;  

		p->tempFanIn->pageCount += FRONT_BUFFER(dbcWrite).pageCount;
		p->tempFanIn->tupleCount += FRONT_BUFFER(dbcWrite).tupleCount;

		utl->ResetBuffer(FRONT_BUFFER(dbcWrite)); 
		dbcWrite->UnLockConsumer();
	}

	// Terminate current run
	PartitionPhase_TerminateRun(p, tupleCount);

	//Reset read buffer
	for(DWORD i = 0; i < k; i++) 
	{   
		utl->ResetBuffer(FRONT_BUFFER(dbcRead[i])); 
	}     


	return SUCCESS;
}  


/// <summary>
/// Get next tuple in mini-runs.
/// </summary>
/// <param name="lpParam">The thread parameter.</param>
/// <param name="record">The record pointer.</param>
/// <param name="index">The index of buffer.</param>
/// <returns></returns>
RC PEMS::PartitionPhase_GetNextTuple(LPVOID lpParam, RECORD *&record, INT index)
{
	PartitionThreadParams *p = (PartitionThreadParams *)lpParam;
	DoubleBuffer *dbcRead = p->dbcRead[index];

	int tupleIndex = p->memoryMergeBuffer[index].currentTupleIndex; 
	if(tupleIndex <= p->memoryMergeBuffer[index].tupleCount)
	{ 
		utl->GetTupleInfo(record, tupleIndex, p->memoryMergeBuffer[index].data, SSD_PAGE_SIZE,  p->keyPosition);

		tupleIndex++;
	}
	else
	{
		// Reset merge buffer index
		tupleIndex = 1; 
		utl->ResetBuffer(p->memoryMergeBuffer[index]);

		// Get new page from input buffer
		FRONT_BUFFER(dbcRead).currentPageIndex++; 

		if(FRONT_BUFFER(dbcRead).currentPageIndex < FRONT_BUFFER(dbcRead).pageCount)
		{     
			utl->GetPageInfo(FRONT_BUFFER(dbcRead).data, p->memoryMergeBuffer[index], FRONT_BUFFER(dbcRead).currentPageIndex, SSD_PAGE_SIZE);

			utl->GetTupleInfo(record, tupleIndex, p->memoryMergeBuffer[index].data, SSD_PAGE_SIZE,  p->keyPosition);

			tupleIndex++; 
		}
		else
		{   
			// Input buffer read complete
			utl->ResetBuffer(FRONT_BUFFER(dbcRead));
			utl->SetMaxTuple(record); 
		}  
	}  

	// update new position of tuple
	p->memoryMergeBuffer[index].currentTupleIndex = tupleIndex;

	return SUCCESS;
}


/// <summary>
/// Get fan-in path.
/// </summary>
/// <param name="fanInName">Name of the fan-in.</param>
/// <param name="threadID">The thread identifier.</param>
/// <returns></returns>
RC PEMS::PartitionPhase_GetFanInPath(LPWSTR &fanInName, INT threadID)  
{    
	swprintf(fanInName, MAX_PATH, L"%s%d_%d_%s.dat", m_Params.WORK_SPACE_PATH, threadID, m_FanInIndex, m_Params.FILE_NAME_NO_EXT);  
	InterlockedExchangeAdd(&m_FanInIndex, 1);  

	return SUCCESS; 
} 

/// <summary>
/// Create new fan-in.
/// </summary>
/// <param name="lpParam">The thread parameter.</param>
/// <returns></returns>
RC PEMS::PartitionPhase_CreateNewRun(LPVOID lpParam)
{
	PartitionThreadParams *p = (PartitionThreadParams *)lpParam;

	PartitionPhase_GetFanInPath(p->tempFanIn->fileName, p->threadID);  

	p->hFanOut=CreateFile((LPCWSTR)p->tempFanIn->fileName,	 
		GENERIC_WRITE,			 
		0,						 
		NULL,					 
		CREATE_ALWAYS,			 
		FILE_FLAG_OVERLAPPED, 
		NULL);					// no attr. template

	if (INVALID_HANDLE_VALUE==p->hFanOut) 
	{  
		//myMessage.Add(L"Cannot create run %s\r\n", p->tempFanIn->fileName);
		return ERR_CANNOT_CREATE_HANDLE;
	}   

	// File systems extend files synchronously. Extend the destination file 
	// now so that I/Os execute asynchronously improving performance. 
	LARGE_INTEGER liTempFileSize = { 0 };
	liTempFileSize.QuadPart =  p->inputBufferCount * m_Params.SORT_READ_BUFFER_SIZE;

	SetFilePointerEx(p->hFanOut, liTempFileSize, NULL, FILE_BEGIN);
	SetEndOfFile(p->hFanOut); 

	// Reset value
	p->overlapWrite.fileSize.QuadPart = 0; // Use for shrink fanout after write complete 
	p->overlapWrite.overlap.Offset = 0;
	p->overlapWrite.overlap.OffsetHigh = 0;  

	p->tempFanIn->pageCount = 0;
	p->tempFanIn->tupleCount = 0;
	p->tempFanIn->lowestKey = 0;
	p->tempFanIn->highestKey = 0;
	p->tempFanIn->fileSize.QuadPart = 0;

	return SUCCESS;
}


/// <summary>
/// Terminate current fan-in.
/// </summary>
/// <param name="lpParam">The thread parameter.</param>
/// <param name="tupleCount">The tuple count.</param>
/// <returns></returns>
RC  PEMS::PartitionPhase_TerminateRun(LPVOID lpParam, DWORD tupleCount)
{
	PartitionThreadParams *p = (PartitionThreadParams *)lpParam;
	DoubleBuffer *dbcWrite = p->dbcWrite;  

	if(tupleCount!=0)
	{
		FANS *_fan = new FANS();  
		wcscpy(_fan->fileName,  p->tempFanIn->fileName); 
		_fan->threadID = p->threadID;
		_fan->pageCount = p->tempFanIn->pageCount; 
		_fan->tupleCount = p->tempFanIn->tupleCount;  
		_fan->fileSize.QuadPart = p->overlapWrite.fileSize.QuadPart;  
		_fan->lowestKey = p->tempFanIn->lowestKey;
		_fan->highestKey = p->tempFanIn->highestKey; 
		p->fanIns.push_back(_fan); 

		// The destination file size is a multiple of the page size. Open the
		// file WITH buffering to shrink its size to the source file's size.
		SetFilePointerEx(p->hFanOut, p->overlapWrite.fileSize, NULL, FILE_BEGIN); 
		SetEndOfFile(p->hFanOut); 

		// Close this file
		CloseHandle(p->hFanOut);  
	}
	else
	{
		// fuck pipeline
		CloseHandle(p->hFanOut);  
		DeleteFile(p->tempFanIn->fileName);
	} 

	// Reset temp value
	p->overlapWrite.overlap.Offset = 0;
	p->overlapWrite.overlap.OffsetHigh = 0;  
	p->overlapWrite.fileSize.QuadPart = 0;  

	p->tempFanIn->pageCount = 0;
	p->tempFanIn->tupleCount = 0;
	p->tempFanIn->lowestKey = 0;
	p->tempFanIn->highestKey = 0;


	return SUCCESS;
}


/// <summary>
/// Write buffer data to disk.
/// </summary>
/// <param name="lpParam">The thread parameter.</param>
/// <returns></returns>
RC PEMS::PartitionPhase_Write(LPVOID lpParam) 
{
	PartitionThreadParams *p = (PartitionThreadParams *)lpParam;
	DoubleBuffer *dbcWrite = p->dbcWrite;  

	//if(m_Params.USE_SYNC_READ_WRITE_MODE) 
	//{     
	//	SignalObjectAndWait(sync.hReady[p->threadID], sync.hWaitAllOk, INFINITE, FALSE);
	//}

	WriteFile(p->hFanOut, 
		FRONT_BUFFER(dbcWrite).data, 
		FRONT_BUFFER(dbcWrite).currentSize, 
		&p->overlapWrite.dwBytesReadWritten, 
		&p->overlapWrite.overlap); 			  

	return SUCCESS;
} 


/// <summary>
/// Sort the mini-runs.
/// </summary>
/// <param name="lpParam">The thread parameter.</param>
/// <param name="bufferIndex">Index of the buffer.</param>
/// <returns></returns>
RC PEMS::PartitionPhase_Sort(LPVOID lpParam, DWORD bufferIndex)  
{    
	PartitionThreadParams *p = (PartitionThreadParams *)lpParam; 
	DoubleBuffer *dbcRead  = p->dbcRead[bufferIndex]; 
	DWORD keyPos = p->keyPosition;
	QuickSort *quicksort = p->quickSort;

	if(FRONT_BUFFER(dbcRead).isSort==FALSE)
	{
		dbcRead->LockConsumer();  

		quicksort->Reset();

		RECORD *recordPtr = new RECORD();

		utl->ResetBuffer(p->quickSortPageBuffer);

		DWORD qsIndex = 0;  // quick sort array index
		for(DWORD pageIndex=0; pageIndex < FRONT_BUFFER(dbcRead).pageCount; pageIndex++)
		{    
			utl->GetPageInfo(FRONT_BUFFER(dbcRead).data, p->quickSortPageBuffer, pageIndex, SSD_PAGE_SIZE);  

			for(DWORD tupleIndex=1; tupleIndex <= p->quickSortPageBuffer.tupleCount; tupleIndex++)
			{ 
				utl->GetTupleInfo(recordPtr, tupleIndex, p->quickSortPageBuffer.data, SSD_PAGE_SIZE, keyPos);

				quicksort->AddRecord(recordPtr, qsIndex); 
				qsIndex++; 
			}   
			utl->ResetBuffer(p->quickSortPageBuffer); 
		} 

		// In memory sort
		//////////////////////////////////////////////////////////////////////////
		quicksort->Sort();
		//////////////////////////////////////////////////////////////////////////
		//Save sort result to buffer
		utl->ResetBuffer(p->runPageBuffer);
		utl->ResetRunPage(p->runPage, p->runPageBuffer);  
		utl->ResetBuffer(FRONT_BUFFER(dbcRead));  
		p->runPage->consumed=TRUE;
		DWORD quicksortSize = quicksort->GetCurrentSize();
		for (DWORD q=0; q < quicksortSize; q++)
		{  
			p->runPage->consumed = FALSE; 

			recordPtr = quicksort->GetRecord(q);

			if(utl->IsPageFull(p->runPage))
			{  
				if(!utl->IsBufferFull(FRONT_BUFFER(dbcRead)))
				{
					utl->AddPageToBuffer(FRONT_BUFFER(dbcRead),  p->runPage->page, SSD_PAGE_SIZE);  
					FRONT_BUFFER(dbcRead).pageCount++;
					p->runPage->consumed=TRUE;  
				}
				else 
				{ 
					break; // Buffer full
				}

				utl->ResetRunPage(p->runPage, p->runPageBuffer);  
			}   
			utl->AddTupleToPage(p->runPage, recordPtr->data, p->runPageBuffer);  // Add this tuple to page  
		} 

		if( (p->runPage->consumed == FALSE) && (utl->IsBufferFull(FRONT_BUFFER(dbcRead))==FALSE) ) 
		{ 
			if(!utl->IsEmptyPage(p->runPage))
			{
				utl->AddPageToBuffer(FRONT_BUFFER(dbcRead), p->runPage->page, SSD_PAGE_SIZE);
				FRONT_BUFFER(dbcRead).pageCount++; 
			} 
		}   

		FRONT_BUFFER(dbcRead).tupleCount = quicksort->GetCurrentSize();   
		FRONT_BUFFER(dbcRead).isSort=TRUE;   

		dbcRead->UnLockConsumer(); 
	}

	return SUCCESS;
}   


/// <summary>
/// Check enough memory for merge phase.
/// </summary>
/// <returns></returns>
RC PEMS::MergePhase_CheckEnoughMemory()
{ 
	DWORD fanInNum = m_FanIns.size();
	if(fanInNum==0)
	{ 
		return ERR_SORT_MERGE_FILE_NOT_ENOUGH;
	}

	// Estimate memory
	////////////////////////////////////////////////////////////////////////// 
	DOUBLE totalMemory = m_Params.BUFFER_POOL_SIZE;  

	DWORD memoryForWriteFanOut = m_Params.MERGE_WRITE_BUFFER_SIZE  * 2;
	DWORD memoryForAssemblyRunPage = SSD_PAGE_SIZE;
	totalMemory = totalMemory - memoryForWriteFanOut;
	totalMemory = totalMemory - memoryForAssemblyRunPage;

	if( totalMemory <= 0)
	{ 
		return ERR_NOT_ENOUGH_MEMORY; 
	}

	// LoserKey + LoserTreeData + MergeBuffer + ReadInputBuffer*2
	DWORD memoryForLoserTree = sizeof(UINT64) + (TUPLE_SIZE * sizeof(CHAR));
	DWORD memoryForMergePage = SSD_PAGE_SIZE;
	DWORD memoryForReadFanIn = m_Params.MERGE_READ_BUFFER_SIZE * 2;

	DWORD memoryNeedForOneFanIn = memoryForLoserTree + memoryForMergePage + memoryForReadFanIn;

	m_MaxFanIn = totalMemory / memoryNeedForOneFanIn;

	// Estimate memory use for current fanIn num, if not enough memory -> multimerge step 
	if(m_MaxFanIn > fanInNum)
	{
		// Enough memory for merge in one step
		m_MaxFanIn = fanInNum;   
	}

	DWORD memoryNeedForMerge = m_MaxFanIn * memoryNeedForOneFanIn; 

	totalMemory = totalMemory - memoryNeedForMerge; 

	if(totalMemory <= 0)
	{ 
		return ERR_NOT_ENOUGH_MEMORY; 
	}

	return SUCCESS;
}


/// <summary>
/// Initialize the merges phase
/// </summary>
/// <returns></returns>
RC PEMS::MergePhase_Initialize()
{    
	RC rc; 
	m_MergeParams = new MergeThreadParams(); 
	m_MergeParams->maxFanIn = m_MaxFanIn;
	m_MergeParams->threadID = GetCurrentThreadId();
	m_MergeParams->keyPosition = m_Params.KEY_POS; 

	// Init write buffer
	m_MergeParams->dbcWrite = new DoubleBuffer(m_Params.MERGE_WRITE_BUFFER_SIZE); 
	rc = utl->InitBuffer(m_MergeParams->dbcWrite->buffer[0], m_Params.MERGE_WRITE_BUFFER_SIZE, &bufferPool);
	if(rc!=SUCCESS) {return rc;}

	rc = utl->InitBuffer(m_MergeParams->dbcWrite->buffer[1], m_Params.MERGE_WRITE_BUFFER_SIZE, &bufferPool); 
	if(rc!=SUCCESS) {return rc;}
	//////////////////////////////////////////////////////////////////////////
	m_MergeParams->dbcRead = new DoubleBuffer*[m_MaxFanIn];
	m_MergeParams->mergeBuffer = new Buffer[m_MaxFanIn];

	for(DWORD i=0; i<m_MaxFanIn; i++)
	{ 
		m_MergeParams->dbcRead[i] = new DoubleBuffer(m_Params.MERGE_READ_BUFFER_SIZE);
		rc = utl->InitBuffer(m_MergeParams->dbcRead[i]->buffer[0], m_Params.MERGE_READ_BUFFER_SIZE, &bufferPool);
		if(rc!=SUCCESS) {return rc;}

		rc = utl->InitBuffer(m_MergeParams->dbcRead[i]->buffer[1], m_Params.MERGE_READ_BUFFER_SIZE, &bufferPool); 
		if(rc!=SUCCESS) {return rc;}

		rc = utl->InitBuffer(m_MergeParams->mergeBuffer[i], SSD_PAGE_SIZE, &bufferPool);
		if(rc!=SUCCESS) {return rc;}
	}

	// Init run page buffer for assembly
	rc = utl->InitBuffer(m_MergeParams->runPageBuffer, SSD_PAGE_SIZE, &bufferPool);
	if(rc!=SUCCESS) {return rc;}

	// Init runpage 
	rc = utl->InitRunPage(m_MergeParams->runPage, m_MergeParams->runPageBuffer);  
	if(rc!=SUCCESS) {return rc;}
	return SUCCESS; 
}


/// <summary>
/// Execute the merges phase
/// </summary>
/// <returns></returns>
RC PEMS::MergePhase_Execute()
{ 
	RC rc;
	rc = MergePhase_Initialize();
	if(rc!=SUCCESS)
	{ 
		return ERR_CANNOT_INITIAL_MEMORY;
	}

	MergeThreadParams *p = m_MergeParams;
	rc = MergePhase_Merge(p);
	if(rc!=SUCCESS)  { return rc; }

	return SUCCESS;
}


/// <summary>
/// k-way merge on-disk fan-in.
/// </summary>
/// <param name="lpParam">The thread parameter.</param>
/// <returns></returns>
RC PEMS::MergePhase_Merge(LPVOID lpParam)
{
	MergeThreadParams *p = (MergeThreadParams *)lpParam;

	DoubleBuffer *dbcWrite = p->dbcWrite;   

	DWORD maxFanIn = p->maxFanIn;

	LPWSTR fanOutPath = new TCHAR[MAX_PATH];

	while( TRUE ) // Multi merge passes
	{  
		MergePhase_GetFanOutPath(fanOutPath, p->threadID); 

		DWORD64 totalPage = 0;
		DWORD64 tupleCount = 0;

		p->hFanOut=CreateFile((LPCWSTR)fanOutPath,		// file to write
			GENERIC_WRITE,			// open for writing
			0,						// Do not share
			NULL,					// default security
			CREATE_ALWAYS,			 
			FILE_FLAG_OVERLAPPED | FILE_FLAG_NO_BUFFERING,	// overlapped operation
			NULL);					// no attr. template

		if (INVALID_HANDLE_VALUE==p->hFanOut) 
		{   
			return ERR_CANNOT_CREATE_HANDLE;
		}  

		//	Init overlap structure for Ouput
		//////////////////////////////////////////////////////////////////////////  
		p->overlapWrite.dwBytesToReadWrite = m_Params.MERGE_WRITE_BUFFER_SIZE; 
		p->overlapWrite.dwBytesReadWritten = 0;
		p->overlapWrite.fileSize.QuadPart = 0;
		p->overlapWrite.overlap.Offset = 0;
		p->overlapWrite.overlap.OffsetHigh = 0;

		// Init handle run file
		////////////////////////////////////////////////////////////////////////// 
		DWORD currentQueueSize = m_FanIns.size();
		DWORD fanInNum = 0;

		if (currentQueueSize <= maxFanIn)  
			fanInNum = currentQueueSize;    
		else  
			fanInNum = maxFanIn;    

		// Init loser tree
		LoserTree ls(fanInNum);
		RECORD *recordPtr = new RECORD();  

		p->hFanIn = new HANDLE[fanInNum];
		p->overlapRead = new OVERLAPPEDEX[fanInNum];

		LARGE_INTEGER liFanOutSize = {0};

		for(DWORD i=0; i<fanInNum; i++)
		{
			FANS *fanIn = new FANS();
			fanIn = m_FanIns.front();
			m_FanIns.pop();
			m_FanInWillDelete.push(fanIn);  
			//liFanOutSize.QuadPart+=fanIn->fileSize.QuadPart; // calculate fanout size

			p->hFanIn[i] = CreateFile(
				(LPCWSTR)fanIn->fileName, // file to open
				GENERIC_READ,			// open for reading
				FILE_SHARE_READ,        // share for reading
				NULL,					// default security
				OPEN_EXISTING,			// existing file only
				FILE_FLAG_OVERLAPPED | FILE_FLAG_NO_BUFFERING,	// overlapped operation //| FILE_FLAG_NO_BUFFERING
				NULL);					// no attr. template 
			if (INVALID_HANDLE_VALUE==p->hFanIn[i]) 
			{  
				return ERR_CANNOT_CREATE_HANDLE; 
			} 

			UINT64 fanSize = GetFanSize(p->hFanIn[i]);

			liFanOutSize.QuadPart+= fanSize;

			DWORD64 chunkSize = m_Params.MERGE_READ_BUFFER_SIZE; 
			DWORD64 totalChunk = chROUNDUP(fanSize, chunkSize) / chunkSize ;

			// Create overlap structure for input runs 
			p->overlapRead[i].dwBytesToReadWrite = m_Params.MERGE_READ_BUFFER_SIZE; 
			p->overlapRead[i].chunkIndex = 0; 
			p->overlapRead[i].startChunk = 0;
			p->overlapRead[i].endChunk = totalChunk;
			p->overlapRead[i].totalChunk = totalChunk; 

			p->dbcRead[i]->bFirstProduce=TRUE;
		} 

		// File systems extend files synchronously. Extend the destination file 
		// now so that I/Os execute asynchronously improving performance. 
		////////////////////////////////////////////////////////////////////////// 
		LARGE_INTEGER liDestSize = { 0 };
		liDestSize.QuadPart = chROUNDUP(liFanOutSize.QuadPart, m_Params.MERGE_WRITE_BUFFER_SIZE);

		SetFilePointerEx(p->hFanOut, liDestSize, NULL, FILE_BEGIN);
		SetEndOfFile(p->hFanOut); 

		// First read from disk to buffer
		//////////////////////////////////////////////////////////////////////////
		for(DWORD i=0; i<fanInNum;i++) 
		{
			MergePhase_Read(p, i);     
		}

		for(DWORD i=0; i < fanInNum;i++)
		{ 
			// Wait for read complete
			if(p->overlapRead[i].chunkIndex < p->overlapRead[i].totalChunk)
				GetOverlappedResult(p->hFanIn[i], &p->overlapRead[i].overlap, &p->overlapRead[i].dwBytesReadWritten, TRUE);  
			p->overlapRead[i].chunkIndex++; 
			utl->ComputeBuffer(BACK_BUFFER(p->dbcRead[i]),  p->overlapRead[i].dwBytesReadWritten);  

			utl->ResetBuffer(p->mergeBuffer[i]); 

			if(p->dbcRead[i]->bFirstProduce==TRUE) { p->dbcRead[i]->bFirstProduce=FALSE; } 

			utl->GetPageInfo(BACK_BUFFER(p->dbcRead[i]).data, p->mergeBuffer[i], 0, SSD_PAGE_SIZE); 
			p->mergeBuffer[i].currentTupleIndex = 1;
			utl->GetTupleInfo(recordPtr, p->mergeBuffer[i].currentTupleIndex, p->mergeBuffer[i].data ,SSD_PAGE_SIZE, p->keyPosition);  
			p->mergeBuffer[i].currentTupleIndex++;
			ls.AddNewNode(recordPtr, i);  

			//p->dbcRead[i]->UnLockProducer();  

			p->dbcRead[i]->SwapBuffers();  // swap read buffer

			MergePhase_Read(p, i);    
		} 

		ls.CreateLoserTree();     

		// Reset buffer to default values
		dbcWrite->bFirstProduce = TRUE;
		utl->ResetBuffer(BACK_BUFFER(dbcWrite));
		utl->ResetBuffer(FRONT_BUFFER(dbcWrite));  
		utl->ResetRunPage(p->runPage, p->runPageBuffer);

		INT index = 0; // file index 
		DWORD64 lowestValue=0;
		DWORD64	highestValue=0;
		BOOL bLowestIsSet = FALSE; 

		//dbcWrite->LockProducer();
		while(TRUE) //p->lsTemp[ p->loserTree[0] ] !=MAX
		{   
			ls.GetMinRecord( recordPtr, index ); // index = loserTree[0]

			if(recordPtr->key==MAX) { break; }

			p->runPage->consumed = FALSE;

			// Save the record has lowest value
			if(bLowestIsSet==FALSE)
			{
				bLowestIsSet=TRUE;
				lowestValue= recordPtr->key;  
			} 

			// Save the record has highest value
			highestValue = recordPtr->key;   

			if(utl->IsBufferFull(BACK_BUFFER(dbcWrite))) // check Is buffer full?
			{    
				if(dbcWrite->bFirstProduce==TRUE)
				{ 
					dbcWrite->bFirstProduce=FALSE;  
					dbcWrite->SwapBuffers();   

					dbcWrite->LockConsumer(); 	// First write 
					MergePhase_Write(p);
				}
				else
				{   
					// Wait for write thread comsume FRONT buffer to be done   
					GetOverlappedResult(p->hFanOut, &p->overlapWrite.overlap, &p->overlapWrite.dwBytesReadWritten, TRUE) ; 
					p->overlapWrite.fileSize.QuadPart += p->overlapWrite.dwBytesReadWritten;  // Save current position to overlap struc
					p->overlapWrite.overlap.Offset=p->overlapWrite.fileSize.LowPart;
					p->overlapWrite.overlap.OffsetHigh=p->overlapWrite.fileSize.HighPart;  
					utl->ResetBuffer(FRONT_BUFFER(dbcWrite));  // Clear data for continue merge 

					dbcWrite->SwapBuffers();   	// Swap BACK FRONT

					MergePhase_Write(p);
				}    
			}

			if(utl->IsPageFull(p->runPage))
			{   
				//Current runPage is FULL, copy this page to BACK output buffer
				utl->AddPageToBuffer(BACK_BUFFER(dbcWrite), p->runPage->page, SSD_PAGE_SIZE);  
				p->runPage->consumed = TRUE;

				BACK_BUFFER(dbcWrite).freeLocation+=SSD_PAGE_SIZE;
				BACK_BUFFER(dbcWrite).pageCount++;   
				totalPage++;

				// Reset runPage, start to employ new one
				utl->ResetRunPage(p->runPage, p->runPageBuffer); 
			} // end check runPage is full

			// Add current min tuple to runpage
			utl->AddTupleToPage(p->runPage, recordPtr->data, p->runPageBuffer);  

			BACK_BUFFER(dbcWrite).tupleCount++;
			tupleCount++;

			MergePhase_GetNextTuple(p, recordPtr, index);  

			ls.AddNewNode(recordPtr, index);// Add new tuple to tree 
			ls.Adjust( index );  // Continue fight LOSER TREE 
		} // end while loser tree

		// If the last page has not consumed
		if((p->runPage->consumed==FALSE) && (utl->IsBufferFull( BACK_BUFFER(dbcWrite))==FALSE) )
		{ 
			if(!utl->IsEmptyPage(p->runPage))
			{
				utl->AddPageToBuffer(BACK_BUFFER(dbcWrite),  p->runPage->page, SSD_PAGE_SIZE); 
				p->runPage->consumed = TRUE;  
				BACK_BUFFER(dbcWrite).currentPageIndex++;
				BACK_BUFFER(dbcWrite).pageCount++; 
				totalPage++;
			} 
		}

		if(BACK_BUFFER(dbcWrite).currentSize > 0)
		{      
			GetOverlappedResult(p->hFanOut, &p->overlapWrite.overlap, &p->overlapWrite.dwBytesReadWritten, TRUE) ;
			// Save current position to overlap struc
			p->overlapWrite.fileSize.QuadPart += p->overlapWrite.dwBytesReadWritten;  
			p->overlapWrite.overlap.Offset=p->overlapWrite.fileSize.LowPart;
			p->overlapWrite.overlap.OffsetHigh=p->overlapWrite.fileSize.HighPart;   
			utl->ResetBuffer(FRONT_BUFFER(dbcWrite));   

			//Write FRONT is done
			dbcWrite->SwapBuffers();  

			// Write the last run to disk  
			MergePhase_Write(p);

			// Get overlap result and wait for overlapEvent is signaled
			GetOverlappedResult(p->hFanOut, &p->overlapWrite.overlap, &p->overlapWrite.dwBytesReadWritten, TRUE); 
			p->overlapWrite.fileSize.QuadPart += p->overlapWrite.dwBytesReadWritten;  
			p->overlapWrite.overlap.Offset=p->overlapWrite.fileSize.LowPart;
			p->overlapWrite.overlap.OffsetHigh=p->overlapWrite.fileSize.HighPart;   
			utl->ResetBuffer(FRONT_BUFFER(dbcWrite)); 
		}

		//// The destination file size is a multiple of the page size. Open the
		//// file WITH buffering to shrink its size to the source file's size.
		SetFilePointerEx(p->hFanOut, liFanOutSize, NULL, FILE_BEGIN);
		SetEndOfFile(p->hFanOut);

		CloseHandle(p->hFanOut);  

		// Reset read buffer
		for(DWORD i = 0; i < fanInNum; i++) 
		{   
			utl->ResetBuffer(FRONT_BUFFER(p->dbcRead[i]));  
			CloseHandle(p->hFanIn[i]);
		}   

		if(m_Params.USE_DELETE_AFTER_OPERATION==TRUE)
		{
			if(m_FanInWillDelete.size() > 0)
			{
				while (m_FanInWillDelete.size() > 0)
				{
					FANS *deleteFanIn = m_FanInWillDelete.front();
					m_FanInWillDelete.pop();
					if(DeleteFile(deleteFanIn->fileName))
					{
						//myMessage.Post(L"Delete file %s OK\r\n", deleteFanIn->fileName);
					} 
				} 
			}
		} 

		FANS *_fanOut= new FANS();
		wcscpy(_fanOut->fileName, fanOutPath);
		_fanOut->threadID = p->threadID;
		_fanOut->pageCount = liFanOutSize.QuadPart / SSD_PAGE_SIZE;
		_fanOut->fileSize = liFanOutSize;
		_fanOut->tupleCount = tupleCount;  
		_fanOut->lowestKey = lowestValue; 
		_fanOut->highestKey = highestValue; 
		m_FanIns.push(_fanOut); 

		// Check is the last fanIn
		if(m_FanIns.size()==1)
		{
			m_FanOut = new FANS();
			m_FanOut = _fanOut;
			break;
		}

		m_FanInWillDelete.push(_fanOut); 
		rp.MergePass++; 
	} // end while multi merge passes

	if(m_Params.USE_DELETE_AFTER_OPERATION==TRUE)
	{
		if(m_FanInWillDelete.size() > 0)
		{
			while (m_FanInWillDelete.size() > 0)
			{
				FANS *deleteFanIn = new FANS();
				deleteFanIn = m_FanInWillDelete.front();
				m_FanInWillDelete.pop();
				if(DeleteFile(deleteFanIn->fileName))
				{
					//myMessage.Post(L"Delete file %s OK\r\n", deleteFanIn->fileName);
				} 
			} 
		}
	}

	delete fanOutPath;
	return SUCCESS;
}


/// <summary>
/// Get next tuple from buffer.
/// </summary>
/// <param name="lpParam">The thread parameter.</param>
/// <param name="record">The record pointer.</param>
/// <param name="index">The index of fan-in.</param>
/// <returns></returns>
RC PEMS::MergePhase_GetNextTuple(LPVOID lpParam, RECORD *&record, INT index)
{
	MergeThreadParams *p = (MergeThreadParams *)lpParam;

	if(p->mergeBuffer[index].currentTupleIndex > p->mergeBuffer[index].tupleCount) 
	{
		// Read complete this page, need to fetch next page from disk  
		utl->ResetBuffer(p->mergeBuffer[index]); 
		FRONT_BUFFER(p->dbcRead[index]).currentPageIndex++; 
		if(FRONT_BUFFER(p->dbcRead[index]).currentPageIndex >= FRONT_BUFFER(p->dbcRead[index]).pageCount)
		{    
			utl->ResetBuffer(FRONT_BUFFER(p->dbcRead[index])); 

			// Waiting for reading in to back buffer completed
			if( (p->overlapRead[index].chunkIndex < p->overlapRead[index].totalChunk)) 
				GetOverlappedResult(p->hFanIn[index], &p->overlapRead[index].overlap, &p->overlapRead[index].dwBytesReadWritten, TRUE);

			p->overlapRead[index].chunkIndex++; 

			if(p->overlapRead[index].dwBytesReadWritten==0)
			{
				utl->ResetBuffer(BACK_BUFFER(p->dbcRead[index]));
				utl->AddPageMAXToBuffer(p->mergeBuffer[index], SSD_PAGE_SIZE); 
				utl->AddPageToBuffer(BACK_BUFFER(p->dbcRead[index]), p->mergeBuffer[index].data, SSD_PAGE_SIZE); 
				p->mergeBuffer[index].isFullMaxValue = TRUE; 
				FRONT_BUFFER(p->dbcRead[index]).currentPageIndex = 0;
			}
			else
			{
				utl->ComputeBuffer(BACK_BUFFER(p->dbcRead[index]), p->overlapRead[index].dwBytesReadWritten);  
			}

			//p->dbcRead[index]->UnLockProducer();

			//Swap buffer to continue merge 
			p->dbcRead[index]->SwapBuffers(); 

			// Prefetch BACK buffer  
			MergePhase_Read(p, index);   
		}  

		utl->GetPageInfo(FRONT_BUFFER(p->dbcRead[index]).data, 
			p->mergeBuffer[index], 
			FRONT_BUFFER(p->dbcRead[index]).currentPageIndex, 
			SSD_PAGE_SIZE);   

		p->mergeBuffer[index].currentTupleIndex=1; 
	}

	utl->GetTupleInfo(record, 
		p->mergeBuffer[index].currentTupleIndex,
		p->mergeBuffer[index].data,
		SSD_PAGE_SIZE,
		m_Params.KEY_POS); 

	p->mergeBuffer[index].currentTupleIndex++;

	return SUCCESS; 
}


/// <summary>
/// Write buffer data to disk.
/// </summary>
/// <param name="lpParam">The thread parameter.</param>
/// <returns></returns>
RC PEMS::MergePhase_Write(LPVOID lpParam)
{
	MergeThreadParams *p = (MergeThreadParams *)lpParam; 
	DoubleBuffer *dbcWrite = p->dbcWrite;   

	WriteFile(p->hFanOut,  
		FRONT_BUFFER(dbcWrite).data,  
		FRONT_BUFFER(dbcWrite).currentSize,  
		&p->overlapWrite.dwBytesReadWritten,  
		&p->overlapWrite.overlap ); 

	return SUCCESS;
}


/// <summary>
/// Read data from disk to buffer.
/// </summary>
/// <param name="lpParam">The thread parameter.</param>
/// <param name="index">The index of fan-in.</param>
/// <returns></returns>
RC PEMS::MergePhase_Read(LPVOID lpParam, DWORD index)
{  
	MergeThreadParams *p = (MergeThreadParams *)lpParam; 

	LARGE_INTEGER chunk;
	chunk.QuadPart =  p->overlapRead[index].chunkIndex * m_Params.MERGE_READ_BUFFER_SIZE;   
	p->overlapRead[index].overlap.Offset = chunk.LowPart;
	p->overlapRead[index].overlap.OffsetHigh = chunk.HighPart;  

	// Attempt an asynchronous read operation.  
	ReadFile(p->hFanIn[index], 
		BACK_BUFFER(p->dbcRead[index]).data, 
		p->overlapRead[index].dwBytesToReadWrite, 
		&p->overlapRead[index].dwBytesReadWritten, 
		&p->overlapRead[index].overlap);

	return SUCCESS; 
}

/// <summary>
/// Gets the size of the fan-in.
/// </summary>
/// <param name="hFile">The fan-in file handle.</param>
/// <returns></returns>
UINT64 PEMS::GetFanSize(HANDLE hFile)  
{
	LARGE_INTEGER liFileSize; 
	if (!GetFileSizeEx(hFile, &liFileSize))
	{      
		return 0;
	} 

	return liFileSize.QuadPart;
}
