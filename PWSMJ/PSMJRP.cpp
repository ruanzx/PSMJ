//
// Name: PSMJRP.cpp   
// Author: hieunt
// Description: Parallel sort-merge join module use replacement selection method
//
#include "stdafx.h"
#include "PSMJRP.h"

// Global variables
extern Loggers g_Logger;

/// <summary>
/// Initializes a new instance of the <see cref="PSMJRP"/> class.
/// </summary>
/// <param name="vParams">The v parameters.</param>
PSMJRP::PSMJRP(const PSMJRP_PARAMS vParams) : m_PsmjRpParams(vParams)
{
	m_FanIndex = 0;
	m_TotalJoinCount = 0;
	m_WorkerThreadNum = m_PsmjRpParams.THREAD_NUM; 
	m_PartitionSize = 0;
	isR = TRUE;
	utl = new PageHelpers2(); 
}

/// <summary>
/// Finalizes an instance of the <see cref="PSMJRP"/> class.
/// </summary>
PSMJRP::~PSMJRP(void)
{
}

/// <summary>
/// Executes this instance.
/// </summary>
/// <returns></returns>
RC PSMJRP::Execute()
{
	RC rc;

	SYSTEM_INFO sysinfo;
	GetSystemInfo(&sysinfo);
	if(m_PsmjRpParams.THREAD_NUM==0)
		m_WorkerThreadNum = sysinfo.dwNumberOfProcessors; 


	m_PsmjRpParams.MERGE_READ_BUFFER_SIZE =  32 * SSD_PAGE_SIZE; // 128K
	m_PsmjRpParams.MERGE_WRITE_BUFFER_SIZE = 512 * SSD_PAGE_SIZE; // 2MB

	// Init buffer pool
	//////////////////////////////////////////////////////////////////////////
	bufferPool.size = m_PsmjRpParams.BUFFER_POOL_SIZE; 
	bufferPool.currentSize = 0;  
	g_Logger.Write("PSMJRP Start...\n");

	// Creates a private heap object that can be used by the calling process. 
	// The function reserves space in the virtual address space of the process and allocates physical storage 
	// for a specified initial portion of this block.
	// Reason: C++ cannot allocate more than 400MB with new[] in MFC
	// http://msdn.microsoft.com/en-us/library/aa366599.aspx
	HANDLE hHeap = HeapCreate(0, 0, 0);
	bufferPool.data  = (CHAR*)HeapAlloc(hHeap, 0, bufferPool.size);//HeapAlloc(hHeap, 0, bufferPool.size);

	if(NOERROR != GetLastError())
	{
		ReportLastError();
		HeapDestroy(hHeap);
		return ERR_CANNOT_INITIAL_BUFFER_POOL;
	} 
	//////////////////////////////////////////////////////////////////////////

	StopWatch stwTotalTime, stwJoinTime;
	rp.BufferPoolSize = m_PsmjRpParams.BUFFER_POOL_SIZE;
	rp.SortReadBufferSize = m_PsmjRpParams.SORT_READ_BUFFER_SIZE;
	rp.SortWriteBufferSize = m_PsmjRpParams.SORT_WRITE_BUFFER_SIZE;
	rp.MergeReadBufferSizeR = 0;
	rp.MergeWriteBufferSizeR = 0;
	rp.MergeReadBufferSizeS = 0;
	rp.MergeWriteBufferSizeS = 0;
	rp.JoinReadBufferSize = 0;
	rp.JoinWriteBufferSize = 0;  

	rp.MergeCpuTimeR[0] = 0; rp.MergeCpuTimeR[1] = 0;
	rp.MergeCpuTimeS[0] = 0; rp.MergeCpuTimeS[1] = 0;
	rp.MergeExecTimeR = 0; rp.MergeExecTimeS = 0;

	//////////////////////////////////////////////////////////////////////////
	stwTotalTime.Start();
	rp.TotalCpuTime[0] = GetCpuTime(); 
	//////////////////////////////////////////////////////////////////////////

	rc = SortTableR();
	if(rc!=SUCCESS) { return rc; }

	rc = SortTableS();
	if(rc!=SUCCESS) { return rc; }

	//////////////////////////////////////////////////////////////////////////
	stwJoinTime.Start();
	rp.JoinCpuTime[0] = GetCpuTime();
	rc = JoinTable();
	if(rc!=SUCCESS) { return rc; }
	rp.JoinCpuTime[1] = rp.TotalCpuTime[1] = GetCpuTime();
	//rp.TotalCpuTime[1] = GetCpuTime();  
	rp.JoinExecTime = stwJoinTime.NowInMilliseconds();
	//////////////////////////////////////////////////////////////////////////
	rp.TotalExecTime = stwTotalTime.NowInMilliseconds();
	//////////////////////////////////////////////////////////////////////////
	g_Logger.Write("PSMJRP End...\n");

	//////////////////////////////////////////////////////////////////////////
	WriteReport();

	// Free memmory
	//////////////////////////////////////////////////////////////////////////
	BOOL bRet = HeapFree(hHeap, 0, bufferPool.data);
	bRet = HeapDestroy(hHeap);
	//////////////////////////////////////////////////////////////////////////

	return SUCCESS;
}

/// <summary>
/// Sorts the table R.
/// </summary>
/// <returns></returns>
RC PSMJRP::SortTableR() 
{
	g_Logger.Write("Sort R...\n");
	RC rc;
	isR	= TRUE;
	bufferPool.currentSize = 0;
	while(!m_FanIns.empty()) m_FanIns.pop();

	StopWatch  stwPartitionTime;
	stwPartitionTime.Start(); 
	rp.PartitionCpuTimeR[0] = GetCpuTime(); 

	m_hSourceTable=CreateFile(
		(LPCWSTR)m_PsmjRpParams.RELATION_R_PATH,
		GENERIC_READ,
		FILE_SHARE_READ,
		NULL,
		OPEN_EXISTING,
		FILE_FLAG_OVERLAPPED,  
		NULL); 

	if(INVALID_HANDLE_VALUE==m_hSourceTable)
	{  
		ShowMB(L"Cannot create handle %s", m_PsmjRpParams.RELATION_R_PATH);
		return ERR_CANNOT_CREATE_HANDLE; 
	} 

	/* Get source table size */
	LARGE_INTEGER *liFileSize = new LARGE_INTEGER();  
	if (!GetFileSizeEx(m_hSourceTable, liFileSize))
	{    
		return ERR_CANNOT_GET_FILE_SIZE;
	} 
	rp.SourceTableSizeR = liFileSize->QuadPart;

	m_hWorkerThread = new HANDLE[m_WorkerThreadNum];
	m_PartitionParams = new PartitionPhaseParams[m_WorkerThreadNum];

	// Init mark current run
	for(UINT tIdx=0; tIdx < m_WorkerThreadNum; tIdx++)
	{
		m_PartitionParams[tIdx].currentMark = 0; 

		m_PartitionParams[tIdx].overlapRead.dwBytesToReadWrite = m_PsmjRpParams.SORT_READ_BUFFER_SIZE;  
		m_PartitionParams[tIdx].overlapRead.dwBytesReadWritten = 0;  
		m_PartitionParams[tIdx].overlapRead.startChunk = 0;  
		m_PartitionParams[tIdx].overlapRead.chunkIndex = 0;  
		m_PartitionParams[tIdx].overlapRead.endChunk = 0;  
		m_PartitionParams[tIdx].overlapRead.totalChunk = 0; 
		m_PartitionParams[tIdx].overlapRead.overlap.Offset = 0; 
		m_PartitionParams[tIdx].overlapRead.overlap.OffsetHigh = 0; 

		m_PartitionParams[tIdx].overlapWrite.dwBytesToReadWrite = m_PsmjRpParams.SORT_WRITE_BUFFER_SIZE; 
		m_PartitionParams[tIdx].overlapWrite.dwBytesReadWritten = 0; 
		m_PartitionParams[tIdx].overlapWrite.overlap.Offset = 0;
		m_PartitionParams[tIdx].overlapWrite.overlap.OffsetHigh = 0;
	}

	/* Set up overlap structure to READ source table */
	DWORD chunkSize = m_PsmjRpParams.SORT_READ_BUFFER_SIZE;
	DWORD totalChunk = chROUNDUP(liFileSize->QuadPart, chunkSize) / chunkSize; 

	DWORD temp = totalChunk;
	while (temp > 0)
	{
		for (DWORD i = 0; i < m_WorkerThreadNum; i++)
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
		for (DWORD i = 0; i < m_WorkerThreadNum; i++)
		{ 
			m_PartitionParams[i].overlapRead.startChunk = startChunk; 
			m_PartitionParams[i].overlapRead.chunkIndex = startChunk; 
			startChunk += m_PartitionParams[i].overlapRead.totalChunk;
			m_PartitionParams[i].overlapRead.endChunk = startChunk - 1;  
			temp -= m_PartitionParams[i].overlapRead.totalChunk;
			if(temp==0) { break; }
		}  	
	}
	//////////////////////////////////////////////////////////////////////////
	// Compute Heap size
	DOUBLE memFree = m_PsmjRpParams.BUFFER_POOL_SIZE - bufferPool.currentSize; 
	memFree = memFree - m_WorkerThreadNum * (m_PsmjRpParams.SORT_READ_BUFFER_SIZE + m_PsmjRpParams.SORT_WRITE_BUFFER_SIZE + SSD_PAGE_SIZE*2);

	if(memFree<=0)
		return ERR_NOT_ENOUGH_MEMORY;

	DWORD  heapSize = chROUNDDOWN( (DWORD)memFree / m_WorkerThreadNum, TUPLE_SIZE) / TUPLE_SIZE; 

	//////////////////////////////////////////////////////////////////////////
	// Init default value
	for (DWORD i = 0; i < m_WorkerThreadNum; i++)
	{
		m_PartitionParams[i]._this = this; ///////////// Importance 
		m_PartitionParams[i].keyPos = m_PsmjRpParams.R_KEY_POS;   
		m_PartitionParams[i].fanPath = new TCHAR[MAX_PATH];


		rc = utl->InitBuffer(m_PartitionParams[i].readBuffer, m_PsmjRpParams.SORT_READ_BUFFER_SIZE, &bufferPool);
		if(rc!=SUCCESS) {return rc;}  

		rc = utl->InitBuffer(m_PartitionParams[i].writeBuffer, m_PsmjRpParams.SORT_WRITE_BUFFER_SIZE, &bufferPool); 
		if(rc!=SUCCESS) {return rc;}
		//////////////////////////////////////////////////////////////////////////

		// Init run buffer
		rc = utl->InitBuffer(m_PartitionParams[i].readPageBuffer, SSD_PAGE_SIZE, &bufferPool); 
		if(rc!=SUCCESS) {return rc;}
		rc = utl->InitBuffer(m_PartitionParams[i].writePageBuffer, SSD_PAGE_SIZE, &bufferPool); 
		if(rc!=SUCCESS) {return rc;}

		//////////////////////////////////////////////////////////////////////////
		// Init runpage 
		rc = utl->InitRunPage(m_PartitionParams[i].readPagePtr, m_PartitionParams[i].readPageBuffer); 
		if(rc!=SUCCESS) {return rc;}
		rc = utl->InitRunPage(m_PartitionParams[i].writePagePtr, m_PartitionParams[i].writePageBuffer); 
		if(rc!=SUCCESS) {return rc;}

		//////////////////////////////////////////////////////////////////////////
		//Working space buffer 
		//////////////////////////////////////////////////////////////////////////

		rc = utl->InitBuffer(m_PartitionParams[i].heapBuffer, heapSize * TUPLE_SIZE, &bufferPool); 
		if(rc!=SUCCESS) {return rc;}

		m_PartitionParams[i].heapSize = heapSize;
	} 


	//////////////////////////////////////////////////////////////////////////

	for(DWORD i = 0; i < m_WorkerThreadNum; i++ )
	{  
		m_hWorkerThread[i] = CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)PartitionPhase_Ex, (LPVOID)&(m_PartitionParams[i]), CREATE_SUSPENDED, NULL);

		SetThreadPriority(m_hWorkerThread[i], THREAD_PRIORITY_NORMAL);
		ResumeThread(m_hWorkerThread[i]); 
	}

	// Wait for partition thread exit
	WaitForMultipleObjects(m_WorkerThreadNum, m_hWorkerThread, TRUE, INFINITE); 
	for(DWORD i = 0; i < m_WorkerThreadNum; i++ )
	{  
		CloseHandle(m_hWorkerThread[i]);
	}


	rp.PartitionNumR = m_FanIns.size();
	rp.PartitionCpuTimeR[1] = GetCpuTime(); 
	rp.PartitionExecTimeR = stwPartitionTime.NowInMilliseconds();  
	rp.PartitionThreadNum = m_WorkerThreadNum;
	// Start merge everything of R
	m_R = m_FanIns;


	StopWatch  stwMergeTime;
	stwMergeTime.Start(); 
	rp.MergeCpuTimeR[0] = GetCpuTime(); 

	rc = MergePhase_Merge(m_R, m_PsmjRpParams.R_KEY_POS);
	if(rc!=SUCCESS) {return rc;}

	m_RRangePartition = m_RangePartition;
	m_RangePartition.clear();

	rp.MergeCpuTimeR[1] = GetCpuTime(); 
	rp.MergeExecTimeR = stwMergeTime.NowInMilliseconds(); 

	return SUCCESS; 
}

/// <summary>
/// Sorts the table S.
/// </summary>
/// <returns></returns>
RC PSMJRP::SortTableS() 
{
	g_Logger.Write("Sort S...\n");
	RC rc;
	isR	= FALSE;
	bufferPool.currentSize = 0; 
	while(!m_FanIns.empty()) m_FanIns.pop();

	StopWatch  stwPartitionTime;
	stwPartitionTime.Start(); 
	rp.PartitionCpuTimeS[0] = GetCpuTime(); 

	m_hSourceTable=CreateFile(
		(LPCWSTR)m_PsmjRpParams.RELATION_S_PATH,
		GENERIC_READ,
		FILE_SHARE_READ,
		NULL,
		OPEN_EXISTING,
		FILE_FLAG_OVERLAPPED,  
		NULL); 

	if(INVALID_HANDLE_VALUE==m_hSourceTable)
	{  
		ShowMB(L"Cannot create handle %s", m_PsmjRpParams.RELATION_S_PATH);
		return ERR_CANNOT_CREATE_HANDLE; 
	}  

	/* Get source table size */
	LARGE_INTEGER *liFileSize = new LARGE_INTEGER();  
	if (!GetFileSizeEx(m_hSourceTable, liFileSize))
	{    
		return ERR_CANNOT_GET_FILE_SIZE;
	} 

	rp.SourceTableSizeS = liFileSize->QuadPart;

	m_hWorkerThread = new HANDLE[m_WorkerThreadNum];
	m_PartitionParams = new PartitionPhaseParams[m_WorkerThreadNum];

	// Init mark current run
	for(UINT tIdx=0; tIdx < m_WorkerThreadNum; tIdx++)
	{
		m_PartitionParams[tIdx].currentMark = 0; 

		m_PartitionParams[tIdx].overlapRead.dwBytesToReadWrite = m_PsmjRpParams.SORT_READ_BUFFER_SIZE;  
		m_PartitionParams[tIdx].overlapRead.dwBytesReadWritten = 0;  
		m_PartitionParams[tIdx].overlapRead.startChunk = 0;  
		m_PartitionParams[tIdx].overlapRead.chunkIndex = 0;  
		m_PartitionParams[tIdx].overlapRead.endChunk = 0;  
		m_PartitionParams[tIdx].overlapRead.totalChunk = 0; 
		m_PartitionParams[tIdx].overlapRead.overlap.Offset = 0; 
		m_PartitionParams[tIdx].overlapRead.overlap.OffsetHigh = 0; 

		m_PartitionParams[tIdx].overlapWrite.dwBytesToReadWrite = m_PsmjRpParams.SORT_WRITE_BUFFER_SIZE; 
		m_PartitionParams[tIdx].overlapWrite.dwBytesReadWritten = 0; 
		m_PartitionParams[tIdx].overlapWrite.overlap.Offset = 0;
		m_PartitionParams[tIdx].overlapWrite.overlap.OffsetHigh = 0;
	}

	/* Set up overlap structure to READ source table */
	DWORD chunkSize = m_PsmjRpParams.SORT_READ_BUFFER_SIZE;
	DWORD totalChunk = chROUNDUP(liFileSize->QuadPart, chunkSize) / chunkSize; 

	DWORD temp = totalChunk;
	while (temp > 0)
	{
		for (DWORD i = 0; i < m_WorkerThreadNum; i++)
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
		for (DWORD i = 0; i < m_WorkerThreadNum; i++)
		{ 
			m_PartitionParams[i].overlapRead.startChunk = startChunk; 
			m_PartitionParams[i].overlapRead.chunkIndex = startChunk; 
			startChunk += m_PartitionParams[i].overlapRead.totalChunk;
			m_PartitionParams[i].overlapRead.endChunk = startChunk - 1;  
			temp -= m_PartitionParams[i].overlapRead.totalChunk;
			if(temp==0) { break; }
		}  	
	}
	//////////////////////////////////////////////////////////////////////////
	// Compute Heap size
	DOUBLE memFree = m_PsmjRpParams.BUFFER_POOL_SIZE - bufferPool.currentSize; 
	memFree = memFree - m_WorkerThreadNum * (m_PsmjRpParams.SORT_READ_BUFFER_SIZE + m_PsmjRpParams.SORT_WRITE_BUFFER_SIZE + SSD_PAGE_SIZE*2);

	if(memFree<=0)
		return ERR_NOT_ENOUGH_MEMORY;

	DWORD  heapSize = chROUNDDOWN( (DWORD)memFree / m_WorkerThreadNum, TUPLE_SIZE) / TUPLE_SIZE; 

	//////////////////////////////////////////////////////////////////////////
	// Init default value
	for (DWORD i = 0; i < m_WorkerThreadNum; i++)
	{
		m_PartitionParams[i]._this = this; ///////////// Importance 
		m_PartitionParams[i].keyPos = m_PsmjRpParams.S_KEY_POS;   
		m_PartitionParams[i].fanPath = new TCHAR[MAX_PATH];


		rc = utl->InitBuffer(m_PartitionParams[i].readBuffer, m_PsmjRpParams.SORT_READ_BUFFER_SIZE, &bufferPool);
		if(rc!=SUCCESS) {return rc;}  

		rc = utl->InitBuffer(m_PartitionParams[i].writeBuffer, m_PsmjRpParams.SORT_WRITE_BUFFER_SIZE, &bufferPool); 
		if(rc!=SUCCESS) {return rc;}
		//////////////////////////////////////////////////////////////////////////

		// Init run buffer
		rc = utl->InitBuffer(m_PartitionParams[i].readPageBuffer, SSD_PAGE_SIZE, &bufferPool); 
		if(rc!=SUCCESS) {return rc;}
		rc = utl->InitBuffer(m_PartitionParams[i].writePageBuffer, SSD_PAGE_SIZE, &bufferPool); 
		if(rc!=SUCCESS) {return rc;}

		//////////////////////////////////////////////////////////////////////////
		// Init runpage 
		rc = utl->InitRunPage(m_PartitionParams[i].readPagePtr, m_PartitionParams[i].readPageBuffer); 
		if(rc!=SUCCESS) {return rc;}
		rc = utl->InitRunPage(m_PartitionParams[i].writePagePtr, m_PartitionParams[i].writePageBuffer); 
		if(rc!=SUCCESS) {return rc;}

		//////////////////////////////////////////////////////////////////////////
		//Working space buffer 
		//////////////////////////////////////////////////////////////////////////

		rc = utl->InitBuffer(m_PartitionParams[i].heapBuffer, heapSize * TUPLE_SIZE, &bufferPool); 
		if(rc!=SUCCESS) {return rc;}

		m_PartitionParams[i].heapSize = heapSize;
	} 


	//////////////////////////////////////////////////////////////////////////

	for(DWORD i = 0; i < m_WorkerThreadNum; i++ )
	{  
		m_hWorkerThread[i] = CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)PartitionPhase_Ex, (LPVOID)&(m_PartitionParams[i]), CREATE_SUSPENDED, NULL);

		SetThreadPriority(m_hWorkerThread[i], THREAD_PRIORITY_NORMAL);
		ResumeThread(m_hWorkerThread[i]); 
	}

	// Wait for partition thread exit
	WaitForMultipleObjects(m_WorkerThreadNum, m_hWorkerThread, TRUE, INFINITE); 
	for(DWORD i = 0; i < m_WorkerThreadNum; i++ )
	{  
		CloseHandle(m_hWorkerThread[i]);
	}

	rp.PartitionCpuTimeS[1] = GetCpuTime(); 
	rp.PartitionExecTimeS = stwPartitionTime.NowInMilliseconds(); 
	rp.PartitionNumS = m_FanIns.size();
	rp.PartitionThreadNum = m_WorkerThreadNum;
	m_S = m_FanIns; 

	// Start merge everything of R
	//while(m_FanIns.size() > 0)
	//{
	//	FANS *fan = new FANS();
	//	fan = m_FanIns.front(); // First in first out
	//	m_FanIns.pop(); 
	//	m_S.push(fan);  
	//} 



	StopWatch  stwMergeTime;
	stwMergeTime.Start(); 
	rp.MergeCpuTimeS[0] = GetCpuTime();

	rc = MergePhase_Merge(m_S, m_PsmjRpParams.S_KEY_POS);
	if(rc!=SUCCESS) {return rc;}

	m_SRangePartition = m_RangePartition;
	m_RangePartition.clear();

	rp.MergeCpuTimeS[1] = GetCpuTime(); 
	rp.MergeExecTimeS = stwMergeTime.NowInMilliseconds(); 

	return SUCCESS; 
}


/// <summary>
/// Joins the tables.
/// </summary>
/// <returns></returns>
RC PSMJRP::JoinTable() 
{
	g_Logger.Write("Join R vs S...\n");
	RC rc; 
	rp.JoinThreadNum = m_WorkerThreadNum;

	rc = JoinPhase_Initialize();
	if(rc!=SUCCESS) { return rc; } 

	// Start working
	for(UINT i=0; i < m_WorkerThreadNum; i++ )
	{  
		m_hWorkerThread[i] = CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)JoinPhase_Ex, (LPVOID)&(m_JoinParams[i]), CREATE_SUSPENDED, NULL); 
		SetThreadPriority(m_hWorkerThread[i], THREAD_PRIORITY_NORMAL);
		ResumeThread(m_hWorkerThread[i]); 
	}

	// Wait for worker thread finish
	WaitForMultipleObjects(m_WorkerThreadNum, m_hWorkerThread, TRUE, INFINITE); 

	FILE* fso=fopen("C:\\skip.csv","w+b");
	CHAR *debugContent = new CHAR[1024];
	for(UINT tIdx=0; tIdx < m_WorkerThreadNum; tIdx++)
	{   
		sprintf(debugContent, "%d, %d\n", m_JoinParams[tIdx].ReadSkipCountS, m_JoinParams[tIdx].ReadCountS);  fprintf(fso, debugContent);  
	}  
	delete debugContent;
	fclose(fso); 


	// Cleaning
	CloseHandle(hR);
	CloseHandle(hS); 
	for(DWORD i = 0; i < m_WorkerThreadNum; i++ )
	{
		CloseHandle(m_hWorkerThread[i]);
	}

	return SUCCESS; 
}
 
/// <summary>
/// Wrapper for partition phase thread.
/// </summary>
/// <param name="lpParam">The thread parameter.</param>
/// <returns></returns>
DWORD WINAPI PSMJRP::PartitionPhase_Ex(LPVOID lpParam)
{
	PartitionPhaseParams* p = (PartitionPhaseParams*)(lpParam);
	p->_this->PartitionPhase_Func((LPVOID)(p));
	return 0;
}
 
/// <summary>
/// Partition phase thread function.
/// </summary>
/// <param name="lpParams">The thread parameters.</param>
/// <returns></returns>
DWORD WINAPI  PSMJRP::PartitionPhase_Func(LPVOID lpParams)
{
	RC rc;
	PartitionPhaseParams *p = (PartitionPhaseParams*)lpParams;

	RECORD *pTempRecord = new RECORD(); /* Working record */
	RECORD *pOutRecord = new RECORD(); /* Output record from heap */
	RECORD *pInRecord = new RECORD(); /* Addition record fill in heap */

	/* Init heap tree*/
	DWORD HEAP_SIZE = p->heapSize; 

	RECORD **heap = new RECORD *[HEAP_SIZE];
	for (UINT i = 0; i < HEAP_SIZE; i++)    
	{ 
		// Alloc memory for heap items
		heap[i]  = new RECORD(0, p->heapBuffer.data + TUPLE_SIZE * i, TUPLE_SIZE); 
		heap[i]->mark = p->currentMark;
	}

	//First read into buffer  
	PartitionPhase_Read(p);

	if(p->overlapRead.dwBytesReadWritten==0  || p->overlapRead.chunkIndex >= p->overlapRead.endChunk)
	{ 
		// Buffer empty, mark it with max page
		utl->AddPageMAXToBuffer(p->readPageBuffer, SSD_PAGE_SIZE); 
		utl->AddPageToBuffer(p->readBuffer, p->readPageBuffer.data, SSD_PAGE_SIZE);
		//utl->GetPageInfo(partitionParams.readPageBuffer.data, BACK_BUFFER(partitionParams.dbcRead), 0, SSD_PAGE_SIZE);
		p->readPageBuffer.isFullMaxValue = TRUE;
		p->readPageBuffer.currentTupleIndex=1;   
	}
	else
	{
		// what do we have in this buffer
		utl->ComputeBuffer(p->readBuffer, p->overlapRead.dwBytesReadWritten, SSD_PAGE_SIZE);
	}

	utl->GetPageInfo(p->readBuffer.data, p->readPageBuffer, 0, SSD_PAGE_SIZE);
	p->readBuffer.currentPageIndex = 0;
	p->readPageBuffer.currentTupleIndex=1;

	// Add tuple to heap
	for (UINT i = 0; i < HEAP_SIZE; i++)     
	{ 
		PartitionPhase_GetNextRecord(p, pTempRecord);  
		if(pTempRecord->key==MAX)
		{
			//#pragma chMSG(Fix problem memory too large)
			// fix current heapsize to runtime value
			HEAP_SIZE = i;
			if(isR)
				rp.HeapSizeR = HEAP_SIZE; 
			else 
				rp.HeapSizeS = HEAP_SIZE; 
			break;
		}
		pTempRecord->mark = p->currentMark;
		CopyRecord(heap[i], pTempRecord);  
	}

	/* Create first run handle and name */
	PartitionPhase_CreateNewRun(p); 

	/* Init fanIn counter */ 
	DWORD lastKey = 0; 
	DWORD pageCount = 0;
	DWORD tupleCount = 0;
	DWORD lowestKey, highestKey;  /* the min and max key in run file */
	BOOL    lowestKeyIsGet = FALSE;
	DWORD   lastNode = HEAP_SIZE-1;
	BOOL    isDone = FALSE; 

	p->writePagePtr->consumed = TRUE;

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
			PartitionPhase_GetNextRecord(p, pInRecord);  

			/* Read input file EOF */
			if(pInRecord->key==MAX)
			{
				isDone= TRUE;
				break;
			} 

			std::swap( heap[0], heap[lastNode] );  //heap[LastNode] is minimum item 

			/* Check new run or not ? */
			if (heap[1]->mark != p->currentMark) 
			{    
				// At this point, push all data exist in write buffer to disk and terminate current run
				if(p->writePagePtr->consumed==FALSE)
				{
					utl->AddPageToBuffer(p->writeBuffer, p->writePagePtr->page, SSD_PAGE_SIZE); //4 
					p->writeBuffer.currentPageIndex++; 
				}

				if(p->writeBuffer.currentSize > 0)
				{ 
					DWORD dwByteWritten = 0; 
					WriteFile(p->hWrite, 
						p->writeBuffer.data, 
						p->writeBuffer.currentSize, 
						&dwByteWritten, 
						NULL);

					utl->ResetBuffer(p->writeBuffer);
				}

				utl->ResetRunPage(p->writePagePtr, p->writePageBuffer); 

				// Terminate current run
				CloseHandle(p->hWrite);  

				// Create new run

				/* Save current run info */  
				PartitionPhase_SaveFanIn(p, lowestKey, highestKey, tupleCount, pageCount); 

				PartitionPhase_CreateNewRun(p); // Get name, begin a new run 

				/* Reset counter for next run */  
				pageCount = 0; tupleCount = 0; lowestKey = 0; highestKey = 0;
				lowestKeyIsGet = FALSE;

				/* Reverse the mark so that all the elements in the heap will be available for the next run */ 
				p->currentMark = heap[1]->mark;

				break;
			}

			/* Get element has minimum key */   
			//pOutRecord = heap[lastNode];  
			CopyRecord(pOutRecord, heap[lastNode]);
			/* Tracking the lowest key, highest key */
			lastKey = pOutRecord->key;  
			if(lowestKeyIsGet==FALSE)
			{
				lowestKeyIsGet = TRUE;
				lowestKey = lastKey;
			}

			highestKey = lastKey;

			/* Send record has minimum key to buffer*/ 
			PartitionPhase_SentToOutput(p, pOutRecord, pageCount); 

			tupleCount++;

			/* Determine if the newly read record belongs in the current run  or the next run. */  
			if(pInRecord->key >= lastKey)
			{ 
				pInRecord->mark = p->currentMark;  
				CopyRecord(heap[lastNode], pInRecord);
			}
			else
			{
				/* New record cannot belong to current run */ 
				pInRecord->mark = p->currentMark ^ 1;  

				CopyRecord(heap[lastNode], pInRecord);

				/* Continue heap sort */
				lastNode--; 
			}  

			MinHeapify(heap, 0,  lastNode); 
		}    
	} 

	/* Push all records still in memory to disk */
	while( lastNode > 0 )
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
		PartitionPhase_SentToOutput(p, pOutRecord, pageCount);

		tupleCount++;
		lastNode--; 
		MinHeapify(heap, 0,  lastNode); 
	}

	if(p->writeBuffer.currentSize > 0)
	{
		if(p->writePagePtr->consumed==FALSE)
		{
			utl->AddPageToBuffer(p->writeBuffer, p->writePagePtr->page, SSD_PAGE_SIZE); //4 
			p->writeBuffer.currentPageIndex++; 
			pageCount++;
		} 

		DWORD dwByteWritten = 0; 
		WriteFile(p->hWrite, 
			p->writeBuffer.data, 
			p->writeBuffer.currentSize, 
			&dwByteWritten, 
			NULL);

		utl->ResetBuffer(p->writeBuffer);

		utl->ResetRunPage(p->writePagePtr, p->writePageBuffer);  
	}

	/* Terminate final run */    
	PartitionPhase_SaveFanIn(p, lowestKey, highestKey, tupleCount, pageCount);
	CloseHandle(p->hWrite);  

	m_CS.Lock();
	while (p->fanIns.size()) 
	{
		FANS *threadFanIn = p->fanIns.front();
		p->fanIns.pop();
		m_FanIns.push(threadFanIn);  
	} 
	m_CS.UnLock();


	delete pTempRecord;
	delete pInRecord;
	delete pOutRecord;


	return 0; 
}

/// <summary>
/// Create new run.
/// </summary>
/// <param name="lpParams">The thread parameters.</param>
/// <returns></returns>
RC PSMJRP::PartitionPhase_CreateNewRun(LPVOID lpParams)
{
	/* Get fanIn name for heap tree */
	PartitionPhaseParams *p = (PartitionPhaseParams*)lpParams;

	GetFanPath(p->fanPath);  

	p->hWrite=CreateFile((LPCWSTR)p->fanPath,		// file to write
		GENERIC_WRITE,			// open for writing
		0,						// Do not share
		NULL,					// default security
		CREATE_ALWAYS,			// Overwrite existing
		FILE_ATTRIBUTE_NORMAL,	// overlapped operation // FILE_FLAG_OVERLAPPED
		NULL);					// no attr. template

	if (INVALID_HANDLE_VALUE==p->hWrite) 
	{ 
		ShowMB(L"Cannot create output file %s", p->fanPath);    
		return ERR_CANNOT_CREATE_HANDLE; 
	}  

	return SUCCESS;
}

/// <summary>
/// Save fan-in info.
/// </summary>
/// <param name="lpParams">The thread parameters.</param>
/// <param name="lowestKey">The lowest key.</param>
/// <param name="highestKey">The highest key.</param>
/// <param name="tupleCount">The tuple count.</param>
/// <param name="pageCount">The page count.</param>
/// <returns></returns>
RC PSMJRP::PartitionPhase_SaveFanIn(LPVOID lpParams, DWORD lowestKey, DWORD highestKey, DWORD tupleCount, DWORD pageCount)
{
	PartitionPhaseParams *p = (PartitionPhaseParams*)lpParams;

	FANS *_fan = new FANS();
	wcscpy(_fan->fileName, p->fanPath);
	_fan->lowestKey = lowestKey;
	_fan->highestKey = highestKey;
	_fan->tupleCount = tupleCount;
	_fan->pageCount = pageCount;
	_fan->fileSize.QuadPart = pageCount * SSD_PAGE_SIZE;
	p->fanIns.push(_fan); 

	return SUCCESS;
}

/// <summary>
/// Read from disk into buffer.
/// </summary>
/// <param name="lpParams">The thread parameters.</param>
/// <returns></returns>
RC PSMJRP::PartitionPhase_Read(LPVOID lpParams)
{     
	PartitionPhaseParams *p = (PartitionPhaseParams*)lpParams;

	// If current thread chunk to be read is over chunk has been allocated we mark them with MAX page
	if ( p->overlapRead.chunkIndex > p->overlapRead.endChunk )  
	{   
		utl->ResetBuffer(p->readBuffer);
		// Add max page to buffer
		utl->AddPageMAXToBuffer(p->readBuffer, SSD_PAGE_SIZE); 

		p->readBuffer.currentPageIndex = 0;
		p->readBuffer.currentSize = SSD_PAGE_SIZE;
		p->readBuffer.pageCount = 1;
		p->readBuffer.tupleCount = utl->GetTupleNumInMaxPage(); 
		p->readBuffer.isSort = TRUE;
		p->readBuffer.isFullMaxValue = TRUE;
		return ERR_END_OF_FILE;
	}  


	// Read next chunk, for read file larger than 4GB
	p->overlapRead.fileSize.QuadPart = p->overlapRead.chunkIndex * p->overlapRead.dwBytesToReadWrite;
	p->overlapRead.overlap.Offset = p->overlapRead.fileSize.LowPart;
	p->overlapRead.overlap.OffsetHigh = p->overlapRead.fileSize.HighPart;

	/* Reset first */  
	utl->ResetBuffer(p->readBuffer);  

	// sync read
	ReadFile(m_hSourceTable, 
		p->readBuffer.data, 
		p->overlapRead.dwBytesToReadWrite, 
		&p->overlapRead.dwBytesReadWritten,  
		&p->overlapRead.overlap);

	// Wait for Read complete 
	GetOverlappedResult(m_hSourceTable, 
		&p->overlapRead.overlap, 
		&p->overlapRead.dwBytesReadWritten, 
		TRUE);  

	// increase current chunk index by 1
	p->overlapRead.chunkIndex++; 


	return SUCCESS;
}


/// <summary>
/// Get next record in buffer.
/// </summary>
/// <param name="lpParams">The thread parameters.</param>
/// <param name="recordPtr">The record PTR.</param>
/// <returns></returns>
RC PSMJRP::PartitionPhase_GetNextRecord(LPVOID lpParams, RECORD *&recordPtr)
{  
	PartitionPhaseParams *p = (PartitionPhaseParams*)lpParams;

	if(p->readPageBuffer.currentTupleIndex > p->readPageBuffer.tupleCount)
	{
		utl->ResetBuffer(p->readPageBuffer); 
		p->readBuffer.currentPageIndex++; // read next page 
		if(p->readBuffer.currentPageIndex >= p->readBuffer.pageCount)
		{  
			PartitionPhase_Read(p);  

			if(p->overlapRead.dwBytesReadWritten==0  || p->overlapRead.chunkIndex >= p->overlapRead.endChunk) 
			{ 
				// at this point, this file have complete read, mark this buffer with max page
				utl->AddPageMAXToBuffer(p->readPageBuffer, SSD_PAGE_SIZE); 
				utl->AddPageToBuffer(p->readBuffer, p->readPageBuffer.data, SSD_PAGE_SIZE); 
				p->readPageBuffer.isFullMaxValue = TRUE;
				p->readPageBuffer.currentTupleIndex=1;  

				// Set current tuple with MAX values avoid infinitve loop
				utl->SetMaxTuple(recordPtr);

				// exit this function now
				return SUCCESS;
			} 
			else
			{
				// Let see what we have in this buffer
				utl->ComputeBuffer(p->readBuffer, p->overlapRead.dwBytesReadWritten, SSD_PAGE_SIZE); 
			}  

			// Reset page index to zero
			//partitionParams.readBuffer.currentPageIndex = 0; 
		} 

		utl->GetPageInfo(p->readBuffer.data, p->readPageBuffer, p->readBuffer.currentPageIndex, SSD_PAGE_SIZE); 
		p->readPageBuffer.currentTupleIndex = 1; 
	}

	utl->GetTupleInfo(recordPtr, p->readPageBuffer.currentTupleIndex, p->readPageBuffer.data, SSD_PAGE_SIZE, p->keyPos);
	p->readPageBuffer.currentTupleIndex++;

	return SUCCESS;
}


/// <summary>
/// Sent tuple to output buffer.
/// </summary>
/// <param name="lpParams">The lp parameters.</param>
/// <param name="recordPtr">The record PTR.</param>
/// <param name="pageCount">The page count.</param>
/// <returns></returns>
RC PSMJRP::PartitionPhase_SentToOutput(LPVOID lpParams, RECORD *recordPtr, DWORD &pageCount)
{  
	PartitionPhaseParams *p = (PartitionPhaseParams*)lpParams;

	if(utl->IsPageFull(p->writePagePtr))
	{   
		utl->AddPageToBuffer(p->writeBuffer, p->writePagePtr->page, SSD_PAGE_SIZE); //4 
		p->writeBuffer.currentPageIndex++; 
		utl->ResetRunPage(p->writePagePtr, p->writePageBuffer);  
		p->writePagePtr->consumed = TRUE;
		pageCount++;
	}  

	if(utl->IsBufferFull(p->writeBuffer))  
	{  
		DWORD dwByteWritten = 0; 
		WriteFile(p->hWrite, 
			p->writeBuffer.data, 
			p->writeBuffer.currentSize, 
			&dwByteWritten, 
			NULL);

		utl->ResetBuffer(p->writeBuffer);
	}

	utl->AddTupleToPage(p->writePagePtr, recordPtr, p->writePageBuffer);   // Add this tuples to page   
	p->writePagePtr->consumed = FALSE;

	return SUCCESS;
}

/// <summary>
/// Read from disk into buffer.
/// </summary>
/// <param name="runIdx">Index of the run.</param>
/// <returns></returns>
RC PSMJRP::MergePhase_Read(DWORD runIdx)
{    
	MergePhaseParams *p = m_FinalMergeParams;

	LARGE_INTEGER chunk = {0};
	chunk.QuadPart = p->overlapRead[runIdx].chunkIndex * m_PsmjRpParams.MERGE_READ_BUFFER_SIZE;   
	p->overlapRead[runIdx].overlap.Offset = chunk.LowPart;
	p->overlapRead[runIdx].overlap.OffsetHigh = chunk.HighPart;  

	// Attempt an asynchronous read operation.  
	ReadFile(p->hFanIn[runIdx], 
		BACK_BUFFER(p->dbcRead[runIdx]).data, 
		p->overlapRead[runIdx].dwBytesToReadWrite, 
		&p->overlapRead[runIdx].dwBytesReadWritten, 
		&p->overlapRead[runIdx].overlap);

	return SUCCESS; 
}


// Merge all sorted runs on diks to single sorted run
RC PSMJRP::MergePhase_Merge(std::queue<FANS*> &runQueue, const DWORD keyPos)
{
	g_Logger.Write("Start merge...\n");
	RC rc;
	m_FinalMergeParams = new MergePhaseParams();
	m_FinalMergeParams->_this = this;
	m_FinalMergeParams->keyPos = keyPos;

	MergePhaseParams *p = m_FinalMergeParams; // shorter name

	// Reset buffer pool
	bufferPool.currentSize = 0;  

	DWORD k = runQueue.size(); 
	LoserTree lsTree(k); // Init loser tree
	RECORD *recordPtr = new RECORD();  
	std::queue<FANS*> fanInPendingDelete;    

	//#pragma chMSG(need to check memory here to merge in single merge pass)
	//////////////////////////////////////////////////////////////////////////
	// Fisrt check with 128K for read, if satisfy then use rest of memory for write
	DWORD readBufferSize = 0, writeBufferSize = 0;
	readBufferSize = m_PsmjRpParams.MERGE_READ_BUFFER_SIZE; //128K
	writeBufferSize = m_PsmjRpParams.MERGE_WRITE_BUFFER_SIZE;
	DWORD memForPages = k*SSD_PAGE_SIZE + SSD_PAGE_SIZE;
	DWORD memForWork = bufferPool.size - memForPages;

	BOOL isOK = FALSE;
	while(!isOK)
	{
		DWORD memforRead = 0;
		for (UINT i=0; i<k; i++)
		{
			memforRead+=readBufferSize * 2;
		}

		if(memforRead > memForWork)
		{
			readBufferSize = (readBufferSize) / 2;
		}
		else
		{
			DWORD memFree = memForWork - memforRead;
			DWORD memforWrite = chROUNDDOWN( memFree / 2, SSD_PAGE_SIZE); 
			if(memforWrite < SSD_PAGE_SIZE * 256 * 4) // 8MB for write
			{
				readBufferSize = (readBufferSize) / 2;
				isOK = FALSE;

				if(readBufferSize < SSD_PAGE_SIZE)
				{
					// Not enough mem
					ShowMB(L"Memory for read fanIn smaller than 4096. Too many fanIn ->Exit\r\n");
					return ERR_NOT_ENOUGH_MEMORY;
				}
			}
			else
			{
				if(memforWrite > 4096 * 256 * 32) // 32MB
				{
					memforWrite = 4096 * 256 * 32; 
				}
				isOK = TRUE;
			}
			writeBufferSize = memforWrite;
		} 
	}

	// for compare with psmj
	//if(readBufferSize >= 4096) //   
	//{
	//	readBufferSize = 4096; 
	//}

	// for compare with psmj
	if(writeBufferSize >= 4096 * 256 * 2) //  2MB
	{
		writeBufferSize = 4096 * 256 * 2; 
	}

	m_PsmjRpParams.MERGE_READ_BUFFER_SIZE = readBufferSize;
	m_PsmjRpParams.MERGE_WRITE_BUFFER_SIZE = writeBufferSize;
	//////////////////////////////////////////////////////////////////////////


	// Create handle to merge multiple Fan-In
	p->hFanIn = new HANDLE[k];
	p->overlapRead = new OVERLAPPEDEX[k];
	LARGE_INTEGER liFanOutSize = {0};
	for (DWORD runIdx=0; runIdx < k; runIdx++)
	{  
		FANS *_fanIn = runQueue.front(); 
		runQueue.pop();

		fanInPendingDelete.push(_fanIn);

		p->hFanIn[runIdx] = CreateFile(
			(LPCWSTR)_fanIn->fileName, // file to open
			GENERIC_READ,			// open for reading
			0,						//  do not share for reading
			NULL,					// default security
			OPEN_EXISTING,			// existing file only
			FILE_FLAG_OVERLAPPED | FILE_FLAG_NO_BUFFERING,	// overlapped operation //| FILE_FLAG_NO_BUFFERING
			NULL);					// no attr. template

		if (INVALID_HANDLE_VALUE==p->hFanIn[runIdx]) 
		{   
			ShowMB(L"Cannot open file %s\r\n", _fanIn->fileName);
			return ERR_CANNOT_CREATE_HANDLE; 
		} 

		UINT64 fanInSize = GetFanSize(p->hFanIn[runIdx]); 
		liFanOutSize.QuadPart+= fanInSize;

		DWORD chunkSize = readBufferSize; 
		DWORD totalChunk = chROUNDUP(fanInSize, chunkSize) / chunkSize ;

		// Init overlapped struct for read Fan-In
		p->overlapRead[runIdx].dwBytesToReadWrite = readBufferSize;  
		p->overlapRead[runIdx].dwBytesReadWritten = 0;  
		p->overlapRead[runIdx].startChunk = 0;  
		p->overlapRead[runIdx].chunkIndex = 0;  
		p->overlapRead[runIdx].endChunk = totalChunk;  
		p->overlapRead[runIdx].totalChunk = totalChunk; 
		p->overlapRead[runIdx].overlap.Offset = 0; 
		p->overlapRead[runIdx].overlap.OffsetHigh = 0;  	
	}

	FANS *finalFan = new FANS(); 
	GetFanPath(finalFan->fileName);

	p->hFanOut = CreateFile(
		(LPCWSTR)finalFan->fileName, // file to open
		GENERIC_WRITE,			// open for write
		0,						// donot share
		NULL,					// default security
		CREATE_ALWAYS,			// create new file
		FILE_FLAG_OVERLAPPED | FILE_FLAG_NO_BUFFERING,	// overlapped operation //| FILE_FLAG_NO_BUFFERING
		NULL);					// no attr. template

	if (INVALID_HANDLE_VALUE==p->hFanOut) 
	{   
		ShowMB(L"Cannot open file %s\r\n", finalFan->fileName);
		return ERR_CANNOT_CREATE_HANDLE; 
	} 

	// Overlap struct for write Fan-Out
	p->overlapWrite.dwBytesToReadWrite = writeBufferSize; 
	p->overlapWrite.dwBytesReadWritten = 0; 
	p->overlapWrite.overlap.Offset = 0;
	p->overlapWrite.overlap.OffsetHigh = 0;

	if(isR)
	{
		rp.MergeReadBufferSizeR = readBufferSize;
		rp.MergeWriteBufferSizeR = writeBufferSize;
	}
	else
	{
		rp.MergeReadBufferSizeS = readBufferSize;
		rp.MergeWriteBufferSizeS = writeBufferSize;
	}

	// Init write buffer size
	p->dbcWrite = new DoubleBuffer(writeBufferSize); 
	rc = utl->InitBuffer(p->dbcWrite->buffer[0], writeBufferSize, &bufferPool);
	if(rc!=SUCCESS) {return rc;}
	rc = utl->InitBuffer(p->dbcWrite->buffer[1], writeBufferSize, &bufferPool);  
	if(rc!=SUCCESS) {return rc;}


	// Init run buffer
	rc = utl->InitBuffer(p->runPageBuffer, SSD_PAGE_SIZE, &bufferPool); 
	if(rc!=SUCCESS) {return rc;}

	//////////////////////////////////////////////////////////////////////////
	// Init runpage 
	rc = utl->InitRunPage(p->runPage, p->runPageBuffer); 
	if(rc!=SUCCESS) {return rc;}

	// Init read buffer and merge buffer


	p->dbcRead = new DoubleBuffer*[k]; 
	p->mergePageBuffer = new Buffer[k];
	for (DWORD runIdx=0; runIdx < k; runIdx++)
	{ 
		p->dbcRead[runIdx] = new DoubleBuffer(readBufferSize); 
		rc = utl->InitBuffer(p->dbcRead[runIdx]->buffer[0], readBufferSize, &bufferPool);
		if(rc!=SUCCESS) {return rc;}
		rc = utl->InitBuffer(p->dbcRead[runIdx]->buffer[1], readBufferSize, &bufferPool);  
		if(rc!=SUCCESS) {return rc;}

		// Init merge page buffer
		rc = utl->InitBuffer(p->mergePageBuffer[runIdx], SSD_PAGE_SIZE, &bufferPool);  
		if(rc!=SUCCESS) {return rc;} 
	} 


	// File systems extend files synchronously. Extend the destination file 
	// now so that I/Os execute asynchronously improving performance. 
	////////////////////////////////////////////////////////////////////////// 
	LARGE_INTEGER liDestSize = { 0 };
	liDestSize.QuadPart = chROUNDUP(liFanOutSize.QuadPart, writeBufferSize);

	SetFilePointerEx(p->hFanOut, liDestSize, NULL, FILE_BEGIN);
	SetEndOfFile(p->hFanOut); 

	// First read from disk to buffer
	//////////////////////////////////////////////////////////////////////////
	for(DWORD runIdx=0; runIdx < k; runIdx++) 
	{
		MergePhase_Read(runIdx);     
	}


	// Prepare data for loser tree
	for(DWORD runIdx = 0; runIdx < k; runIdx++) 
	{     
		// Wait for read complete
		if(p->overlapRead[runIdx].chunkIndex < p->overlapRead[runIdx].totalChunk)
		{
			GetOverlappedResult(p->hFanIn[runIdx], 
				&p->overlapRead[runIdx].overlap,
				&p->overlapRead[runIdx].dwBytesReadWritten, 
				TRUE);  
		}

		p->overlapRead[runIdx].chunkIndex++; 
		utl->ComputeBuffer(BACK_BUFFER(p->dbcRead[runIdx]),  p->overlapRead[runIdx].dwBytesReadWritten);  

		utl->ResetBuffer(p->mergePageBuffer[runIdx]); 

		if(p->dbcRead[runIdx]->bFirstProduce==TRUE) { p->dbcRead[runIdx]->bFirstProduce=FALSE; } 

		utl->GetPageInfo(BACK_BUFFER(p->dbcRead[runIdx]).data, p->mergePageBuffer[runIdx], 0, SSD_PAGE_SIZE); 
		p->mergePageBuffer[runIdx].currentTupleIndex = 1;

		utl->GetTupleInfo(recordPtr, 
			p->mergePageBuffer[runIdx].currentTupleIndex, 
			p->mergePageBuffer[runIdx].data,
			SSD_PAGE_SIZE,
			p->keyPos);  

		p->mergePageBuffer[runIdx].currentTupleIndex++;

		lsTree.AddNewNode(recordPtr, runIdx);  

		//p->dbcRead[i]->UnLockProducer();  

		p->dbcRead[runIdx]->SwapBuffers();  // swap read buffer

		MergePhase_Read( runIdx );       
	}    

	lsTree.CreateLoserTree();  

	p->dbcWrite->bFirstProduce = TRUE;
	utl->ResetBuffer(BACK_BUFFER(p->dbcWrite));
	utl->ResetBuffer(FRONT_BUFFER(p->dbcWrite));

	utl->ResetRunPage(p->runPage, p->runPageBuffer);  

	p->dbcWrite->LockProducer();

	INT		lsIdx = 0; // index in loser tree point to minimum record 
	BOOL	bFirstWrite = TRUE;
	BOOL	bLowestIsGet = FALSE;  

	UINT64 partitionFileSize = 0;
	if(isR)
	{
		m_PartitionSize = bufferPool.size / m_WorkerThreadNum ; // memory for each thread
		m_PartitionSize = m_PartitionSize - 2 * m_PsmjRpParams.SORT_WRITE_BUFFER_SIZE; // memory for read
		m_PartitionSize = m_PartitionSize / 2; // use double buffer thus mus divive for two
		m_PartitionSize = chROUNDDOWN(m_PartitionSize, SSD_PAGE_SIZE);  // round down with page size
	}
	const DWORD numPageInPartititon = m_PartitionSize / SSD_PAGE_SIZE;

	UINT64 crrPageIdx = 0;
	UINT64 pageIdx = 0;

	BOOL lol = FALSE;
	DWORD lowestKey = 0;
	DWORD highestKey = 0;

	while(TRUE)  
	{   
		lsTree.GetMinRecord(recordPtr, lsIdx); // index = loserTree[0]

		if(recordPtr->key==MAX) { break; }

		if(bLowestIsGet==FALSE)
		{
			finalFan->lowestKey = recordPtr->key; 
			bLowestIsGet = TRUE;
		}

		if(lol==FALSE)
		{
			lowestKey = recordPtr->key; 
			lol = TRUE;
		}

		finalFan->highestKey = recordPtr->key;
		highestKey = recordPtr->key;

		p->runPage->consumed = FALSE;

		if(utl->IsPageFull(p->runPage))
		{  
			utl->AddPageToBuffer(BACK_BUFFER(p->dbcWrite),  p->runPage->page, SSD_PAGE_SIZE); 
			p->runPage->consumed = TRUE;

			// Reset page về ban đầu, bắt đầu ghép 1 page mới  
			utl->ResetRunPage(p->runPage, p->runPageBuffer);

			BACK_BUFFER(p->dbcWrite).currentPageIndex++;
			BACK_BUFFER(p->dbcWrite).pageCount++;

			crrPageIdx++;

			if(crrPageIdx>=numPageInPartititon)
			{
				RangePartition _myRange;
				_myRange.fileOffsetStart.QuadPart = pageIdx * SSD_PAGE_SIZE;
				_myRange.fileOffsetEnd.QuadPart = (pageIdx + crrPageIdx)  * SSD_PAGE_SIZE;
				_myRange.lowestKey = lowestKey;
				_myRange.highestKey = highestKey;

				m_RangePartition.push_back(_myRange);

				pageIdx += crrPageIdx;
				crrPageIdx = 0; 
				lol = FALSE;
			}
		} 

		if(utl->IsBufferFull(BACK_BUFFER(p->dbcWrite))) // write buffer is full
		{    
			p->dbcWrite->UnLockProducer();   

			if(p->dbcWrite->bFirstProduce) // Back Buffer is full, check is it the first product
			{ 
				p->dbcWrite->bFirstProduce=FALSE;  
				p->dbcWrite->SwapBuffers();  

				p->dbcWrite->LockConsumer();  
				WriteFile(p->hFanOut,  
					FRONT_BUFFER(p->dbcWrite).data,  
					FRONT_BUFFER(p->dbcWrite).currentSize,  
					&p->overlapWrite.dwBytesReadWritten,  
					&p->overlapWrite.overlap); 
			}
			else
			{    
				//Wait for write complete   
				GetOverlappedResult(p->hFanOut, 
					&p->overlapWrite.overlap, 
					&p->overlapWrite.dwBytesReadWritten, 
					TRUE) ;  

				p->overlapWrite.fileSize.QuadPart += p->overlapWrite.dwBytesReadWritten;  
				p->overlapWrite.overlap.Offset = p->overlapWrite.fileSize.LowPart;
				p->overlapWrite.overlap.OffsetHigh = p->overlapWrite.fileSize.HighPart;  
				p->overlapWrite.dwBytesReadWritten = 0;

				finalFan->pageCount += FRONT_BUFFER(p->dbcWrite).pageCount;
				finalFan->tupleCount += FRONT_BUFFER(p->dbcWrite).tupleCount;

				utl->ResetBuffer(FRONT_BUFFER(p->dbcWrite));  
				p->dbcWrite->UnLockConsumer(); 

				p->dbcWrite->SwapBuffers();  

				p->dbcWrite->LockConsumer();   
				WriteFile(p->hFanOut,  
					FRONT_BUFFER(p->dbcWrite).data,  
					FRONT_BUFFER(p->dbcWrite).currentSize,  
					&p->overlapWrite.dwBytesReadWritten,  
					&p->overlapWrite.overlap);
			}  
			p->dbcWrite->LockProducer();  
		} 

		utl->AddTupleToPage(p->runPage, recordPtr->data, p->runPageBuffer);  
		BACK_BUFFER(p->dbcWrite).tupleCount++;

		MergePhase_GetNextRecord(recordPtr, lsIdx); 

		lsTree.AddNewNode(recordPtr, lsIdx);// Add this tuple to loser tree  
		lsTree.Adjust(lsIdx); //Shrink loser tree 
	}   // end while

	//Wait until current write process complete 
	GetOverlappedResult(p->hFanOut,
		&p->overlapWrite.overlap, 
		&p->overlapWrite.dwBytesReadWritten,
		TRUE); 

	if(p->overlapWrite.dwBytesReadWritten > 0)
	{
		if(FRONT_BUFFER(p->dbcWrite).currentSize > 0)
		{
			p->overlapWrite.fileSize.QuadPart += p->overlapWrite.dwBytesReadWritten;  
			p->overlapWrite.overlap.Offset = p->overlapWrite.fileSize.LowPart;
			p->overlapWrite.overlap.OffsetHigh = p->overlapWrite.fileSize.HighPart;  

			finalFan->pageCount += FRONT_BUFFER(p->dbcWrite).pageCount;
			finalFan->tupleCount += FRONT_BUFFER(p->dbcWrite).tupleCount;

			p->overlapWrite.dwBytesReadWritten = 0;

			utl->ResetBuffer(FRONT_BUFFER(p->dbcWrite));  
		}
	}


	RangePartition _myRangeLast;
	_myRangeLast.fileOffsetStart.QuadPart = pageIdx * SSD_PAGE_SIZE;
	_myRangeLast.fileOffsetEnd.QuadPart = (pageIdx + crrPageIdx)  * SSD_PAGE_SIZE;
	_myRangeLast.lowestKey = lowestKey;
	_myRangeLast.highestKey = highestKey;

	m_RangePartition.push_back(_myRangeLast);

	pageIdx += crrPageIdx;
	crrPageIdx = 0; 

	//////////////////////////////////////////////////////////////////////////

	// If the last page has not consumed
	if((p->runPage->consumed==FALSE) && (utl->IsBufferFull(BACK_BUFFER(p->dbcWrite))==FALSE))
	{ 
		if(!utl->IsEmptyPage(p->runPage))
		{
			utl->AddPageToBuffer(BACK_BUFFER(p->dbcWrite),  p->runPage->page, SSD_PAGE_SIZE);  
			p->runPage->consumed = TRUE; 
			BACK_BUFFER(p->dbcWrite).currentPageIndex++;
			BACK_BUFFER(p->dbcWrite).pageCount++;    
		}
		else
		{
			//int fuck=0;
		}
	}

	p->dbcWrite->UnLockProducer();

	p->dbcWrite->UnLockConsumer();

	if(BACK_BUFFER(p->dbcWrite).currentSize > 0)
	{    
		//write completed , swap buffer
		p->dbcWrite->SwapBuffers();   
		// Write the last run to disk  
		p->dbcWrite->LockConsumer();  

		WriteFile(p->hFanOut,  
			FRONT_BUFFER(p->dbcWrite).data,  
			FRONT_BUFFER(p->dbcWrite).currentSize,  
			&p->overlapWrite.dwBytesReadWritten,  
			&p->overlapWrite.overlap);

		GetOverlappedResult(p->hFanOut,
			&p->overlapWrite.overlap,
			&p->overlapWrite.dwBytesReadWritten, 
			TRUE);

		p->overlapWrite.fileSize.QuadPart += p->overlapWrite.dwBytesReadWritten;  
		p->overlapWrite.overlap.Offset = p->overlapWrite.fileSize.LowPart;
		p->overlapWrite.overlap.OffsetHigh = p->overlapWrite.fileSize.HighPart;  

		finalFan->pageCount += FRONT_BUFFER(p->dbcWrite).pageCount;
		finalFan->tupleCount += FRONT_BUFFER(p->dbcWrite).tupleCount;

		utl->ResetBuffer(FRONT_BUFFER(p->dbcWrite)); 
		p->dbcWrite->UnLockConsumer();
	}

	//Reset read buffer
	for(DWORD runIdx = 0; runIdx < k; runIdx++) 
	{   
		utl->ResetBuffer(FRONT_BUFFER(p->dbcRead[runIdx])); 
		CloseHandle(p->hFanIn[runIdx]);
	}     

	// Clean
	SetFilePointerEx(p->hFanOut, p->overlapWrite.fileSize, NULL, FILE_BEGIN); 
	SetEndOfFile(p->hFanOut); 

	CloseHandle(p->hFanOut);

	// Push final fan to queue
	runQueue.push(finalFan);

	//if(fanInPendingDelete.size() > 0)
	//{
	//	while (fanInPendingDelete.size() > 0)
	//	{
	//		FANS *_byeBye = fanInPendingDelete.front();
	//		fanInPendingDelete.pop();
	//		if(!DeleteFile(_byeBye->fileName))
	//		{
	//			ShowMB(L"Cannot delete file %s\r\n", _byeBye->fileName); 
	//		} 
	//	} 
	//}

	return SUCCESS; 
} 

/// <summary>
/// Get next record from buffer.
/// </summary>
/// <param name="recordPtr">The record PTR.</param>
/// <param name="runIdx">Index of the run.</param>
/// <returns></returns>
RC PSMJRP::MergePhase_GetNextRecord(RECORD *&recordPtr, INT runIdx)
{     
	MergePhaseParams *p = m_FinalMergeParams;

	if(p->mergePageBuffer[runIdx].currentTupleIndex > p->mergePageBuffer[runIdx].tupleCount)  
	{
		// Read complete this page, need to fetch next page from disk  
		utl->ResetBuffer(p->mergePageBuffer[runIdx]);  
		FRONT_BUFFER(p->dbcRead[runIdx]).currentPageIndex++;   
		if(FRONT_BUFFER(p->dbcRead[runIdx]).currentPageIndex >= FRONT_BUFFER(p->dbcRead[runIdx]).pageCount)
		{    
			utl->ResetBuffer(FRONT_BUFFER(p->dbcRead[runIdx])); 

			// Waiting for reading in to back buffer completed, do not wait the last one
			if(p->overlapRead[runIdx].chunkIndex < p->overlapRead[runIdx].totalChunk)
				GetOverlappedResult(p->hFanIn[runIdx], 
				&p->overlapRead[runIdx].overlap, 
				&p->overlapRead[runIdx].dwBytesReadWritten, 
				TRUE); 

			p->overlapRead[runIdx].chunkIndex++;  
			if(p->overlapRead[runIdx].dwBytesReadWritten==0)
			{
				utl->ResetBuffer(BACK_BUFFER(p->dbcRead[runIdx]));
				utl->AddPageMAXToBuffer(p->mergePageBuffer[runIdx], SSD_PAGE_SIZE); 
				utl->AddPageToBuffer(BACK_BUFFER(p->dbcRead[runIdx]),
					p->mergePageBuffer[runIdx].data, 
					SSD_PAGE_SIZE); 

				p->mergePageBuffer[runIdx].isFullMaxValue = TRUE; 
				FRONT_BUFFER(p->dbcRead[runIdx]).currentPageIndex = 0;
			}
			else
			{
				utl->ComputeBuffer(BACK_BUFFER(p->dbcRead[runIdx]), 
					p->overlapRead[runIdx].dwBytesReadWritten);   
			}

			//Swap buffer to continue merge 
			p->dbcRead[runIdx]->SwapBuffers();  
			// read next chunk
			MergePhase_Read(runIdx);   
		}   

		utl->GetPageInfo(FRONT_BUFFER(p->dbcRead[runIdx]).data, 
			p->mergePageBuffer[runIdx], 
			FRONT_BUFFER(p->dbcRead[runIdx]).currentPageIndex, 
			SSD_PAGE_SIZE);   

		p->mergePageBuffer[runIdx].currentTupleIndex=1; 
	}

	utl->GetTupleInfo(recordPtr,
		p->mergePageBuffer[runIdx].currentTupleIndex, 
		p->mergePageBuffer[runIdx].data, 
		SSD_PAGE_SIZE,
		p->keyPos);  

	p->mergePageBuffer[runIdx].currentTupleIndex++;

	return SUCCESS; 
}

/// <summary>
/// Wrapper for join thread function.
/// </summary>
/// <param name="lpParam">The thread parameter.</param>
/// <returns></returns>
DWORD WINAPI PSMJRP::JoinPhase_Ex(LPVOID lpParam)
{ 
	JoinPhaseParams* p = (JoinPhaseParams*)lpParam;
	p->_this->JoinPhase_Func((LPVOID)(p)); 
	return 0;
}

 
/// <summary>
/// Join thread function.
/// </summary>
/// <param name="lpParam">The thread parameter.</param>
/// <returns></returns>
DWORD WINAPI PSMJRP::JoinPhase_Func(LPVOID lpParam)
{  
	JoinPhaseParams* p = (JoinPhaseParams*)lpParam; 

	DWORD dwBytesReadR = 0;
	DWORD dwBytesReadS = 0;
	OVERLAPPED overlapR = {0};
	OVERLAPPED overlapS = {0};

	// Do join operation 
	for(UINT rIdx=0; rIdx < p->R_FanInCount; rIdx++)
	{
		// Read R 
		utl->ResetBuffer(p->readBufferR); 

		overlapR.Offset = p->PartitionR[rIdx].fileOffsetStart.LowPart;
		overlapR.OffsetHigh = p->PartitionR[rIdx].fileOffsetStart.HighPart;
		ReadFile(hR, p->readBufferR.data, p->readBufferR.size, &dwBytesReadR, &overlapR); 
		GetOverlappedResult(hR, &overlapR, &dwBytesReadR, TRUE);
		utl->ComputeBuffer(p->readBufferR, dwBytesReadR);

		if(dwBytesReadR > 0)
		{
			// Join with all S
			for(UINT sIdx=0; sIdx < p->S_FanInCount; sIdx++)
			{ 
				// TODO: Check for key R and S are in range or not?
				if((p->PartitionR[rIdx].lowestKey > p->PartitionS[sIdx].highestKey) || ( p->PartitionS[sIdx].lowestKey > p->PartitionR[rIdx].highestKey))
				{ 
					// Skip this sIdx
					p->ReadSkipCountS++;
					continue;
				}

				// Read S  
				utl->ResetBuffer(p->readBufferS);

				overlapS.Offset = p->PartitionS[sIdx].fileOffsetStart.LowPart;
				overlapS.OffsetHigh = p->PartitionS[sIdx].fileOffsetStart.HighPart;
				ReadFile(hS, p->readBufferS.data, p->readBufferS.size, &dwBytesReadS, &overlapS);
				GetOverlappedResult(hS, &overlapS, &dwBytesReadS, TRUE); 
				utl->ComputeBuffer(p->readBufferS, dwBytesReadS);

				p->ReadCountS++;

				JoinPhase_Join(p); 
			}   
		} // dwBytesReadR > 0 
	} // for rIndex

	//Wait until current write process complete 
	GetOverlappedResult(p->hWrite, &p->overlapWrite.overlap, &p->overlapWrite.dwBytesReadWritten, TRUE); 
	if(p->overlapWrite.dwBytesReadWritten > 0)
	{
		if(FRONT_BUFFER(p->dbcWrite).currentSize > 0)
		{
			p->overlapWrite.fileSize.QuadPart += p->overlapWrite.dwBytesReadWritten;  
			p->overlapWrite.overlap.Offset = p->overlapWrite.fileSize.LowPart;
			p->overlapWrite.overlap.OffsetHigh = p->overlapWrite.fileSize.HighPart;  
			p->overlapWrite.dwBytesReadWritten = 0; 
			utl->ResetBuffer(FRONT_BUFFER(p->dbcWrite));  
		}
	}
	p->dbcWrite->UnLockConsumer();

	//////////////////////////////////////////////////////////////////////////

	if(BACK_BUFFER(p->dbcWrite).currentSize > 0)
	{    
		WriteFile(p->hWrite, 
			BACK_BUFFER(p->dbcWrite).data, 
			BACK_BUFFER(p->dbcWrite).currentSize, 
			&p->overlapWrite.dwBytesReadWritten, 
			&p->overlapWrite.overlap);  

		GetOverlappedResult(p->hWrite, &p->overlapWrite.overlap, &p->overlapWrite.dwBytesReadWritten, TRUE);

		p->overlapWrite.fileSize.QuadPart += p->overlapWrite.dwBytesReadWritten;  
		p->overlapWrite.overlap.Offset = p->overlapWrite.fileSize.LowPart;
		p->overlapWrite.overlap.OffsetHigh = p->overlapWrite.fileSize.HighPart;  
		p->overlapWrite.dwBytesReadWritten = 0;  
		utl->ResetBuffer(BACK_BUFFER(p->dbcWrite));  
	}


	// Cleaning
	delete p->joinFilePath;
	CloseHandle(p->hWrite);

	return 0;
}


/// <summary>
/// Initialize join thread.
/// </summary>
/// <returns></returns>
RC PSMJRP::JoinPhase_Initialize()
{
	RC rc;
	// Reset buffer pool
	bufferPool.currentSize = 0;


	FANS * finalFanInR = m_R.front();
	m_R.pop(); 

	FANS * finalFanInS = m_S.front();
	m_S.pop(); 

	hR = CreateFile(
		(LPCWSTR)finalFanInR->fileName, // file to open
		GENERIC_READ,			// open for reading
		FILE_SHARE_READ,        // share for reading
		NULL,					// default security
		OPEN_EXISTING,			// existing file only
		FILE_FLAG_OVERLAPPED,	// overlapped operation //| FILE_FLAG_NO_BUFFERING
		NULL);					// no attr. template

	if (INVALID_HANDLE_VALUE==hR) 
	{   
		ShowMB(L"Cannot open file %s\r\n", finalFanInR->fileName);
		return ERR_CANNOT_CREATE_HANDLE; 
	} 

	hS = CreateFile(
		(LPCWSTR)finalFanInS->fileName, // file to open
		GENERIC_READ,			// open for reading
		FILE_SHARE_READ,        // share for reading
		NULL,					// default security
		OPEN_EXISTING,			// existing file only
		FILE_FLAG_OVERLAPPED,	// overlapped operation //| FILE_FLAG_NO_BUFFERING
		NULL);					// no attr. template

	if (INVALID_HANDLE_VALUE==hS) 
	{   
		ShowMB(L"Cannot open file %s\r\n", finalFanInS->fileName);
		return ERR_CANNOT_CREATE_HANDLE; 
	} 

	LARGE_INTEGER  finalFanInSizeR = {0}; 
	LARGE_INTEGER  finalFanInSizeS = {0}; 

	if (!GetFileSizeEx(hR, &finalFanInSizeR))
	{       
		ShowMB(L"Cannot get size of file %s\r\n", finalFanInR->fileName);
		return ERR_CANNOT_GET_FILE_SIZE;
	} 

	if (!GetFileSizeEx(hS, &finalFanInSizeS))
	{       
		ShowMB(L"Cannot get size of file %s\r\n", finalFanInS->fileName);
		return ERR_CANNOT_GET_FILE_SIZE;
	} 


	// Allocate range partition
	DWORD chunkSize = m_PartitionSize; 
	UINT64 totalChunkR = chROUNDUP(finalFanInSizeR.QuadPart, chunkSize) / chunkSize;
	UINT64 totalChunkS = chROUNDUP(finalFanInSizeS.QuadPart, chunkSize) / chunkSize; 
	UINT64 _totalChunkR = totalChunkR; 

	if( totalChunkR < m_WorkerThreadNum )
		m_WorkerThreadNum = totalChunkR;



	m_JoinParams = new JoinPhaseParams[m_WorkerThreadNum];


	for(int wIdx = 0; wIdx < m_WorkerThreadNum; wIdx++)
	{
		m_JoinParams[wIdx]._this = this;
		m_JoinParams[wIdx].RunSize = chunkSize; 
		m_JoinParams[wIdx].R_FanInCount = 0;
		m_JoinParams[wIdx].S_FanInCount = 0;
		m_JoinParams[wIdx].R_Key = m_PsmjRpParams.R_KEY_POS;
		m_JoinParams[wIdx].S_Key = m_PsmjRpParams.S_KEY_POS;

		m_JoinParams[wIdx].ReadSkipCountS = 0;
		m_JoinParams[wIdx].ReadCountS = 0; 
	}

	while(_totalChunkR > 0)
	{
		for(UINT tIdx=0; tIdx < m_WorkerThreadNum; tIdx++)
		{ 
			m_JoinParams[tIdx].R_FanInCount++; 
			_totalChunkR--; 
			if(_totalChunkR==0) { break; }
		} 
	}  

	for(UINT tIdx=0; tIdx < m_WorkerThreadNum; tIdx++)
	{ 
		m_JoinParams[tIdx].S_FanInCount = totalChunkS;  
		m_JoinParams[tIdx].PartitionS = new RangePartition[m_JoinParams[tIdx].S_FanInCount];
	} 

	DWORD nIdx = 0;
	for(UINT tIdx=0; tIdx < m_WorkerThreadNum; tIdx++)
	{ 
		m_JoinParams[tIdx].PartitionR = new RangePartition[m_JoinParams[tIdx].R_FanInCount];
		for(UINT runIdx = 0; runIdx < m_JoinParams[tIdx].R_FanInCount; runIdx++)
		{
			m_JoinParams[tIdx].PartitionR[runIdx].Idx = nIdx; 
			m_JoinParams[tIdx].PartitionR[runIdx].lowestKey = m_RRangePartition[nIdx].lowestKey;
			m_JoinParams[tIdx].PartitionR[runIdx].highestKey = m_RRangePartition[nIdx].highestKey;
			m_JoinParams[tIdx].PartitionR[runIdx].fileOffsetStart = m_RRangePartition[nIdx].fileOffsetStart; 
			m_JoinParams[tIdx].PartitionR[runIdx].fileOffsetEnd = m_RRangePartition[nIdx].fileOffsetEnd; 
			nIdx++;
		} 
	}
	nIdx = 0;
	// Table S
	for(UINT runIdx = 0; runIdx < m_JoinParams[0].S_FanInCount; runIdx++)
	{ 
		m_JoinParams[0].PartitionS[runIdx].Idx = runIdx;
		m_JoinParams[0].PartitionS[runIdx].lowestKey = m_SRangePartition[nIdx].lowestKey;
		m_JoinParams[0].PartitionS[runIdx].highestKey = m_SRangePartition[nIdx].highestKey; 
		m_JoinParams[0].PartitionS[runIdx].fileOffsetStart = m_SRangePartition[nIdx].fileOffsetStart; 
		m_JoinParams[0].PartitionS[runIdx].fileOffsetEnd = m_SRangePartition[nIdx].fileOffsetEnd; 
		nIdx++;
	}
	for(UINT tIdx=0; tIdx < m_WorkerThreadNum; tIdx++)
	{ 
		if(tIdx!=0)
		{
			// Update other thread with thread 0 parameter
			for(UINT runIdx = 0; runIdx < m_JoinParams[tIdx].S_FanInCount; runIdx++)
			{
				m_JoinParams[tIdx].PartitionS[runIdx].Idx = m_JoinParams[0].PartitionS[runIdx].Idx; 
				m_JoinParams[tIdx].PartitionS[runIdx].lowestKey = m_JoinParams[0].PartitionS[runIdx].lowestKey; 
				m_JoinParams[tIdx].PartitionS[runIdx].highestKey = m_JoinParams[0].PartitionS[runIdx].highestKey;
				m_JoinParams[tIdx].PartitionS[runIdx].fileOffsetStart = m_JoinParams[0].PartitionS[runIdx].fileOffsetStart;
				m_JoinParams[tIdx].PartitionS[runIdx].fileOffsetEnd = m_JoinParams[0].PartitionS[runIdx].fileOffsetEnd;
			} 
		}
	}


	// DEBUG
	//////////////////////////////////////////////////////////////////////////
	CHAR *debugContent = new CHAR[2048]; 
	FILE* fso=fopen("C:\\debugPsmjrp.csv","w+b");

	for(UINT tIdx=0; tIdx < m_WorkerThreadNum; tIdx++)
	{   
		for(UINT runIdx = 0; runIdx < m_JoinParams[tIdx].R_FanInCount; runIdx++)
		{
			sprintf(debugContent, "%d,%lld,%lld,%lld,%lld\n",
				runIdx,
				m_JoinParams[tIdx].PartitionR[runIdx].fileOffsetStart.QuadPart,
				m_JoinParams[tIdx].PartitionR[runIdx].fileOffsetEnd.QuadPart,
				m_JoinParams[tIdx].PartitionR[runIdx].lowestKey,
				m_JoinParams[tIdx].PartitionR[runIdx].highestKey);  fprintf(fso, debugContent); 
		} 

		fprintf(fso, "\n"); 
	} 

	for(UINT tIdx=0; tIdx < m_WorkerThreadNum; tIdx++)
	{   
		for(UINT runIdx = 0; runIdx < m_JoinParams[tIdx].S_FanInCount; runIdx++)
		{
			sprintf(debugContent, "%d,%lld,%lld,%lld,%lld\n",
				runIdx,
				m_JoinParams[tIdx].PartitionS[runIdx].fileOffsetStart.QuadPart,
				m_JoinParams[tIdx].PartitionS[runIdx].fileOffsetEnd.QuadPart,
				m_JoinParams[tIdx].PartitionS[runIdx].lowestKey,
				m_JoinParams[tIdx].PartitionS[runIdx].highestKey);  fprintf(fso, debugContent); 
		}  
		fprintf(fso, "\n"); 
	}  
	delete debugContent;
	fclose(fso); 

	// Reset buffer pool
	bufferPool.currentSize = 0;
	rp.JoinThreadNum = m_WorkerThreadNum;
	rp.JoinReadBufferSize = chunkSize;

	//////////////////////////////////////////////////////////////////////////
	// Init buffer 
	for(UINT tIdx=0; tIdx < m_WorkerThreadNum; tIdx++)
	{  
		m_JoinParams[tIdx].tupleR = new RECORD();
		m_JoinParams[tIdx].tupleS = new RECORD();
		m_JoinParams[tIdx].tupleRS = new RECORD(TUPLE_SIZE*2); 

		// Init buffer
		// Read buffer for R
		rc = utl->InitBuffer(m_JoinParams[tIdx].readBufferR, chunkSize, &bufferPool);
		if(rc!=SUCCESS) {  return rc; } 
		rc = utl->InitBuffer(m_JoinParams[tIdx].pageBufferR, SSD_PAGE_SIZE, &bufferPool);
		if(rc!=SUCCESS) {  return rc; } 
		rc = utl->InitRunPage(m_JoinParams[tIdx].pageR, m_JoinParams[tIdx].pageBufferR);
		if(rc!=SUCCESS) {  return rc; }

		// Read buffer for S
		rc = utl->InitBuffer(m_JoinParams[tIdx].readBufferS, chunkSize, &bufferPool);
		if(rc!=SUCCESS) {  return rc; }  
		rc = utl->InitBuffer(m_JoinParams[tIdx].pageBufferS, SSD_PAGE_SIZE, &bufferPool);
		if(rc!=SUCCESS) {  return rc; }  
		rc = utl->InitRunPage(m_JoinParams[tIdx].pageS, m_JoinParams[tIdx].pageBufferS);
		if(rc!=SUCCESS) {  return rc; } 


		// Variable for write join tuple to disk
		m_JoinParams[tIdx].joinFilePath = new TCHAR[MAX_PATH];
		JoinPhase_GetFanOutPath(m_JoinParams[tIdx].joinFilePath, tIdx);

		m_JoinParams[tIdx].hWrite = CreateFile(
			m_JoinParams[tIdx].joinFilePath,	 
			GENERIC_WRITE,			 
			0,						 
			NULL,					 
			CREATE_ALWAYS,			 
			FILE_FLAG_OVERLAPPED, 
			NULL);		

		if (INVALID_HANDLE_VALUE==m_JoinParams[tIdx].hWrite) 
		{   
			ShowMB(L"Cannot open file %s\r\n", m_JoinParams[tIdx].joinFilePath);
			return ERR_CANNOT_CREATE_HANDLE; 
		}  

		// Init write buffers 
		rc = utl->InitBuffer(m_JoinParams[tIdx].pageWriteBuffer, SSD_PAGE_SIZE, &bufferPool);
		if(rc!=SUCCESS) {  return rc; }

		rc = utl->InitRunPage(m_JoinParams[tIdx].pageWrite, m_JoinParams[tIdx].pageWriteBuffer);
		if(rc!=SUCCESS) {  return rc; }
	}

	// Calculate write buffer size
	DWORD memSizeFree = bufferPool.size - bufferPool.currentSize;
	if(memSizeFree==0) {return ERR_NOT_ENOUGH_MEMORY;}

	// must be > 0 and multiple of 4096, divide 2 because use double buffer
	DWORD writeMemSize = chROUNDDOWN((memSizeFree / m_WorkerThreadNum) / 2, SSD_PAGE_SIZE);
	rp.JoinWriteBufferSize = writeMemSize;

	if(writeMemSize < SSD_PAGE_SIZE*256)
	{
		chMB("Alert::Memory for write below 1MB");
	}

	rp.JoinWriteBufferSize = writeMemSize;

	for(UINT tIdx=0; tIdx < m_WorkerThreadNum; tIdx++)
	{ 
		m_JoinParams[tIdx].dbcWrite = new DoubleBuffer(writeMemSize); 
		rc = utl->InitBuffer(m_JoinParams[tIdx].dbcWrite->buffer[0], writeMemSize, &bufferPool);
		if(rc!=SUCCESS) {return rc;}
		rc = utl->InitBuffer(m_JoinParams[tIdx].dbcWrite->buffer[1], writeMemSize, &bufferPool);  
		if(rc!=SUCCESS) {return rc;}

		m_JoinParams[tIdx].overlapWrite.dwBytesToReadWrite = writeMemSize; 
		m_JoinParams[tIdx].overlapWrite.dwBytesReadWritten = 0;  
		m_JoinParams[tIdx].overlapWrite.totalChunk = 0;
		m_JoinParams[tIdx].overlapWrite.chunkIndex = 0;
		m_JoinParams[tIdx].overlapWrite.startChunk = 0;
		m_JoinParams[tIdx].overlapWrite.endChunk = 0;
		m_JoinParams[tIdx].overlapWrite.fileSize.QuadPart = 0; 
		m_JoinParams[tIdx].overlapWrite.overlap.Offset = 0;
		m_JoinParams[tIdx].overlapWrite.overlap.OffsetHigh = 0; 
	}

	// It's OK, return

	return SUCCESS;
}


/// <summary>
/// Join operation.
/// </summary>
/// <param name="lpParam">The thread parameter.</param>
/// <returns></returns>
RC PSMJRP::JoinPhase_Join(LPVOID lpParam)
{
	RC rc;

	JoinPhaseParams* p = (JoinPhaseParams*)lpParam;

	p->readBufferR.currentPageIndex = 0;
	rc = utl->GetPageInfo(p->readBufferR.data, p->pageBufferR, p->readBufferR.currentPageIndex,  SSD_PAGE_SIZE);  

	p->readBufferS.currentPageIndex = 0;
	rc = utl->GetPageInfo(p->readBufferS.data, p->pageBufferS, p->readBufferS.currentPageIndex,  SSD_PAGE_SIZE);  

	p->pageBufferR.currentTupleIndex = 1;
	rc = utl->GetTupleInfo(p->tupleR, p->pageBufferR.currentTupleIndex, p->pageBufferR.data, SSD_PAGE_SIZE, p->R_Key);  
	p->pageBufferR.currentTupleIndex++;

	p->pageBufferS.currentTupleIndex = 1;
	rc = utl->GetTupleInfo(p->tupleS, p->pageBufferS.currentTupleIndex, p->pageBufferS.data, SSD_PAGE_SIZE, p->S_Key);  
	p->pageBufferS.currentTupleIndex++;

	while((p->tupleR->key!=MAX) && (p->tupleS->key!=MAX))
	{  
		// while left is less than right, move left up
		while (p->tupleR->key < p->tupleS->key) 
		{
			rc = JoinPhase_GetNextTuple(&p->readBufferR, p->pageBufferR, p->tupleR, p->R_Key); 
			if (p->tupleR->key == MAX)  { break; }
		} 

		// if done, no more joins, break
		if (p->tupleR->key == MAX)  { break; } 

		// while left is greater than right, move right up
		while (p->tupleR->key > p->tupleS->key) 
		{
			rc = JoinPhase_GetNextTuple(&p->readBufferS, p->pageBufferS, p->tupleS, p->S_Key); 
			if (p->tupleS->key == MAX) { break; }
		} 

		// if done, no more joins, break
		if (p->tupleS->key == MAX) {  break;  }

		// while the two are equal, segment equal
		while (p->tupleR->key == p->tupleS->key) 
		{   
			JoinPhase_MakeJoinRecord(p->tupleRS, p->tupleR, p->tupleS);

			// Send this join tuple to BACK write buffer 
			rc = JoinPhase_SentOutput(p, p->tupleRS); 

			// Get next S tuple 
			rc = JoinPhase_GetNextTuple(&p->readBufferS, p->pageBufferS, p->tupleS, p->S_Key);

			while (p->tupleS->key == p->tupleR->key) 
			{  
				JoinPhase_MakeJoinRecord(p->tupleRS, p->tupleR, p->tupleS);

				// Save this to Output buffer
				JoinPhase_SentOutput(p, p->tupleRS); 

				// Get next S tuple
				rc = JoinPhase_GetNextTuple(&p->readBufferS, p->pageBufferS, p->tupleS, p->S_Key); 
				if (p->tupleS->key == MAX)  { break; }
			}

			// Get next R tuple
			rc = JoinPhase_GetNextTuple(&p->readBufferR, p->pageBufferR, p->tupleR, p->R_Key);   

			if (p->tupleR->key == MAX)  {  break;  }
		}

		// Get next S tuple
		rc = JoinPhase_GetNextTuple(&p->readBufferS, p->pageBufferS, p->tupleS, p->S_Key);   
	}


	return SUCCESS;
}

/// <summary>
/// Get next tuple from buffer.
/// </summary>
/// <param name="bufferPtr">The buffer PTR.</param>
/// <param name="pageBuffer">The page buffer.</param>
/// <param name="recordPtr">The record PTR.</param>
/// <param name="keyPos">The key position in table.</param>
/// <returns></returns>
RC PSMJRP::JoinPhase_GetNextTuple(Buffer *bufferPtr, Buffer &pageBuffer, RECORD *recordPtr, DWORD keyPos)
{
	RC rc;

	if(pageBuffer.currentTupleIndex > pageBuffer.tupleCount)
	{ 
		bufferPtr->currentPageIndex++;   
		if(bufferPtr->currentPageIndex >= bufferPtr->pageCount)
		{     
			// Buffer empty
			rc = utl->ResetBuffer(pageBuffer);  
			rc = utl->SetMaxTuple(recordPtr); 
			return SUCCESS;
		}

		rc = utl->GetPageInfo(bufferPtr->data, pageBuffer, bufferPtr->currentPageIndex,  SSD_PAGE_SIZE);  
		pageBuffer.currentTupleIndex=1; 
	}

	rc = utl->GetTupleInfo(recordPtr, pageBuffer.currentTupleIndex, pageBuffer.data, SSD_PAGE_SIZE, keyPos);  
	pageBuffer.currentTupleIndex++; 

	return SUCCESS; 
}

/// <summary>
/// Sent join tuple to output buffer.
/// </summary>
/// <param name="lpParam">The thread parameter.</param>
/// <param name="recordPtr">The record PTR.</param>
/// <returns></returns>
RC PSMJRP::JoinPhase_SentOutput(LPVOID lpParam, RECORD *recordPtr)
{
	RC rc; 
	JoinPhaseParams* p = (JoinPhaseParams*)lpParam;

	InterlockedExchangeAdd(&m_TotalJoinCount, 1); 

	if(utl->IsJoinPageFull(p->pageWrite))
	{   
		rc = utl->AddPageToBuffer(BACK_BUFFER(p->dbcWrite), p->pageWrite->page, SSD_PAGE_SIZE); //4 
		//BACK_BUFFER(p->dbcWrite).currentPageIndex++; 
		BACK_BUFFER(p->dbcWrite).pageCount++;
		utl->ResetRunPage(p->pageWrite, p->pageWriteBuffer);  
		p->pageWrite->consumed = TRUE; 
	}  

	if(utl->IsBufferFull(BACK_BUFFER(p->dbcWrite)))  
	{     
		if(p->dbcWrite->bFirstProduce==TRUE) // Back Buffer is full, check is it the first product
		{ 
			p->dbcWrite->bFirstProduce=FALSE;  
			p->dbcWrite->SwapBuffers();   

			p->dbcWrite->LockConsumer();  // lock no lai

			WriteFile(p->hWrite, 
				FRONT_BUFFER(p->dbcWrite).data, 
				FRONT_BUFFER(p->dbcWrite).currentSize, 
				&p->overlapWrite.dwBytesReadWritten, 
				&p->overlapWrite.overlap); 	
		}
		else
		{    
			//Wait for write complete   
			GetOverlappedResult(p->hWrite, &p->overlapWrite.overlap, &p->overlapWrite.dwBytesReadWritten, TRUE) ;  

			p->overlapWrite.fileSize.QuadPart += p->overlapWrite.dwBytesReadWritten;  
			p->overlapWrite.overlap.Offset = p->overlapWrite.fileSize.LowPart;
			p->overlapWrite.overlap.OffsetHigh = p->overlapWrite.fileSize.HighPart;  
			p->overlapWrite.dwBytesReadWritten = 0;

			rc = utl->ResetBuffer(FRONT_BUFFER(p->dbcWrite));  // reset for next data

			p->dbcWrite->UnLockConsumer(); 

			p->dbcWrite->SwapBuffers();  

			p->dbcWrite->LockConsumer();  
			WriteFile(p->hWrite, 
				FRONT_BUFFER(p->dbcWrite).data, 
				FRONT_BUFFER(p->dbcWrite).currentSize, 
				&p->overlapWrite.dwBytesReadWritten, 
				&p->overlapWrite.overlap); 
		}   
	}

	rc = utl->AddTupleToPage(p->pageWrite, recordPtr, p->pageWriteBuffer);   // Add this tuples to page   
	p->pageWrite->consumed = FALSE;

	return SUCCESS;  
}

/// <summary>
/// Get fan-out path.
/// </summary>
/// <param name="fanPath">The fan path.</param>
/// <param name="threadID">The thread identifier.</param>
/// <returns></returns>
RC PSMJRP::JoinPhase_GetFanOutPath(LPWSTR &fanPath, INT threadID)
{
	swprintf(fanPath, MAX_PATH, L"%sjoined_%d.dat", m_PsmjRpParams.WORK_SPACE_PATH, threadID);   
	return SUCCESS; 
}

/// <summary>
/// Make join record.
/// </summary>
/// <param name="joinRecord">The join record pointer.</param>
/// <param name="leftRecord">The left record pointer.</param>
/// <param name="rightRecord">The right record pointer.</param>
VOID PSMJRP::JoinPhase_MakeJoinRecord(RECORD *&joinRecord, RECORD *leftRecord, RECORD *rightRecord) 
{
	joinRecord->key = leftRecord->key;
	joinRecord->length = leftRecord->length + rightRecord->length;
	memcpy(joinRecord->data, leftRecord->data, leftRecord->length);
	memcpy(joinRecord->data + leftRecord->length, rightRecord->data, rightRecord->length); 
}


/// <summary>
/// Writes the report to disk.
/// </summary>
VOID PSMJRP::WriteReport()
{ 
	FILE *fp;   
	CHAR *reportFilePath = new CHAR[MAX_PATH];   
	LPWSTR tempReportPath = new TCHAR[MAX_PATH];

	swprintf(tempReportPath, MAX_PATH, L"%s%s", m_PsmjRpParams.WORK_SPACE_PATH, L"PSMJRP_Report.csv" ); 

	// convert file path to char  
	size_t  count = wcstombs(reportFilePath, tempReportPath, MAX_PATH); 

	// open file log to write
	fp=fopen(reportFilePath, "w+b"); 

	CHAR *reportContent = new CHAR[2048]; 

	sprintf(reportContent, "%s,%d,%d\n", "Memory Size", rp.BufferPoolSize,  rp.BufferPoolSize / (SSD_PAGE_SIZE * 256));  fprintf(fp, reportContent);
	sprintf(reportContent, "%s,%d\n", "THREAD Num", m_WorkerThreadNum);  fprintf(fp, reportContent);

	//////////////////////////////////////////////////////////////////////////
	sprintf(reportContent, "%s\n", "Partition Phase");  fprintf(fp, reportContent); 
	sprintf(reportContent, "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n", "Table","Relation Size","Thread Num","Run Size","Partition Num","Read Buffer Size(sort)","Write Buffer Size(sort)","Read Buffer Size (merge)","Write Buffer Size (merge)","Partition Time(ms)","CPU Time Parition", "Merge Time", "CPU Time Merge"); fprintf(fp, reportContent);
	sprintf(reportContent, "%s,%lld,%d,%d,%d,%d,%d,%d,%d,%lld,%.f,%lld,%.f\n",   "R"  ,rp.SourceTableSizeR, rp.PartitionThreadNum, rp.RunSize, rp.PartitionNumR, rp.SortReadBufferSize,  rp.SortWriteBufferSize, rp.MergeReadBufferSizeR,rp.MergeWriteBufferSizeR,  rp.PartitionExecTimeR, rp.PartitionCpuTimeR[1] - rp.PartitionCpuTimeR[0], rp.MergeExecTimeR, rp.MergeCpuTimeR[1] - rp.MergeCpuTimeR[0]);  fprintf(fp, reportContent);
	sprintf(reportContent, "%s,%lld,%d,%d,%d,%d,%d,%d,%d,%lld,%.f,%lld,%.f\n",   "S"  ,rp.SourceTableSizeS, rp.PartitionThreadNum, rp.RunSize, rp.PartitionNumS, rp.SortReadBufferSize,  rp.SortWriteBufferSize, rp.MergeReadBufferSizeS,rp.MergeWriteBufferSizeS,  rp.PartitionExecTimeS, rp.PartitionCpuTimeS[1] - rp.PartitionCpuTimeS[0], rp.MergeExecTimeS, rp.MergeCpuTimeS[1] - rp.MergeCpuTimeS[0]);  fprintf(fp, reportContent);

	//////////////////////////////////////////////////////////////////////////
	sprintf(reportContent, "%s\n", "Join Phase");  fprintf(fp, reportContent);
	sprintf(reportContent, "%s,%s,%s,%s,%s,  %s\n",   "Thread Num",       "Run Size","Read Buffer Size(join)","Write Buffer Size(join)","Join Time(ms)","CPU Time");  fprintf(fp, reportContent);
	rp.JoinReadBufferSize = rp.RunSize;
	sprintf(reportContent, "%d,%d,%d,%d,%lld,%.f\n",   rp.JoinThreadNum, rp.RunSize, rp.JoinReadBufferSize, rp.JoinWriteBufferSize, rp.JoinExecTime, rp.JoinCpuTime[1] - rp.JoinCpuTime[0]);  fprintf(fp, reportContent);

	//////////////////////////////////////////////////////////////////////////
	sprintf(reportContent, "%s, %d  \n", "Total Join Count", m_TotalJoinCount);  fprintf(fp, reportContent);
	sprintf(reportContent, "%s, %d  \n", "Total Partition Time", rp.PartitionExecTimeR+rp.PartitionExecTimeS+rp.MergeExecTimeR+rp.MergeExecTimeS);  fprintf(fp, reportContent);
	sprintf(reportContent, "%s, %d  \n", "Total Join Time", rp.JoinExecTime);  fprintf(fp, reportContent);
	sprintf(reportContent, "%s, %lld\n", "Total Exec Time", rp.TotalExecTime);  fprintf(fp, reportContent); 
	sprintf(reportContent, "%s, %.f \n", "Total Cpu Time", rp.TotalCpuTime[1] - rp.TotalCpuTime[0] ); fprintf(fp, reportContent);

	// Close file
	fclose(fp);

	delete reportFilePath;
	delete tempReportPath; 
	delete reportContent;
}

/// <summary>
/// Gets the fan-in path.
/// </summary>
/// <param name="fanPath">The fan path.</param>
/// <returns></returns>
RC  PSMJRP::GetFanPath(LPWSTR &fanPath)
{      
	m_CS.Lock();
	if(isR)
	{
		swprintf_s(fanPath, MAX_PATH, L"%s%d_%s.dat", m_PsmjRpParams.WORK_SPACE_PATH,  m_FanIndex, m_PsmjRpParams.RELATION_R_NO_EXT);  
	}
	else
	{
		swprintf_s(fanPath, MAX_PATH, L"%s%d_%s.dat", m_PsmjRpParams.WORK_SPACE_PATH,  m_FanIndex, m_PsmjRpParams.RELATION_S_NO_EXT);  
	}
	m_FanIndex++;
	m_CS.UnLock();

	return SUCCESS; 
}  

/// <summary>
/// Copies two record.
/// </summary>
/// <param name="des">The DES.</param>
/// <param name="src">The source.</param>
/// <returns></returns>
RC PSMJRP::CopyRecord(RECORD *des, RECORD *&src)
{
	des->key = src->key;
	//strcpy(des->data, src->data);
	strncpy(des->data, src->data, src->length);
	des->length= src->length;
	des->mark = src->mark;

	return SUCCESS;
} 


/// <summary>
/// Gets the size of the fan-ins.
/// </summary>
/// <param name="hFile">The fan-in file handle.</param>
/// <returns></returns>
UINT64 PSMJRP::GetFanSize(HANDLE hFile)  
{
	LARGE_INTEGER liFileSize; 
	if (!GetFileSizeEx(hFile, &liFileSize))
	{      
		return 0;
	} 

	return liFileSize.QuadPart;
}


INT PSMJRP::TreeParent(int i)
{
	return floor(i/2); 
}

INT  PSMJRP::TreeLeft(INT i)
{
	return   2 * i + 1; // 2 * i;  
} 

INT PSMJRP::TreeRight (INT i)
{
	return  2 * i + 2;  
} 

RC PSMJRP::MinHeapify(RECORD **rec, INT i, INT heapSize)
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

RC PSMJRP::MaxHeapify(RECORD **rec, INT i, INT heapSize)
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

RC PSMJRP::BuildMinHeap(RECORD **rec, INT heapSize)
{  
	for(int i=heapSize/2; i>=0; i--)
		MinHeapify(rec, i, heapSize);	// array[0] is the largest item 

	return SUCCESS;
}

RC PSMJRP::BuildMaxHeap(RECORD **rec, INT heapSize)
{  
	for(int i=heapSize/2; i>=0; i--)
		MaxHeapify(rec, i, heapSize); // array[0] is the smallest item

	return SUCCESS;
}
