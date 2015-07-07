// 
// Name: ExsPartitionPhase.cpp : implementation file 
// Author: hieunt
// Description: Implementation of sort phase in parallel external merge sort
//				Create mini run in memory, after memory is filled full of data		     
//				use replacement selection to merge these minirun
 
#include "stdafx.h" 
#include "ExsPartitionPhase.h" 
#include "ExsMergePhase.h"

/// <summary>
/// Initializes a new instance of the <see cref="ExsPartitionPhase"/> class.
/// </summary>
/// <param name="vParams">The PEMS parameters.</param>
ExsPartitionPhase::ExsPartitionPhase(const PEMS_PARAMS vParams) : m_Params(vParams) 
{  
	m_FanInIndex = 0;    
	m_PartitionThreadNum = vParams.THREAD_NUM;  
	utl = new PageHelpers2();  
	// If thread num is zero, set thread num equal max logical core
	SYSTEM_INFO sysinfo;
	GetSystemInfo(&sysinfo);
	if(vParams.THREAD_NUM==0)
		m_PartitionThreadNum = sysinfo.dwNumberOfProcessors; 
}

/// <summary>
/// Finalizes an instance of the <see cref="ExsPartitionPhase"/> class.
/// </summary>
ExsPartitionPhase::~ExsPartitionPhase()
{  

}

/// <summary>
/// Check enough memory for sort phase.
/// </summary>
/// <returns></returns>
RC ExsPartitionPhase::PartitionPhase_CheckEnoughMemory()
{
	// Estimate memory 
	DOUBLE m_TotalMemory = m_Params.BUFFER_POOL_SIZE; 
	m_MemoryEachThreadSize = m_TotalMemory / m_PartitionThreadNum;
	//myMessage.Add(L"Memory allocate each thread: %d\r\n", m_MemoryEachThreadSize);


	// Caculate ouput sort buffer
	DWORD memoryForWriteSortRun = 0;
	memoryForWriteSortRun = (m_Params.SORT_WRITE_BUFFER_SIZE * 2) * m_PartitionThreadNum;

	m_TotalMemory = m_TotalMemory - memoryForWriteSortRun;

	if( m_TotalMemory < 0)
	{
		ShowMB(L"Not enough momory\nSort Write Buffer is too big");
		return ERR_NOT_ENOUGH_MEMORY; 
	}

	// Calculate inputBuffer num with current size  
	DWORD maxQuickSortItem = (m_Params.SORT_READ_BUFFER_SIZE / 4096) * 40; // The maximum tuple for quick sort 

	DWORD memoryForQuickSort = 0;
	memoryForQuickSort = (maxQuickSortItem * sizeof(UINT64)) + (maxQuickSortItem * TUPLE_SIZE * sizeof(CHAR)) + SSD_PAGE_SIZE;
	memoryForQuickSort = memoryForQuickSort * m_PartitionThreadNum; 

	m_TotalMemory = m_TotalMemory - memoryForQuickSort;

	if( m_TotalMemory <= 0)
	{
		ShowMB(L"Not enough memory for quick sort");
		return ERR_NOT_ENOUGH_MEMORY; 
	}

	// (LoserKey + LoserTreeData + MergeBuffer + ReadInputBuffer*2) * NumberOfThread
	DWORD memoryForLoserTreeTuple = sizeof(UINT64) + (TUPLE_SIZE * sizeof(CHAR));
	DWORD memoryForMergePage = SSD_PAGE_SIZE;
	DWORD memoryForSortRun = m_Params.SORT_READ_BUFFER_SIZE * 2;

	m_InputBufferNum = m_TotalMemory / ((memoryForLoserTreeTuple+memoryForMergePage+memoryForSortRun) * m_PartitionThreadNum);

	if( m_InputBufferNum == 0)
	{
		ShowMB(L"Not enough memory\nSort Read Buffer Size is too big\nNumber of Input buffer is zero");
		return ERR_NOT_ENOUGH_MEMORY; 
	}

	m_TotalMemory = m_TotalMemory - (m_InputBufferNum * m_PartitionThreadNum);

	if( m_TotalMemory < 0 )
	{ 
		ShowMB(L"Not enough memory for %d thread\nIncrease pool size or choose other buffer size");
		return ERR_NOT_ENOUGH_MEMORY; 
	}   

	return SUCCESS; 
}

/// <summary>
/// Initialize sort phase.
/// </summary>
/// <returns></returns>
RC ExsPartitionPhase::PartitionPhase_Initialize()
{    
	RC rc;  
	bufferPool.size = m_Params.BUFFER_POOL_SIZE;
	bufferPool.data = new CHAR[bufferPool.size];
	bufferPool.currentSize = 0;

	if(NULL==bufferPool.data)
	{  
		return ERR_CANNOT_INITIAL_BUFFER_POOL;
	}

	m_hPartitionThread = new HANDLE[m_PartitionThreadNum];
	m_PartitionParams = new ExsPartitionThreadParams[m_PartitionThreadNum]; 


	if(m_Params.USE_SYNC_READ_WRITE_MODE)
	{
		m_MonitorParams = new MonitorParams();
		m_MonitorParams->ClassPointer = this; /// IMPORTANCE

		sync.bQuit = FALSE;
		sync.dwThreadNum = m_PartitionThreadNum;
		sync.hReady = new HANDLE[m_PartitionThreadNum];
		sync.bIsDone = new BOOL[m_PartitionThreadNum]; // which thread is done their work?
		sync.hWaitAllOk = CreateEvent( NULL, TRUE, FALSE, NULL); // Manual reset, nonsignaled 
		for(DWORD i=0; i < m_PartitionThreadNum; i++)
		{ 
			sync.hReady[i] = CreateEvent( NULL, FALSE, FALSE, NULL); // Auto reset, nonsignaled
			sync.bIsDone[i] = FALSE;
		}
	}

	m_hInputFile=CreateFile(
		(LPCWSTR)m_Params.SORT_FILE_PATH, // file to open
		GENERIC_READ,			// open for reading
		FILE_SHARE_READ,        // share for reading
		NULL,					// default security
		OPEN_EXISTING,			// existing file only
		FILE_FLAG_OVERLAPPED | FILE_FLAG_NO_BUFFERING,	// overlapped operation //| FILE_FLAG_NO_BUFFERING
		NULL);					// no attr. template

	if (INVALID_HANDLE_VALUE==m_hInputFile) 
	{   
		ShowMB(L"Cannot open file %s\r\n", m_Params.SORT_FILE_PATH);
		return ERR_CANNOT_CREATE_HANDLE; 
	} 

	LARGE_INTEGER  liSourceFileSize = {0}; 

	if (!GetFileSizeEx(m_hInputFile, &liSourceFileSize))
	{       
		ShowMB(L"Cannot get size of file %s\r\n", m_Params.SORT_FILE_PATH);
		return ERR_CANNOT_GET_FILE_SIZE;
	} 

	DWORD chunkSize = m_Params.SORT_READ_BUFFER_SIZE; 
	UINT64 totalChunk = chROUNDUP(liSourceFileSize.QuadPart, chunkSize) / chunkSize;

	// Init default value
	for (DWORD i = 0; i < m_PartitionThreadNum; i++)
	{
		m_PartitionParams[i].ClassPointer = this; ///////////// Importance
		m_PartitionParams[i].threadID = i; 
		m_PartitionParams[i].keyPosition = m_Params.KEY_POS;  
		m_PartitionParams[i].tempFanIn = new FANS(); 
		m_PartitionParams[i].hInputFile = m_hInputFile; 

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
RC ExsPartitionPhase::PartitionPhase_Execute()
{ 
	if(SUCCESS!=PartitionPhase_Initialize())
	{ 
		ShowMB(L"Cannot Initialize ExsPartitionPhase");
		return ERR_CANNOT_INITIAL_MEMORY;
	}

	if(m_Params.USE_SYNC_READ_WRITE_MODE==TRUE)
	{ 
		m_hMonitorThread = CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)MonitorEx, (LPVOID)&(m_MonitorParams[0]), CREATE_SUSPENDED, NULL);

		SetThreadPriority(m_hMonitorThread, THREAD_PRIORITY_NORMAL);
		ResumeThread(m_hMonitorThread);  
	}

	for(DWORD i = 0; i < m_PartitionThreadNum; i++ )
	{  
		m_hPartitionThread[i] = CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)RunEx, (LPVOID)&(m_PartitionParams[i]), CREATE_SUSPENDED, NULL);

		SetThreadPriority(m_hPartitionThread[i], THREAD_PRIORITY_NORMAL);
		ResumeThread(m_hPartitionThread[i]); 
	}

	WaitForMultipleObjects(m_PartitionThreadNum, m_hPartitionThread, TRUE, INFINITE); 

	if(m_Params.USE_SYNC_READ_WRITE_MODE==TRUE)
	{
		// Wait for monitor thread exit
		for(DWORD i = 0; i < m_PartitionThreadNum; i++ )
		{
			sync.bIsDone[i] = TRUE;
			SetEvent(sync.hReady[i]);
		}  

		WaitForSingleObject(m_hMonitorThread, INFINITE);
	}

	for(DWORD i = 0; i < m_PartitionThreadNum; i++ )
	{
		CloseHandle(m_hPartitionThread[i]);
	}

	if(m_Params.USE_SYNC_READ_WRITE_MODE==TRUE)
	{ 
		CloseHandle(m_hMonitorThread);
	}


	delete bufferPool.data; 
	delete utl;

	return SUCCESS;
}

DWORD WINAPI ExsPartitionPhase::MonitorEx(LPVOID lpParam)
{
	MonitorParams* p = (MonitorParams*)lpParam;
	p->ClassPointer->Monitor((LPVOID)(p));
	return 0;
}

DWORD WINAPI ExsPartitionPhase::Monitor(LPVOID lpParam)
{ 
	while( !sync.bQuit )
	{  
		ResetEvent(sync.hWaitAllOk);

		// If one thread is exited--> need signal its event
		for(DWORD i=0; i < m_PartitionThreadNum; i++) 
			if(sync.bIsDone[i]==TRUE) 
				SetEvent(sync.hReady[i]); 

		// wait until all thread is ready to write
		WaitForMultipleObjects(m_PartitionThreadNum, sync.hReady, TRUE, INFINITE);

		// allow all thread write
		SetEvent(sync.hWaitAllOk);

		// Terminate thread
		BOOL bDone = TRUE;
		for(DWORD i=0; i< m_PartitionThreadNum; i++)
		{
			if(sync.bIsDone[i]==FALSE)
			{
				bDone = FALSE;
				break;
			}
		}

		if(bDone==TRUE)
		{ 
			sync.bQuit=TRUE;
		}
	} 

	return 0;
}

DWORD WINAPI ExsPartitionPhase::RunEx( LPVOID lpParam )
{
	ExsPartitionThreadParams* pThreadInfo = reinterpret_cast<ExsPartitionThreadParams*>(lpParam);
	pThreadInfo->ClassPointer->Run((void *)(pThreadInfo));
	return 0;
}

DWORD WINAPI ExsPartitionPhase::Run(LPVOID lpParam)
{ 
	ExsPartitionThreadParams *p = (ExsPartitionThreadParams *)lpParam;
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

	if(m_Params.USE_SYNC_READ_WRITE_MODE==TRUE)
	{
		sync.bIsDone[p->threadID] = TRUE; 
		SetEvent(sync.hReady[p->threadID]);
	} 

	if(m_Params.USE_PARALLEL_MERGE==TRUE)
	{ 
		std::queue<FANS*> fanIns;
		for(UINT i=0; i<p->fanIns.size();i++)
		{
			FANS *fan = new FANS();
			fan = p->fanIns[i];
			fanIns.push(fan);
		}
		//
		ExsMergePhase mergePhase(m_Params, fanIns, m_MemoryEachThreadSize); /// bo nho cap phat la bao nhiu???  
		if(SUCCESS==mergePhase.MergePhase_CheckEnoughMemory())
		{ 
			mergePhase.MergePhase_Execute();

			FANS*  finalFan = mergePhase.GetFinalFanOut();
			if(finalFan!=NULL)
			{ 
				m_Mutex.Lock();
				m_ReturnFanIns.push(finalFan);
				m_Mutex.UnLock(); 
			}
		} 
		return 0;
	}

	//// Cleaning, release memory 
	m_Mutex.Lock();
	while(p->fanIns.size() > 0)
	{
		FANS *fan = new FANS();
		fan = p->fanIns.back();
		p->fanIns.pop_back();

		m_ReturnFanIns.push(fan);  
	} 
	m_Mutex.UnLock(); 

	return 0;
}

/// <summary>
/// Read from disk into buffer.
/// </summary>
/// <param name="lpParam">The thread parameter.</param>
/// <param name="bufferIndex">Index of the buffer.</param>
/// <returns></returns>
RC ExsPartitionPhase::PartitionPhase_Read(LPVOID lpParam, DWORD bufferIndex)
{
	ExsPartitionThreadParams *p = (ExsPartitionThreadParams *)lpParam;

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
/// Merge mini-runs in memory .
/// </summary>
/// <param name="lpParam">The lp parameter.</param>
/// <returns></returns>
RC ExsPartitionPhase::PartitionPhase_Merge(LPVOID lpParam)
{ 
	ExsPartitionThreadParams *p = (ExsPartitionThreadParams *)lpParam;
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
/// Gext tuple from buffer.
/// </summary>
/// <param name="lpParam">The lp parameter.</param>
/// <param name="record">The record pointer.</param>
/// <param name="index">The index of buffer.</param>
/// <returns></returns>
RC ExsPartitionPhase::PartitionPhase_GetNextTuple(LPVOID lpParam, RECORD *&record, INT index)
{
	ExsPartitionThreadParams *p = (ExsPartitionThreadParams *)lpParam;
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
/// Create new run.
/// </summary>
/// <param name="lpParam">The thread parameter.</param>
/// <returns></returns>
RC ExsPartitionPhase::PartitionPhase_CreateNewRun(LPVOID lpParam)
{
	ExsPartitionThreadParams *p = (ExsPartitionThreadParams *)lpParam;

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
/// Terminate current run.
/// </summary>
/// <param name="lpParam">The thread parameter.</param>
/// <param name="tupleCount">The tuple count.</param>
/// <returns></returns>
RC  ExsPartitionPhase::PartitionPhase_TerminateRun(LPVOID lpParam, DWORD tupleCount)
{
	ExsPartitionThreadParams *p = (ExsPartitionThreadParams *)lpParam;
	DoubleBuffer *dbcWrite = p->dbcWrite;  

	if(tupleCount!=0)
	{
		FANS *fan = new FANS();  
		wcscpy(fan->fileName,  p->tempFanIn->fileName); 
		fan->threadID = p->threadID;
		fan->pageCount = p->tempFanIn->pageCount; 
		fan->tupleCount = p->tempFanIn->tupleCount;  
		fan->fileSize.QuadPart = p->overlapWrite.fileSize.QuadPart;  
		fan->lowestKey = p->tempFanIn->lowestKey;
		fan->highestKey = p->tempFanIn->highestKey; 
		p->fanIns.push_back(fan); 

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
/// Write buffer to disk.
/// </summary>
/// <param name="lpParam">The lp parameter.</param>
/// <returns></returns>
RC ExsPartitionPhase::PartitionPhase_Write(LPVOID lpParam) 
{
	ExsPartitionThreadParams *p = (ExsPartitionThreadParams *)lpParam;
	DoubleBuffer *dbcWrite = p->dbcWrite;  

	if(m_Params.USE_SYNC_READ_WRITE_MODE) 
	{     
		SignalObjectAndWait(sync.hReady[p->threadID], sync.hWaitAllOk, INFINITE, FALSE);
	}

	WriteFile(p->hFanOut, 
		FRONT_BUFFER(dbcWrite).data, 
		FRONT_BUFFER(dbcWrite).currentSize, 
		&p->overlapWrite.dwBytesReadWritten, 
		&p->overlapWrite.overlap); 			  

	return SUCCESS;
} 

/// <summary>
/// Sort in-memory buffer.
/// </summary>
/// <param name="lpParam">The lp parameter.</param>
/// <param name="bufferIndex">Index of the buffer.</param>
/// <returns></returns>
RC ExsPartitionPhase::PartitionPhase_Sort(LPVOID lpParam, DWORD bufferIndex)  
{    
	ExsPartitionThreadParams *p = (ExsPartitionThreadParams *)lpParam; 
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
/// Get fan-in path.
/// </summary>
/// <param name="fanInName">Name of the fan in.</param>
/// <param name="threadID">The thread identifier.</param>
/// <returns></returns>
RC ExsPartitionPhase::PartitionPhase_GetFanInPath(LPWSTR &fanInName, INT threadID)  
{    
	swprintf(fanInName, MAX_PATH, L"%s%d_%d_%s.dat", m_Params.WORK_SPACE_PATH, threadID, m_FanInIndex, m_Params.FILE_NAME_NO_EXT);  
	InterlockedExchangeAdd(&m_FanInIndex, 1);  

	return SUCCESS; 
} 