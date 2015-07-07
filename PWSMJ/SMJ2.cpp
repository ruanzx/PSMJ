#include "stdafx.h"
#include "SMJ2.h"

#pragma chMSG(Why with table have primary key at pos 1 may occur error)

extern Loggers g_Logger;
SMJ2::SMJ2(const SMJ2_PARAMS vParams) : m_SmjParams(vParams)
{
	utl = new PageHelpers2();  
	m_FanPath = new TCHAR[MAX_PATH]; 
	m_JoinFilePath = new TCHAR[MAX_PATH]; 
	m_FanIndex = 0; 
	m_HeapSize = 0;  
	m_HeapSize = 0;
	isR = TRUE;
}

SMJ2::~SMJ2()
{
	delete utl;
	delete m_FanPath;
	delete m_JoinFilePath; 
} 

RC SMJ2::RP_CalculateHeapSize()
{
	// TODO: calculate Heap size
	// Use all available memory for heap
	DOUBLE memorySize = m_SmjParams.BUFFER_POOL_SIZE - bufferPool.currentSize;
	//memorySize = memorySize - (2*m_SmjParams.SORT_READ_BUFFER_SIZE) - (m_SmjParams.SORT_WRITE_BUFFER_SIZE);
	//memorySize = memorySize - SSD_PAGE_SIZE - SSD_PAGE_SIZE; // for read, write page
	if(memorySize<=0)
		return ERR_NOT_ENOUGH_MEMORY;

	m_HeapSize = chROUNDDOWN( memorySize, TUPLE_SIZE) / TUPLE_SIZE;

	return SUCCESS;
}

RC SMJ2::Execute()
{
	RC rc;
	stwTotalTime.Start();
	rp.TotalCpuTime[0] = GetCpuTime(); 

	rc = SMJ_CheckEnoughMemory();
	if(rc!=SUCCESS)  { ShowMB(L"Not enough memory for join"); return rc; }  

	g_Logger.Write("SMJ2 Start...\n");
	// Init buffer pool
	//////////////////////////////////////////////////////////////////////////
	bufferPool.size = m_SmjParams.BUFFER_POOL_SIZE; 
	bufferPool.currentSize = 0; 
	// Creates a private heap object that can be used by the calling process. 
	// The function reserves space in the virtual address space of the process and allocates physical storage 
	// for a specified initial portion of this block.
	// Reason: C++ cannot allocate more than 400MB with new[] in MFC
	// http://msdn.microsoft.com/en-us/library/aa366599.aspx
	HANDLE hHeap = HeapCreate(0, 0, 0);
	bufferPool.data  = (CHAR*)HeapAlloc(hHeap, 0, bufferPool.size);

	if(NOERROR != GetLastError())
	{
		ReportLastError();
		HeapDestroy(hHeap);
		return ERR_CANNOT_INITIAL_BUFFER_POOL;
	} 

	//////////////////////////////////////////////////////////////////////////
	m_RpParams.BUFFER_POOL_SIZE = m_SmjParams.BUFFER_POOL_SIZE;  
	m_RpParams.WORK_SPACE_PATH = m_SmjParams.WORK_SPACE_PATH;
	m_RpParams.SORT_READ_BUFFER_SIZE = m_SmjParams.SORT_READ_BUFFER_SIZE;
	m_RpParams.SORT_WRITE_BUFFER_SIZE = m_SmjParams.SORT_WRITE_BUFFER_SIZE;
	m_RpParams.MERGE_READ_BUFFER_SIZE = m_SmjParams.MERGE_READ_BUFFER_SIZE;
	m_RpParams.MERGE_WRITE_BUFFER_SIZE = m_SmjParams.MERGE_WRITE_BUFFER_SIZE;
	//////////////////////////////////////////////////////////////////////////
	rp.BufferPoolSize = m_SmjParams.BUFFER_POOL_SIZE;   
	rp.SortReadBufferSize = m_SmjParams.SORT_READ_BUFFER_SIZE;
	rp.SortWriteBufferSize = m_SmjParams.SORT_WRITE_BUFFER_SIZE;
	rp.MergeReadBufferSize = m_SmjParams.MERGE_READ_BUFFER_SIZE;
	rp.MergeWriteBufferSize = m_SmjParams.MERGE_WRITE_BUFFER_SIZE;
	rp.JoinReadBufferSize = m_SmjParams.JOIN_READ_BUFFER_SIZE;
	rp.JoinWriteBufferSize= m_SmjParams.JOIN_WRITE_BUFFER_SIZE;
	//////////////////////////////////////////////////////////////////////////
	bufferPool.currentSize = 0; // Reset buffer pool size
	m_RpParams.SORT_FILE_PATH = m_SmjParams.RELATION_R_PATH;
	m_RpParams.FILE_NAME_NO_EXT = m_SmjParams.RELATION_R_NO_EXT;
	m_RpParams.KEY_POS = m_SmjParams.R_KEY_POS; 


	//////////////// Sort and Merge R //////////////////////////////////////////////////////////
	g_Logger.Write("Sort R...\n");
	rc = RP_Execute();
	if(rc!=SUCCESS) { return rc; }

	FANS *fanR;
	if(m_FanIns.size() > 0)
	{
		fanR = m_FanIns.front();
		m_FanIns.pop();
	}
	else{ return ERR_UNKNOWN_EXCEPTION; }

	//rp.HeapSize = m_HeapSize;
	isR = FALSE;

	////////////// Sort and Merge S ////////////////////////////////////////////////////////////
	bufferPool.currentSize = 0; // Reset buffer pool size
	m_RpParams.SORT_FILE_PATH = m_SmjParams.RELATION_S_PATH;
	m_RpParams.FILE_NAME_NO_EXT = m_SmjParams.RELATION_S_NO_EXT;
	m_RpParams.KEY_POS = m_SmjParams.S_KEY_POS;

	g_Logger.Write("Sort S...\n");
	rc = RP_Execute();
	if(rc!=SUCCESS) { return rc; }

	FANS *fanS;
	if(m_FanIns.size() > 0)
	{
		fanS = m_FanIns.front();
		m_FanIns.pop();
	}
	else{ return ERR_UNKNOWN_EXCEPTION; } 

	//////////////////////////////////////////////////////////////////////////
	bufferPool.currentSize = 0;  

	stwJoinTime.Start(); 
	rc = SMJ_Initialize(fanR, fanS);
	if(rc!=SUCCESS) { return ERR_SMJ_INIT_PARAM_FAILED; }

	g_Logger.Write("Join start...\n");
	//////////////////////////////////////////////////////////////////////////
	rc = SMJ_Join();
	if(rc!=SUCCESS) { return rc; }

	rp.JoinTime = stwJoinTime.NowInMilliseconds();
	rp.TotalCpuTime[1] = GetCpuTime();
	rp.TotalTime = stwTotalTime.NowInMilliseconds();

	rp.TotalPartitionTime = rp.PartitionTimeR + rp.PartitionTimeS;
	rp.TotalMergeTime = rp.MergeTimeR + rp.MergeTimeS;

	//////////////////////////////////////////////////////////////////////////
	WriteReport(); 

	//////////////////////////////////////////////////////////////////////////
	if(m_RpParams.USE_DELETE_AFTER_OPERATION==TRUE)
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
	//////////////////////////////////////////////////////////////////////////
	g_Logger.Write("SMJ2 End...\n");
	//////////////////////////////////////////////////////////////////////////
	BOOL bRet = HeapFree(hHeap, 0, bufferPool.data);
	bRet = HeapDestroy(hHeap);
	//////////////////////////////////////////////////////////////////////////
	return SUCCESS;
}

RC SMJ2::WriteReport()
{
	// write report to file

	FILE *fp;   
	CHAR *reportFilePath = new CHAR[MAX_PATH];   
	LPWSTR tempReportPath = new TCHAR[MAX_PATH];
	swprintf(tempReportPath, MAX_PATH, L"%s%s", m_SmjParams.WORK_SPACE_PATH, L"SMJ_RP_Report.csv" ); 
	// convert file path to char  
	size_t  count = wcstombs(reportFilePath, tempReportPath, MAX_PATH); 

	CHAR *reportContent = new CHAR[2048];

	// Open file
	fp=fopen(reportFilePath, "w+b");


	sprintf(reportContent, "%s,%d,%d\n", "Memory Size", rp.BufferPoolSize, rp.BufferPoolSize / (SSD_PAGE_SIZE*256) );  fprintf(fp, reportContent);
	sprintf(reportContent, "%s,%d\n", "THREAD Num", 1);  fprintf(fp, reportContent); 

	sprintf(reportContent, "%s\n", "Sort and Merge Phase");  fprintf(fp, reportContent); 
	sprintf(reportContent, "%s\n", "Table,Relation Size,Heap Size,Partition Num,Merge Pass,Read Buffer Size(sort),Write Buffer Size(sort),Read Buffer Size(merge),Write Buffer Size(merge),Partition Time(ms),Merge Time(ms)"); fprintf(fp, reportContent);

	sprintf(reportContent, "%s,%lld,%d,%d,%d,%d,%d,%d,%d,%lld,%lld\n", "R", rp.SourceTableSizeR, rp.HeapSizeR, rp.PartitionNumR, rp.MergePassR,  rp.SortReadBufferSize, rp.SortWriteBufferSize,  rp.MergeReadBufferSize, rp.MergeWriteBufferSize,  rp.PartitionTimeR,  rp.MergeTimeR );   fprintf(fp, reportContent);

	sprintf(reportContent, "%s,%lld,%d,%d,%d,%d,%d,%d,%d,%lld,%lld\n", "S", rp.SourceTableSizeS, rp.HeapSizeS, rp.PartitionNumS,  rp.MergePassS,  rp.SortReadBufferSize, rp.SortWriteBufferSize,  rp.MergeReadBufferSize, rp.MergeWriteBufferSize,  rp.PartitionTimeS,  rp.MergeTimeS );   fprintf(fp, reportContent);

	sprintf(reportContent, "%s\n", "Join Phase");  fprintf(fp, reportContent); 

	sprintf(reportContent, "%s\n", "Read Buffer Size(join),Write Buffer Size(join), Join Time(ms))"); fprintf(fp, reportContent);

	sprintf(reportContent, "%d,%d,%lld\n",   rp.JoinReadBufferSize, rp.JoinWriteBufferSize, rp.JoinTime); fprintf(fp, reportContent);

	sprintf(reportContent, "%s, %lld\n", "Total Partition Time", rp.TotalPartitionTime); fprintf(fp, reportContent);

	sprintf(reportContent, "%s, %lld\n", "Total Merge Time", rp.TotalMergeTime); fprintf(fp, reportContent);

	sprintf(reportContent, "%s, %.f\n", "Cpu Time", rp.TotalCpuTime[1] - rp.TotalCpuTime[0]); fprintf(fp, reportContent);

	sprintf(reportContent, "%s, %lld\n", "Total Execute Time", rp.TotalTime); fprintf(fp, reportContent);


	fclose(fp); 

	delete reportFilePath;
	delete tempReportPath; 
	delete reportContent; 

	return SUCCESS;
}
DWORD SMJ2::RP_GetHeapSize() const
{
	return m_HeapSize;
} 

RC SMJ2::RP_Execute() 
{  
	RC rc; 

	stwPartitionTime.Start();
	//////////////////////////////////////////////////////////////////////////
	rc = RP_Initialize();
	if(rc!=SUCCESS) { return rc; }
	////////////////////////////////////////////////////////////////////////// 

	rc = RP_PartitionPhase();
	if(rc!=SUCCESS) { return rc; }

	if(isR)
		rp.PartitionTimeR = stwPartitionTime.NowInMilliseconds();
	else 
		rp.PartitionTimeS = stwPartitionTime.NowInMilliseconds();


	stwMergeTime.Start();

	// reset buffer pool
	bufferPool.currentSize = 0; 

	rc = RP_MergePhase();
	if(rc!=SUCCESS) { return rc; }

	if(isR)
		rp.MergeTimeR = stwMergeTime.NowInMilliseconds();
	else 
		rp.MergeTimeS = stwMergeTime.NowInMilliseconds();


	CloseHandle(hInputFile);

	return SUCCESS;
}


RC SMJ2::RP_Initialize()
{
	/* Create handle for source table */ 
	RC rc;
	hInputFile=CreateFile(
		(LPCWSTR)m_RpParams.SORT_FILE_PATH,
		GENERIC_READ,
		FILE_SHARE_READ,
		NULL,
		OPEN_EXISTING,
		FILE_FLAG_OVERLAPPED | FILE_FLAG_NO_BUFFERING,
		NULL); 

	if(INVALID_HANDLE_VALUE==hInputFile)
	{  
		ShowMB(L"Cannot create handle %s", m_RpParams.SORT_FILE_PATH);
		return ERR_CANNOT_CREATE_HANDLE; 
	}


	//utl->InitBufferPool(&bufferPool, m_Params.BUFFER_POOL_SIZE);

	partitionParams.dbcRead = new DoubleBuffer(m_RpParams.SORT_READ_BUFFER_SIZE);
	rc = utl->InitBuffer(partitionParams.dbcRead->buffer[0], m_RpParams.SORT_READ_BUFFER_SIZE, &bufferPool);
	if(rc!=SUCCESS) { return rc; }

	rc = utl->InitBuffer(partitionParams.dbcRead->buffer[1], m_RpParams.SORT_READ_BUFFER_SIZE, &bufferPool); 
	if(rc!=SUCCESS) { return rc; }

	rc = utl->InitBuffer(partitionParams.writeBuffer, m_RpParams.SORT_WRITE_BUFFER_SIZE, &bufferPool);
	if(rc!=SUCCESS) { return rc; }

	partitionParams.currentMark = 0;  

	/* Get source table size */
	LARGE_INTEGER *liFileSize = new LARGE_INTEGER();  
	if (!GetFileSizeEx(hInputFile, liFileSize))
	{    
		return ERR_CANNOT_GET_FILE_SIZE;
	} 

	// Save table size for report
	if(isR)
		rp.SourceTableSizeR = (UINT64)liFileSize->QuadPart;
	else
		rp.SourceTableSizeS = (UINT64)liFileSize->QuadPart;

	/* Set up overlap structure to READ source table */
	DWORD64 chunkSize = m_RpParams.SORT_READ_BUFFER_SIZE;
	DWORD64 totalChunk = chROUNDUP(liFileSize->QuadPart, chunkSize) / chunkSize; 

	partitionParams.overlapRead = new OVERLAPPEDEX();
	partitionParams.overlapRead->dwBytesToReadWrite = m_RpParams.SORT_READ_BUFFER_SIZE;   
	partitionParams.overlapRead->startChunk = 0;
	partitionParams.overlapRead->chunkIndex = 0;
	partitionParams.overlapRead->endChunk = totalChunk;
	partitionParams.overlapRead->totalChunk = totalChunk;

	//partitionParams.overlapWrite = new OVERLAPPEDEX(); 
	//partitionParams.overlapWrite->dwBytesToReadWrite = m_Params.SORT_WRITE_BUFFER_SIZE; 

	/* Set up SSD read run page */
	rc = utl->InitBuffer(partitionParams.readPageBuffer, SSD_PAGE_SIZE, &bufferPool);
	if(rc!=SUCCESS) { return rc; }

	rc = utl->InitRunPage(partitionParams.readPage, partitionParams.readPageBuffer);
	if(rc!=SUCCESS) { return rc; }

	/* Set up SSD write run page */
	rc = utl->InitBuffer(partitionParams.writePageBuffer, SSD_PAGE_SIZE, &bufferPool);
	if(rc!=SUCCESS) { return rc; }

	rc = utl->InitRunPage(partitionParams.writePage, partitionParams.writePageBuffer);
	if(rc!=SUCCESS) { return rc; }

	/* Init heap buffer */

	rc = RP_CalculateHeapSize();
	if(rc!=SUCCESS) {ShowMB(L"Not enough memory for sort");return rc;}

	utl->InitBuffer(m_HeapBuffer, m_HeapSize * TUPLE_SIZE, &bufferPool);
	if(rc!=SUCCESS) { return rc; }

	return SUCCESS;
}


RC SMJ2::RP_PartitionPhase()
{
	RECORD *pTempRecord = new RECORD(); /* Working record */
	RECORD *pOutRecord = new RECORD(); /* Output record from heap */
	RECORD *pInRecord = new RECORD(); /* Addition record fill in heap */

	/* Init heap tree*/
	DWORD	HEAP_SIZE = RP_GetHeapSize(); 
	if(isR)
		rp.HeapSizeR = HEAP_SIZE;
	else 
		rp.HeapSizeS = HEAP_SIZE;

	RECORD **heap = new RECORD *[HEAP_SIZE];
	for (UINT i = 0; i < HEAP_SIZE; i++)    
	{ 
		heap[i]  = new RECORD(0, m_HeapBuffer.data + TUPLE_SIZE * i, TUPLE_SIZE); 
	}

	//First read into buffer 
	partitionParams.dbcRead->LockProducer();
	RP_PartitionPhase_Read();
	if(partitionParams.dbcRead->bFirstProduce==TRUE)
	{ 
		partitionParams.dbcRead->bFirstProduce = FALSE;
	}

	// Wait for Read complete 
	GetOverlappedResult(hInputFile, &partitionParams.overlapRead->overlap, &partitionParams.overlapRead->dwBytesReadWritten, TRUE);  
	partitionParams.overlapRead->chunkIndex++; 
	if(partitionParams.overlapRead->dwBytesReadWritten==0  || partitionParams.overlapRead->chunkIndex >= partitionParams.overlapRead->totalChunk)
	{ 
		utl->AddPageMAXToBuffer(partitionParams.readPageBuffer, SSD_PAGE_SIZE); 
		utl->AddPageToBuffer(BACK_BUFFER(partitionParams.dbcRead), partitionParams.readPageBuffer.data, SSD_PAGE_SIZE);
		//utl->GetPageInfo(partitionParams.readPageBuffer.data, BACK_BUFFER(partitionParams.dbcRead), 0, SSD_PAGE_SIZE);
		partitionParams.readPageBuffer.isFullMaxValue = TRUE;
		partitionParams.readPageBuffer.currentTupleIndex=1;   
	}
	else
	{
		utl->ComputeBuffer(BACK_BUFFER(partitionParams.dbcRead), partitionParams.overlapRead->dwBytesReadWritten, SSD_PAGE_SIZE);
	}
	partitionParams.dbcRead->UnLockProducer();
	partitionParams.dbcRead->SwapBuffers(); 

	utl->GetPageInfo(FRONT_BUFFER(partitionParams.dbcRead).data, partitionParams.readPageBuffer, 0, SSD_PAGE_SIZE);
	FRONT_BUFFER(partitionParams.dbcRead).currentPageIndex = 0;
	partitionParams.readPageBuffer.currentTupleIndex=1;

	// Next read
	RP_PartitionPhase_Read();  

	// Add tuple to heap
	for (UINT i = 0; i < HEAP_SIZE; i++)     
	{ 
		RP_PartitionPhase_GetNextRecord(pTempRecord);  
		if(pTempRecord->key==MAX) // fix for memory too large compare with relation size
		{ 
			HEAP_SIZE = i; 
			if(isR)
				rp.HeapSizeR = HEAP_SIZE;
			else 
				rp.HeapSizeS = HEAP_SIZE;

			break;
		}
		pTempRecord->mark = partitionParams.currentMark;
		CopyRecord(heap[i], pTempRecord);  
	}

	/* Create first run handle and name */
	RP_PartitionPhase_CreateNewRun(); 

	/* Init fanIn counter */ 
	DWORD64 lastKey = 0; 
	DWORD64 pageCount = 0;
	DWORD64 tupleCount = 0;
	DWORD64 lowestKey, highestKey;  /* the min and max key in run file */
	BOOL    lowestKeyIsGet = FALSE;
	DWORD   lastNode = HEAP_SIZE-1;
	BOOL    isDone = FALSE; 

	partitionParams.writePage->consumed = TRUE;


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
			RP_PartitionPhase_GetNextRecord(pInRecord);  

			/* Read input file EOF */
			if(pInRecord->key==MAX)
			{
				isDone= TRUE;
				break;
			} 

			std::swap( heap[0], heap[lastNode] );  //heap[LastNode] is minimum item 

			/* Check new run or not ? */
			if (heap[1]->mark != partitionParams.currentMark) 
			{    
				if(partitionParams.writePage->consumed==FALSE)
				{
					utl->AddPageToBuffer(partitionParams.writeBuffer, partitionParams.writePage->page, SSD_PAGE_SIZE); //4 
					partitionParams.writeBuffer.currentPageIndex++; 
				}

				if(partitionParams.writeBuffer.currentSize > 0)
				{
					RP_PartitionPhase_Write();  
				}

				utl->ResetRunPage(partitionParams.writePage, partitionParams.writePageBuffer); 

				CloseHandle(hOutputFile); 

				/* Save current run info */  
				RP_PartitionPhase_SaveFanIn(lowestKey, highestKey, tupleCount, pageCount); 
				RP_PartitionPhase_CreateNewRun(); // Get name, begin a new run 

				/* Reset counter for next run */  
				pageCount = 0; tupleCount = 0; lowestKey = 0; highestKey = 0;
				lowestKeyIsGet = FALSE;

				/* Reverse the mark so that all the elements in the heap will be available for the next run */ 
				partitionParams.currentMark = heap[1]->mark;

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
			RP_PartitionPhase_SentToOutput(pOutRecord, pageCount); 

			tupleCount++;

			/* Determine if the newly read record belongs in the current run  or the next run. */  
			if(pInRecord->key >= lastKey)
			{ 
				pInRecord->mark = partitionParams.currentMark;  
				CopyRecord(heap[lastNode], pInRecord);
			}
			else
			{
				/* New record cannot belong to current run */ 
				pInRecord->mark = partitionParams.currentMark ^ 1;  

				CopyRecord(heap[lastNode], pInRecord);

				/* Continue heap sort */
				lastNode--; 
			}  

			MinHeapify(heap, 0,  lastNode); 
		}    
	} 
	//partitionParams.dbcWrite->UnLockProducer();

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
		RP_PartitionPhase_SentToOutput(pOutRecord, pageCount);

		tupleCount++;
		lastNode--; 
		MinHeapify(heap, 0,  lastNode); 
	}

	if(partitionParams.writeBuffer.currentSize > 0)
	{
		if(partitionParams.writePage->consumed==FALSE)
		{
			utl->AddPageToBuffer(partitionParams.writeBuffer, partitionParams.writePage->page, SSD_PAGE_SIZE); //4 
			partitionParams.writeBuffer.currentPageIndex++; 
			pageCount++;
		} 

		RP_PartitionPhase_Write();    
		utl->ResetRunPage(partitionParams.writePage, partitionParams.writePageBuffer);  
	}

	/* Terminate final run */    
	RP_PartitionPhase_SaveFanIn(lowestKey, highestKey, tupleCount, pageCount);
	CloseHandle(hOutputFile);  


	delete pTempRecord;
	delete pInRecord;
	delete pOutRecord;

	return SUCCESS;  
}

RC SMJ2::RP_MergePhase()
{
	RC rc;

	if(isR)
		rp.MergePassR=1;
	else
		rp.MergePassS=1;

	// Compute fanIn num


	// Estimate memory
	////////////////////////////////////////////////////////////////////////// 
	DOUBLE totalMemory = m_RpParams.BUFFER_POOL_SIZE;  
	totalMemory = totalMemory - (m_RpParams.MERGE_WRITE_BUFFER_SIZE * 2);
	totalMemory = totalMemory - SSD_PAGE_SIZE; // write page buffer

	if(totalMemory <= 0)
	{ 
		return ERR_NOT_ENOUGH_MEMORY; 
	}

	// LoserKey + LoserTreeData + MergeBuffer + ReadInputBuffer*2 
	DOUBLE memoryNeedForOneFanIn = sizeof(UINT64) + (TUPLE_SIZE * sizeof(CHAR)) + SSD_PAGE_SIZE + (m_RpParams.MERGE_READ_BUFFER_SIZE*2);

	DWORD m_MaxFanInNum = totalMemory / memoryNeedForOneFanIn;
	DWORD x = m_FanIns.size();

	// Estimate memory use for current fanIn num, if not enough memory -> multimerge step 
	if(m_MaxFanInNum > x)
	{
		// Enough memory for merge in one step
		m_MaxFanInNum = x;   
	}

	DWORD memoryNeedForMerge = m_MaxFanInNum * memoryNeedForOneFanIn; 

	totalMemory = totalMemory - memoryNeedForMerge; 

	if(totalMemory <= 0)
	{ 
		return ERR_NOT_ENOUGH_MEMORY; 
	}

	//////////////////////////////////////////////////////////////////////////
	// Init write buffer
	mergeParams.dbcWrite = new DoubleBuffer(m_RpParams.MERGE_WRITE_BUFFER_SIZE); 
	rc = utl->InitBuffer(mergeParams.dbcWrite->buffer[0], m_RpParams.MERGE_WRITE_BUFFER_SIZE, &bufferPool);
	if(rc!=SUCCESS) { return rc; }

	rc = utl->InitBuffer(mergeParams.dbcWrite->buffer[1], m_RpParams.MERGE_WRITE_BUFFER_SIZE, &bufferPool); 
	if(rc!=SUCCESS) { return rc; }
	//////////////////////////////////////////////////////////////////////////
	mergeParams.dbcRead = new DoubleBuffer*[m_MaxFanInNum];
	mergeParams.mergeBuffer = new Buffer[m_MaxFanInNum];

	for(DWORD i=0; i<m_MaxFanInNum; i++)
	{ 
		mergeParams.dbcRead[i] = new DoubleBuffer(m_RpParams.MERGE_READ_BUFFER_SIZE);
		rc = utl->InitBuffer(mergeParams.dbcRead[i]->buffer[0], m_RpParams.MERGE_READ_BUFFER_SIZE, &bufferPool);
		if(rc!=SUCCESS) { return rc; }

		rc = utl->InitBuffer(mergeParams.dbcRead[i]->buffer[1], m_RpParams.MERGE_READ_BUFFER_SIZE, &bufferPool); 
		if(rc!=SUCCESS) { return rc; }
		rc = utl->InitBuffer(mergeParams.mergeBuffer[i], SSD_PAGE_SIZE, &bufferPool);
		if(rc!=SUCCESS) { return rc; }
	}

	// Init run page buffer for assembly
	rc = utl->InitBuffer(mergeParams.runPageBuffer, SSD_PAGE_SIZE, &bufferPool);
	if(rc!=SUCCESS) { return rc; }
	// Init runpage 
	rc = utl->InitRunPage(mergeParams.runPage, mergeParams.runPageBuffer);  
	if(rc!=SUCCESS) { return rc; }
	////////////////////////////////////////////////////////////////////////// 

	if(m_FanIns.size()==1)
	{
		if(isR)
			rp.MergePassR=1;
		else
			rp.MergePassS=1;

		FANS* _fan = m_FanIns.front();
		m_FanInWillDelete.push(_fan); 
		return SUCCESS;
	}

	while( TRUE ) // Multi merge passes
	{  
		RP_GetFanPath(); 

		DWORD64 totalPage = 0;
		DWORD64 tupleCount = 0;

		hOutputFile = CreateFile((LPCWSTR)m_FanPath,		// file to write
			GENERIC_WRITE,			// open for writing
			0,						// Do not share
			NULL,					// default security
			CREATE_ALWAYS,			 
			FILE_FLAG_OVERLAPPED | FILE_FLAG_NO_BUFFERING,	// overlapped operation
			NULL);					// no attr. template

		if (INVALID_HANDLE_VALUE==hOutputFile) 
		{   
			return ERR_CANNOT_CREATE_HANDLE;
		}  

		//	Init overlap structure for Ouput
		//////////////////////////////////////////////////////////////////////////  

		mergeParams.overlapWrite.dwBytesToReadWrite = m_RpParams.MERGE_WRITE_BUFFER_SIZE; 
		mergeParams.overlapWrite.dwBytesReadWritten = 0;
		mergeParams.overlapWrite.fileSize.QuadPart = 0;
		mergeParams.overlapWrite.overlap.Offset = 0;
		mergeParams.overlapWrite.overlap.OffsetHigh = 0;

		mergeParams.dbcWrite->bFirstProduce = TRUE;

		// Init handle run file
		////////////////////////////////////////////////////////////////////////// 
		DWORD currentQueueSize = m_FanIns.size();
		DWORD fanInNum = 0;

		if (currentQueueSize <= m_MaxFanInNum)  
			fanInNum = currentQueueSize;    
		else  
			fanInNum = m_MaxFanInNum;    

		// Init loser tree
		LoserTree ls(fanInNum);
		RECORD *record = new RECORD();  

		mergeParams.hFanIn = new HANDLE[fanInNum];
		mergeParams.overlapRead = new OVERLAPPEDEX[fanInNum];

		LARGE_INTEGER liFanOutSize = {0};

		for(DWORD i=0; i<fanInNum; i++)
		{
			FANS *fanIn = new FANS();
			fanIn = m_FanIns.front();
			m_FanIns.pop();
			m_FanInWillDelete.push(fanIn);  

			mergeParams.hFanIn[i] = CreateFile(
				(LPCWSTR)fanIn->fileName, // file to open
				GENERIC_READ,			 
				0,        
				NULL,					 
				OPEN_EXISTING,			// existing file only
				FILE_FLAG_OVERLAPPED | FILE_FLAG_NO_BUFFERING,	// overlapped operation //| FILE_FLAG_NO_BUFFERING
				NULL);					// no attr. template 

			if (INVALID_HANDLE_VALUE==mergeParams.hFanIn[i]) 
			{  
				return ERR_CANNOT_CREATE_HANDLE; 
			} 

			UINT64 fanSize = RP_GetFanSize(mergeParams.hFanIn[i]);
			// TODO: check size =0 

			liFanOutSize.QuadPart+= fanSize;

			DWORD64 chunkSize = m_RpParams.MERGE_READ_BUFFER_SIZE; 
			DWORD64 totalChunk = chROUNDUP(fanSize, chunkSize) / chunkSize ;

			// Create overlap structure for input runs 
			mergeParams.overlapRead[i].dwBytesToReadWrite = chunkSize; 
			mergeParams.overlapRead[i].chunkIndex = 0; 
			mergeParams.overlapRead[i].totalChunk = totalChunk; 
		} 

		// File systems extend files synchronously. Extend the destination file 
		// now so that I/Os execute asynchronously improving performance. 
		////////////////////////////////////////////////////////////////////////// 
		LARGE_INTEGER liDestSize = { 0 };
		liDestSize.QuadPart = chROUNDUP(liFanOutSize.QuadPart, m_RpParams.MERGE_WRITE_BUFFER_SIZE);

		SetFilePointerEx(hOutputFile, liDestSize, NULL, FILE_BEGIN);
		SetEndOfFile(hOutputFile); 

		// First read from disk to buffer
		//////////////////////////////////////////////////////////////////////////
		for(DWORD fanIndex=0; fanIndex<fanInNum;fanIndex++) 
			RP_MergePhase_Read(fanIndex);     

		for(DWORD f=0; f < fanInNum;f++)
		{ 
			// Wait for read complete
			if (mergeParams.overlapRead[f].chunkIndex < mergeParams.overlapRead[f].totalChunk)
				GetOverlappedResult(mergeParams.hFanIn[f], &mergeParams.overlapRead[f].overlap,  &mergeParams.overlapRead[f].dwBytesReadWritten, TRUE);  

			mergeParams.overlapRead[f].chunkIndex++;  
			if(mergeParams.dbcRead[f]->bFirstProduce==TRUE) { mergeParams.dbcRead[f]->bFirstProduce=FALSE; } 

			if(mergeParams.overlapRead[f].dwBytesReadWritten==0)
			{
				utl->ResetBuffer(BACK_BUFFER(mergeParams.dbcRead[f]));
				utl->AddPageMAXToBuffer(mergeParams.mergeBuffer[f], SSD_PAGE_SIZE); 
				utl->AddPageToBuffer(BACK_BUFFER(mergeParams.dbcRead[f]), mergeParams.mergeBuffer[f].data, SSD_PAGE_SIZE);
				//utl->GetPageInfo(partitionParams.readPageBuffer.data, BACK_BUFFER(partitionParams.dbcRead), 0, SSD_PAGE_SIZE);
				mergeParams.mergeBuffer[f].isFullMaxValue = TRUE;
				mergeParams.mergeBuffer[f].currentTupleIndex=1; 
			}
			else
			{
				utl->ComputeBuffer(BACK_BUFFER(mergeParams.dbcRead[f]), mergeParams.overlapRead[f].dwBytesReadWritten);   
			}

			utl->GetPageInfo(BACK_BUFFER(mergeParams.dbcRead[f]).data, mergeParams.mergeBuffer[f], 0, SSD_PAGE_SIZE);  
			mergeParams.mergeBuffer[f].currentTupleIndex = 1;
			utl->GetTupleInfo(record, mergeParams.mergeBuffer[f].currentTupleIndex, mergeParams.mergeBuffer[f].data ,SSD_PAGE_SIZE, m_RpParams.KEY_POS);   
			mergeParams.mergeBuffer[f].currentTupleIndex++;
			ls.AddNewNode(record, f);  

			//mergeParams.dbcRead[f]->UnLockProducer();  

			mergeParams.dbcRead[f]->SwapBuffers();  // swap read buffer

			// Read next
			RP_MergePhase_Read(f);
		} 

		ls.CreateLoserTree();     

		// Reset buffer to default values
		utl->ResetBuffer(BACK_BUFFER(mergeParams.dbcWrite));
		utl->ResetBuffer(FRONT_BUFFER(mergeParams.dbcWrite));  
		utl->ResetRunPage(mergeParams.runPage, mergeParams.runPageBuffer);

		INT index = 0; // file index 
		DWORD64 lowestValue=0;
		DWORD64	highestValue=0;
		BOOL bLowestIsSet = FALSE; 

		//mergeParams.dbcWrite->LockProducer();
		while(TRUE) //p->lsTemp[ p->loserTree[0] ] !=MAX
		{   
			ls.GetMinRecord( record, index ); // index = loserTree[0]

			if(record->key==MAX) { break; }

			mergeParams.runPage->consumed = FALSE;

			// Save the record has lowest value
			if(bLowestIsSet==FALSE)
			{
				bLowestIsSet=TRUE;
				lowestValue= record->key;  
			} 

			// Save the record has highest value
			highestValue = record->key;   

			if(utl->IsBufferFull(BACK_BUFFER(mergeParams.dbcWrite))) // check Is buffer full?
			{   
				//mergeParams.dbcWrite->UnLockProducer();

				if(mergeParams.dbcWrite->bFirstProduce==TRUE)
				{ 
					mergeParams.dbcWrite->bFirstProduce=FALSE;  
					mergeParams.dbcWrite->SwapBuffers();   

					//mergeParams.dbcWrite->LockConsumer(); 	// First write 
					RP_MergePhase_Write();  
				}
				else
				{   
					// Wait for write thread comsume FRONT buffer to be done   
					GetOverlappedResult(hOutputFile, &mergeParams.overlapWrite.overlap, &mergeParams.overlapWrite.dwBytesReadWritten, TRUE) ; 

					mergeParams.overlapWrite.fileSize.QuadPart += mergeParams.overlapWrite.dwBytesReadWritten;  // Save current position to overlap struc
					mergeParams.overlapWrite.overlap.Offset=mergeParams.overlapWrite.fileSize.LowPart;
					mergeParams.overlapWrite.overlap.OffsetHigh=mergeParams.overlapWrite.fileSize.HighPart;  
					utl->ResetBuffer(FRONT_BUFFER(mergeParams.dbcWrite));  // Clear data for continue merge 
					//mergeParams.dbcWrite->UnLockConsumer();   

					mergeParams.dbcWrite->SwapBuffers();   	// Swap BACK FRONT

					//mergeParams.dbcWrite->LockConsumer(); // Ghi tiep 
					RP_MergePhase_Write(); 
				}  

				//mergeParams.dbcWrite->LockProducer();  
			}

			if(utl->IsPageFull(mergeParams.runPage))
			{   
				//Current runPage is FULL, copy this page to BACK output buffer
				utl->AddPageToBuffer(BACK_BUFFER(mergeParams.dbcWrite), mergeParams.runPage->page, SSD_PAGE_SIZE);  
				mergeParams.runPage->consumed = TRUE;

				BACK_BUFFER(mergeParams.dbcWrite).freeLocation+=SSD_PAGE_SIZE;
				BACK_BUFFER(mergeParams.dbcWrite).pageCount++;   
				totalPage++;

				// Reset runPage, start to employ new one
				utl->ResetRunPage(mergeParams.runPage, mergeParams.runPageBuffer); 
			} // end check runPage is full

			// Add current min tuple to runpage
			utl->AddTupleToPage(mergeParams.runPage, record->data, mergeParams.runPageBuffer);  

			BACK_BUFFER(mergeParams.dbcWrite).tupleCount++;
			tupleCount++;

			RP_MergePhase_GetNextRecord(record, index);  

			ls.AddNewNode(record, index);// Add new tuple to tree 
			ls.Adjust( index );  // Continue fight LOSER TREE 
		} // end while loser tree

		// If the last page has not consumed
		if((mergeParams.runPage->consumed==FALSE) && (utl->IsBufferFull( BACK_BUFFER(mergeParams.dbcWrite))==FALSE) )
		{ 
			if(!utl->IsEmptyPage(mergeParams.runPage))
			{
				utl->AddPageToBuffer(BACK_BUFFER(mergeParams.dbcWrite),  mergeParams.runPage->page, SSD_PAGE_SIZE); 
				mergeParams.runPage->consumed = TRUE;  
				BACK_BUFFER(mergeParams.dbcWrite).currentPageIndex++;
				BACK_BUFFER(mergeParams.dbcWrite).pageCount++; 
				totalPage++;
			} 
		}

		//mergeParams.dbcWrite->UnLockProducer(); 

		if(BACK_BUFFER(mergeParams.dbcWrite).currentSize > 0)
		{      
			GetOverlappedResult(hOutputFile, &mergeParams.overlapWrite.overlap, &mergeParams.overlapWrite.dwBytesReadWritten, TRUE) ;
			// Save current position to overlap struc
			mergeParams.overlapWrite.fileSize.QuadPart += mergeParams.overlapWrite.dwBytesReadWritten;  
			mergeParams.overlapWrite.overlap.Offset=mergeParams.overlapWrite.fileSize.LowPart;
			mergeParams.overlapWrite.overlap.OffsetHigh=mergeParams.overlapWrite.fileSize.HighPart;   
			utl->ResetBuffer(FRONT_BUFFER(mergeParams.dbcWrite));  
			//mergeParams.dbcWrite->UnLockConsumer(); 

			//Write FRONT is done
			mergeParams.dbcWrite->SwapBuffers();  

			// Write the last run to disk 
			//mergeParams.dbcWrite->LockConsumer(); 
			RP_MergePhase_Write();

			// Get overlap result and wait for overlapEvent is signaled
			GetOverlappedResult(hOutputFile, 
				&mergeParams.overlapWrite.overlap, 
				&mergeParams.overlapWrite.dwBytesReadWritten, TRUE); 

			mergeParams.overlapWrite.fileSize.QuadPart += mergeParams.overlapWrite.dwBytesReadWritten;  
			mergeParams.overlapWrite.overlap.Offset=mergeParams.overlapWrite.fileSize.LowPart;
			mergeParams.overlapWrite.overlap.OffsetHigh=mergeParams.overlapWrite.fileSize.HighPart;   

			utl->ResetBuffer(FRONT_BUFFER(mergeParams.dbcWrite));
			//mergeParams.dbcWrite->UnLockConsumer(); 
		}

		//// The destination file size is a multiple of the page size. Open the
		//// file WITH buffering to shrink its size to the source file's size.
		SetFilePointerEx(hOutputFile, liFanOutSize, NULL, FILE_BEGIN);
		SetEndOfFile(hOutputFile);

		CloseHandle(hOutputFile);  

		// Reset read buffer
		for(DWORD i = 0; i < fanInNum; i++) 
		{   
			utl->ResetBuffer(FRONT_BUFFER(mergeParams.dbcRead[i]));  
			CloseHandle(mergeParams.hFanIn[i]);
		}   


		FANS *_fan= new FANS();
		wcscpy(_fan->fileName, m_FanPath);
		_fan->threadID = 0;
		_fan->pageCount = liFanOutSize.QuadPart / SSD_PAGE_SIZE;
		_fan->fileSize = liFanOutSize;
		_fan->tupleCount = tupleCount;  
		_fan->lowestKey = lowestValue; 
		_fan->highestKey = highestValue; 
		m_FanIns.push(_fan); 

		// Check is the last fanIn
		if(m_FanIns.size()==1)
		{ 
			break;
		}

		if(isR)
			rp.MergePassR++;
		else
			rp.MergePassS++;

		m_FanInWillDelete.push(_fan);  
	} // end while multi merge passes

	return SUCCESS;
}

RC SMJ2::RP_MergePhase_Read(DWORD index)
{    
	LARGE_INTEGER chunk = {0};
	chunk.QuadPart = mergeParams.overlapRead[index].chunkIndex * m_RpParams.MERGE_READ_BUFFER_SIZE;   
	mergeParams.overlapRead[index].overlap.Offset = chunk.LowPart;
	mergeParams.overlapRead[index].overlap.OffsetHigh = chunk.HighPart;  

	// Attempt an asynchronous read operation.  
	ReadFile(mergeParams.hFanIn[index], 
		BACK_BUFFER(mergeParams.dbcRead[index]).data, 
		mergeParams.overlapRead[index].dwBytesToReadWrite, 
		&mergeParams.overlapRead[index].dwBytesReadWritten, 
		&mergeParams.overlapRead[index].overlap);

	return SUCCESS; 
}


RC SMJ2::SMJ_Initialize(const FANS *fanR, const FANS *fanS)
{
	////////////////////////// Init R relation //////////////////////////////////////////////// 
	R = new RELATION();   

	// Key to join
	R->keyPos = m_SmjParams.R_KEY_POS;

	// Init double read buffer
	R->dbcRead = new DoubleBuffer(m_SmjParams.JOIN_READ_BUFFER_SIZE);
	utl->InitBuffer(R->dbcRead->buffer[0], m_SmjParams.JOIN_READ_BUFFER_SIZE, &bufferPool);
	utl->InitBuffer(R->dbcRead->buffer[1], m_SmjParams.JOIN_READ_BUFFER_SIZE, &bufferPool);  

	// Init run page for R
	utl->InitBuffer(R->probePageBuffer, SSD_PAGE_SIZE, &bufferPool);
	utl->InitRunPage(R->probePage, R->probePageBuffer); 

	// Create file handle
	R->hFile = CreateFile(
		(LPCWSTR)fanR->fileName, // file to open
		GENERIC_READ,			// open for reading
		FILE_SHARE_READ,        // share for reading
		NULL,					// default security
		OPEN_EXISTING,			// existing file only
		FILE_FLAG_OVERLAPPED | FILE_FLAG_NO_BUFFERING ,	// overlapped operation //| FILE_FLAG_NO_BUFFERING
		NULL);					// no attr. template 

	if (INVALID_HANDLE_VALUE==R->hFile) 
	{ 
		ShowMB(L"Cannot create handle of file %s\r\n", fanR->fileName);
		return ERR_CANNOT_CREATE_HANDLE;
	} 

	// Create overlap srtructure
	LARGE_INTEGER *liSizeR = new LARGE_INTEGER();  
	if (!GetFileSizeEx(R->hFile, liSizeR))
	{  
		ShowMB(L"Cannot get size of file %s\r\n", fanR->fileName);
		return ERR_CANNOT_GET_FILE_SIZE;
	}  

	R->overlapRead.totalChunk = chROUNDUP(liSizeR->QuadPart, m_SmjParams.JOIN_READ_BUFFER_SIZE) / m_SmjParams.JOIN_READ_BUFFER_SIZE;
	R->overlapRead.chunkIndex = 0;
	R->overlapRead.startChunk = 0;
	R->overlapRead.endChunk = R->overlapRead.totalChunk; 
	R->overlapRead.dwBytesToReadWrite = m_SmjParams.JOIN_READ_BUFFER_SIZE;
	R->overlapRead.dwBytesReadWritten = 0;
	R->overlapRead.fileSize.QuadPart = 0;
	R->overlapRead.overlap.Offset = 0;
	R->overlapRead.overlap.OffsetHigh = 0;

	////////////////////////// Init S relation ////////////////////////////////////////////////  
	S = new RELATION(); 

	// Create key join atttribute
	S->keyPos = m_SmjParams.S_KEY_POS;

	// Init double read buffer
	S->dbcRead = new DoubleBuffer(m_SmjParams.JOIN_READ_BUFFER_SIZE);
	utl->InitBuffer(S->dbcRead->buffer[0], m_SmjParams.JOIN_READ_BUFFER_SIZE, &bufferPool);
	utl->InitBuffer(S->dbcRead->buffer[1], m_SmjParams.JOIN_READ_BUFFER_SIZE, &bufferPool);  

	// Init run page for S
	utl->InitBuffer(S->probePageBuffer, SSD_PAGE_SIZE, &bufferPool);
	utl->InitRunPage(S->probePage, S->probePageBuffer);

	// Create file handle
	S->hFile = CreateFile(
		(LPCWSTR)fanS->fileName, // file to open
		GENERIC_READ,			// open for reading
		FILE_SHARE_READ,        // share for reading
		NULL,					// default security
		OPEN_EXISTING,			// existing file only
		FILE_FLAG_OVERLAPPED | FILE_FLAG_NO_BUFFERING ,	// overlapped operation //| FILE_FLAG_NO_BUFFERING
		NULL);					// no attr. template 

	if (INVALID_HANDLE_VALUE==S->hFile) 
	{  
		//myMessage.Add(L"Cannot create handle of file %s\r\n", m_Params.RELATION_S_PATH);
		return ERR_CANNOT_CREATE_HANDLE;
	} 

	// Create overlap structure for S
	LARGE_INTEGER *liSizeS = new LARGE_INTEGER();  
	if (!GetFileSizeEx(S->hFile, liSizeS))
	{  
		ShowMB(L"Cannot get size of file %s\r\n", fanS->fileName);
		return ERR_CANNOT_GET_FILE_SIZE;
	}  

	S->overlapRead.totalChunk = chROUNDUP(liSizeS->QuadPart, m_SmjParams.JOIN_READ_BUFFER_SIZE) / m_SmjParams.JOIN_READ_BUFFER_SIZE;
	S->overlapRead.chunkIndex = 0;
	S->overlapRead.startChunk = 0;
	S->overlapRead.endChunk = S->overlapRead.totalChunk; 
	S->overlapRead.dwBytesToReadWrite = m_SmjParams.JOIN_READ_BUFFER_SIZE;
	S->overlapRead.dwBytesReadWritten = 0;
	S->overlapRead.fileSize.QuadPart = 0; 
	S->overlapRead.overlap.Offset = 0;
	S->overlapRead.overlap.OffsetHigh = 0;

	////////////////////////// Init Join relation ////////////////////////////////////////////////  
	SMJ_GetJoinFilePath(m_JoinFilePath, m_SmjParams.RELATION_R_NO_EXT, m_SmjParams.RELATION_S_NO_EXT); 

	// Init doube buffer for write
	RS = new RELATION_JOIN();
	RS->dbcWrite = new DoubleBuffer(m_SmjParams.JOIN_WRITE_BUFFER_SIZE);
	utl->InitBuffer(RS->dbcWrite->buffer[0], m_SmjParams.JOIN_WRITE_BUFFER_SIZE, &bufferPool);
	utl->InitBuffer(RS->dbcWrite->buffer[1], m_SmjParams.JOIN_WRITE_BUFFER_SIZE, &bufferPool);  

	// Init run page for join relation
	utl->InitBuffer(RS->runPageBuffer, SSD_PAGE_SIZE, &bufferPool);
	utl->InitRunPage(RS->runPage, RS->runPageBuffer);

	// Create handle to write file
	RS->hFile =CreateFile(
		(LPCWSTR)m_JoinFilePath,		// file to write
		GENERIC_WRITE,			// open for writing
		0,						// Do not share
		NULL,					// default security
		CREATE_ALWAYS,			// Overwrite existing
		FILE_FLAG_OVERLAPPED,	// overlapped operation //| FILE_FLAG_OVERLAPPED
		NULL);					// no attr. template

	if (INVALID_HANDLE_VALUE==RS->hFile) 
	{ 
		ShowMB(L"Cannot create handle of file %s\r\n", m_JoinFilePath);
		return ERR_CANNOT_CREATE_HANDLE;
	}   

	// Init overlap write structure
	RS->overlapWrite.totalChunk = 0;
	RS->overlapWrite.chunkIndex = 0;
	RS->overlapWrite.startChunk = 0;
	RS->overlapWrite.endChunk = 0; 
	RS->overlapWrite.dwBytesToReadWrite = m_SmjParams.JOIN_WRITE_BUFFER_SIZE;
	RS->overlapWrite.dwBytesReadWritten = 0;
	RS->overlapWrite.fileSize.QuadPart = 0; 
	RS->overlapWrite.overlap.Offset = 0;
	RS->overlapWrite.overlap.OffsetHigh = 0;

	return SUCCESS;
}

RC SMJ2::RP_MergePhase_Write()
{
	WriteFile(hOutputFile,  
		FRONT_BUFFER(mergeParams.dbcWrite).data,  
		FRONT_BUFFER(mergeParams.dbcWrite).currentSize,  
		&mergeParams.overlapWrite.dwBytesReadWritten,  
		&mergeParams.overlapWrite.overlap); 

	return SUCCESS;
}


RC SMJ2::RP_PartitionPhase_SaveFanIn(DWORD64 lowestKey, DWORD64 highestKey, DWORD64 tupleCount, DWORD64 pageCount)
{
	FANS *tempFan = new FANS();
	wcscpy(tempFan->fileName, m_FanPath);
	tempFan->lowestKey = lowestKey;
	tempFan->highestKey = highestKey;
	tempFan->tupleCount = tupleCount;
	tempFan->pageCount = pageCount;
	tempFan->fileSize.QuadPart = pageCount * SSD_PAGE_SIZE;
	m_FanIns.push(tempFan);

	if(isR)
		rp.PartitionNumR++;
	else
		rp.PartitionNumS++;

	return SUCCESS;
}


RC SMJ2::RP_PartitionPhase_SentToOutput(RECORD *recordPtr, DWORD64 &pageCount)
{  
	if(utl->IsPageFull(partitionParams.writePage))
	{   
		utl->AddPageToBuffer(partitionParams.writeBuffer, partitionParams.writePage->page, SSD_PAGE_SIZE); //4 
		partitionParams.writeBuffer.currentPageIndex++; 
		utl->ResetRunPage(partitionParams.writePage, partitionParams.writePageBuffer);  
		partitionParams.writePage->consumed = TRUE;
		pageCount++;
	}  

	if(utl->IsBufferFull(partitionParams.writeBuffer))  
	{ 
		RP_PartitionPhase_Write();  
	}

	utl->AddTupleToPage(partitionParams.writePage, recordPtr, partitionParams.writePageBuffer);   // Add this tuples to page   
	partitionParams.writePage->consumed = FALSE;
	return SUCCESS;
}


RC SMJ2::RP_PartitionPhase_Read()
{    
	// read next chunk
	partitionParams.overlapRead->fileSize.QuadPart = partitionParams.overlapRead->chunkIndex * partitionParams.overlapRead->dwBytesToReadWrite;
	partitionParams.overlapRead->overlap.Offset = partitionParams.overlapRead->fileSize.LowPart;
	partitionParams.overlapRead->overlap.OffsetHigh = partitionParams.overlapRead->fileSize.HighPart;
	/* Read source table to buffer, always read into BACK */  
	utl->ResetBuffer(BACK_BUFFER(partitionParams.dbcRead));  
	// async read
	ReadFile(hInputFile, 
		BACK_BUFFER(partitionParams.dbcRead).data, 
		partitionParams.overlapRead->dwBytesToReadWrite, 
		&partitionParams.overlapRead->dwBytesReadWritten,  
		&partitionParams.overlapRead->overlap);

	return SUCCESS;
}

RC SMJ2::RP_PartitionPhase_Write()
{   
	DWORD dwByteWritten = 0;
	WriteFile(hOutputFile, 
		partitionParams.writeBuffer.data, 
		partitionParams.writeBuffer.currentSize, 
		&dwByteWritten, 
		NULL);

	utl->ResetBuffer(partitionParams.writeBuffer);

	return SUCCESS;
} 


RC SMJ2::RP_MergePhase_GetNextRecord(RECORD *&record, INT index)
{    
	if(mergeParams.mergeBuffer[index].currentTupleIndex > mergeParams.mergeBuffer[index].tupleCount)  
	{
		// Read complete this page, need to fetch next page from disk  
		utl->ResetBuffer(mergeParams.mergeBuffer[index]);  
		FRONT_BUFFER(mergeParams.dbcRead[index]).currentPageIndex++;   
		if(FRONT_BUFFER(mergeParams.dbcRead[index]).currentPageIndex >= FRONT_BUFFER(mergeParams.dbcRead[index]).pageCount)
		{    
			utl->ResetBuffer(FRONT_BUFFER(mergeParams.dbcRead[index])); 

			// Waiting for reading in to back buffer completed 
			if(mergeParams.overlapRead[index].chunkIndex < mergeParams.overlapRead[index].totalChunk)
				GetOverlappedResult(mergeParams.hFanIn[index], &mergeParams.overlapRead[index].overlap, &mergeParams.overlapRead[index].dwBytesReadWritten,  TRUE); 

			mergeParams.overlapRead[index].chunkIndex++;  
			if(mergeParams.overlapRead[index].dwBytesReadWritten==0)
			{
				utl->ResetBuffer(BACK_BUFFER(mergeParams.dbcRead[index]));
				utl->AddPageMAXToBuffer(mergeParams.mergeBuffer[index], SSD_PAGE_SIZE); 
				utl->AddPageToBuffer(BACK_BUFFER(mergeParams.dbcRead[index]),mergeParams.mergeBuffer[index].data, SSD_PAGE_SIZE); 
				mergeParams.mergeBuffer[index].isFullMaxValue = TRUE; 
				FRONT_BUFFER(mergeParams.dbcRead[index]).currentPageIndex = 0;
			}
			else
			{
				utl->ComputeBuffer(BACK_BUFFER(mergeParams.dbcRead[index]), mergeParams.overlapRead[index].dwBytesReadWritten);   
			}

			//Swap buffer to continue merge 
			mergeParams.dbcRead[index]->SwapBuffers();  
			// read next chunk
			RP_MergePhase_Read(index);   
		}   

		utl->GetPageInfo(FRONT_BUFFER(mergeParams.dbcRead[index]).data, 
			mergeParams.mergeBuffer[index], 
			FRONT_BUFFER(mergeParams.dbcRead[index]).currentPageIndex, 
			SSD_PAGE_SIZE);   

		mergeParams.mergeBuffer[index].currentTupleIndex=1; 
	}

	utl->GetTupleInfo(record, mergeParams.mergeBuffer[index].currentTupleIndex, mergeParams.mergeBuffer[index].data, SSD_PAGE_SIZE, m_RpParams.KEY_POS);  
	mergeParams.mergeBuffer[index].currentTupleIndex++;

	return SUCCESS; 
}


RC SMJ2::RP_PartitionPhase_GetNextRecord(RECORD *&recordPtr)
{  
	if(partitionParams.readPageBuffer.currentTupleIndex > partitionParams.readPageBuffer.tupleCount)
	{
		utl->ResetBuffer(partitionParams.readPageBuffer); 
		FRONT_BUFFER(partitionParams.dbcRead).currentPageIndex++; // read next page 
		if(FRONT_BUFFER(partitionParams.dbcRead).currentPageIndex >= FRONT_BUFFER(partitionParams.dbcRead).pageCount)
		{  
			utl->ResetBuffer(FRONT_BUFFER(partitionParams.dbcRead)); 

			// Wait for Read complete 
			if(partitionParams.overlapRead->chunkIndex < partitionParams.overlapRead->totalChunk)
				GetOverlappedResult(hInputFile, &partitionParams.overlapRead->overlap,  &partitionParams.overlapRead->dwBytesReadWritten, TRUE);   

			partitionParams.overlapRead->chunkIndex++;

			if(partitionParams.overlapRead->dwBytesReadWritten==0 ) 
			{ 
				utl->AddPageMAXToBuffer(partitionParams.readPageBuffer, SSD_PAGE_SIZE); 
				utl->AddPageToBuffer(BACK_BUFFER(partitionParams.dbcRead), partitionParams.readPageBuffer.data, SSD_PAGE_SIZE);
				//utl->GetPageInfo(partitionParams.readPageBuffer.data, BACK_BUFFER(partitionParams.dbcRead), 0, SSD_PAGE_SIZE);
				partitionParams.readPageBuffer.isFullMaxValue = TRUE;
				partitionParams.readPageBuffer.currentTupleIndex=1;   
				utl->SetMaxTuple(recordPtr);
				return SUCCESS;
			} 
			else
			{
				utl->ComputeBuffer(BACK_BUFFER(partitionParams.dbcRead), partitionParams.overlapRead->dwBytesReadWritten, SSD_PAGE_SIZE); 
			}

			partitionParams.dbcRead->UnLockProducer();

			partitionParams.dbcRead->SwapBuffers();

			// Next async read
			partitionParams.dbcRead->LockProducer();
			RP_PartitionPhase_Read(); 
		} 

		utl->GetPageInfo(FRONT_BUFFER(partitionParams.dbcRead).data, partitionParams.readPageBuffer, FRONT_BUFFER(partitionParams.dbcRead).currentPageIndex, SSD_PAGE_SIZE); 
		partitionParams.readPageBuffer.currentTupleIndex = 1; 
	}

	utl->GetTupleInfo(recordPtr, partitionParams.readPageBuffer.currentTupleIndex, partitionParams.readPageBuffer.data, SSD_PAGE_SIZE, m_RpParams.KEY_POS);
	partitionParams.readPageBuffer.currentTupleIndex++;


	return SUCCESS;
}

RC SMJ2::RP_PartitionPhase_CreateNewRun()
{
	/* Get fanIn name for heap tree */

	RP_GetFanPath();  

	hOutputFile=CreateFile((LPCWSTR)m_FanPath,		// file to write
		GENERIC_WRITE,			// open for writing
		0,						// Do not share
		NULL,					// default security
		CREATE_ALWAYS,			// Overwrite existing
		FILE_ATTRIBUTE_NORMAL,	// overlapped operation // FILE_FLAG_OVERLAPPED
		NULL);					// no attr. template

	if (INVALID_HANDLE_VALUE==hOutputFile) 
	{ 
		ShowMB(L"Cannot create output file %s", m_FanPath);    
		return ERR_CANNOT_CREATE_HANDLE; 
	}  

	//partitionParams.overlapWrite->fileSize.QuadPart = 0;
	//partitionParams.overlapWrite->overlap.Offset = 0;
	//partitionParams.overlapWrite->overlap.OffsetHigh = 0;
	//partitionParams.dbcWrite->bFirstProduce = TRUE; 
	return SUCCESS;
}


RC  SMJ2::RP_GetFanPath()
{      
	swprintf_s(m_FanPath, MAX_PATH, L"%s%d_%s.dat", m_RpParams.WORK_SPACE_PATH,  m_FanIndex, m_RpParams.FILE_NAME_NO_EXT);   
	InterlockedExchangeAdd(&m_FanIndex, 1);   
	return SUCCESS; 
}  

RC SMJ2::CopyRecord(RECORD *des, RECORD *&src)
{
	des->key = src->key;
	//strcpy(des->data, src->data);
	strncpy(des->data, src->data, src->length);
	des->length= src->length;
	des->mark = src->mark;

	return SUCCESS;
} 

INT SMJ2::TreeParent(int i)
{
	return floor(i/2); 
}

INT  SMJ2::TreeLeft(INT i)
{
	return   2 * i + 1; // 2 * i;  
} 

INT SMJ2::TreeRight (INT i)
{
	return  2 * i + 2;  
} 

RC SMJ2::MinHeapify(RECORD **rec, INT i, INT heapSize)
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

RC SMJ2::MaxHeapify(RECORD **rec, INT i, INT heapSize)
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

RC SMJ2::BuildMinHeap(RECORD **rec, INT heapSize)
{  
	for(int i=heapSize/2; i>=0; i--)
		MinHeapify(rec, i, heapSize);	// array[0] is the largest item 

	return SUCCESS;
}

RC SMJ2::BuildMaxHeap(RECORD **rec, INT heapSize)
{  
	for(int i=heapSize/2; i>=0; i--)
		MaxHeapify(rec, i, heapSize); // array[0] is the smallest item

	return SUCCESS;
}


RC SMJ2::SMJ_GetNextRecord(RELATION *&rel, RECORD *&record, BOOL bFirstRead)
{  
	RC rc;
	// Get tuple in Front buffer 
	if(bFirstRead==TRUE)
	{ 
		// First time, two read buffer are empty, 
		// we read into the BACK buffer, wait for read operation 
		// complete then swap BACK to FRONT 
		utl->ResetBuffer(BACK_BUFFER(rel->dbcRead));

		if(SUCCESS==SMJ_Read(rel))
		{
			// Wait for read operation complete    
			GetOverlappedResult(rel->hFile, 
				&rel->overlapRead.overlap, 
				&rel->overlapRead.dwBytesReadWritten, 
				TRUE);  

			// Compute info in BACK buffer
			utl->ComputeBuffer(BACK_BUFFER(rel->dbcRead), rel->overlapRead.dwBytesReadWritten);

			// Increase already read chunk number
			rel->overlapRead.chunkIndex++; 
			rel->dbcRead->UnLockProducer();


			//////////////////////////////////////////////////////////////////////////
			// Swap BACK to FRONT
			rel->dbcRead->SwapBuffers();
			//////////////////////////////////////////////////////////////////////////

			// Read next chunk into BACK buffer
			SMJ_Read(rel);

			rel->dbcRead->LockConsumer();

			// Read in to probe page
			utl->ResetBuffer(rel->probePageBuffer); 
			utl->GetPageInfo(FRONT_BUFFER(rel->dbcRead).data, rel->probePageBuffer, 0, SSD_PAGE_SIZE);  
		} 
	}

	// Get info from FRONT buffer
	if(rel->probePageBuffer.currentTupleIndex > rel->probePageBuffer.tupleCount)
	{ 
		FRONT_BUFFER(rel->dbcRead).currentPageIndex++;  // read next page in input buffer
		if(FRONT_BUFFER(rel->dbcRead).currentPageIndex >= FRONT_BUFFER(rel->dbcRead).pageCount)
		{
			// All page in FRONT buffer read complete, reset FRONT buffer

			utl->ResetBuffer(FRONT_BUFFER(rel->dbcRead)); 
			rel->dbcRead->UnLockConsumer();

			// Wait for operation read into BACK complete 
			GetOverlappedResult(rel->hFile, 
				&rel->overlapRead.overlap, 
				&rel->overlapRead.dwBytesReadWritten, 
				TRUE);  

			// Compute info in BACK buffer
			utl->ComputeBuffer(BACK_BUFFER(rel->dbcRead), rel->overlapRead.dwBytesReadWritten); 
			rel->overlapRead.chunkIndex++;  // Increase already read chunk number

			rel->dbcRead->UnLockProducer();

			rel->dbcRead->SwapBuffers();

			// Continue read into BACK
			rc = SMJ_Read(rel);

			rel->dbcRead->LockConsumer();

			// Read in to probe page
			utl->ResetBuffer(rel->probePageBuffer); 
			utl->GetPageInfo(FRONT_BUFFER(rel->dbcRead).data, rel->probePageBuffer, 0, SSD_PAGE_SIZE);  
		}
		else
		{
			// Read current page
			utl->GetPageInfo(FRONT_BUFFER(rel->dbcRead).data, 
				rel->probePageBuffer, 
				FRONT_BUFFER(rel->dbcRead).currentPageIndex, 
				SSD_PAGE_SIZE); 

			rel->probePageBuffer.currentTupleIndex=1;  
		} 
	}

	utl->GetTupleInfo(record, 
		rel->probePageBuffer.currentTupleIndex, 
		rel->probePageBuffer.data, 
		SSD_PAGE_SIZE, 
		rel->keyPos); 

	rel->probePageBuffer.currentTupleIndex++; 

	return SUCCESS;
} 

RC SMJ2::SMJ_SentOutput(RECORD *joinRecord)
{
	// Alway build join page into BACK(writeBuffer)

	if(utl->IsJoinPageFull(RS->runPage))
	{    
		utl->AddPageToBuffer(BACK_BUFFER(RS->dbcWrite),  RS->runPage->page, SSD_PAGE_SIZE); //4

		//bPageConsumed=true; 

		BACK_BUFFER(RS->dbcWrite).currentPageIndex+=1;

		utl->ResetRunPage(RS->runPage, RS->runPageBuffer);       
	}  

	if(utl->IsBufferFull(BACK_BUFFER(RS->dbcWrite)))
	{
		// output buffer is full, write to disk
		if(RS->dbcWrite->bFirstProduce == TRUE)
		{
			RS->dbcWrite->bFirstProduce = FALSE;

			RS->dbcWrite->UnLockProducer();

			RS->dbcWrite->SwapBuffers();

			SMJ_Write();

			RS->dbcWrite->LockProducer(); 
		}
		else
		{ 
			// Must wait for previous write complete
			GetOverlappedResult(RS->hFile,
				&RS->overlapWrite.overlap,
				&RS->overlapWrite.dwBytesReadWritten,
				TRUE); 

			// Update new position in output file
			RS->overlapWrite.fileSize.QuadPart+=RS->overlapWrite.dwBytesReadWritten;
			RS->overlapWrite.overlap.Offset = RS->overlapWrite.fileSize.LowPart;
			RS->overlapWrite.overlap.OffsetHigh = RS->overlapWrite.fileSize.HighPart; 

			// Reset FRONT buffer
			utl->ResetBuffer(FRONT_BUFFER(RS->dbcWrite));

			RS->dbcWrite->UnLockConsumer();

			RS->dbcWrite->UnLockProducer();

			RS->dbcWrite->SwapBuffers();

			// Write to disk
			SMJ_Write();

			RS->dbcWrite->LockProducer(); 
		} 
	}

	utl->AddTupleToJoinPage(RS->runPage, joinRecord, RS->runPageBuffer);   // Add this pair of tuples to page   

	return SUCCESS;
}

VOID SMJ2::SMJ_MakeNewRecord(RECORD *&joinRecord, RECORD *leftRecord, RECORD *rightRecord) 
{
	joinRecord->key = leftRecord->key;
	joinRecord->length = leftRecord->length + rightRecord->length;
	memcpy(joinRecord->data, leftRecord->data, leftRecord->length);
	memcpy(joinRecord->data + leftRecord->length, rightRecord->data, rightRecord->length); 
}

RC SMJ2::SMJ_Read(RELATION *&rel)
{   
	rel->dbcRead->LockProducer();

	if (rel->overlapRead.chunkIndex >= rel->overlapRead.totalChunk)  
	{    
		utl->ResetBuffer(BACK_BUFFER(rel->dbcRead));
		utl->AddPageToBuffer(BACK_BUFFER(rel->dbcRead), NULL, SSD_PAGE_SIZE); 

		BACK_BUFFER(rel->dbcRead).currentPageIndex = 0;
		BACK_BUFFER(rel->dbcRead).currentSize = SSD_PAGE_SIZE;
		BACK_BUFFER(rel->dbcRead).pageCount = 1;
		BACK_BUFFER(rel->dbcRead).tupleCount = utl->GetTupleNumInMaxPage(); 
		BACK_BUFFER(rel->dbcRead).isSort = TRUE;
		BACK_BUFFER(rel->dbcRead).isFullMaxValue = TRUE;

		// Read in to probe page
		utl->ResetBuffer(rel->probePageBuffer); 
		utl->GetPageInfo(BACK_BUFFER(rel->dbcRead).data, rel->probePageBuffer, 0, SSD_PAGE_SIZE);

		rel->dbcRead->UnLockProducer();

		return ERR_END_OF_FILE;
	}  

	rel->overlapRead.fileSize.QuadPart = rel->overlapRead.chunkIndex * m_SmjParams.JOIN_READ_BUFFER_SIZE; // chunkNum * chunkSize; 
	rel->overlapRead.overlap.Offset = rel->overlapRead.fileSize.LowPart;
	rel->overlapRead.overlap.OffsetHigh = rel->overlapRead.fileSize.HighPart;  

	// Attempt an asynchronous read operation.  
	ReadFile(rel->hFile, 
		BACK_BUFFER(rel->dbcRead).data, 
		rel->overlapRead.dwBytesToReadWrite, 
		&rel->overlapRead.dwBytesReadWritten, 
		&rel->overlapRead.overlap); 


	// return immediately

	return SUCCESS;
}  

RC SMJ2::SMJ_Write() 
{     
	RS->dbcWrite->LockConsumer();

	// File systems extend files synchronously. Extend the destination file 
	// now so that I/Os execute asynchronously improving performance. 
	WriteFile(RS->hFile, 
		FRONT_BUFFER(RS->dbcWrite).data, 
		FRONT_BUFFER(RS->dbcWrite).currentSize,  
		&RS->overlapWrite.dwBytesReadWritten, 
		&RS->overlapWrite.overlap); 			  


	return SUCCESS;
} 

LPWSTR SMJ2::SMJ_GetJoinFile()
{
	return m_JoinFilePath;
}

VOID SMJ2::SMJ_GetJoinFilePath(LPWSTR &joinFilePath, LPWSTR rNameNoExt, LPWSTR sNameNoExt)  
{    
	swprintf(joinFilePath, MAX_PATH, L"%s%s_join_%s.dat", m_SmjParams.WORK_SPACE_PATH, rNameNoExt, sNameNoExt);  
} 

RC SMJ2::SMJ_Join()
{    
	//First read to fill buffer
	RECORD *tupleR = new RECORD();
	RECORD *tupleS = new RECORD();
	RECORD *tupleJoin = new RECORD(TUPLE_SIZE*2);

	SMJ_GetNextRecord(R, tupleR, TRUE);
	SMJ_GetNextRecord(S, tupleS, TRUE); 

	RS->dbcWrite->LockProducer();

	while((tupleR->key!=MAX) && (tupleS->key!=MAX))
	{  
		// while left is less than right, move left up
		while (tupleR->key < tupleS->key) 
		{
			SMJ_GetNextRecord(R, tupleR, FALSE); 
			if (tupleR->key == MAX)  { break; }
		} 

		// if done, no more joins, break
		if (tupleR->key == MAX)  { break; } 

		// while left is greater than right, move right up
		while (tupleR->key > tupleS->key) 
		{
			SMJ_GetNextRecord(S, tupleS, FALSE);  
			if (tupleS->key == MAX) { break; }
		} 

		// if done, no more joins, break
		if (tupleS->key == MAX) {  break;  }

		// while the two are equal, segment equal
		while (tupleR->key == tupleS->key) 
		{   
			SMJ_MakeNewRecord(tupleJoin, tupleR, tupleS);

			// Send this join tuple to BACK write buffer 
			SMJ_SentOutput(tupleJoin); 

			// Get next S tuple
			SMJ_GetNextRecord(S, tupleS, FALSE); 

			while (tupleS->key == tupleR->key) 
			{  
				SMJ_MakeNewRecord(tupleJoin, tupleR, tupleS);

				// Save this to Output buffer
				SMJ_SentOutput(tupleJoin);

				// Get next S tuple
				SMJ_GetNextRecord(S, tupleS, FALSE); 

				if (tupleS->key == MAX)  { break; }
			}

			// Get next R tuple
			SMJ_GetNextRecord(R, tupleR, FALSE);   

			if (tupleR->key == MAX)  { break; }
		}

		// Get next S tuple
		SMJ_GetNextRecord(S, tupleS, FALSE);  
	}


	if(FRONT_BUFFER(RS->dbcWrite).currentSize > 0)
	{ 
		SMJ_Write();

		GetOverlappedResult(RS->hFile,
			&RS->overlapWrite.overlap,
			&RS->overlapWrite.dwBytesReadWritten,
			TRUE); 

		// Update new position in output file
		RS->overlapWrite.fileSize.QuadPart+=RS->overlapWrite.dwBytesReadWritten;
		RS->overlapWrite.overlap.Offset = RS->overlapWrite.fileSize.LowPart;
		RS->overlapWrite.overlap.OffsetHigh = RS->overlapWrite.fileSize.HighPart; 

		// Reset FRONT buffer
		utl->ResetBuffer(FRONT_BUFFER(RS->dbcWrite));

	}

	// Terminate join file
	CloseHandle(RS->hFile); 

	delete tupleR;
	delete tupleS;
	delete tupleJoin;

	return SUCCESS;
} 

RC SMJ2::SMJ_CheckEnoughMemory()
{
	// TODO: calculate Heap size
	DOUBLE memorySize = m_SmjParams.BUFFER_POOL_SIZE;
	memorySize = memorySize - (2*m_SmjParams.JOIN_READ_BUFFER_SIZE) - (2 * m_SmjParams.JOIN_WRITE_BUFFER_SIZE);
	memorySize = memorySize - SSD_PAGE_SIZE - SSD_PAGE_SIZE; // for read, write page
	if(memorySize<=0)
		return ERR_NOT_ENOUGH_MEMORY; 

	return SUCCESS;
}

UINT64 SMJ2::RP_GetFanSize(HANDLE hFile)  
{
	LARGE_INTEGER liFileSize; 
	if (!GetFileSizeEx(hFile, &liFileSize))
	{      
		return 0;
	} 

	return liFileSize.QuadPart;
}
