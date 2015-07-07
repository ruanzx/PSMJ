#include "stdafx.h"
#include "ReplacementSelection.h"

// constructor
ReplacementSelection::ReplacementSelection(const RP_PARAMS vParams) : m_Params(vParams)
{
	m_FanIndex = 0; 
	m_HeapSize = 0;
	m_FanPath = new TCHAR[MAX_PATH];
	utl = new PageHelpers2();  
	m_MergePass = 1;
	m_HeapSize = 0;
}

// Destructor
ReplacementSelection::~ReplacementSelection()
{
	delete m_FanPath;
	delete utl;
}

RC ReplacementSelection::RP_CheckEnoughMemory()
{
	// TODO: calculate Heap size
	DOUBLE memorySize = m_Params.BUFFER_POOL_SIZE;
	memorySize = memorySize - (2*m_Params.SORT_READ_BUFFER_SIZE) - (m_Params.SORT_WRITE_BUFFER_SIZE); // not use double buffer for write output run
	memorySize = memorySize - SSD_PAGE_SIZE - SSD_PAGE_SIZE; // for read, write page
	
	if(memorySize<=0)
		return ERR_NOT_ENOUGH_MEMORY;

	m_HeapSize = chROUNDDOWN( memorySize, TUPLE_SIZE) / TUPLE_SIZE;

	return SUCCESS;
}

RC ReplacementSelection::RP_Initialize()
{
	/* Create handle for source table */ 
	RC rc;
	hInputFile=CreateFile(
		(LPCWSTR)m_Params.SORT_FILE_PATH,
		GENERIC_READ,
		FILE_SHARE_READ,
		NULL,
		OPEN_EXISTING,
		FILE_FLAG_OVERLAPPED | FILE_FLAG_NO_BUFFERING,
		NULL); 

	if(INVALID_HANDLE_VALUE==hInputFile)
	{  
		ShowMB(L"Cannot create handle %s", m_Params.SORT_FILE_PATH);
		return ERR_CANNOT_CREATE_HANDLE; 
	}

	// Init buffer pool
	bufferPool.size = m_Params.BUFFER_POOL_SIZE;
	bufferPool.data = new CHAR[bufferPool.size];
	bufferPool.currentSize = 0;

	if(NULL==bufferPool.data)
	{  
		return ERR_CANNOT_INITIAL_BUFFER_POOL;
	}
	//utl->InitBufferPool(&bufferPool, m_Params.BUFFER_POOL_SIZE);

	partitionParams.dbcRead = new DoubleBuffer(m_Params.SORT_READ_BUFFER_SIZE);
	rc = utl->InitBuffer(partitionParams.dbcRead->buffer[0], m_Params.SORT_READ_BUFFER_SIZE, &bufferPool);
	if(rc!=SUCCESS) { return rc; }

	rc = utl->InitBuffer(partitionParams.dbcRead->buffer[1], m_Params.SORT_READ_BUFFER_SIZE, &bufferPool); 
	if(rc!=SUCCESS) { return rc; }

	rc = utl->InitBuffer(partitionParams.writeBuffer, m_Params.SORT_WRITE_BUFFER_SIZE, &bufferPool);
	if(rc!=SUCCESS) { return rc; }

	partitionParams.currentMark = 0;  

	/* Get source table size */
	LARGE_INTEGER *liFileSize = new LARGE_INTEGER();  
	if (!GetFileSizeEx(hInputFile, liFileSize))
	{    
		return ERR_CANNOT_GET_FILE_SIZE;
	} 

	/* Set up overlap structure to READ source table */
	DWORD64 chunkSize = m_Params.SORT_READ_BUFFER_SIZE;
	DWORD64 totalChunk = chROUNDUP(liFileSize->QuadPart, chunkSize) / chunkSize; 

	partitionParams.overlapRead = new OVERLAPPEDEX();
	partitionParams.overlapRead->dwBytesToReadWrite = m_Params.SORT_READ_BUFFER_SIZE;   
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
	utl->InitBuffer(m_HeapBuffer, m_HeapSize * TUPLE_SIZE, &bufferPool);
	if(rc!=SUCCESS) { return rc; }

	return SUCCESS;
}


// Heap size is depened on available memory
DWORD  ReplacementSelection::RP_GetHeapSize() const
{
	return m_HeapSize;
}

UINT64 ReplacementSelection::GetFanSize(HANDLE hFile)  
{
	LARGE_INTEGER liFileSize; 
	if (!GetFileSizeEx(hFile, &liFileSize))
	{      
		return 0;
	} 

	return liFileSize.QuadPart;
}

RC ReplacementSelection::RP_Execute() 
{  
	RC rc;
	DOUBLE cpuTimeBefore = 0, cpuTimeAfter = 0,  cpuTime = 0;
	StopWatch stwTotalTime, stwMergeTime, stwPartitionTime;
	UINT64 totalTime = 0, partitionTime = 0, mergeTime = 0;
	DWORD partitionNum = 0;

	stwTotalTime.Start();
	stwPartitionTime.Start();
	cpuTimeBefore = GetCpuTime();

	//////////////////////////////////////////////////////////////////////////
	rc = RP_Initialize();
	if(rc!=SUCCESS) { return rc; }

	rc = PartitionPhase();
	if(rc!=SUCCESS) { return rc; }

	partitionNum = m_FanIns.size();
	partitionTime = stwPartitionTime.NowInMilliseconds();
	stwMergeTime.Start();

	//////////////////////////////////////////////////////////////////////////
	// reset buffer pool
	bufferPool.currentSize = 0; 

	rc = MergePhase();
	if(rc!=SUCCESS) { return rc; }

	//////////////////////////////////////////////////////////////////////////

	cpuTimeAfter = GetCpuTime(); 
	totalTime = stwTotalTime.NowInMilliseconds();
	mergeTime = stwMergeTime.NowInMilliseconds();
	cpuTime = cpuTimeAfter - cpuTimeBefore;

	//////////////////////////////////////////////////////////////////////////

	FILE *fp;   
	CHAR *reportFilePath = new CHAR[MAX_PATH];   
	LPWSTR tempReportPath = new TCHAR[MAX_PATH];
	swprintf(tempReportPath, MAX_PATH, L"%s%s", m_Params.WORK_SPACE_PATH, L"RP_Report.csv" ); 
	// convert file path to char  
	size_t  count = wcstombs(reportFilePath, tempReportPath, MAX_PATH); 

	CHAR *reportTitle = "Relation Size,Memory Size,Heap Size,Partition Num,Merge Pass,Read Buffer Size(sort),Write Buffer Size(sort),Read Buffer Size(merge),Write Buffer Size(merge),Total Execute Time(ms),Partition Time(ms),Merge Time(ms),CPU Time\n";
	CHAR *reportContent = new CHAR[1024];
	sprintf(reportContent, "%d,%d,%d,%d,%d,%d,%d,%d,%d,%lld,%lld,%lld,%.f", 
		(DWORD)GetFanSize(hInputFile)/SSD_PAGE_SIZE, 
		m_Params.BUFFER_POOL_SIZE/SSD_PAGE_SIZE, 
		m_HeapSize,
		partitionNum, 
		m_MergePass, 
		m_Params.SORT_READ_BUFFER_SIZE/SSD_PAGE_SIZE,
		m_Params.SORT_WRITE_BUFFER_SIZE/SSD_PAGE_SIZE, 
		m_Params.MERGE_READ_BUFFER_SIZE/SSD_PAGE_SIZE,
		m_Params.MERGE_WRITE_BUFFER_SIZE/SSD_PAGE_SIZE, 
		totalTime, 
		partitionTime, 
		mergeTime, 
		cpuTime);

	fp=fopen(reportFilePath, "w+b"); 
	fprintf(fp, reportTitle);
	fprintf(fp, reportContent);
	fclose(fp);

	delete reportFilePath;
	delete tempReportPath; 
	delete reportContent;


	CloseHandle(hInputFile);
	delete bufferPool.data;
	bufferPool.data = NULL;
	return SUCCESS;
}

RC ReplacementSelection::PartitionPhase()
{
	RECORD *pTempRecord = new RECORD(); /* Working record */
	RECORD *pOutRecord = new RECORD(); /* Output record from heap */
	RECORD *pInRecord = new RECORD(); /* Addition record fill in heap */

	/* Init heap tree*/
	const DWORD	HEAP_SIZE = RP_GetHeapSize(); 

	RECORD **heap = new RECORD *[HEAP_SIZE];
	for (UINT i = 0; i < HEAP_SIZE; i++)    
	{ 
		heap[i]  = new RECORD(0, m_HeapBuffer.data + TUPLE_SIZE * i, TUPLE_SIZE); 
	}

	//First read into buffer 
	partitionParams.dbcRead->LockProducer();
	PartitionPhase_Read();
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
	PartitionPhase_Read();  

	// Add tuple to heap
	for (UINT i = 0; i < HEAP_SIZE; i++)     
	{ 
		PartitionPhase_GetNextRecord(pTempRecord);  
		pTempRecord->mark = partitionParams.currentMark;
		CopyRecord(heap[i], pTempRecord);  
	}

	/* Create first run handle and name */
	PartitionPhase_CreateNewRun(); 

	/* Init fanIn counter */ 
	DWORD64 lastKey = 0; 
	DWORD64 pageCount = 0;
	DWORD64 tupleCount = 0;
	DWORD64 lowestKey, highestKey;  /* the min and max key in run file */
	BOOL    lowestKeyIsGet = FALSE;
	DWORD   lastNode = HEAP_SIZE-1;
	BOOL    isDone = FALSE; 

	partitionParams.writePage->consumed = TRUE;

	int ccccccc=0;
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
			PartitionPhase_GetNextRecord(pInRecord);  

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
					PartitionPhase_Write();  
				}

				utl->ResetRunPage(partitionParams.writePage, partitionParams.writePageBuffer); 

				CloseHandle(hOutputFile); 

				/* Save current run info */  
				PartitionPhase_SaveFanIn(lowestKey, highestKey, tupleCount, pageCount); 
				PartitionPhase_CreateNewRun(); // Get name, begin a new run 

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
			PartitionPhase_SentToOutput(pOutRecord, pageCount); 
			ccccccc++;
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
		PartitionPhase_SentToOutput(pOutRecord, pageCount);
		ccccccc++;
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

		PartitionPhase_Write();    
		utl->ResetRunPage(partitionParams.writePage, partitionParams.writePageBuffer);  
	}

	/* Terminate final run */    
	PartitionPhase_SaveFanIn(lowestKey, highestKey, tupleCount, pageCount);
	CloseHandle(hOutputFile);  


	delete pTempRecord;
	delete pInRecord;
	delete pOutRecord;

	return SUCCESS;  
}

RC ReplacementSelection::MergePhase()
{
	RC rc;
	// Compute fanIn num

	// Estimate memory
	////////////////////////////////////////////////////////////////////////// 
	DOUBLE totalMemory = m_Params.BUFFER_POOL_SIZE;  
	totalMemory = totalMemory - (m_Params.MERGE_WRITE_BUFFER_SIZE * 2);
	totalMemory = totalMemory - SSD_PAGE_SIZE; // write page buffer

	if(totalMemory <= 0)
	{ 
		return ERR_NOT_ENOUGH_MEMORY; 
	}

	// LoserKey + LoserTreeData + MergeBuffer + ReadInputBuffer*2 
	DOUBLE memoryNeedForOneFanIn = sizeof(UINT64) + (TUPLE_SIZE * sizeof(CHAR)) + SSD_PAGE_SIZE + (m_Params.MERGE_READ_BUFFER_SIZE*2);

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
	mergeParams.dbcWrite = new DoubleBuffer(m_Params.MERGE_WRITE_BUFFER_SIZE); 
	rc = utl->InitBuffer(mergeParams.dbcWrite->buffer[0], m_Params.MERGE_WRITE_BUFFER_SIZE, &bufferPool);
	if(rc!=SUCCESS) { return rc; }

	rc = utl->InitBuffer(mergeParams.dbcWrite->buffer[1], m_Params.MERGE_WRITE_BUFFER_SIZE, &bufferPool); 
	if(rc!=SUCCESS) { return rc; }
	//////////////////////////////////////////////////////////////////////////
	mergeParams.dbcRead = new DoubleBuffer*[m_MaxFanInNum];
	mergeParams.mergeBuffer = new Buffer[m_MaxFanInNum];

	for(DWORD i=0; i<m_MaxFanInNum; i++)
	{ 
		mergeParams.dbcRead[i] = new DoubleBuffer(m_Params.MERGE_READ_BUFFER_SIZE);
		rc = utl->InitBuffer(mergeParams.dbcRead[i]->buffer[0], m_Params.MERGE_READ_BUFFER_SIZE, &bufferPool);
		if(rc!=SUCCESS) { return rc; }

		rc = utl->InitBuffer(mergeParams.dbcRead[i]->buffer[1], m_Params.MERGE_READ_BUFFER_SIZE, &bufferPool); 
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

	while( TRUE ) // Multi merge passes
	{  
		GetFanPath(); 

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

		mergeParams.overlapWrite.dwBytesToReadWrite = m_Params.MERGE_WRITE_BUFFER_SIZE; 
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
		LoserTree lsTree(fanInNum);
		RECORD *recordPtr = new RECORD();  

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

			UINT64 fanSize = GetFanSize(mergeParams.hFanIn[i]);
			// TODO: check size =0 

			liFanOutSize.QuadPart+= fanSize;

			DWORD64 chunkSize = m_Params.MERGE_READ_BUFFER_SIZE; 
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
		liDestSize.QuadPart = chROUNDUP(liFanOutSize.QuadPart, m_Params.MERGE_WRITE_BUFFER_SIZE);

		SetFilePointerEx(hOutputFile, liDestSize, NULL, FILE_BEGIN);
		SetEndOfFile(hOutputFile); 

		// First read from disk to buffer
		//////////////////////////////////////////////////////////////////////////
		for(DWORD fanIndex=0; fanIndex<fanInNum;fanIndex++) 
			MergePhase_Read(fanIndex);     

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
			utl->GetTupleInfo(recordPtr, mergeParams.mergeBuffer[f].currentTupleIndex, mergeParams.mergeBuffer[f].data ,SSD_PAGE_SIZE, m_Params.KEY_POS);   
			mergeParams.mergeBuffer[f].currentTupleIndex++;
			lsTree.AddNewNode(recordPtr, f);  

			//mergeParams.dbcRead[f]->UnLockProducer();  

			mergeParams.dbcRead[f]->SwapBuffers();  // swap read buffer

			// Read next
			MergePhase_Read(f);
		} 

		lsTree.CreateLoserTree();     

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
			lsTree.GetMinRecord( recordPtr, index ); // index = loserTree[0]

			if(recordPtr->key==MAX) { break; }

			mergeParams.runPage->consumed = FALSE;

			// Save the record has lowest value
			if(bLowestIsSet==FALSE)
			{
				bLowestIsSet=TRUE;
				lowestValue= recordPtr->key;  
			} 

			// Save the record has highest value
			highestValue = recordPtr->key;   

			if(utl->IsBufferFull(BACK_BUFFER(mergeParams.dbcWrite))) // check Is buffer full?
			{   
				//mergeParams.dbcWrite->UnLockProducer();

				if(mergeParams.dbcWrite->bFirstProduce==TRUE)
				{ 
					mergeParams.dbcWrite->bFirstProduce=FALSE;  
					mergeParams.dbcWrite->SwapBuffers();   

					//mergeParams.dbcWrite->LockConsumer(); 	// First write 
					MergePhase_Write();  
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
					MergePhase_Write(); 
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
			utl->AddTupleToPage(mergeParams.runPage, recordPtr->data, mergeParams.runPageBuffer);  

			BACK_BUFFER(mergeParams.dbcWrite).tupleCount++;
			tupleCount++;

			MergePhase_GetNextRecord(recordPtr, index);  

			lsTree.AddNewNode(recordPtr, index);// Add new tuple to tree 
			lsTree.Adjust( index );  // Continue fight LOSER TREE 
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
			MergePhase_Write();

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


		FANS *fanOut= new FANS();
		wcscpy(fanOut->fileName, m_FanPath);
		fanOut->threadID = 0;
		fanOut->pageCount = liFanOutSize.QuadPart / SSD_PAGE_SIZE;
		fanOut->fileSize = liFanOutSize;
		fanOut->tupleCount = tupleCount;  
		fanOut->lowestKey = lowestValue; 
		fanOut->highestKey = highestValue; 
		m_FanIns.push(fanOut); 

		// Check is the last fanIn
		if(m_FanIns.size()==1)
		{ 
			break;
		}
		m_MergePass++;

		m_FanInWillDelete.push(fanOut);  
	} // end while multi merge passes

	if(m_Params.USE_DELETE_AFTER_OPERATION==TRUE)
	{
		//if(m_FanInWillDelete.size() > 0)
		//{
		//	while (m_FanInWillDelete.size() > 0)
		//	{
		//		FANS *deleteFanIn = new FANS();
		//		deleteFanIn = m_FanInWillDelete.front();
		//		m_FanInWillDelete.pop();
		//		if(DeleteFile(deleteFanIn->fileName))
		//		{
		//			//myMessage.Post(L"Delete file %s OK\r\n", deleteFanIn->fileName);
		//		} 
		//	} 
		//}
	} 

	return SUCCESS;
}

RC ReplacementSelection::MergePhase_Read(DWORD index)
{    
	LARGE_INTEGER chunk = {0};
	chunk.QuadPart = mergeParams.overlapRead[index].chunkIndex * m_Params.MERGE_READ_BUFFER_SIZE;   
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

RC ReplacementSelection::MergePhase_Write()
{
	WriteFile(hOutputFile,  
		FRONT_BUFFER(mergeParams.dbcWrite).data,  
		FRONT_BUFFER(mergeParams.dbcWrite).currentSize,  
		&mergeParams.overlapWrite.dwBytesReadWritten,  
		&mergeParams.overlapWrite.overlap); 

	return SUCCESS;
}

RC ReplacementSelection::PartitionPhase_SaveFanIn(DWORD64 lowestKey, DWORD64 highestKey, DWORD64 tupleCount, DWORD64 pageCount)
{
	FANS *tempFan = new FANS();
	wcscpy(tempFan->fileName, m_FanPath);
	tempFan->lowestKey = lowestKey;
	tempFan->highestKey = highestKey;
	tempFan->tupleCount = tupleCount;
	tempFan->pageCount = pageCount;
	tempFan->fileSize.QuadPart = pageCount * SSD_PAGE_SIZE;
	m_FanIns.push(tempFan);

	return SUCCESS;
}

RC ReplacementSelection::PartitionPhase_SentToOutput(RECORD *recordPtr, DWORD64 &pageCount)
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
		PartitionPhase_Write();  
	}

	utl->AddTupleToPage(partitionParams.writePage, recordPtr, partitionParams.writePageBuffer);   // Add this tuples to page   
	partitionParams.writePage->consumed = FALSE;
	return SUCCESS;
}

RC ReplacementSelection::PartitionPhase_Read()
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

RC ReplacementSelection::PartitionPhase_Write()
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

RC ReplacementSelection::MergePhase_GetNextRecord(RECORD *&record, INT index)
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
			MergePhase_Read(index);   
		}   

		utl->GetPageInfo(FRONT_BUFFER(mergeParams.dbcRead[index]).data, 
			mergeParams.mergeBuffer[index], 
			FRONT_BUFFER(mergeParams.dbcRead[index]).currentPageIndex, 
			SSD_PAGE_SIZE);   

		mergeParams.mergeBuffer[index].currentTupleIndex=1; 
	}

	utl->GetTupleInfo(record, mergeParams.mergeBuffer[index].currentTupleIndex, mergeParams.mergeBuffer[index].data, SSD_PAGE_SIZE, m_Params.KEY_POS);  
	mergeParams.mergeBuffer[index].currentTupleIndex++;

	return SUCCESS; 
}

RC ReplacementSelection::PartitionPhase_GetNextRecord(RECORD *&recordPtr)
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
			PartitionPhase_Read(); 
		} 

		utl->GetPageInfo(FRONT_BUFFER(partitionParams.dbcRead).data, partitionParams.readPageBuffer, FRONT_BUFFER(partitionParams.dbcRead).currentPageIndex, SSD_PAGE_SIZE); 
		partitionParams.readPageBuffer.currentTupleIndex = 1; 
	}

	utl->GetTupleInfo(recordPtr, partitionParams.readPageBuffer.currentTupleIndex, partitionParams.readPageBuffer.data, SSD_PAGE_SIZE, m_Params.KEY_POS);
	partitionParams.readPageBuffer.currentTupleIndex++;


	return SUCCESS;
}


RC ReplacementSelection::PartitionPhase_CreateNewRun()
{
	/* Get fanIn name for heap tree */

	GetFanPath();  

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

RC  ReplacementSelection::GetFanPath()
{      
	swprintf_s(m_FanPath, MAX_PATH, L"%s%d_%s.dat", m_Params.WORK_SPACE_PATH,  m_FanIndex, m_Params.FILE_NAME_NO_EXT);   
	InterlockedExchangeAdd(&m_FanIndex, 1);   
	return SUCCESS; 
} 

RC ReplacementSelection::CopyRecord(RECORD *des, RECORD *&src)
{
	des->key = src->key;
	//strcpy(des->data, src->data);
	strncpy(des->data, src->data, src->length);
	des->length= src->length;
	des->mark = src->mark;

	return SUCCESS;
} 

INT ReplacementSelection::TreeParent(int i)
{
	return floor(i/2); 
}

INT  ReplacementSelection::TreeLeft(INT i)
{
	return   2 * i + 1; // 2 * i;  
} 

INT ReplacementSelection::TreeRight (INT i)
{
	return  2 * i + 2;  
} 

RC ReplacementSelection::MinHeapify(RECORD **rec, INT i, INT heapSize)
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

RC ReplacementSelection::MaxHeapify(RECORD **rec, INT i, INT heapSize)
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

RC ReplacementSelection::BuildMinHeap(RECORD **rec, INT heapSize)
{  
	for(int i=heapSize/2; i>=0; i--)
		MinHeapify(rec, i, heapSize);	// array[0] is the largest item 

	return SUCCESS;
}

RC ReplacementSelection::BuildMaxHeap(RECORD **rec, INT heapSize)
{  
	for(int i=heapSize/2; i>=0; i--)
		MaxHeapify(rec, i, heapSize); // array[0] is the smallest item

	return SUCCESS;
}