// 
// Name: ExsMergePhase.cpp : implementation file 
// Author: hieunt
// Description: Implementation of merge phase in parallel external merge sort
//			    Merge all run created, if not enough memory, then multiple merge step is needed
//
#include "stdafx.h" 
#include "ExsMergePhase.h"  

/// <summary>
/// Initializes a new instance of the <see cref="ExsMergePhase"/> class.
/// </summary>
/// <param name="vParams">The PEMS parameters.</param>
/// <param name="vFanIns">The fan-ins queue.</param>
/// <param name="vAvailableMemorySize">Size of available memory.</param>
ExsMergePhase::ExsMergePhase(PEMS_PARAMS vParams, std::queue<FANS*> vFanIns, DWORD vAvailableMemorySize) : m_Params(vParams), m_FanIns(vFanIns)
{  
	m_FanOutName = new TCHAR[MAX_PATH];

	m_FanOutIndex = 0;
	m_MergePassNum = 0; 
	m_MaxFanIn = 0;
	if(vAvailableMemorySize==0)
		m_AvailableMemorySize = m_Params.BUFFER_POOL_SIZE;
	else
		m_AvailableMemorySize = vAvailableMemorySize;

	utl = new PageHelpers2();
} 

ExsMergePhase::~ExsMergePhase()
{  
	delete m_FanOutName; 
}

/// <summary>
/// Check enough memory for merges phase.
/// </summary>
/// <returns></returns>
RC ExsMergePhase::MergePhase_CheckEnoughMemory()
{ 
	DWORD fanInNum = m_FanIns.size();
	if(fanInNum==0)
	{ 
		return ERR_SORT_MERGE_FILE_NOT_ENOUGH;
	}

	// Estimate memory
	////////////////////////////////////////////////////////////////////////// 
	DOUBLE totalMemory = m_AvailableMemorySize;  

	DWORD memoryForWriteFanOut = m_Params.MERGE_WRITE_BUFFER_SIZE  * 2;
	DWORD memoryForAssemblyRunPage = SSD_PAGE_SIZE;
	totalMemory = totalMemory - memoryForWriteFanOut;
	totalMemory = totalMemory - memoryForAssemblyRunPage;

	if( totalMemory < 0)
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

	if(totalMemory < 0)
	{ 
		return ERR_NOT_ENOUGH_MEMORY; 
	}

	return SUCCESS;
}

/// <summary>
/// Initialize merges phase.
/// </summary>
/// <returns></returns>
RC ExsMergePhase::MergePhase_Initialize()
{    
	RC rc;
	bufferPool.size = m_Params.BUFFER_POOL_SIZE;
	bufferPool.currentSize = 0;
	bufferPool.data = new CHAR[bufferPool.size];

	if(NULL==bufferPool.data)
	{  
		return ERR_CANNOT_INITIAL_BUFFER_POOL;
	}


	m_ThreadParams = new ExsMergeThreadParams(); 
	m_ThreadParams->maxFanIn = m_MaxFanIn;
	m_ThreadParams->threadID = GetCurrentThreadId();
	m_ThreadParams->keyPosition = m_Params.KEY_POS; 

	// Init write buffer
	m_ThreadParams->dbcWrite = new DoubleBuffer(m_Params.MERGE_WRITE_BUFFER_SIZE); 
	rc = utl->InitBuffer(m_ThreadParams->dbcWrite->buffer[0], m_Params.MERGE_WRITE_BUFFER_SIZE, &bufferPool);
	if(rc!=SUCCESS) {return rc;}
	rc = utl->InitBuffer(m_ThreadParams->dbcWrite->buffer[1], m_Params.MERGE_WRITE_BUFFER_SIZE, &bufferPool); 
	if(rc!=SUCCESS) {return rc;}
	//////////////////////////////////////////////////////////////////////////
	m_ThreadParams->dbcRead = new DoubleBuffer*[m_MaxFanIn];
	m_ThreadParams->mergeBuffer = new Buffer[m_MaxFanIn];

	for(DWORD i=0; i<m_MaxFanIn; i++)
	{ 
		m_ThreadParams->dbcRead[i] = new DoubleBuffer(m_Params.MERGE_READ_BUFFER_SIZE);
		rc = utl->InitBuffer(m_ThreadParams->dbcRead[i]->buffer[0], m_Params.MERGE_READ_BUFFER_SIZE, &bufferPool);
		if(rc!=SUCCESS) {return rc;}

		rc = utl->InitBuffer(m_ThreadParams->dbcRead[i]->buffer[1], m_Params.MERGE_READ_BUFFER_SIZE, &bufferPool); 
		if(rc!=SUCCESS) {return rc;}

		rc = utl->InitBuffer(m_ThreadParams->mergeBuffer[i], SSD_PAGE_SIZE, &bufferPool);
		if(rc!=SUCCESS) {return rc;}
	}

	// Init run page buffer for assembly
	rc = utl->InitBuffer(m_ThreadParams->runPageBuffer, SSD_PAGE_SIZE, &bufferPool);
	if(rc!=SUCCESS) {return rc;}
	// Init runpage 
	rc = utl->InitRunPage(m_ThreadParams->runPage, m_ThreadParams->runPageBuffer);  
	if(rc!=SUCCESS) {return rc;}
	return SUCCESS; 
}

/// <summary>
/// Gets the size of the fan-in.
/// </summary>
/// <param name="hFile">The h file.</param>
/// <returns></returns>
UINT64 ExsMergePhase::GetFanSize(HANDLE hFile)  
{
	LARGE_INTEGER liFileSize; 
	if (!GetFileSizeEx(hFile, &liFileSize))
	{      
		return 0;
	} 

	return liFileSize.QuadPart;
}

/// <summary>
/// Execute the Merges phase.
/// </summary>
/// <returns></returns>
RC ExsMergePhase::MergePhase_Execute()
{ 
	RC rc;
	rc = MergePhase_Initialize();
	if(rc!=SUCCESS)
	{ 
		return ERR_CANNOT_INITIAL_MEMORY;
	}

	ExsMergeThreadParams *p = m_ThreadParams;
	rc = MergePhase_Merge(p);
	if(rc!=SUCCESS) 
	{ 
		delete bufferPool.data;
		return rc; 
	}

	delete bufferPool.data;
	return SUCCESS;
}

/// <summary>
/// Merges operation.
/// </summary>
/// <param name="lpParam">The thread parameter.</param>
/// <returns></returns>
RC ExsMergePhase::MergePhase_Merge(LPVOID lpParam)
{
	ExsMergeThreadParams *p = (ExsMergeThreadParams *)lpParam;

	DoubleBuffer *dbcWrite = p->dbcWrite;   

	DWORD maxFanIn = p->maxFanIn;

	while( TRUE ) // Multi merge passes
	{  
		MergePhase_GetFanOutPath(m_FanOutName, p->threadID); 

		DWORD64 totalPage = 0;
		DWORD64 tupleCount = 0;

		p->hFanOut=CreateFile((LPCWSTR)m_FanOutName,		// file to write
			GENERIC_WRITE,			// open for writing
			0,						// Do not share
			NULL,					// default security
			CREATE_ALWAYS,			 
			FILE_FLAG_OVERLAPPED | FILE_FLAG_NO_BUFFERING,	// overlapped operation
			NULL);					// no attr. template

		if (INVALID_HANDLE_VALUE==p->hFanOut) 
		{  
			//myMessage.Post(L"Cannot create fanOut file %s\r\n", m_FanOutName);
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

		FANS *fanOut= new FANS();
		wcscpy(fanOut->fileName, m_FanOutName);
		fanOut->threadID = p->threadID;
		fanOut->pageCount = liFanOutSize.QuadPart / SSD_PAGE_SIZE;
		fanOut->fileSize = liFanOutSize;
		fanOut->tupleCount = tupleCount;  
		fanOut->lowestKey = lowestValue; 
		fanOut->highestKey = highestValue; 
		m_FanIns.push(fanOut); 

		// Check is the last fanIn
		if(m_FanIns.size()==1)
		{
			m_FanOut = new FANS();
			m_FanOut = fanOut;
			break;
		}

		m_FanInWillDelete.push(fanOut); 
		m_MergePassNum++; 
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

	return SUCCESS;
}


/// <summary>
/// Get next tuple from buffer.
/// </summary>
/// <param name="lpParam">The thread parameter.</param>
/// <param name="record">The record pointer.</param>
/// <param name="index">The current index of fan-in.</param>
/// <returns></returns>
RC ExsMergePhase::MergePhase_GetNextTuple(LPVOID lpParam, RECORD *&record, INT index)
{
	ExsMergeThreadParams *p = (ExsMergeThreadParams *)lpParam;

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

	utl->GetTupleInfo(record, p->mergeBuffer[index].currentTupleIndex, p->mergeBuffer[index].data, SSD_PAGE_SIZE, m_Params.KEY_POS);  
	p->mergeBuffer[index].currentTupleIndex++;

	return SUCCESS; 
}

/// <summary>
/// Write buffer to disk.
/// </summary>
/// <param name="lpParam">The thread parameter.</param>
/// <returns></returns>
RC ExsMergePhase::MergePhase_Write(LPVOID lpParam)
{
	ExsMergeThreadParams *p = (ExsMergeThreadParams *)lpParam; 
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
/// <param name="index">The index of current fan-in.</param>
/// <returns></returns>
RC ExsMergePhase::MergePhase_Read(LPVOID lpParam, DWORD index)
{  
	ExsMergeThreadParams *p = (ExsMergeThreadParams *)lpParam; 
  
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
/// Get the fan-out path.
/// </summary>
/// <param name="fanOutName">Name of the fan-out.</param>
/// <param name="threadID">The thread identifier.</param>
/// <returns></returns>
RC ExsMergePhase::MergePhase_GetFanOutPath(LPWSTR &fanOutName, INT threadID)  
{    
	swprintf(fanOutName, MAX_PATH, L"%s%d_%d_%s_merge.dat", m_Params.WORK_SPACE_PATH, threadID, m_FanOutIndex, m_Params.FILE_NAME_NO_EXT);  
	InterlockedExchangeAdd(&m_FanOutIndex, 1);
	return SUCCESS;
} 
