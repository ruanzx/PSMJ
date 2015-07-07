
// 
// Name: BNLJ.cpp : implementation file 
// Author: hieunt
// Description: Block nested loop join implementation
//

#include "stdafx.h"
#include "BNLJ.h"

/// <summary>
/// Initializes a new instance of the <see cref="BNLJ"/> class.
/// </summary>
/// <param name="vParams">The BNLJ struct parameters.</param>
BNLJ::BNLJ(const BNLJ_PARAMS vParams) : m_Params(vParams)
{
	utl = new PageHelpers();
	totalJoin = 0; 

	probeRecord = new RECORD(TUPLE_SIZE);
	bigRelationRecord = new RECORD(TUPLE_SIZE);
	smallRelationRecord = new RECORD(TUPLE_SIZE);
	joinRecord = new RECORD(TUPLE_SIZE * 2);
}

/// <summary>
/// Finalizes an instance of the <see cref="BNLJ"/> class.
/// </summary>
BNLJ::~BNLJ()
{
	delete probeRecord;
	delete bigRelationRecord;
	delete smallRelationRecord;
	delete joinRecord;

}

/// <summary>
/// Initializes buffers, thread params for this instance.
/// </summary>
/// <returns></returns>
RC BNLJ::Initialize()
{
	// get file size
	LARGE_INTEGER *liFileSize = new LARGE_INTEGER(); 

	// Buffer for read customer.dat
	dbcReadSmall = new DoubleBuffer(m_Params.READ_BUFFER_SIZE); 
	utl->InitBuffer(dbcReadSmall->buffer[0], m_Params.READ_BUFFER_SIZE);
	utl->InitBuffer(dbcReadSmall->buffer[1], m_Params.READ_BUFFER_SIZE);  

	// Buffer for read order.dat
	dbcReadBig = new DoubleBuffer(m_Params.READ_BUFFER_SIZE); 
	utl->InitBuffer(dbcReadBig->buffer[0], m_Params.READ_BUFFER_SIZE);
	utl->InitBuffer(dbcReadBig->buffer[1], m_Params.READ_BUFFER_SIZE);  

	// Buffer for write join result
	dbcWrite = new DoubleBuffer(m_Params.WRITE_BUFFER_SIZE);
	utl->InitBuffer(dbcWrite->buffer[0], m_Params.WRITE_BUFFER_SIZE);
	utl->InitBuffer(dbcWrite->buffer[1], m_Params.WRITE_BUFFER_SIZE); 

	// Init run buffer
	utl->InitBuffer(hashBuildPageBuffer, SSD_PAGE_SIZE);  
	utl->InitRunPage(hashBuildPage, hashBuildPageBuffer); 

	// Init hash table
	DWORD hashTableBufferSize = (m_Params.READ_BUFFER_SIZE / 4096) * 40 * sizeof(HashTuple); // averange 40 tuples/page
	utl->InitBuffer(hashTableBuffer, hashTableBufferSize); 
	hashTable.data = hashTableBuffer.data;
	hashTable.size = hashTableBufferSize;
	hashTable.currentSize = 0;
	hashTable.startLocation = 0;

	// Init run buffer
	utl->InitBuffer(probePageBuffer, SSD_PAGE_SIZE);  
	utl->InitRunPage(probePage, probePageBuffer); 

	utl->InitBuffer(writePageBuffer, SSD_PAGE_SIZE);  
	utl->InitRunPage(writePage, writePageBuffer); 

	// handle for customer.dat file
	hFileSmall=CreateFile(
		(LPCWSTR)m_Params.RELATION_R_PATH, // file to open
		GENERIC_READ,			// open for reading
		FILE_SHARE_READ,        // share for reading
		NULL,					// default security
		OPEN_EXISTING,			// existing file only
		FILE_FLAG_OVERLAPPED | FILE_FLAG_NO_BUFFERING,	// overlapped operation //| FILE_FLAG_NO_BUFFERING
		NULL);					// no attr. template

	if (INVALID_HANDLE_VALUE==hFileSmall) 
	{    
		return ERR_CANNOT_CREATE_HANDLE; 
	}  

	if (!GetFileSizeEx(hFileSmall, liFileSize))
	{        
		return ERR_CANNOT_GET_FILE_SIZE;
	}  

	// Setup overlap structure
	overlapReadSmall.totalChunk = (chROUNDUP(liFileSize->QuadPart,  m_Params.READ_BUFFER_SIZE) / m_Params.READ_BUFFER_SIZE) - 1;
	overlapReadSmall.chunkIndex = 0;
	overlapReadSmall.startChunk = 0;
	overlapReadSmall.endChunk = overlapReadSmall.totalChunk;
	overlapReadSmall.overlap.Offset = 0;
	overlapReadSmall.overlap.OffsetHigh = 0;
	overlapReadSmall.fileSize.QuadPart = 0;
	overlapReadSmall.dwBytesToReadWrite = m_Params.READ_BUFFER_SIZE;
	overlapReadSmall.dwBytesReadWritten = 0;

	// handle for order.dat file
	hFileBig=CreateFile(
		(LPCWSTR)m_Params.RELATION_S_PATH, // file to open
		GENERIC_READ,			// open for reading
		FILE_SHARE_READ,        // share for reading
		NULL,					// default security
		OPEN_EXISTING,			// existing file only
		FILE_FLAG_OVERLAPPED | FILE_FLAG_NO_BUFFERING,	// overlapped operation //| FILE_FLAG_NO_BUFFERING
		NULL);					// no attr. template

	if (INVALID_HANDLE_VALUE==hFileBig) 
	{   
		//myMessage.Add(L"Cannot open file %s\r\n", m_Params.RELATION_S_PATH);
		return ERR_CANNOT_CREATE_HANDLE; 
	}  

	if (!GetFileSizeEx(hFileBig, liFileSize))
	{       
		//myMessage.Add(L"Cannot get size of file %s\r\n", m_Params.RELATION_S_PATH);
		return ERR_CANNOT_GET_FILE_SIZE;
	}  

	overlapReadBig.totalChunk = (chROUNDUP(liFileSize->QuadPart,  m_Params.READ_BUFFER_SIZE) / m_Params.READ_BUFFER_SIZE) - 1;
	overlapReadBig.chunkIndex = 0;
	overlapReadBig.startChunk = 0;
	overlapReadBig.endChunk = overlapReadSmall.totalChunk;
	overlapReadBig.overlap.Offset = 0;
	overlapReadBig.overlap.OffsetHigh = 0;
	overlapReadBig.fileSize.QuadPart = 0;
	overlapReadBig.dwBytesToReadWrite = m_Params.READ_BUFFER_SIZE;
	overlapReadBig.dwBytesReadWritten = 0;


	//////////////////////////////////////////////////////////////////////////
	// Create output
	//////////////////////////////////////////////////////////////////////////
	LPWSTR joinPathFile = new TCHAR[MAX_PATH];
	GetJoinFilePath(joinPathFile);

	hFileOut =CreateFile(
		(LPCWSTR)joinPathFile,		// file to write
		GENERIC_WRITE,			// open for writing
		0,						// Do not share
		NULL,					// default security
		CREATE_ALWAYS,			// Overwrite existing
		FILE_FLAG_OVERLAPPED,	// overlapped operation //| FILE_FLAG_OVERLAPPED
		NULL);					// no attr. template

	if (INVALID_HANDLE_VALUE==hFileOut) 
	{    
		return ERR_CANNOT_CREATE_HANDLE; 
	}  

	overlapWrite.totalChunk = 0;
	overlapWrite.chunkIndex = 0;
	overlapWrite.startChunk = 0;
	overlapWrite.endChunk = overlapWrite.totalChunk;
	overlapWrite.overlap.Offset = 0;
	overlapWrite.overlap.OffsetHigh = 0;
	overlapWrite.fileSize.QuadPart = 0;
	overlapWrite.dwBytesToReadWrite = m_Params.WRITE_BUFFER_SIZE;
	overlapWrite.dwBytesReadWritten = 0;


	return SUCCESS;
}

/// <summary>
/// Entry point, executes this instance.
/// </summary>
/// <returns></returns>
RC BNLJ::Execute()
{
	RC rc = Initialize();
	if(rc!=SUCCESS)
	{
		return rc;
	}

	// First read R, S
	ReadSmall();
	ReadBig();

	// Wait for read small relation complete
	GetOverlappedResult(hFileSmall, &overlapReadSmall.overlap, &overlapReadSmall.dwBytesReadWritten, TRUE); 
	overlapReadSmall.chunkIndex++; 
	utl->ComputeBuffer(BACK_BUFFER(dbcReadSmall), overlapReadSmall.dwBytesReadWritten);  
	dbcReadSmall->UnLockProducer();

	// Wait for read big relation complete
	GetOverlappedResult(hFileBig, &overlapReadBig.overlap, &overlapReadBig.dwBytesReadWritten, TRUE); 
	overlapReadBig.chunkIndex++; 
	utl->ComputeBuffer(BACK_BUFFER(dbcReadBig), overlapReadBig.dwBytesReadWritten);  
	dbcReadBig->UnLockProducer();

	// Swap buffer
	dbcReadSmall->SwapBuffers(); // Swap BACK to FRONT
	dbcReadBig->SwapBuffers(); // Swap BACK to FRONT

	dbcWrite->LockProducer(); 
	// Loop through small relation
	while (TRUE)
	{ 
		// Continue read R into BACK buffer
		rc = ReadSmall(); 
		if(rc!=SUCCESS)
		{ 
			// Read small relation complete
			break;
		} 

		// At the same time, build hash table in FRONT buffer 
		HashBuild();

		// Wait for read small relation complete
		GetOverlappedResult(hFileSmall, &overlapReadSmall.overlap, &overlapReadSmall.dwBytesReadWritten, TRUE); 
		overlapReadSmall.chunkIndex++; 
		utl->ComputeBuffer(BACK_BUFFER(dbcReadSmall), overlapReadSmall.dwBytesReadWritten);  
		dbcReadSmall->UnLockProducer();  

		// Loop through big relation
		while (TRUE)
		{ 
			// Read big relation into BACK
			rc = ReadBig();   
			if(rc!=SUCCESS)
			{
				// Read big relation complete, reset read position
				overlapReadBig.fileSize.QuadPart = 0;
				overlapReadBig.chunkIndex = 0;
				overlapReadBig.startChunk = 0;
				overlapReadBig.overlap.Offset = 0;
				overlapReadBig.overlap.OffsetHigh = 0;

				break;
			} 

			// Start join
			HashProbe(); 

			// Wait for read big relation complete
			GetOverlappedResult(hFileBig, &overlapReadBig.overlap, &overlapReadBig.dwBytesReadWritten, TRUE); 
			overlapReadBig.chunkIndex++; 
			utl->ComputeBuffer(BACK_BUFFER(dbcReadBig), overlapReadBig.dwBytesReadWritten);  
			dbcReadBig->UnLockProducer();

			dbcReadBig->SwapBuffers();  
		}

		dbcReadSmall->SwapBuffers(); // Swap BACK to FRONT

	} 

	dbcWrite->UnLockProducer();
	dbcWrite->UnLockConsumer();

	if(FRONT_BUFFER(dbcWrite).currentSize > 0)
	{ 
		Write();

		GetOverlappedResult(hFileOut,
			&overlapWrite.overlap,
			&overlapWrite.dwBytesReadWritten,
			TRUE); 

		// Update new position in output file
		overlapWrite.fileSize.QuadPart+=overlapWrite.dwBytesReadWritten;
		overlapWrite.overlap.Offset = overlapWrite.fileSize.LowPart;
		overlapWrite.overlap.OffsetHigh = overlapWrite.fileSize.HighPart; 

		// Reset FRONT buffer
		utl->ResetBuffer(FRONT_BUFFER(dbcWrite)); 
		dbcWrite->UnLockConsumer();
	}

	if(BACK_BUFFER(dbcWrite).currentSize > 0)
	{ 
		dbcWrite->SwapBuffers();

		Write();

		GetOverlappedResult(hFileOut,
			&overlapWrite.overlap,
			&overlapWrite.dwBytesReadWritten,
			TRUE); 

		// Update new position in output file
		overlapWrite.fileSize.QuadPart+=overlapWrite.dwBytesReadWritten;
		overlapWrite.overlap.Offset = overlapWrite.fileSize.LowPart;
		overlapWrite.overlap.OffsetHigh = overlapWrite.fileSize.HighPart; 

		// Reset FRONT buffer
		utl->ResetBuffer(FRONT_BUFFER(dbcWrite)); 
		dbcWrite->UnLockConsumer();
	}

	// Terminate current run
	CloseHandle(hFileOut); 
	return SUCCESS;
}

/// <summary>
/// Build Hash table function.
/// </summary>
VOID  BNLJ::HashBuild()
{ 
	dbcReadSmall->LockConsumer();

	DWORD totalTuple = FRONT_BUFFER(dbcReadSmall).tupleCount;
	if(totalTuple % 2==0)
		hashTable.hashFn = totalTuple + 1;
	else
		hashTable.hashFn = totalTuple; 
	hashTable.currentSize = hashTable.hashFn * sizeof(HashTuple);

	INT hashValue;
	HashTuple *hashTuple; 

	hashTable.hashTupleCount = new INT[hashTable.hashFn]; 

	//初始散列表每个桶的标记为
	for(UINT i=0; i < hashTable.hashFn; i++)
	{
		hashTable.hashTupleCount[i] = 0;
	}

	RECORD *rec = new RECORD();
	utl->ResetRunPage(hashBuildPage, hashBuildPageBuffer);

	for (DWORD pageIndex=0; pageIndex < FRONT_BUFFER(dbcReadSmall).pageCount; pageIndex++)
	{
		utl->GetPageInfo(FRONT_BUFFER(dbcReadSmall).data, hashBuildPage, hashBuildPageBuffer, pageIndex, SSD_PAGE_SIZE);  

		for(DWORD tupleIndex=1; tupleIndex<=hashBuildPageBuffer.tupleCount; tupleIndex++)
		{ 
			utl->GetTupleInfo(rec, tupleIndex, hashBuildPageBuffer.data, SSD_PAGE_SIZE, m_Params.R_KEY_POS);
			hashValue = rec->key % hashTable.hashFn;

			if(hashTable.hashTupleCount[hashValue]==0)
			{
				hashTuple=(HashTuple *)(hashTable.data + hashValue * sizeof(HashTuple)); 
				hashTuple->key = rec->key; 
				hashTuple->offset = SSD_PAGE_SIZE * pageIndex + rec->offset; 
				hashTuple->next = NULL;
				hashTable.hashTupleCount[hashValue]++; 
			}
			//冲突发生
			else
			{
				// This should not appear, the table to hash should have primary key

				//hashTuple=(HashTuple *)(hashTable->data + hashTable->size);  //新生成的散列表也放在内存中
				hashTuple = new HashTuple;
				hashTuple->key = rec->key;
				hashTuple->offset = SSD_PAGE_SIZE * pageIndex + rec->offset; 
				hashTuple->next = NULL;
				//hashTable->currentSize+=sizeof(HashTuple);

				HashTuple *p=(HashTuple *)(hashTable.data + hashValue * sizeof(HashTuple));

				while(p->next!=NULL)
					p=p->next;
				p->next=hashTuple;
			} 
		}   

		utl->ResetBuffer(hashBuildPageBuffer); 
		utl->ResetRunPage(hashBuildPage, hashBuildPageBuffer); 
	} 

	dbcReadSmall->UnLockConsumer();
}
 
/// <summary>
/// probe hash table.
/// </summary>
VOID BNLJ::HashProbe()
{ 
	dbcReadBig->LockConsumer();

	for(UINT pageIndex=0; pageIndex < FRONT_BUFFER(dbcReadBig).pageCount; pageIndex++)//一页一页处理
	{
		utl->GetPageInfo(FRONT_BUFFER(dbcReadBig).data, probePage, probePageBuffer, pageIndex, SSD_PAGE_SIZE);
		for (UINT tupleIndex = 1; tupleIndex <= probePageBuffer.tupleCount; tupleIndex++)
		{
			utl->GetTupleInfo(probeRecord, tupleIndex, probePageBuffer.data, SSD_PAGE_SIZE, m_Params.S_KEY_POS);
			UINT hashValue=probeRecord->key % hashTable.hashFn;
			if(hashTable.hashTupleCount[hashValue]!=0)//如果散列桶被标记,搜索链表进行连接
			{
				HashTuple *p=(HashTuple *)(hashTable.data + hashValue * sizeof(HashTuple));
				while(p!=NULL)
				{
					if(p->key==probeRecord->key)
					{ 
						totalJoin++;

						bigRelationRecord->data = probeRecord->data; 
						bigRelationRecord->length = probeRecord->length;
						utl->GetTupleKey(bigRelationRecord->data, bigRelationRecord->key, m_Params.S_KEY_POS);

						//customerRecord->data = hashTable->data + p->offset;
						smallRelationRecord->data = FRONT_BUFFER(dbcReadSmall).data + p->offset;

						smallRelationRecord->length = strlen(smallRelationRecord->data);
						utl->GetTupleKey(smallRelationRecord->data, smallRelationRecord->key, m_Params.R_KEY_POS);

						CreateJoinRecord(joinRecord, smallRelationRecord, bigRelationRecord);

						SentOutput(joinRecord);  
					}
					p=p->next;
				}
			} 
		} 
	} 

	dbcReadBig->UnLockConsumer();
}

RC BNLJ::ReadSmall()
{ 
	// Read the small relation

	dbcReadSmall->LockProducer();

	if (overlapReadSmall.chunkIndex > overlapReadSmall.totalChunk)  
	{  
		utl->ResetBuffer(BACK_BUFFER(dbcReadSmall));
		utl->AddPageToBuffer(BACK_BUFFER(dbcReadSmall), NULL, SSD_PAGE_SIZE); 

		BACK_BUFFER(dbcReadSmall).currentPageIndex = 0;
		BACK_BUFFER(dbcReadSmall).currentSize = SSD_PAGE_SIZE;
		BACK_BUFFER(dbcReadSmall).pageCount = 1;
		BACK_BUFFER(dbcReadSmall).tupleCount = utl->GetTupleNumInMaxPage(); 
		BACK_BUFFER(dbcReadSmall).isSort = TRUE;
		BACK_BUFFER(dbcReadSmall).isFullMaxValue = TRUE;

		dbcReadSmall->UnLockProducer();
		return ERR_END_OF_FILE;
	}  

	overlapReadSmall.fileSize.QuadPart = overlapReadSmall.chunkIndex * m_Params.READ_BUFFER_SIZE;   
	overlapReadSmall.overlap.Offset = overlapReadSmall.fileSize.LowPart;
	overlapReadSmall.overlap.OffsetHigh = overlapReadSmall.fileSize.HighPart;  

	// Attempt an asynchronous read operation.  
	ReadFile(hFileSmall, 
		BACK_BUFFER(dbcReadSmall).data, 
		overlapReadSmall.dwBytesToReadWrite, 
		&overlapReadSmall.dwBytesReadWritten, 
		&overlapReadSmall.overlap);

	return SUCCESS;  
}


RC BNLJ::ReadBig()
{ 
	// Read the big relation

	dbcReadBig->LockProducer();

	if (overlapReadBig.chunkIndex > overlapReadBig.totalChunk)  
	{  
		utl->ResetBuffer(BACK_BUFFER(dbcReadBig));
		utl->AddPageToBuffer(BACK_BUFFER(dbcReadBig), NULL, SSD_PAGE_SIZE); 

		BACK_BUFFER(dbcReadBig).currentPageIndex = 0;
		BACK_BUFFER(dbcReadBig).currentSize = SSD_PAGE_SIZE;
		BACK_BUFFER(dbcReadBig).pageCount = 1;
		BACK_BUFFER(dbcReadBig).tupleCount = utl->GetTupleNumInMaxPage(); 
		BACK_BUFFER(dbcReadBig).isSort = TRUE;
		BACK_BUFFER(dbcReadBig).isFullMaxValue = TRUE;

		dbcReadBig->UnLockProducer();
		return ERR_END_OF_FILE;
	}  

	overlapReadBig.fileSize.QuadPart = overlapReadBig.chunkIndex * m_Params.READ_BUFFER_SIZE;   
	overlapReadBig.overlap.Offset = overlapReadBig.fileSize.LowPart;
	overlapReadBig.overlap.OffsetHigh = overlapReadBig.fileSize.HighPart;  

	// Attempt an asynchronous read operation.  
	ReadFile(hFileBig, 
		BACK_BUFFER(dbcReadBig).data, 
		overlapReadBig.dwBytesToReadWrite, 
		&overlapReadBig.dwBytesReadWritten, 
		&overlapReadBig.overlap);

	return SUCCESS;  
}


/// <summary>
/// Sents the output to output buffer, if it's full then write to disk.
/// </summary>
/// <param name="joinRecord">The join record.</param>
/// <returns></returns>
RC BNLJ::SentOutput(RECORD *joinRecord)
{
	// Alway build join page into BACK(writeBuffer)

	if(utl->IsJoinPageFull(writePage))
	{    
		utl->AddPageToBuffer(BACK_BUFFER(dbcWrite),  writePage->page, SSD_PAGE_SIZE); //4

		//bPageConsumed=true; 

		BACK_BUFFER(dbcWrite).currentPageIndex+=1;

		utl->ResetRunPage(writePage, writePageBuffer);       
	}  

	if(utl->IsBufferFull(BACK_BUFFER(dbcWrite)))
	{
		dbcWrite->UnLockProducer();
		// output buffer is full, write to disk
		if(dbcWrite->bFirstProduce == TRUE)
		{
			dbcWrite->bFirstProduce = FALSE;

			dbcWrite->SwapBuffers();

			Write(); 
		}
		else
		{ 
			// Must wait for previous write complete
			GetOverlappedResult(hFileOut,
				&overlapWrite.overlap,
				&overlapWrite.dwBytesReadWritten,
				TRUE); 

			// Update new position in output file
			overlapWrite.fileSize.QuadPart+=overlapWrite.dwBytesReadWritten;
			overlapWrite.overlap.Offset = overlapWrite.fileSize.LowPart;
			overlapWrite.overlap.OffsetHigh = overlapWrite.fileSize.HighPart; 

			// Reset FRONT buffer
			utl->ResetBuffer(FRONT_BUFFER(dbcWrite));

			dbcWrite->UnLockConsumer(); 

			dbcWrite->SwapBuffers();

			// Write to disk
			Write(); 
		} 

		dbcWrite->LockProducer(); 
	}

	utl->AddTupleToJoinPage(writePage, joinRecord, writePageBuffer);   // Add this pair of tuples to page   

	return SUCCESS;
}



/// <summary>
/// Writes to disk function.
/// </summary>
/// <returns></returns>
RC  BNLJ::Write()
{ 
	dbcWrite->LockConsumer();
	// File systems extend files synchronously. Extend the destination file 
	// now so that I/Os execute asynchronously improving performance. 
	WriteFile(hFileOut, 
		FRONT_BUFFER(dbcWrite).data, 
		FRONT_BUFFER(dbcWrite).currentSize,  
		&overlapWrite.dwBytesReadWritten, 
		&overlapWrite.overlap); 			  


	return SUCCESS;
}

/// <summary>
/// Creates the join record.
/// </summary>
/// <param name="joinRecord">The join record.</param>
/// <param name="leftRecord">The left record.</param>
/// <param name="rightRecord">The right record.</param>
VOID BNLJ::CreateJoinRecord(RECORD *&joinRecord, RECORD *leftRecord, RECORD *rightRecord) 
{
	joinRecord->key = leftRecord->key;
	joinRecord->length = leftRecord->length + rightRecord->length;
	memcpy(joinRecord->data, leftRecord->data, leftRecord->length);
	memcpy(joinRecord->data + leftRecord->length, rightRecord->data, rightRecord->length); 
}

/// <summary>
/// Gets the join file path.
/// </summary>
/// <param name="joinFilePath">The join file path.</param>
VOID BNLJ::GetJoinFilePath(LPWSTR &joinFilePath)  
{    
	swprintf(joinFilePath, MAX_PATH, L"%s%s_join_%s.dat", m_Params.WORK_SPACE_PATH, m_Params.RELATION_R_NO_EXT, m_Params.RELATION_S_NO_EXT);  
} 