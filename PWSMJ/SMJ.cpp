#include "stdafx.h"
#include "SMJ.h" 

SMJ::SMJ(const SMJ_PARAMS vParams) : m_Params(vParams)
{ 
	m_JoinFilePath = new TCHAR[MAX_PATH]; 
	utl = new PageHelpers(); 
}

SMJ::~SMJ()
{
	delete m_JoinFilePath; 
	delete utl;
} 

RC SMJ::SMJ_Initialize()
{
	RC rc;
	////////////////////////// Init R relation //////////////////////////////////////////////// 
	R = new RELATION();   

	// Key to join
	R->keyPos = m_Params.R_KEY_POS;

	// Init double read buffer
	R->dbcRead = new DoubleBuffer(m_Params.READ_BUFFER_SIZE);
	rc = utl->InitBuffer(R->dbcRead->buffer[0], m_Params.READ_BUFFER_SIZE);
	if(rc!=SUCCESS) {return rc;}

	rc = utl->InitBuffer(R->dbcRead->buffer[1], m_Params.READ_BUFFER_SIZE);  
	if(rc!=SUCCESS) {return rc;}
	// Init run page for R
	rc = utl->InitBuffer(R->probePageBuffer, SSD_PAGE_SIZE);
	if(rc!=SUCCESS) {return rc;}

	rc = utl->InitRunPage(R->probePage, R->probePageBuffer);
	if(rc!=SUCCESS) {return rc;}

	// Create file handle
	R->hFile = CreateFile(
		(LPCWSTR)m_Params.RELATION_R_PATH, // file to open
		GENERIC_READ,			// open for reading
		FILE_SHARE_READ,        // share for reading
		NULL,					// default security
		OPEN_EXISTING,			// existing file only
		FILE_FLAG_OVERLAPPED | FILE_FLAG_NO_BUFFERING ,	// overlapped operation //| FILE_FLAG_NO_BUFFERING
		NULL);					// no attr. template 

	if (INVALID_HANDLE_VALUE==R->hFile) 
	{ 
		ShowMB(L"Cannot create handle of file %s\r\n", m_Params.RELATION_R_PATH);
		return ERR_CANNOT_CREATE_HANDLE;
	} 

	// Create overlap srtructure
	LARGE_INTEGER *liSizeR = new LARGE_INTEGER();  
	if (!GetFileSizeEx(R->hFile, liSizeR))
	{  
		ShowMB(L"Cannot get size of file %s\r\n", m_Params.RELATION_R_PATH);
		return ERR_CANNOT_GET_FILE_SIZE;
	}  

	R->ovlRead.totalChunk = chROUNDUP(liSizeR->QuadPart, m_Params.READ_BUFFER_SIZE) / m_Params.READ_BUFFER_SIZE;
	R->ovlRead.chunkIndex = 0;
	R->ovlRead.startChunk = 0;
	R->ovlRead.endChunk = R->ovlRead.totalChunk; 
	R->ovlRead.dwBytesToReadWrite = m_Params.READ_BUFFER_SIZE;
	R->ovlRead.dwBytesReadWritten = 0;
	R->ovlRead.fileSize.QuadPart = 0;
	R->ovlRead.overlap.Offset = 0;
	R->ovlRead.overlap.OffsetHigh = 0;

	////////////////////////// Init S relation ////////////////////////////////////////////////  
	S = new RELATION(); 

	// Create key join atttribute
	S->keyPos = m_Params.S_KEY_POS;

	// Init double read buffer
	S->dbcRead = new DoubleBuffer(m_Params.READ_BUFFER_SIZE);
	rc = utl->InitBuffer(S->dbcRead->buffer[0], m_Params.READ_BUFFER_SIZE);
	if(rc!=SUCCESS) {return rc;}

	rc = utl->InitBuffer(S->dbcRead->buffer[1], m_Params.READ_BUFFER_SIZE);  
	if(rc!=SUCCESS) {return rc;}

	// Init run page for S
	rc = utl->InitBuffer(S->probePageBuffer, SSD_PAGE_SIZE);
	if(rc!=SUCCESS) {return rc;}

	rc = utl->InitRunPage(S->probePage, S->probePageBuffer);
	if(rc!=SUCCESS) {return rc;}

	// Create file handle
	S->hFile = CreateFile(
		(LPCWSTR)m_Params.RELATION_S_PATH, // file to open
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
		ShowMB(L"Cannot get size of file %s\r\n", m_Params.RELATION_S_PATH);
		return ERR_CANNOT_GET_FILE_SIZE;
	}  

	S->ovlRead.totalChunk = chROUNDUP(liSizeS->QuadPart, m_Params.READ_BUFFER_SIZE) / m_Params.READ_BUFFER_SIZE;
	S->ovlRead.chunkIndex = 0;
	S->ovlRead.startChunk = 0;
	S->ovlRead.endChunk = S->ovlRead.totalChunk; 
	S->ovlRead.dwBytesToReadWrite = m_Params.READ_BUFFER_SIZE;
	S->ovlRead.dwBytesReadWritten = 0;
	S->ovlRead.fileSize.QuadPart = 0; 
	S->ovlRead.overlap.Offset = 0;
	S->ovlRead.overlap.OffsetHigh = 0;

	////////////////////////// Init Join relation ////////////////////////////////////////////////  
	SMJ_GetJoinFilePath(m_JoinFilePath, m_Params.RELATION_R_NO_EXT, m_Params.RELATION_S_NO_EXT); 

	// Init doube buffer for write
	RS = new RELATION_JOIN();
	RS->dbcWrite = new DoubleBuffer(m_Params.WRITE_BUFFER_SIZE);
	rc = utl->InitBuffer(RS->dbcWrite->buffer[0], m_Params.WRITE_BUFFER_SIZE);
	if(rc!=SUCCESS) {return rc;}

	rc = utl->InitBuffer(RS->dbcWrite->buffer[1], m_Params.WRITE_BUFFER_SIZE);  
	if(rc!=SUCCESS) {return rc;}

	// Init run page for join relation
	rc = utl->InitBuffer(RS->runPageBuffer, SSD_PAGE_SIZE);
	if(rc!=SUCCESS) {return rc;}

	rc = utl->InitRunPage(RS->runPage, RS->runPageBuffer);
	if(rc!=SUCCESS) {return rc;}

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
		//myMessage.Add(L"Cannot create handle of file %s\r\n", m_JoinFilePath);
		return ERR_CANNOT_CREATE_HANDLE;
	}   

	// Init overlap write structure
	RS->ovlWrite.totalChunk = 0;
	RS->ovlWrite.chunkIndex = 0;
	RS->ovlWrite.startChunk = 0;
	RS->ovlWrite.endChunk = 0; 
	RS->ovlWrite.dwBytesToReadWrite = m_Params.WRITE_BUFFER_SIZE;
	RS->ovlWrite.dwBytesReadWritten = 0;
	RS->ovlWrite.fileSize.QuadPart = 0; 
	RS->ovlWrite.overlap.Offset = 0;
	RS->ovlWrite.overlap.OffsetHigh = 0;

	return SUCCESS;
}

RC SMJ::SMJ_GetNextRecord(RELATION *&rel, RECORD *&record, BOOL bFirstRead)
{  
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
				&rel->ovlRead.overlap, 
				&rel->ovlRead.dwBytesReadWritten, 
				TRUE);  

			// Compute info in BACK buffer
			utl->ComputeBuffer(BACK_BUFFER(rel->dbcRead), rel->ovlRead.dwBytesReadWritten);

			// Increase already read chunk number
			rel->ovlRead.chunkIndex++; 
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
				&rel->ovlRead.overlap, 
				&rel->ovlRead.dwBytesReadWritten, 
				TRUE);  

			// Compute info in BACK buffer
			utl->ComputeBuffer(BACK_BUFFER(rel->dbcRead), rel->ovlRead.dwBytesReadWritten); 
			rel->ovlRead.chunkIndex++;  // Increase already read chunk number

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

RC SMJ::Execute()
{   
	if(SUCCESS!=SMJ_Initialize())
	{
		return ERR_SMJ_INIT_PARAM_FAILED;
	}

	//First read to fill buffer
	RECORD *tupleR = new RECORD();
	RECORD *tupleS = new RECORD();
	RECORD *tupleJoin = new RECORD(TUPLE_SIZE*2);

	SMJ_GetNextRecord(R, tupleR, TRUE);
	SMJ_GetNextRecord(S, tupleS, TRUE); 

	BOOL bNeedWait = FALSE;
	// Lock BACK(Write) buffer
	RS->dbcWrite->LockProducer();

	while( (tupleR->key!=MAX) && (tupleS->key!=MAX))
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

			if (tupleR->key == MAX)  {  break;  }
		}

		// Get next S tuple
		SMJ_GetNextRecord(S, tupleS, FALSE);  
	}
	 

	if(FRONT_BUFFER(RS->dbcWrite).currentSize > 0)
	{ 
		SMJ_Write();

		GetOverlappedResult(RS->hFile,
			&RS->ovlWrite.overlap,
			&RS->ovlWrite.dwBytesReadWritten,
			TRUE); 

		// Update new position in output file
		RS->ovlWrite.fileSize.QuadPart+=RS->ovlWrite.dwBytesReadWritten;
		RS->ovlWrite.overlap.Offset = RS->ovlWrite.fileSize.LowPart;
		RS->ovlWrite.overlap.OffsetHigh = RS->ovlWrite.fileSize.HighPart; 

		// Reset FRONT buffer
		utl->ResetBuffer(FRONT_BUFFER(RS->dbcWrite));
 
	}

	// Terminate current run
	CloseHandle(RS->hFile); 

	return SUCCESS;
} 

RC SMJ::SMJ_SentOutput(RECORD *joinRecord)
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
				&RS->ovlWrite.overlap,
				&RS->ovlWrite.dwBytesReadWritten,
				TRUE); 

			// Update new position in output file
			RS->ovlWrite.fileSize.QuadPart+=RS->ovlWrite.dwBytesReadWritten;
			RS->ovlWrite.overlap.Offset = RS->ovlWrite.fileSize.LowPart;
			RS->ovlWrite.overlap.OffsetHigh = RS->ovlWrite.fileSize.HighPart; 

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

VOID SMJ::SMJ_MakeNewRecord(RECORD *&joinRecord, RECORD *leftRecord, RECORD *rightRecord) 
{
	joinRecord->key = leftRecord->key;
	joinRecord->length = leftRecord->length + rightRecord->length;
	memcpy(joinRecord->data, leftRecord->data, leftRecord->length);
	memcpy(joinRecord->data + leftRecord->length, rightRecord->data, rightRecord->length); 
}

RC SMJ::SMJ_Read(RELATION *&rel)
{   
	rel->dbcRead->LockProducer();

	if (rel->ovlRead.chunkIndex >= rel->ovlRead.totalChunk)  
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

	rel->ovlRead.fileSize.QuadPart = rel->ovlRead.chunkIndex * m_Params.READ_BUFFER_SIZE; // chunkNum * chunkSize; 
	rel->ovlRead.overlap.Offset = rel->ovlRead.fileSize.LowPart;
	rel->ovlRead.overlap.OffsetHigh = rel->ovlRead.fileSize.HighPart;  

	// Attempt an asynchronous read operation.  
	ReadFile(rel->hFile, 
		BACK_BUFFER(rel->dbcRead).data, 
		rel->ovlRead.dwBytesToReadWrite, 
		&rel->ovlRead.dwBytesReadWritten, 
		&rel->ovlRead.overlap); 


	// return immediately

	return SUCCESS;
} 

RC SMJ::SMJ_Write() 
{     
	RS->dbcWrite->LockConsumer();

	// File systems extend files synchronously. Extend the destination file 
	// now so that I/Os execute asynchronously improving performance. 
	WriteFile(RS->hFile, 
		FRONT_BUFFER(RS->dbcWrite).data, 
		FRONT_BUFFER(RS->dbcWrite).currentSize,  
		&RS->ovlWrite.dwBytesReadWritten, 
		&RS->ovlWrite.overlap); 			  

	 
	return SUCCESS;
} 

LPWSTR SMJ::SMJ_GetJoinFile()
{
	return m_JoinFilePath;
}

VOID SMJ::SMJ_GetJoinFilePath(LPWSTR &joinFilePath, LPWSTR rNameNoExt, LPWSTR sNameNoExt)  
{    
	swprintf(joinFilePath, MAX_PATH, L"%s%s_join_%s.dat", m_Params.WORK_SPACE_PATH, rNameNoExt, sNameNoExt);  
} 