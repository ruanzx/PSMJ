// 
// Name: PageHelpers.cpp : implementation file 
// Author: hieunt
// Description: Helpers functions for working with N-ary struct page
//

#include "stdafx.h"

#include "PageHelpers2.h"

/// <summary>
/// Initializes a new instance of the <see cref="PageHelpers2"/> class.
/// </summary>
PageHelpers2::PageHelpers2()  
{  
	InitMaxPage(m_PageMAX, m_TupleMAX); 
}

/// <summary>
/// Finalizes an instance of the <see cref="PageHelpers2"/> class.
/// </summary>
PageHelpers2::~PageHelpers2()
{  
	delete m_PageMAX; 
	delete m_TupleMAX;
} 

/// <summary>
/// Write debug infos to file.
/// </summary>
/// <param name="data">The debug data.</param>
/// <param name="index">The file index.</param>
/// <param name="size">The size to write.</param>
VOID PageHelpers2::DebugToFile(CHAR *data, UINT index, UINT size)
{ 
	LPWSTR filePath = new TCHAR[MAX_PATH];   
	swprintf(filePath, MAX_PATH, L"C:\\debug_%d.txt", index);

	CHAR *fileNamePath = new CHAR[MAX_PATH];
	// convert from wchar_t to char*
	size_t  count = wcstombs(fileNamePath, filePath, MAX_PATH); // C4996 

	FILE* fso=fopen(fileNamePath,"w+b");

	fwrite(data, sizeof(CHAR),  size, fso); 
	fclose(fso); 

	delete filePath;
	delete fileNamePath;
} 

/// <summary>
/// Sets the tuple with MAX value.
/// </summary>
/// <param name="recordPtr">The record PTR.</param>
/// <returns></returns>
RC PageHelpers2::SetMaxTuple(RECORD *&recordPtr)
{
	if(m_TupleMAX==NULL)
		return ERR_CANNOT_INITIAL_MEMORY;

	recordPtr->key = MAX;
	//strcpy(record->tupleData, m_MaxTupleValue);
	recordPtr->data = m_TupleMAX;
	recordPtr->length = -1;

	return SUCCESS;
} 

/// <summary>
/// Gets the tuple number in MAX page.
/// </summary>
/// <returns></returns>
DWORD PageHelpers2::GetTupleNumInMaxPage()
{
	return  m_PageMAX->pageHeader->totalTuple;
}

/// <summary>
/// Initializes the buffer.
/// </summary>
/// <param name="myBuffer">My buffer.</param>
/// <param name="bufferSize">Size of the buffer.</param>
/// <returns></returns>
RC PageHelpers2::InitBuffer(Buffer &myBuffer, const DWORD bufferSize)
{   
	/* Init buffer without buffer pool */

	myBuffer.data = new CHAR[bufferSize]; 

	myBuffer.size = bufferSize;
	myBuffer.currentTupleIndex = 1;
	myBuffer.tupleCount = 0;
	myBuffer.currentPageIndex = 0;
	myBuffer.pageCount = 0;
	myBuffer.currentSize = 0; 
	myBuffer.isSort = FALSE; 
	myBuffer.isFullMaxValue = FALSE; 

	myBuffer.lowestKey = 0;
	myBuffer.highestKey = 0; 

	return SUCCESS;
}


/// <summary>
/// Initializes the buffer.
/// </summary>
/// <param name="myBuffer">My buffer.</param>
/// <param name="bufferSize">Size of the buffer.</param>
/// <param name="bufferPool">The buffer pool pointer.</param>
/// <returns></returns>
RC PageHelpers2::InitBuffer(Buffer &myBuffer, const DWORD bufferSize, BufferPool *bufferPool)
{  
	if(NULL==bufferPool->data)
	{
		return ERR_NOT_INIT_MEMORY;
	}

	myBuffer.startLocation = bufferPool->currentSize;
	myBuffer.freeLocation = myBuffer.startLocation;
	myBuffer.data = bufferPool->data + myBuffer.startLocation;

	//myBuffer.data = new CHAR[bufferSize];

	myBuffer.size = bufferSize;
	myBuffer.currentTupleIndex = 1;
	myBuffer.tupleCount = 0;
	myBuffer.currentPageIndex = 0;
	myBuffer.pageCount = 0;
	myBuffer.currentSize = 0; 
	myBuffer.isSort = FALSE; 
	myBuffer.isFullMaxValue = FALSE; 

	myBuffer.lowestKey = 0;
	myBuffer.highestKey = 0; 

	bufferPool->currentSize+=bufferSize;

	if(bufferPool->currentSize > bufferPool->size)
	{
		return ERR_NOT_ENOUGH_MEMORY;
	}


	return SUCCESS;
}

/// <summary>
/// Computes the buffer. What buffer have?
/// </summary>
/// <param name="myBuffer">My buffer.</param>
/// <param name="totalBytesInBuffer">The total bytes in buffer.</param>
/// <returns></returns>
RC PageHelpers2::ComputeBuffer(Buffer &myBuffer, const DWORD totalBytesInBuffer)
{  
	if(totalBytesInBuffer > 0)
	{ 
		if(0==(totalBytesInBuffer % SSD_PAGE_SIZE)) 
			myBuffer.pageCount = (totalBytesInBuffer / SSD_PAGE_SIZE);  
		else 
			myBuffer.pageCount = (totalBytesInBuffer / SSD_PAGE_SIZE) + 1;  

		myBuffer.currentSize = totalBytesInBuffer;
		myBuffer.currentPageIndex = 0; 
		myBuffer.freeLocation += totalBytesInBuffer;  
		myBuffer.isFullMaxValue = FALSE;

		PageHeader *pageHeader;
		DWORD totalTuple = 0;
		for(UINT pageIndex=0; pageIndex < myBuffer.pageCount; pageIndex++)
		{
			pageHeader = (PageHeader*)(myBuffer.data + pageIndex * SSD_PAGE_SIZE);
			totalTuple += pageHeader->totalTuple;
		}
		myBuffer.tupleCount = totalTuple;
	}
	else
	{     
		AddPageToBuffer(myBuffer,  m_PageMAX->page, SSD_PAGE_SIZE); 
		myBuffer.currentPageIndex = 0;
		myBuffer.currentSize = SSD_PAGE_SIZE;
		myBuffer.pageCount = 1;
		myBuffer.tupleCount = m_PageMAX->pageHeader->totalTuple; 
		myBuffer.isFullMaxValue = TRUE;
	}   

	return SUCCESS;
}

/// <summary>
/// Gets the lowest key, highest key values in buffer.
/// </summary>
/// <param name="myBuffer">My buffer.</param>
/// <param name="keyPos">The key position in table.</param>
/// <returns></returns>
RC PageHelpers2::GetLowestHighestValues(Buffer &myBuffer, const DWORD keyPos)
{
	if(myBuffer.currentSize > 0)
	{ 
		CHAR *tuple = new CHAR[TUPLE_SIZE];
		CHAR *page = new CHAR[SSD_PAGE_SIZE];
		INT tupleCountInPage = 0;
		INT pageIndex = 0;
		INT tupleIndex = 1;
		DWORD tupleKey = 0;
		PageHeader *pageHeader;
		PageSlot *tupleInfo;

		// First page, first tuple
		strcpy(page,  myBuffer.data + pageIndex*SSD_PAGE_SIZE);
		//page = myBuffer.data + pageIndex*SSD_PAGE_SIZE;   
		pageHeader = (PageHeader *)page; 
		tupleCountInPage = pageHeader->totalTuple; 

		tupleIndex = 1;
		tupleInfo = (PageSlot *)(page + SSD_PAGE_SIZE - sizeof(PageSlot)*tupleIndex);
		strcpy(tuple, page+((tupleInfo)->tupleOffset));
		//tuple = page+((tupleInfo)->tupleOffset);  
		GetTupleKey(tuple, tupleKey, keyPos);
		myBuffer.lowestKey = tupleKey;


		// Last page, last tuple
		pageIndex = myBuffer.pageCount - 1;
		strcpy(page,  myBuffer.data + pageIndex*SSD_PAGE_SIZE);
		//page = myBuffer.data + pageIndex*SSD_PAGE_SIZE;   
		pageHeader = (PageHeader *)page; 
		tupleCountInPage = pageHeader->totalTuple; 
		tupleIndex = tupleCountInPage;

		tupleInfo = (PageSlot *)(page + SSD_PAGE_SIZE - sizeof(PageSlot)*tupleIndex);
		strcpy(tuple, page+((tupleInfo)->tupleOffset));
		//tuple = page+((tupleInfo)->tupleOffset);  
		GetTupleKey(tuple, tupleKey, keyPos);
		myBuffer.highestKey = tupleKey;

		//delete tuple;
		//delete page;
	}
	else
	{
		myBuffer.lowestKey = 0;
		myBuffer.highestKey = 0;
	}

	return SUCCESS;
}

/// <summary>
/// Computes the buffer.
/// </summary>
/// <param name="myBuffer">My buffer.</param>
/// <param name="totalBytesInBuffer">The total bytes in buffer.</param>
/// <param name="pageSize">Size of the page.</param>
/// <returns></returns>
RC PageHelpers2::ComputeBuffer(Buffer &myBuffer, const DWORD totalBytesInBuffer, const DWORD pageSize)
{  
	if(totalBytesInBuffer > 0)
	{
		if(0==(totalBytesInBuffer % pageSize)) 
			myBuffer.pageCount = (totalBytesInBuffer / pageSize);  
		else 
			myBuffer.pageCount = (totalBytesInBuffer / pageSize) + 1;  

		myBuffer.currentSize = totalBytesInBuffer;
		myBuffer.currentPageIndex = 0; 
		myBuffer.freeLocation += totalBytesInBuffer;  
		myBuffer.isFullMaxValue = FALSE;
	}
	else
	{     
		AddPageToBuffer(myBuffer,  m_PageMAX->page, pageSize); 
		myBuffer.currentPageIndex = 0;
		myBuffer.currentSize = pageSize;
		myBuffer.pageCount = 1;
		myBuffer.tupleCount = m_PageMAX->pageHeader->totalTuple; 
		myBuffer.isFullMaxValue = TRUE;
	}   

	return SUCCESS;
}

/// <summary>
/// Adds the page maximum to buffer.
/// </summary>
/// <param name="myBuffer">My buffer.</param>
/// <param name="pageSize">Size of the page.</param>
/// <returns></returns>
RC PageHelpers2::AddPageMAXToBuffer(Buffer &myBuffer, DWORD pageSize)
{
	ResetBuffer(myBuffer);

	AddPageToBuffer(myBuffer,  m_PageMAX->page, pageSize); 
	myBuffer.currentPageIndex = 0;
	myBuffer.currentSize = pageSize;
	myBuffer.pageCount = 1;
	myBuffer.tupleCount = m_PageMAX->pageHeader->totalTuple; 
	myBuffer.isFullMaxValue = TRUE;

	return SUCCESS;
}  

/// <summary>
/// Initializes the MAX page.
/// </summary>
/// <param name="pageMAX">The MAX page pointer.</param>
/// <param name="tupleMAX">The MAX tuple pointer.</param>
/// <returns></returns>
RC PageHelpers2::InitMaxPage(PagePtr *&pageMAX, CHAR *&tupleMAX)
{   
	tupleMAX = new CHAR[50];
	sprintf(tupleMAX, "%d|%d|%d\n", MAX, MAX, MAX);

	pageMAX = (PagePtr *)malloc(sizeof(PagePtr));
	pageMAX->page = new char[SSD_PAGE_SIZE]; 
	pageMAX->pageHeader = (PageHeader *)(pageMAX->page);
	pageMAX->pageHeader->totalTuple = 0;
	pageMAX->pageHeader->slotLocation = SSD_PAGE_SIZE;
	pageMAX->tuple = pageMAX->page+8;
	pageMAX->pageSlot = (PageSlot *)(pageMAX->page+SSD_PAGE_SIZE); 
	pageMAX->offset = 8;
	pageMAX->freeSpace = SSD_PAGE_SIZE-8;  

	UINT tupleLength = strlen(tupleMAX)+1;

	while(!IsPageFull(pageMAX)) 
	{
		for (UINT i=0; i<tupleLength; i++)
		{
			*(pageMAX->tuple) = tupleMAX[i];
			*pageMAX->tuple++; 
		}  

		pageMAX->pageHeader->totalTuple++;
		pageMAX->pageHeader->slotLocation -= sizeof(PageSlot); 

		pageMAX->pageSlot--; 
		pageMAX->pageSlot->tupleSize = tupleLength;
		pageMAX->pageSlot->tupleOffset = pageMAX->offset;   

		pageMAX->offset += tupleLength; 
		pageMAX->freeSpace = pageMAX->freeSpace - tupleLength - sizeof(PageSlot);  
	}

	return SUCCESS;
}

/// <summary>
/// Initializes the run page.
/// </summary>
/// <param name="pagePtr">The page PTR.</param>
/// <param name="pageBuffer">The page buffer.</param>
/// <returns></returns>
RC PageHelpers2::InitRunPage(PagePtr *&pagePtr, Buffer &pageBuffer)
{ 
	// Init runpage 
	if(NULL==pageBuffer.data) {  return ERR_NOT_INIT_MEMORY; }

	pagePtr = (PagePtr *)malloc(sizeof(PagePtr));

	pagePtr->page = pageBuffer.data;
	pagePtr->pageHeader = (PageHeader *)(pagePtr->page);
	pagePtr->pageHeader->totalTuple = 0;
	pagePtr->pageHeader->slotLocation = SSD_PAGE_SIZE;

	pagePtr->tuple = pagePtr->page+8;
	pagePtr->pageSlot = (PageSlot *)(pagePtr->page+SSD_PAGE_SIZE); 
	pagePtr->offset = 8; 
	pagePtr->freeSpace = SSD_PAGE_SIZE-8;   

	return SUCCESS;
}

/// <summary>
/// Resets the run page.
/// </summary>
/// <param name="pagePtr">The page PTR.</param>
/// <param name="pageBuffer">The page buffer.</param>
VOID PageHelpers2::ResetRunPage(PagePtr *&pagePtr, Buffer &pageBuffer)
{  
	pageBuffer.currentPageIndex = 0; 
	pageBuffer.currentSize = 0;
	pageBuffer.freeLocation = pageBuffer.startLocation;

	pagePtr->page = pageBuffer.data + (pageBuffer.currentPageIndex * SSD_PAGE_SIZE);   
	pagePtr->tuple = pagePtr->page+8;   
	pagePtr->pageHeader = (PageHeader *)(pagePtr->page); 
	pagePtr->pageHeader->totalTuple=0;
	pagePtr->pageHeader->slotLocation=SSD_PAGE_SIZE;
	pagePtr->pageSlot=(PageSlot *)(pagePtr->page + SSD_PAGE_SIZE); 

	pagePtr->offset=8;
	pagePtr->freeSpace = SSD_PAGE_SIZE-8;  
	pagePtr->consumed = FALSE;
} 

/// <summary>
/// Gets the tuple information.
/// </summary>
/// <param name="recordPtr">The record PTR.</param>
/// <param name="tupleIndex">Index of the tuple in buffer.</param>
/// <param name="pageData">The page data.</param>
/// <param name="pageSize">Size of the page.</param>
/// <param name="keyPos">The key position in table.</param>
/// <returns></returns>
RC PageHelpers2::GetTupleInfo(RECORD *&recordPtr, const DWORD tupleIndex, CHAR *pageData, const DWORD pageSize, const DWORD keyPos)
{
	PageSlot *tupleInfo = (PageSlot *)(pageData + pageSize - sizeof(PageSlot)*tupleIndex); 
	recordPtr->data = pageData+((tupleInfo)->tupleOffset); 
	recordPtr->length = (tupleInfo)->tupleSize; 
	recordPtr->offset = (tupleInfo)->tupleOffset;

	if(recordPtr->length==0) { return ERR_CANNOT_GET_TUPLE_KEY;  }

	return GetTupleKey( recordPtr->data, recordPtr->key, keyPos); 
}

/// <summary>
/// Gets the tuple key.
/// </summary>
/// <param name="tupleData">The tuple data.</param>
/// <param name="tupleKey">The tuple key.</param>
/// <param name="keyPos">The key position in table.</param>
/// <returns></returns>
RC PageHelpers2::GetTupleKey(CHAR *tupleData, DWORD &tupleKey, const  DWORD keyPos)
{ 
	if(tupleData==NULL)  { return ERR_CANNOT_INITIAL_MEMORY; } 

	char *keyFieldStartPtr;

	if(keyPos==1) 
	{
		keyFieldStartPtr=tupleData;
		while(*tupleData!='|'){
			tupleData++;
		}
		*tupleData='\0'; 
	}
	else 
	{ 
		while(*tupleData!='|'){
			tupleData++;
		}
		tupleData++;
		//now it's the start position of the second position
		keyFieldStartPtr=tupleData;
		//find the end position of the second field
		while(*tupleData!='|'){
			tupleData++;
		}

		*tupleData='\0';// End of one tuple, add null value
	}

	tupleKey=atoi(keyFieldStartPtr);  //_atoi64

	*tupleData='|';	//recover the tuple to its original form 

	return SUCCESS;
}  

/// <summary>
/// Adds the page to buffer.
/// </summary>
/// <param name="myBuffer">My buffer.</param>
/// <param name="page">The page data.</param>
/// <param name="pageSize">Size of the page.</param>
/// <returns></returns>
RC PageHelpers2::AddPageToBuffer(Buffer &myBuffer, CHAR *page, const DWORD pageSize)
{   
	if(NULL==page)
	{
		if(m_PageMAX->page==NULL) {return ERR_CANNOT_INITIAL_MEMORY;}

		for (UINT i=0; i < pageSize; i++)
		{
			myBuffer.data[ myBuffer.currentSize ] = m_PageMAX->page[i];
			myBuffer.currentSize++; 
		}  
	}
	else
	{
		for (UINT i=0; i < pageSize; i++)
		{
			myBuffer.data[ myBuffer.currentSize ] = page[i];
			myBuffer.currentSize++; 
		}  
	} 

	return SUCCESS;
} 

/// <summary>
/// Gets the first page in buffer.
/// </summary>
/// <param name="srcBufferPtr">The source buffer PTR.</param>
/// <param name="desBufferPtr">The DES buffer PTR.</param>
/// <param name="pageSize">Size of the page.</param>
/// <returns></returns>
RC  PageHelpers2::GetFirstPage(Buffer *srcBufferPtr, Buffer *desBufferPtr, const DWORD pageSize)
{
	/* 
	Get the first page in inputbuffer then increase input buffer page index up 
	Each time read only ONE page
	*/ 

	if(srcBufferPtr==NULL) {return ERR_CANNOT_INITIAL_MEMORY;}

	/* Reset probe buffer to default value */
	ResetBuffer(desBufferPtr); 

	DWORD pageIndex = 0;
	desBufferPtr->data = srcBufferPtr->data + pageIndex*pageSize;  
	PageHeader *pageHeader = (PageHeader *)desBufferPtr->data;
	desBufferPtr->tupleCount = pageHeader->totalTuple; 
	desBufferPtr->currentSize = pageSize;
	desBufferPtr->pageCount = 1;

	/* for next read */
	srcBufferPtr->currentPageIndex++;

	return SUCCESS; 
}

/// <summary>
/// Gets the next page in buffer.
/// </summary>
/// <param name="srcBufferPtr">The source buffer PTR.</param>
/// <param name="desBufferPtr">The DES buffer PTR.</param>
/// <param name="isExhausted">Is buffer exhausted.</param>
/// <param name="pageSize">Size of the page.</param>
/// <returns></returns>
RC PageHelpers2::GetNextPage(Buffer *srcBufferPtr, Buffer *desBufferPtr, BOOL &isExhausted, const DWORD pageSize)
{
	if(srcBufferPtr==NULL)
		return ERR_CANNOT_INITIAL_MEMORY;

	/* Reset probe buffer to default value */
	ResetBuffer(desBufferPtr); 

	/* Check current input buffer has page to read or not */
	if(srcBufferPtr->currentPageIndex >= srcBufferPtr->pageCount)
	{
		isExhausted = TRUE;
		return SUCCESS; 
	}

	desBufferPtr->data = srcBufferPtr->data + ( srcBufferPtr->currentPageIndex * pageSize );  
	PageHeader *pageHeader = (PageHeader *)desBufferPtr->data;
	desBufferPtr->tupleCount = pageHeader->totalTuple; 
	desBufferPtr->currentSize = pageSize;
	desBufferPtr->pageCount = 1;

	/* for next read */
	isExhausted = FALSE;
	srcBufferPtr->currentPageIndex++;

	return SUCCESS; 
}

/// <summary>
/// Releases the buffer.
/// </summary>
/// <param name="bufferPtr">The buffer PTR.</param>
/// <returns></returns>
RC PageHelpers2::ReleaseBuffer(Buffer *bufferPtr)
{
	/* TODO: Tracking memory size */

	return SUCCESS;
}
 
/// <summary>
/// Gets the page information.
/// Fill probe buffer with data from input buffer at pageIndex
/// </summary>
/// <param name="src">The buffer data.</param>
/// <param name="pageBuffer">The page buffer.</param>
/// <param name="pageIndex">Index of the page in buffer.</param>
/// <param name="pageSize">Size of the page.</param>
/// <returns></returns>
RC PageHelpers2::GetPageInfo(CHAR *src, Buffer &pageBuffer, const DWORD pageIndex, const DWORD pageSize)
{ 
	if(src==NULL)
		return ERR_NOT_INIT_MEMORY;

	pageBuffer.data = src + pageIndex*pageSize;  
	PageHeader *pageHeader = (PageHeader *)pageBuffer.data;
	pageBuffer.tupleCount = pageHeader->totalTuple; 

	return SUCCESS;
}
 
/// <summary>
/// Gets the page information.
/// Fill probe buffer with data from input buffer at pageIndex
/// </summary>
/// <param name="src">The buffer data.</param>
/// <param name="pagePtr">The page PTR.</param>
/// <param name="pageBuffer">The page buffer.</param>
/// <param name="pageIndex">Index of the page in buffer.</param>
/// <param name="pageSize">Size of the page.</param>
/// <returns></returns>
RC PageHelpers2::GetPageInfo(CHAR *src, PagePtr *pagePtr, Buffer &pageBuffer, const DWORD pageIndex, const DWORD pageSize)
{ 
	if(src==NULL)
		return ERR_NOT_INIT_MEMORY;

	pageBuffer.data = src + pageIndex*pageSize;  
	PageHeader *pageHeader = (PageHeader *)pageBuffer.data;
	pageBuffer.tupleCount = pageHeader->totalTuple; 

	pagePtr->page = pageBuffer.data;
	pagePtr->pageHeader = pageHeader;
	pagePtr->pageSlot =(PageSlot*)(pagePtr->page + SSD_PAGE_SIZE-sizeof(PageSlot));

	return SUCCESS;
}

/// <summary>
/// Determines whether page is full 
/// </summary>
/// <param name="pagePtr">The page PTR.</param>
/// <returns></returns>
BOOL PageHelpers2::IsPageFull(PagePtr *pagePtr)
{
	return pagePtr->freeSpace < 258 ? TRUE : FALSE;
}

/// <summary>
/// Determines whether join page is full 
/// </summary>
/// <param name="pagePtr">The page PTR.</param>
/// <returns></returns>
BOOL PageHelpers2::IsJoinPageFull(PagePtr *pagePtr)
{
	return pagePtr->freeSpace < 508 ? TRUE : FALSE;
}

/// <summary>
/// Determines whether buffer is full.
/// </summary>
/// <param name="myBuffer">My buffer.</param>
/// <returns></returns>
BOOL PageHelpers2::IsBufferFull(Buffer myBuffer)
{
	return myBuffer.size - myBuffer.currentSize <= 0 ? TRUE : FALSE;
}

/// <summary>
/// Adds the tuple to join page.
/// </summary>
/// <param name="pagePtr">The page PTR.</param>
/// <param name="recordPtr">The record PTR.</param>
/// <param name="pageBuffer">The page buffer.</param>
/// <returns></returns>
RC PageHelpers2::AddTupleToJoinPage(PagePtr *&pagePtr, RECORD *recordPtr, Buffer &pageBuffer)
{ 
	if(recordPtr->data==NULL)
		return ERR_NOT_INIT_MEMORY;

	UINT tupleLength = recordPtr->length;
	for (UINT i=0; i < tupleLength; i++)
	{
		*(pagePtr->tuple) = recordPtr->data[i];
		*pagePtr->tuple++; 
	}  

	pagePtr->pageHeader->totalTuple++;
	pagePtr->pageHeader->slotLocation -= sizeof(PageSlot); 

	pagePtr->pageSlot--; 
	pagePtr->pageSlot->tupleSize = tupleLength;
	pagePtr->pageSlot->tupleOffset = pagePtr->offset;   

	pagePtr->offset += tupleLength; 
	pagePtr->freeSpace = pagePtr->freeSpace - tupleLength - sizeof(PageSlot);  

	pageBuffer.freeLocation += tupleLength + sizeof(PageSlot);  
	pageBuffer.currentSize += tupleLength + sizeof(PageSlot);   

	return SUCCESS;
} 

/// <summary>
/// Adds the tuple to page.
/// </summary>
/// <param name="pagePtr">The page PTR.</param>
/// <param name="tupleData">The tuple data.</param>
/// <param name="pageBuffer">The page buffer.</param>
/// <returns></returns>
RC PageHelpers2::AddTupleToPage(PagePtr *&pagePtr, CHAR *tupleData, Buffer &pageBuffer)
{ 
	if(tupleData==NULL)
		return ERR_NOT_INIT_MEMORY;

	UINT tupleLength = strlen(tupleData)+1; // plus 1 for NULL
	for (UINT i=0; i<tupleLength; i++)
	{
		*(pagePtr->tuple) = tupleData[i];
		*pagePtr->tuple++; 
	}  

	// strncpy(runPage->tuple, tupleData, tupleLength);

	pagePtr->pageHeader->totalTuple++;
	pagePtr->pageHeader->slotLocation -= sizeof(PageSlot); 

	pagePtr->pageSlot--; 
	pagePtr->pageSlot->tupleSize = tupleLength;
	pagePtr->pageSlot->tupleOffset = pagePtr->offset;   

	pagePtr->offset += tupleLength; 
	pagePtr->freeSpace = pagePtr->freeSpace - tupleLength - sizeof(PageSlot);  

	pageBuffer.freeLocation += tupleLength + sizeof(PageSlot);  
	pageBuffer.currentSize += tupleLength + sizeof(PageSlot);   

	return SUCCESS;
}  


/// <summary>
/// Adds the tuple to page.
/// </summary>
/// <param name="pagePtr">The page PTR.</param>
/// <param name="recordPtr">The record PTR.</param>
/// <param name="pageBuffer">The page buffer.</param>
/// <returns></returns>
RC PageHelpers2::AddTupleToPage(PagePtr *&pagePtr, RECORD *recordPtr, Buffer &pageBuffer)
{ 
	if(recordPtr->data==NULL)
		return ERR_NOT_INIT_MEMORY;

	UINT tupleLength = recordPtr->length; // plus 1 for NULL
	for (UINT i=0; i<tupleLength; i++)
	{
		*(pagePtr->tuple) = recordPtr->data[i];
		*pagePtr->tuple++; 
	}  

	// strncpy(runPage->tuple, tupleData, tupleLength);

	pagePtr->pageHeader->totalTuple++;
	pagePtr->pageHeader->slotLocation -= sizeof(PageSlot); 

	pagePtr->pageSlot--; 
	pagePtr->pageSlot->tupleSize = tupleLength;
	pagePtr->pageSlot->tupleOffset = pagePtr->offset;   

	pagePtr->offset += tupleLength; 
	pagePtr->freeSpace = pagePtr->freeSpace - tupleLength - sizeof(PageSlot);  

	pageBuffer.freeLocation += tupleLength + sizeof(PageSlot);  
	pageBuffer.currentSize += tupleLength + sizeof(PageSlot);   

	return SUCCESS;
} 

/// <summary>
/// Resets the buffer to default values.
/// </summary>
/// <param name="bufferPtr">The buffer PTR.</param>
/// <returns></returns>
RC PageHelpers2::ResetBuffer(Buffer *bufferPtr)
{  
	bufferPtr->freeLocation = bufferPtr->startLocation;

	bufferPtr->currentSize = 0;  
	bufferPtr->currentTupleIndex = 1;  
	bufferPtr->currentPageIndex = 0;  
	bufferPtr->pageCount = 0;  
	bufferPtr->tupleCount = 0;  
	bufferPtr->isSort = FALSE;
	bufferPtr->isFullMaxValue = FALSE;

	bufferPtr->lowestKey = 0;
	bufferPtr->highestKey = 0; 

	return SUCCESS;
} 

/// <summary>
/// Resets the buffer to default values.
/// </summary>
/// <param name="myBuffer">My buffer.</param>
/// <returns></returns>
RC PageHelpers2::ResetBuffer(Buffer &myBuffer)
{  
	myBuffer.freeLocation = myBuffer.startLocation;

	myBuffer.currentSize = 0;  
	myBuffer.currentTupleIndex = 1;  
	myBuffer.currentPageIndex = 0;  
	myBuffer.pageCount = 0;  
	myBuffer.tupleCount = 0;  
	myBuffer.isSort = FALSE;
	myBuffer.isFullMaxValue = FALSE;

	myBuffer.lowestKey = 0;
	myBuffer.highestKey = 0; 

	return SUCCESS;
} 

/// <summary>
/// Determines whether is empty page.
/// </summary>
/// <param name="pagePtr">The page PTR.</param>
/// <returns></returns>
BOOL PageHelpers2::IsEmptyPage(PagePtr *pagePtr)
{
	PageHeader *pageHeader = (PageHeader *)pagePtr->page;
	if( pageHeader->totalTuple==0)
		return TRUE;
	return FALSE; 
}