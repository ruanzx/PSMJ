// 
// Name: PageHelpers.cpp : implementation file 
// Author: hieunt
// Description: Helpers for working with N-ary struct page
//

#include "stdafx.h"

#include "PageHelpers.h"

/// <summary>
/// Initializes a new instance of the <see cref="PageHelpers"/> class.
/// </summary>
PageHelpers::PageHelpers()  
{  
	InitMaxPage(m_PageMAX, m_TupleMAX); 
}

/// <summary>
/// Finalizes an instance of the <see cref="PageHelpers"/> class.
/// </summary>
PageHelpers::~PageHelpers()
{  
	delete m_PageMAX; 
	delete m_TupleMAX;
} 

/// <summary>
/// Write debug infos to file.
/// </summary>
/// <param name="data">The data content.</param>
/// <param name="index">Set the file index.</param>
/// <param name="size">The size to write.</param>
VOID PageHelpers::DebugToFile(CHAR *data, UINT index, UINT size)
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
/// Sets the MAX value to tuple.
/// </summary>
/// <param name="recordPtr">The record PTR.</param>
/// <returns></returns>
RC PageHelpers::SetMaxTuple(RECORD *&recordPtr)
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
DWORD PageHelpers::GetTupleNumInMaxPage()
{
	return  m_PageMAX->pageHeader->totalTuple;
}

/// <summary>
/// Initializes the buffer.
/// </summary>
/// <param name="myBuffer">My buffer.</param>
/// <param name="bufferSize">Size of the buffer.</param>
/// <returns></returns>
RC PageHelpers::InitBuffer(Buffer &myBuffer, const DWORD bufferSize)
{  
	//buff.startLocation = bufferPool->currentSize;
	//buff.freeLocation = buff.startLocation;	
	//buff.data = bufferPool->data + buff.startLocation;
	//bufferPool->currentSize += bufferSize;

	myBuffer.startLocation = 0;
	myBuffer.freeLocation = 0;	
	myBuffer.data = new CHAR[bufferSize];

	myBuffer.size = bufferSize;
	myBuffer.currentTupleIndex = 1;
	myBuffer.tupleCount = 0;
	myBuffer.currentPageIndex = 0;
	myBuffer.pageCount = 0;
	myBuffer.currentSize = 0; 
	myBuffer.isSort = FALSE; 
	myBuffer.isFullMaxValue = FALSE; 

	return SUCCESS;
}

/// <summary>
/// Computes tuple count, page in the buffer.
/// </summary>
/// <param name="myBuffer">My buffer.</param>
/// <param name="totalBytesInBuffer">The total bytes in buffer.</param>
/// <returns></returns>
RC PageHelpers::ComputeBuffer(Buffer &myBuffer, const DWORD totalBytesInBuffer)
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
/// Computes what have in the buffer.
/// </summary>
/// <param name="myBuffer">My buffer.</param>
/// <param name="totalBytesInBuffer">The total bytes in buffer.</param>
/// <param name="pageSize">Size of the page.</param>
/// <returns></returns>
RC PageHelpers::ComputeBuffer(Buffer &myBuffer, const DWORD totalBytesInBuffer, const DWORD pageSize)
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
/// Adds the MAX page to buffer.
/// </summary>
/// <param name="myBuffer">My buffer.</param>
/// <param name="pageSize">Size of the page.</param>
/// <returns></returns>
RC PageHelpers::AddPageMAXToBuffer(Buffer &myBuffer, DWORD pageSize)
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
//
//RC PageHelpers::InitBlankPage(CHAR *&blankPage)
//{
//	blankPage = new char[SSD_PAGE_SIZE]; 
//	for(unsigned int i=0; i<SSD_PAGE_SIZE; i++) 
//		blankPage[i] = '#'; 
//
//	return SUCCESS;
//}

/// <summary>
/// Initializes the MAX page.
/// </summary>
/// <param name="pageMAX">The MAX page pointer.</param>
/// <param name="tupleMAX">The MAX tuple pointer.</param>
/// <returns></returns>
RC PageHelpers::InitMaxPage(PagePtr *&pageMAX, CHAR *&tupleMAX)
{   
	tupleMAX = new char[50];
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
RC PageHelpers::InitRunPage(PagePtr *&pagePtr, Buffer &pageBuffer)
{ 
	// Init runpage 
	if(NULL==pageBuffer.data)
	{ 
		return ERR_NOT_INIT_MEMORY;
	}

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
/// <param name="runPage">The run page pointer.</param>
/// <param name="runPageBuffer">The run page buffer.</param>
VOID PageHelpers::ResetRunPage(PagePtr *&runPage, Buffer &runPageBuffer)
{ 
	//runPageBuffer.freeLocation=runPageBuffer.startLocation; 
	//runPageBuffer.data=bufferPool->data + runPageBuffer.startLocation; 

	runPageBuffer.currentSize = 0;

	runPage->page = runPageBuffer.data;

	runPage->tuple = runPage->page+8;   
	runPage->pageHeader = (PageHeader *)(runPage->page); 
	runPage->pageHeader->totalTuple=0;
	runPage->pageHeader->slotLocation=SSD_PAGE_SIZE;
	runPage->pageSlot=(PageSlot *)(runPage->page + SSD_PAGE_SIZE); 

	runPage->offset=8;
	runPage->freeSpace = SSD_PAGE_SIZE-8;  
	runPage->consumed = FALSE;
} 

/// <summary>
/// Gets the tuple information.
/// </summary>
/// <param name="recordPtr">The record PTR.</param>
/// <param name="tupleIndex">Index of the tuple.</param>
/// <param name="pageData">The page data.</param>
/// <param name="pageSize">Size of the page.</param>
/// <param name="keyPos">The key position in table.</param>
/// <returns></returns>
RC PageHelpers::GetTupleInfo(RECORD *&recordPtr, const DWORD tupleIndex, CHAR *pageData, const DWORD pageSize, const DWORD keyPos)
{
	PageSlot *tupleInfo = (PageSlot *)(pageData + pageSize - sizeof(PageSlot)*tupleIndex); 
	recordPtr->data = pageData+((tupleInfo)->tupleOffset); 
	recordPtr->length = (tupleInfo)->tupleSize; 
	recordPtr->offset = (tupleInfo)->tupleOffset;

	if(recordPtr->length==0) {
		return ERR_CANNOT_GET_TUPLE_KEY; 
	}

	return GetTupleKey( recordPtr->data, recordPtr->key, keyPos); 
}

/// <summary>
/// Gets the tuple key.
/// </summary>
/// <param name="tupleData">The tuple data pointer.</param>
/// <param name="tupleKey">The tuple key.</param>
/// <param name="keyPos">The key position in table.</param>
/// <returns></returns>
RC PageHelpers::GetTupleKey(CHAR *tupleData, DWORD &tupleKey, const  DWORD keyPos)
{ 
	if(tupleData==NULL)  
		return ERR_CANNOT_INITIAL_MEMORY; 

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

	tupleKey= atoi(keyFieldStartPtr);  //_atoi64

	*tupleData='|';	//recover the tuple to its original form 

	return SUCCESS;
}  

/// <summary>
/// Adds the page to buffer.
/// </summary>
/// <param name="myBuffer">My buffer.</param>
/// <param name="page">The page pointer.</param>
/// <param name="pageSize">Size of the page.</param>
/// <returns></returns>
RC PageHelpers::AddPageToBuffer(Buffer &myBuffer, CHAR *page, const DWORD pageSize)
{   
	if(NULL==page)
	{
		if(m_PageMAX->page==NULL)
			return ERR_CANNOT_INITIAL_MEMORY;

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
//
//RC PageHelpers::GetLowestHighestValue(Buffer &buff, DWORD keyPos, DWORD64 &lowestKey, DWORD64 &highestKey)
//{
//	if(NULL==buff.data)
//		return ERR_CANNOT_INITIAL_MEMORY;
//	//
//	ResetBuffer(m_PageBuffer);
//
//	GetPageInfo(buff.data, m_PageBuffer, 0, SSD_PAGE_SIZE);
//	GetTupleInfo(m_ProbeRecord, 1, m_PageBuffer.data, SSD_PAGE_SIZE, keyPos);
//	lowestKey = m_ProbeRecord->key;
//
//	ResetBuffer(m_PageBuffer);
//	GetPageInfo(buff.data, m_PageBuffer, buff.totalPage-1, SSD_PAGE_SIZE);
//	m_pageHeader =(PageHeader *)m_PageBuffer.data;
//
//	GetTupleInfo(m_ProbeRecord, m_pageHeader->totalTuple, m_PageBuffer.data, SSD_PAGE_SIZE, keyPos);
//	highestKey = m_ProbeRecord->key;
//
//	return SUCCESS; 
//}


/// <summary>
/// Gets the first page in buffer.
/// </summary>
/// <param name="srcBufferPtr">The source buffer PTR.</param>
/// <param name="desBufferPtr">The DES buffer PTR.</param>
/// <param name="pageSize">Size of the page.</param>
/// <returns></returns>
RC  PageHelpers::GetFirstPage(Buffer *srcBufferPtr, Buffer *desBufferPtr, const DWORD pageSize)
{
	/* 
 Get the first page in inputbuffer then increase input buffer page index up 
 Each time read only ONE page
 */ 

	if(srcBufferPtr==NULL)
		return ERR_CANNOT_INITIAL_MEMORY;

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
/// <param name="isExhausted"> Is buffer exhausted.</param>
/// <param name="pageSize">Size of the page.</param>
/// <returns></returns>
RC PageHelpers::GetNextPage(Buffer *srcBufferPtr, Buffer *desBufferPtr, BOOL &isExhausted, const DWORD pageSize)
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
RC PageHelpers::ReleaseBuffer(Buffer *bufferPtr)
{
	/* TODO: Tracking memory size */

	return SUCCESS;
}
 
/// <summary>
/// Gets the page information. 
/// Fill probe buffer with data from input buffer at pageIndex
/// </summary>
/// <param name="src">The source.</param>
/// <param name="pageBuffer">The page buffer.</param>
/// <param name="pageIndex">Index of the page.</param>
/// <param name="pageSize">Size of the page.</param>
/// <returns></returns>
RC PageHelpers::GetPageInfo(CHAR *src, Buffer &pageBuffer, const DWORD pageIndex, const DWORD pageSize)
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
/// <param name="pageIndex">Index of the page.</param>
/// <param name="pageSize">Size of the page.</param>
/// <returns></returns>
RC PageHelpers::GetPageInfo(CHAR *src, PagePtr *pagePtr, Buffer &pageBuffer, const DWORD pageIndex, const DWORD pageSize)
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
BOOL PageHelpers::IsPageFull(PagePtr *pagePtr)
{
	return pagePtr->freeSpace < 258 ? TRUE : FALSE;
}

/// <summary>
/// Determines whether join page is full
/// </summary>
/// <param name="pagePtr">The page PTR.</param>
/// <returns></returns>
BOOL PageHelpers::IsJoinPageFull(PagePtr *pagePtr)
{
	return pagePtr->freeSpace < 508 ? TRUE : FALSE;
}

/// <summary>
/// Determines whether buffer is full
/// </summary>
/// <param name="myBuffer">My buffer.</param>
/// <returns></returns>
BOOL PageHelpers::IsBufferFull(Buffer myBuffer)
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
RC PageHelpers::AddTupleToJoinPage(PagePtr *&pagePtr, RECORD *recordPtr, Buffer &pageBuffer)
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
RC PageHelpers::AddTupleToPage(PagePtr *&pagePtr, CHAR *tupleData, Buffer &pageBuffer)
{ 
	if(tupleData==NULL)
		return ERR_NOT_INIT_MEMORY;

	UINT tupleLength = strlen(tupleData)+1;
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
/// Resets the buffer to default values.
/// </summary>
/// <param name="bufferPtr">The buffer PTR.</param>
/// <returns></returns>
RC PageHelpers::ResetBuffer(Buffer *bufferPtr)
{ 
	//buff.freeLocation = buff.startLocation;
	//buff.data=bufferPool->data + buff.startLocation; 

	bufferPtr->currentSize = 0;  
	bufferPtr->currentTupleIndex = 1;  
	bufferPtr->currentPageIndex = 0;  
	bufferPtr->pageCount = 0;  
	bufferPtr->tupleCount = 0;  
	bufferPtr->isSort = FALSE;
	bufferPtr->isFullMaxValue = FALSE;

	return SUCCESS;
} 
 
/// <summary>
/// Resets the buffer.
/// </summary>
/// <param name="myBuffer">My buffer.</param>
/// <returns></returns>
RC PageHelpers::ResetBuffer(Buffer &myBuffer)
{ 
	//buff.freeLocation = buff.startLocation;
	//buff.data=bufferPool->data + buff.startLocation; 

	myBuffer.currentSize = 0;  
	myBuffer.currentTupleIndex = 1;  
	myBuffer.currentPageIndex = 0;  
	myBuffer.pageCount = 0;  
	myBuffer.tupleCount = 0;  
	myBuffer.isSort = FALSE;
	myBuffer.isFullMaxValue = FALSE;

	return SUCCESS;
} 