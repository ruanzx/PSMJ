// 
// Name: PageHelpers.cpp : implementation file 
// Author: hieunt
// Description: Helpers functions for working with N-ary struct page
//

#pragma once 

#include <stdio.h>  
#include <string>

#include "DataTypes.h"
using namespace std; 

class PageHelpers2 
{     
	PagePtr *m_PageMAX; 
	CHAR *m_TupleMAX;  
	RC rc;
public:    
	PageHelpers2();
	~PageHelpers2();
	VOID DebugToFile(CHAR *data, UINT index, UINT size);  
	RC SetMaxTuple(RECORD *&recordPtr);
	DWORD GetTupleNumInMaxPage(); 

	RC InitBuffer(Buffer &myBuffer, const DWORD bufferSize, BufferPool *bufferPool);
	RC InitBuffer(Buffer &myBuffer, const DWORD bufferSize);

	RC ComputeBuffer(Buffer &myBuffer, const DWORD totalBytesInBuffer);
	RC ComputeBuffer(Buffer &myBuffer, const DWORD totalBytesInBuffer, const DWORD pageSize);
	RC AddPageMAXToBuffer(Buffer &myBuffer, const DWORD pageSize);

	RC GetLowestHighestValues(Buffer &myBuffer, const DWORD keyPos);

#pragma region Page Functions

	RC   InitMaxPage(PagePtr *&pageMAX, CHAR *&tupleMAX);
	RC   InitRunPage(PagePtr *&pagePtr, Buffer &pageBuffer);
	VOID ResetRunPage(PagePtr *&pagePtr, Buffer &pageBuffer);

	RC   GetTupleInfo(RECORD *&recordPtr, const DWORD tupleIndex, CHAR *pageData, const DWORD pageSize, const DWORD keyPos);
	RC   GetTupleKey(CHAR *tupleData, DWORD &tupleKey, const  DWORD keyPos);

	RC   AddPageToBuffer(Buffer &myBuffer, CHAR *page, const DWORD pageSize);

	RC   GetFirstPage(Buffer *srcBufferPtr, Buffer *desBufferPtr, const DWORD pageSize); 

	RC   GetNextPage(Buffer *srcBufferPtr, Buffer *desBufferPtr, BOOL &isExhausted, const DWORD pageSize);
	RC   ReleaseBuffer(Buffer *bufferPtr); 
	RC   GetPageInfo(CHAR *src, Buffer &pageBuffer, const DWORD pageIndex, const DWORD pageSize); 
	RC   GetPageInfo(CHAR *src, PagePtr *pagePtr, Buffer &pageBuffer, const DWORD pageIndex, const DWORD pageSize);
	BOOL IsPageFull(PagePtr *pagePtr);
	BOOL IsJoinPageFull(PagePtr *pagePtr); 
	BOOL IsBufferFull(Buffer myBuffer); 
	RC   AddTupleToJoinPage(PagePtr *&pagePtr, RECORD *recordPtr, Buffer &pageBuffer);
	RC   AddTupleToPage(PagePtr *&pagePtr, RECORD *recordPtr, Buffer &pageBuffer);
	RC   AddTupleToPage(PagePtr *&pagePtr, CHAR *tupleData, Buffer &pageBuffer); 
	RC   ResetBuffer(Buffer *bufferPtr);
	RC   ResetBuffer(Buffer &myBuffer); 
	BOOL IsEmptyPage(PagePtr *pagePtr);

#pragma endregion 
};  //end class   

