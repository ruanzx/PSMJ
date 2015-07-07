// 
// Name: PageHelpers.cpp : implementation file 
// Author: hieunt
// Description: Helpers for working with N-ary struct page
//

#pragma once

#include <stdio.h>  
#include <string>

#include "DataTypes.h"
using namespace std; 

class PageHelpers  
{     
	PagePtr *m_PageMAX; 
	CHAR *m_TupleMAX;  
	RC rc;
public:    
	PageHelpers();
	~PageHelpers();
	VOID DebugToFile(CHAR *data, UINT index, UINT size);  
	RC SetMaxTuple(RECORD *&recordPtr);
	DWORD GetTupleNumInMaxPage(); 

	RC InitBuffer(Buffer &myBuffer, const DWORD bufferSize);
	RC ComputeBuffer(Buffer &myBuffer, const DWORD totalBytesInBuffer);
	RC ComputeBuffer(Buffer &myBuffer, const DWORD totalBytesInBuffer, const DWORD pageSize);
	RC AddPageMAXToBuffer(Buffer &myBuffer, const DWORD pageSize);
	 
#pragma region Page Functions
	 
	RC   InitMaxPage(PagePtr *&pageMAX, CHAR *&tupleMAX);
	RC   InitRunPage(PagePtr *&pagePtr, Buffer &pageBuffer);
	VOID ResetRunPage(PagePtr *&runPage, Buffer &runPageBuffer);

	RC   GetTupleInfo(RECORD *&recordPtr, const DWORD tupleIndex, CHAR *pageData, const DWORD pageSize, const DWORD keyPos);
	RC   GetTupleKey(CHAR *tupleData, DWORD &tupleKey, const  DWORD keyPos);

	RC   AddPageToBuffer(Buffer &buff, CHAR *page, DWORD pageSize);

	//RC   GetLowestHighestValue(Buffer &buff, DWORD keyPos, DWORD64 &lowestKey, DWORD64 &highestKey); 
	RC   GetFirstPage(Buffer *srcBufferPtr, Buffer *desBufferPtr, const DWORD pageSize); 

	RC   GetNextPage(Buffer *srcBufferPtr, Buffer *desBufferPtr, BOOL &isExhausted, const DWORD pageSize);
	RC   ReleaseBuffer(Buffer *bufferPtr); 
	RC   GetPageInfo(CHAR *src, Buffer &pageBuffer, const DWORD pageIndex, const DWORD pageSize); 
	RC   GetPageInfo(CHAR *src, PagePtr *pagePtr, Buffer &pageBuffer, const DWORD pageIndex, const DWORD pageSize);
	BOOL IsPageFull(PagePtr *pagePtr);
	BOOL IsJoinPageFull(PagePtr *pagePtr); 
	BOOL IsBufferFull(Buffer myBuffer); 
	RC   AddTupleToJoinPage(PagePtr *&pagePtr, RECORD *recordPtr, Buffer &pageBuffer);
	RC   AddTupleToPage(PagePtr *&pagePtr, CHAR *tupleData, Buffer &pageBuffer); 
	RC   ResetBuffer(Buffer *bufferPtr);
	RC   ResetBuffer(Buffer &myBuffer); 

#pragma endregion 
};  //end class   
   