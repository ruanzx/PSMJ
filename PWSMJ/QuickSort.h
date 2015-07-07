//
// Name: QuickSort.cpp   
// Author: hieunt
// Description: Implement Quicksort with reference (key,rid)
//


#pragma once
   
#include "DataTypes.h"
#include "PageHelpers.h"

class QuickSort  
{    
	const UINT m_Size;
	UINT m_CurrentSize;
	Buffer m_DataBuffer;
	RECORD **m_Objects; // record array 
	UINT *m_RefKey;
//public:
	//RECORD *recordPtr; // use for get record from buffer
public:    
	QuickSort(UINT vSize, Buffer vDataBuffer);
	~QuickSort();
	VOID Reset();
	UINT GetMaxSize();
	UINT GetCurrentSize();
	RECORD * GetRecord(UINT pos);
	VOID AddRecord(RECORD *record, UINT pos);
	VOID Sort();
	VOID  QuickSortByReference(RECORD **&arrObject, UINT *&arrRefKey, INT startIndex, INT endIndex);
	VOID QuickSortClassic(FANS *runs, INT64 *key, INT left, INT right);
	 
}; 