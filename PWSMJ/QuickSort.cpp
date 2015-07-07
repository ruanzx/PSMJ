//
// Name: QuickSort.cpp   
// Author: hieunt
// Description: Implement Quicksort with reference (key,rid)
//

#include "stdafx.h"
#include "QuickSort.h"

/// <summary>
/// Quicks the sort.
/// </summary>
/// <param name="vSize">Size of the quicksort.</param>
/// <param name="vDataBuffer">The quicksort data buffer.</param>
/// <returns></returns>
QuickSort::QuickSort(const UINT vSize, Buffer vDataBuffer) : m_Size(vSize)
{    
	m_CurrentSize = 0;
	m_RefKey = new UINT[vSize]; 
	m_Objects = new RECORD*[vSize];
	//recordPtr = new RECORD();
	for(UINT i=0; i < vSize; i++)
	{
		m_RefKey[i] = 0;
		m_Objects[i] = new RECORD(0, vDataBuffer.data + TUPLE_SIZE * i, TUPLE_SIZE); 
	} 
} 

/// <summary>
/// Finalizes an instance of the <see cref="QuickSort"/> class.
/// </summary>
QuickSort::~QuickSort()
{  
	//delete recordPtr;
	delete [] m_Objects;
	delete [] m_RefKey;
}  

/// <summary>
/// Resets this instance.
/// </summary>
VOID QuickSort::Reset()
{
	m_CurrentSize = 0;
}

/// <summary>
/// Gets the maximum size.
/// </summary>
/// <returns></returns>
UINT QuickSort::GetMaxSize()
{
	return m_Size;
}

/// <summary>
/// Gets the size of the current.
/// </summary>
/// <returns></returns>
UINT QuickSort::GetCurrentSize()
{
	return m_CurrentSize;
}

/// <summary>
/// Gets the record at position.
/// </summary>
/// <param name="pos">The position.</param>
/// <returns></returns>
RECORD* QuickSort::GetRecord(UINT pos)
{
	return m_Objects[m_RefKey[pos]];
}

/// <summary>
/// Adds the record into array.
/// </summary>
/// <param name="record">The record.</param>
/// <param name="pos">The position.</param>
VOID QuickSort::AddRecord(RECORD *record, UINT pos)
{
	m_Objects[pos]->key = record->key;
	m_Objects[pos]->length = record->length;
	strcpy(m_Objects[pos]->data, record->data); 
	m_RefKey[pos] = pos;
	m_CurrentSize++;
}

/// <summary>
/// Sorts this instance.
/// </summary>
VOID QuickSort::Sort()
{
	QuickSortByReference(m_Objects, m_RefKey, 0, m_CurrentSize - 1);
}

/// <summary>
/// Quicks the sort by reference.
/// QuickSort By-Reference does not move any of the original unsorted data, 
/// it simply sorts an array of indices that reference that data. 
/// The following snippet accepts an array of TUPLES, sorts on TUPLES.tupleKey, 
/// and avoids moving any records around. Instead, it moves 32-bit integer references about,
/// a breeze for a 32-bit machine. The result is a sorted arrRefKey as Int32() array of references 
/// to the arrRefKey as TUPLES() array of unsorted, original data. 
/// </summary>
/// <param name="arrObject">The object array.</param>
/// <param name="arrRefKey">The reference key array.</param>
/// <param name="startIndex">The start index.</param>
/// <param name="endIndex">The end index.</param>
VOID QuickSort::QuickSortByReference( RECORD **&arrObject, UINT *&arrRefKey, INT startIndex, INT endIndex)  
{     
	int low = startIndex;
	int high = endIndex;  
	int pivot = arrRefKey[(low+high)/2];  

	// partition  
	while (low < high) {  
		while (arrObject[arrRefKey[low]]->key < arrObject[pivot]->key)   
			low++;   

		while (arrObject[arrRefKey[high]]->key > arrObject[pivot]->key)
			high--;   

		if (low <= high) 
		{   
			std::swap( arrRefKey[low], arrRefKey[high] ); 
			low++;
			high--;
		}  
	}  

	// recursion  
	if (low < endIndex)  
		QuickSortByReference(arrObject, arrRefKey, low, endIndex);  

	if (high > startIndex)  
		QuickSortByReference(arrObject, arrRefKey, startIndex, high);  
} 

/// <summary>
/// Quicks the sort classic.
/// </summary>
/// <param name="runs">The runs queue.</param>
/// <param name="key">The key.</param>
/// <param name="left">The left.</param>
/// <param name="right">The right.</param>
VOID QuickSort::QuickSortClassic(FANS *runs, INT64 *key, INT left, INT right)  
{  
	int i=left;
	int j=right;  

	__int64 pivot = key[(i+j)/2];  

	// partition  
	while (i <= j) 
	{  
		while (key[i] < pivot)   
			i++;   

		while (key[j] > pivot)   
			j--;   

		if (i <= j) 
		{  
			std::swap( key[i], key[j] ); 
			std::swap( runs[i], runs[j] );
			i++;  
			j--;  
		}  
	}  

	// recursion  
	if (left < j)  
		QuickSortClassic( runs, key, left, j );  

	if (i < right)  
		QuickSortClassic( runs, key,  i, right );  
}  