// 
// Name: GHJ.cpp : implementation file  
// Description: Implementation of Grace Hash Join 
//

#pragma once

#include "DataTypes.h"  
#include "PageHelpers.h"

class GHJ
{
public:
	GHJ(const GHJ_PARAMS vParams);
	~GHJ();
	RC Execute();
protected:
	VOID HashBuild(Buffer *inBuffer, HashTable *hashTable);
	VOID HashProbe(PagePtr *probePage, HashTable *hashTable, Buffer *inBuffer);
	RC PartitionTable(const LPWSTR tableName, const DWORD keyPos, Buffer *inBuffer, Buffer *bucketBuffer, HANDLE *&hFile);
	VOID ProcessPage(const INT pageIndex, Buffer *inBuffer, Buffer *bucketBuffer, HANDLE *&hFile, const DWORD keyPos); 
	RC HashJoin(BufferPool *bufferPool);
	RC Initialize();
	VOID UpdateBucket(const DWORD tupleIndex,const PagePtr pagePtr, PagePtr *bucketPage, const DWORD hashKey);
	VOID RefreshBucket(const DWORD hashKey, PagePtr* bucketPage, Buffer *bucketBuffer);
	DWORD GetKey(CHAR * tuple, DWORD keyPos);
	RC SentOutput(RECORD * tuple);

	RC InitBufferPool(BufferPool *poolPtr, const DWORD size)
	{
		poolPtr->size = size;
		poolPtr->data = new CHAR[poolPtr->size];
		poolPtr->currentSize = 0;

		if(NULL==poolPtr->data)
		{  
			return ERR_CANNOT_INITIAL_BUFFER_POOL;
		}

		return SUCCESS;
	}

	VOID ResetBufferPool(BufferPool *poolPtr)
	{
		poolPtr->currentSize = 0;
	}

	RC ReleaseBufferPool(BufferPool *poolPtr)
	{
		delete poolPtr->data;
		return SUCCESS;
	}

	RC InitRunPage(PagePtr *pagePtr, Buffer *pageBuffer)
	{
		pageBuffer->currentPageIndex = 0; 

		pagePtr->offset = 8;
		pagePtr->freeSpace = SSD_PAGE_SIZE-8;   
		pagePtr->page = pageBuffer->data + (pageBuffer->currentPageIndex * SSD_PAGE_SIZE); 
		pagePtr->pageHeader = (PageHeader *) pagePtr->page; 
		pagePtr->tuple = pagePtr->page + sizeof(PageHeader);
		pagePtr->pageSlot=(PageSlot *)(pagePtr->page + SSD_PAGE_SIZE);  
		pagePtr->pageHeader->totalTuple = 0;
		pagePtr->pageHeader->slotLocation = SSD_PAGE_SIZE;
		return SUCCESS;
	}


	RC AddTupleToPage(RECORD *tuple, PagePtr *pagePtr,Buffer *pageBuffer)
	{
		int tupleLength = tuple->length;
		//strncpy(m_OutPage.tuple+pagePtr->offset, tupleData, tupleLength);

		for (int i=0; i<tupleLength; i++)
		{
			*(pagePtr->tuple) = tuple->data[i];
			*pagePtr->tuple++; 
		} 

		//装配
		pagePtr->pageSlot--;
		pagePtr->pageSlot->tupleSize = tupleLength;
		pagePtr->pageSlot->tupleOffset = pagePtr->offset;
		pagePtr->offset+=tupleLength;
		pagePtr->freeSpace = pagePtr->freeSpace - tupleLength - sizeof(PageSlot); 
		pagePtr->pageHeader->totalTuple++;
		pagePtr->pageHeader->slotLocation-=sizeof(PageSlot);	

		pageBuffer->tupleCount++;
		pageBuffer->freeLocation+=tupleLength + sizeof(PageSlot);  
		pageBuffer->currentSize+=tupleLength + sizeof(PageSlot);   

		return SUCCESS;
	}

	RC AddPageToBuffer(PagePtr *pagePtr, Buffer *bufferPtr)
	{
		//strncpy(bufferPtr->data + (bufferPtr->currentPageIndex * SSD_PAGE_SIZE), pagePtr->page, SSD_PAGE_SIZE);
		for (int i=0; i < SSD_PAGE_SIZE; i++)
		{
			bufferPtr->data[bufferPtr->currentSize] = pagePtr->page[i];
			bufferPtr->currentSize++; 
		}   

		bufferPtr->currentPageIndex++; 
		bufferPtr->freeLocation+=SSD_PAGE_SIZE;  
		bufferPtr->pageCount++;   
		return SUCCESS;
	}

	RC InitBuffer(Buffer *bufferPtr, const DWORD bufferSize, BufferPool *poolPtr)
	{ 
		if(NULL==poolPtr->data)
		{
			return ERR_NOT_INIT_MEMORY;
		}

		bufferPtr->startLocation = poolPtr->currentSize;
		bufferPtr->freeLocation = bufferPtr->startLocation;
		bufferPtr->data = poolPtr->data + bufferPtr->startLocation;
		bufferPtr->size = bufferSize;
		bufferPtr->currentSize = 0;  
		bufferPtr->currentPageIndex = 0;
		bufferPtr->currentTupleIndex = 0;
		bufferPtr->pageCount = 0;
		bufferPtr->tupleCount = 0; 
		bufferPtr->isSort = FALSE;
		bufferPtr->isFullMaxValue = FALSE; 

		poolPtr->currentSize+=bufferSize;

		if(poolPtr->currentSize > poolPtr->size)
		{
			return ERR_NOT_ENOUGH_MEMORY;
		}

		return SUCCESS;
	}

	RC ResetBuffer(Buffer *bufferPtr)
	{
		bufferPtr->freeLocation = bufferPtr->startLocation;
		bufferPtr->currentSize = 0;
		bufferPtr->currentTupleIndex = 0;
		bufferPtr->currentPageIndex = 0;
		bufferPtr->pageCount = 0;
		bufferPtr->tupleCount = 0; 

		return SUCCESS;
	}


	RC ResetPage(PagePtr *pagePtr, Buffer *pageBuffer)
	{
		pageBuffer->currentPageIndex = 0;

		pageBuffer->freeLocation = pageBuffer->startLocation;
		pageBuffer->currentSize = 0; 

		pagePtr->page = pageBuffer->data + (pageBuffer->currentPageIndex * SSD_PAGE_SIZE); 
		pagePtr->pageHeader = (PageHeader *)pagePtr->page; 
		pagePtr->tuple = pagePtr->page + sizeof(PageHeader);
		pagePtr->pageSlot = (PageSlot *)(pagePtr->page + SSD_PAGE_SIZE);  
		pagePtr->pageHeader->totalTuple = 0;
		pagePtr->pageHeader->slotLocation = SSD_PAGE_SIZE; 
		pagePtr->offset = 8;
		pagePtr->freeSpace = SSD_PAGE_SIZE-8; 

		return SUCCESS;
	}

protected: 
	const GHJ_PARAMS m_Params; 

	// Unit Page
	DOUBLE m_AvailableMemorySize;

	DOUBLE m_BucketSize;
	DOUBLE m_ReadBufferSize;
	DOUBLE anchorBucketSize; 
	DOUBLE m_HashTableSize;
	DWORD  m_PartitionNum;
	DWORD  m_JoinCount;

	UINT   m_R_FileSize;

	// Buffer
	BufferPool m_Pool;
	Buffer m_InBuffer;
	Buffer *m_BucketBuffer;
	PagePtr *m_BucketPage; //用于操作各个bucket pages 


	HashTable m_HashTable;//小表分区时建立的散列表
	Buffer m_ProbeBuffer;
	HANDLE *m_FileHandle;

	PagePtr m_OutPage;//输出操作页
	Buffer m_OutPageBuffer;
	HANDLE m_hOutFile;
	Buffer m_OutBuffer;

	RECORD *m_R_Tuple;
	RECORD *m_S_Tuple;
	RECORD *m_JoinTuple;
};

