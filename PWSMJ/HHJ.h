// 
// Name: HHJ.cpp : implementation file  
// Description: Implementation of Hybrid Hash Join 
//				The hybrid hash join algorithm is a refinement of the grace hash join 
//				which takes advantage of more available memory. During the partitioning phase, 
//				the hybrid hash join uses the available memory for two purposes:
//				    1. To hold the current output buffer page for each of the k partitions
//					2. To hold an entire partition in-memory, known as "partition 0"
//				Because partition 0 is never written to or read from disk, the hybrid hash join 
//				typically performs fewer I/O operations than the grace hash join. 
//				Note that this algorithm is memory-sensitive, because there are two competing demands
//				for memory (the hash table for partition 0, and the output buffers for the remaining partitions). 
//				Choosing too large a hash table might cause the algorithm to recurse 
//				because one of the non-zero partitions is too large to fit into memory.
//
#pragma once

#include "DataTypes.h"  
#include "PageHelpers.h" 
class HHJ
{
public:
	HHJ(const HHJ_PARAMS vParams);
	~HHJ();
	RC Execute();
protected:
	VOID HashBuild(Buffer *inBuffer, HashTable *hashTable);
	VOID HashProbe(PagePtr *probePage, HashTable *hashTable, Buffer *inBuffer);
	RC R_PartitionTable(Buffer *Inbuffer, Buffer *bucketBuffer, HANDLE *&hFile);
	VOID R_ProcessPage(INT pageIndex, Buffer *InBuffer, Buffer *bucketBuffer, HANDLE *&hFile);
	RC S_PartitionTable(Buffer *inbuffer, Buffer *bucketBuffer, HANDLE *&hFile);
	VOID S_ProcessPage(INT pageIndex, Buffer *inBuffer, Buffer *bucketBuffer, HANDLE *&hFile);
	RC Join(BufferPool *bufpool,Buffer *ibuffer,Buffer *probebuffer);
	RC Initialize();
	VOID UpdateBucket(DWORD tupleIndex, PagePtr pagePtr, PagePtr* bucketPage, DWORD hashKey);
	VOID RefreshBucket(DWORD hashKey, PagePtr* bucketPage, Buffer *bucketBuffer);
	DWORD GetKey(CHAR * tuple, DWORD keyPos);
protected: 
	const HHJ_PARAMS m_Params; 
	PageHelpers *utl;
	// Unit Page
	DOUBLE m_AvailableMemorySize;
	
	DOUBLE m_BucketSize;
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

	Buffer m_PartitionFirstBuffer; //留在内存的分区
	PagePtr  m_PartitionFirstPage;//留在内存的分区当前被处理的页

	PagePtr *outPage;//输出操作页

	HANDLE *m_FileHandle;

};

