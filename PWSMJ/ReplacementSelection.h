#pragma once

// External Merge Sort using Replacement Selection method
// Advantage: Best situation Run size may be large as 2*HeapSize
//			  If input table is already sort, only one run is created	
// TODO:      Write operation is not overlapped with CPU computation

#include "DataTypes.h"
#include "PageHelpers2.h"
#include "DoubleBuffer.h" 
#include "LoserTree.h"    

class ReplacementSelection
{ 

protected:
	typedef struct ExsRpPartitionPhase
	{
		DoubleBuffer *dbcRead;
		//DoubleBuffer *dbcWrite;
		Buffer writeBuffer;
		OVERLAPPEDEX *overlapRead;  
		//OVERLAPPEDEX *overlapWrite;  

		DWORD        currentMark;  /* Determine tuple belongs to current run or next run */
		PagePtr     *readPage;  
		Buffer       readPageBuffer;  
		PagePtr     *writePage;  
		Buffer       writePageBuffer;  
	} ExsRpPartitionPhase;

	struct ExsRpMergePhase
	{  
		HANDLE *hFanIn; // Array handle k input run files  
		OVERLAPPEDEX *overlapRead; // Overlap structure for k input run files 
		OVERLAPPEDEX overlapWrite; // Over lap structure for output run file  
		DoubleBuffer **dbcRead; // Buffer for merge k run files
		DoubleBuffer *dbcWrite; // Buffer for write output run file to disk 

		PagePtr *runPage;  // Pointer to run page
		Buffer   runPageBuffer; // Buffer of run page 
		Buffer  *mergeBuffer; // Buffer for in memory merge 

		DWORD maxFanIn;  // max fan in can hold  
	};

	const RP_PARAMS m_Params; // input params  
	LONG m_FanIndex; 
	std::queue<FANS*> m_FanIns;

	PageHelpers2 *utl; /* Pointer to utilites */
	LPWSTR m_FanPath; 

	Buffer m_HeapBuffer;

	HANDLE hInputFile; /* Handle for source table */
	HANDLE hOutputFile; /* Handle for write fanIn from heapsort output */
	ExsRpPartitionPhase partitionParams;
	ExsRpMergePhase mergeParams;
	std::queue<FANS*> m_FanInWillDelete;    
	DWORD m_MergePass;
	DWORD m_HeapSize;
	BufferPool bufferPool;  

protected:
	RC RP_Initialize(); 
	DWORD RP_GetHeapSize() const;
	//////////////////////////////////////////////////////////////////////////
	RC PartitionPhase(); 
	RC PartitionPhase_Read(); 
	RC PartitionPhase_Write(); 
	RC PartitionPhase_GetNextRecord(RECORD *&record); 
	RC PartitionPhase_CreateNewRun(); 
	RC PartitionPhase_SentToOutput(RECORD *recordPtr, DWORD64 &pageCount); 
	RC PartitionPhase_SaveFanIn(DWORD64 lowestKey, DWORD64 highestKey, DWORD64 tupleCount, DWORD64 pageCount);
	//////////////////////////////////////////////////////////////////////////
	RC MergePhase();
	RC MergePhase_Read(DWORD index);
	RC MergePhase_Write();
	RC MergePhase_GetNextRecord(RECORD *&record, INT index);

	//////////////////////////////////////////////////////////////////////////
	RC GetFanPath();  
	UINT64 GetFanSize(HANDLE hFile);  
	RC CopyRecord(RECORD *des, RECORD *&src);
	//////////////////////////////////////////////////////////////////////////
	/* Replacement selection utilites function */ 
	INT TreeParent(INT i);
	INT TreeLeft(INT i);
	INT TreeRight (INT i);
	RC MinHeapify(RECORD **rec, INT i, INT heapSize);
	RC MaxHeapify(RECORD **rec, INT i, INT heapSize);
	RC BuildMinHeap(RECORD **rec, INT heapSize);
	RC BuildMaxHeap(RECORD **rec, INT heapSize);  
public:
	ReplacementSelection(const RP_PARAMS vParams); 
	~ReplacementSelection(); 
	RC RP_CheckEnoughMemory(); 
	RC RP_Execute(); 
};
