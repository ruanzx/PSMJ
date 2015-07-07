// 
// Name: PEMS.cpp : implementation file 
// Author: hieunt
// Description: Parallel external merge-sort implementation
//              In-memory sorting use quick sort to create mini runs in memory, then 
//              use replacement selection to merge these in memory minirun,
//              run size is equal NumberOfReadBuffer * ReadBufferSize
//              Merge Phase is merge all Runs created, 
//              if not enough memory, multiple merge step is needed
//              I/O and CPU computation all are overlapped
//

#pragma once
 
#include "DataTypes.h"
#include "PageHelpers2.h"
#include "DoubleBuffer.h" 
#include "LoserTree.h"  
#include "QuickSort.h" 
#include "Loggers.h"

class PEMS
{
protected:
	// Working struct
	struct PartitionThreadParams
	{
		class PEMS* _this;

		DWORD threadID; 
		DWORD keyPosition; // Attribute key of this table 
		DWORD inputBufferCount; // depend on memory size
		DWORD inputBufferIndex; // Index for input buffer 
		HANDLE hInputFile; // Handle for reading source file 

		FANS *tempFanIn;
		HANDLE hFanOut; 

		OVERLAPPEDEX overlapRead; // Overlap structure for reading source table file  
		OVERLAPPEDEX overlapWrite; // Overlap structure for write run file  
		DoubleBuffer **dbcRead;  // Input buffer 
		DoubleBuffer *dbcWrite;    // Output buffer

		PagePtr *runPage; // Pointer to run page
		Buffer   runPageBuffer; // Buffer for run page   
		Buffer  *memoryMergeBuffer; // Buffer for in memory merge

		QuickSort *quickSort;
		Buffer quickSortPageBuffer; // Buffer for Quicksort page
		Buffer quickSortDataBuffer; // Buffer for Quicksort data

		vector<FANS*> fanIns;  // if parallel merge is TRUE, then this variable use for save fanIn path 
	};

	struct MergeThreadParams
	{ 
		DWORD threadID; 
		HANDLE *hFanIn; // Array handle k input run files 
		HANDLE hFanOut;
		OVERLAPPEDEX *overlapRead; // Overlap structure for k input run files 
		OVERLAPPEDEX overlapWrite; // Over lap structure for output run file  
		DoubleBuffer **dbcRead; // Buffer for merge k run files
		DoubleBuffer *dbcWrite; // Buffer for write output run file to disk 

		PagePtr *runPage;  // Pointer to run page
		Buffer   runPageBuffer; // Buffer of run page 
		Buffer  *mergeBuffer; // Buffer for in memory merge 

		DWORD keyPosition;  // Attribute key of this table 
		DWORD maxFanIn;  // max fan in can hold  
	};

	struct PemsReport
	{
		DOUBLE CpuTime[2];
		UINT64 SourceTableSize;
		UINT64 TotalTime;
		UINT64 PartitionTime;
		UINT64 MergeTime;
		DWORD  PartitionNum;
		DWORD  RunSize;
		DWORD  MergePass;
		DWORD BufferPoolSize;
		DWORD SortReadBufferSize;
		DWORD SortWriteBufferSize;
		DWORD MergeReadBufferSize;
		DWORD MergeWriteBufferSize;
	};
protected:  // partition phase variables 
	DWORD m_InputBufferNum; 
	DOUBLE m_MemoryEachThreadSize; 
	DWORD m_PartitionThreadNum;
	HANDLE *m_hPartitionThread;
	DWORD WINAPI PartitionPhase(LPVOID lpParam);
	static DWORD WINAPI PartitionPhaseEx(LPVOID lpParam);   
	PartitionThreadParams *m_PartitionParams;
protected: // merge phase variables
	DWORD m_MaxFanIn;
	std::queue<FANS*> m_FanIns;    
	std::queue<FANS*> m_FanInWillDelete;   
	MergeThreadParams *m_MergeParams; 
	FANS    *m_FanOut; 
	LONG     m_FanOutIndex;  // Index for naming run file
protected:
	const PEMS_PARAMS m_Params; // input params 
	PemsReport rp;
	HANDLE	m_hTableFile;  
	MUTEX m_Mutex; 
	BufferPool bufferPool;
	LONG  m_FanInIndex ;  // Index for naming run file
	PageHelpers2 *utl; 
public:
	PEMS(const PEMS_PARAMS vParams);
	~PEMS(); 
	RC Execute();

protected:
	RC PartitionPhase_Execute(); 
	RC PartitionPhase_CheckEnoughMemory(); 
	RC PartitionPhase_Initialize();
	RC PartitionPhase_Read(LPVOID lpParam, DWORD bufferIndex);
	RC PartitionPhase_Write(LPVOID lpParam);
	RC PartitionPhase_Merge(LPVOID lpParam);
	RC PartitionPhase_Sort(LPVOID lpParam, DWORD bufferIndex); 
	RC PartitionPhase_GetNextTuple(LPVOID lpParam, RECORD *&record, INT index);
	RC PartitionPhase_GetFanInPath(LPWSTR &fanInPath, INT threadID);  
	RC PartitionPhase_CreateNewRun(LPVOID lpParam);
	RC PartitionPhase_TerminateRun(LPVOID lpParam, DWORD tupleCount);

	RC MergePhase_Execute();
	RC MergePhase_CheckEnoughMemory(); 
	RC MergePhase_Initialize();  
	RC MergePhase_Read(LPVOID lpParam, DWORD f);  
	RC MergePhase_Write(LPVOID lpParam); 
	RC MergePhase_Merge(LPVOID lpParam); 
	RC MergePhase_GetFanOutPath(LPWSTR &fanOutName, INT threadID);  
	RC MergePhase_GetNextTuple(LPVOID lpParam, RECORD *&record, INT index); 

	UINT64 GetFanSize(HANDLE hFile);
};

