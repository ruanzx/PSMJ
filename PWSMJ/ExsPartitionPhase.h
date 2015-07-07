// 
// Name: ExsPartitionPhase.h : header file 
// Author: hieunt
// Description: Implementation of sort phase in parallel external merge sort
//				Create mini run in memory, after memory is filled full of data		     
//				use replacement selection to merge these minirun

#pragma once 

#include "DataTypes.h"
#include "PageHelpers2.h"
#include "DoubleBuffer.h" 
#include "LoserTree.h"  
#include "QuickSort.h" 
  
class ExsPartitionPhase
{ 
	/* Private variable */
protected: 
	struct MonitorParams
	{
		class ExsPartitionPhase* ClassPointer; 
	};

	struct ExsPartitionThreadParams
	{
		class ExsPartitionPhase* ClassPointer;

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

	typedef struct ExsSyncThreadParams
	{
		DWORD dwThreadNum; // total thread
		HANDLE *hReady; // thread [x] is ready to write
		HANDLE hWaitAllOk; // event allow all thread write at same time
		volatile BOOL *bIsDone; // thread [x] is complete its work
		volatile BOOL bQuit; // exit monitor thread
	} ExsSyncThreadParams;


protected:
	const PEMS_PARAMS m_Params; // input params
	DWORD  m_PartitionThreadNum;

	// Sort woker thread
	DWORD WINAPI Run(LPVOID p);
	static DWORD WINAPI RunEx(LPVOID lpParam);  
	HANDLE *m_hPartitionThread;
	ExsPartitionThreadParams *m_PartitionParams;

	// Monitor thread
	DWORD WINAPI Monitor(LPVOID p);
	static DWORD WINAPI MonitorEx(LPVOID lpParam); 
	HANDLE	m_hMonitorThread; 
	MonitorParams *m_MonitorParams;

	ExsSyncThreadParams sync;

	HANDLE	m_hInputFile;  
	std::queue<FANS*> m_ReturnFanIns;   

	MUTEX m_Mutex; 

	PageHelpers2 *utl; 
	LONG m_FanInIndex ;  // Index for naming run file

	DWORD m_MemoryEachThreadSize; 
	DWORD m_InputBufferNum; 

	BufferPool bufferPool;
	/*  Protected function */
protected:
	RC PartitionPhase_Initialize();
	RC PartitionPhase_Read(LPVOID lpParam, DWORD bufferIndex);
	RC PartitionPhase_Write(LPVOID lpParam);
	RC PartitionPhase_Merge(LPVOID lpParam);
	RC PartitionPhase_Sort(LPVOID lpParam, DWORD bufferIndex); 
	RC PartitionPhase_GetNextTuple(LPVOID lpParam, RECORD *&record, INT index);
	RC PartitionPhase_GetFanInPath(LPWSTR &fanInPath, INT threadID);  
	RC PartitionPhase_CreateNewRun(LPVOID lpParam);
	RC PartitionPhase_TerminateRun(LPVOID lpParam, DWORD tupleCount);
public:
	ExsPartitionPhase(const PEMS_PARAMS vParams); 
	~ExsPartitionPhase(); 
	RC PartitionPhase_CheckEnoughMemory();

	RC PartitionPhase_Execute();  
	std::queue<FANS*> GetFanIns(){ return m_ReturnFanIns;}  
};
 