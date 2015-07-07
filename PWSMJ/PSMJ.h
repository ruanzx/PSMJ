
// 
// Name: PSMJ.cpp : implementation file 
// Author: hieunt
// Description:  Parallel Sort merge join
//				 Partition Source table File to Multiple part
//				 Join these run part in parallel
//
#pragma once
  
#include "DataTypes.h"
#include "CriticalSection.h"
#include "PageHelpers2.h" 
#include "DoubleBuffer.h" 
#include "LoserTree.h"  
#include "QuickSort.h" 
#include "Loggers.h"


// Plan 1: naive
//////////////////////////////////////////////////////////////////////////
// Use multiple thread to partition R, S to multiple sorted run, 
// then each with each Ri we scan all Si in parallel to take out join result
// -> require many IO if number of sorted runs is large
//
//-------------------------------------------------------------------------------------------------

// Plan 2:
//////////////////////////////////////////////////////////////////////////
// Use multiple thread to partition R, S to multiple sorted run
// then with each Ri we scan all shared buffer of Si 
// --> also need to scan all Si & 
// --> need one barrier to ensure each Si is complete read into buffer before do join operate
//
//-------------------------------------------------------------------------------------------------

// Plan 3: < Sort-merge join with multi-way merging >
//////////////////////////////////////////////////////////////////////////
// Use multiple thread to partition R, S to multiple sorted run
// Use multi-way merge to merge R, S to single-sorted run
// Logical partition to allocate range to each thread to join R, S
//
//-------------------------------------------------------------------------------------------------
 
class PSMJ
{

protected: 
	// Defines synchronization info structure. All threads will
	// use the same instance of this struct to implement randezvous/
	// barrier synchronization pattern.
	typedef struct Barrier
	{
		Barrier(LONG threadsCount) : ThreadWaiting(threadsCount), ThreadsCount(threadsCount), hSemaphore(CreateSemaphore(0, 0, threadsCount, 0)) {};
		~Barrier() { CloseHandle(this->hSemaphore); }
		CriticalSection cs; 
		LONG ThreadWaiting; // how many threads still have to complete their iteration
		LONG ThreadsCount;
		const HANDLE hSemaphore; 
	}Barrier;

	VOID WaitOtherThreads(Barrier *barrierPtr, BOOL bReset);

#pragma region Power Capping 
protected:
	// Power capping variables
	struct PowerCapParams
	{
		class PSMJ* _this;
	};

	volatile BOOL bQuitCapping; 
	DWORD WINAPI PowerCap(LPVOID lpParam);
	static DWORD WINAPI PowerCapEx(LPVOID lpParam);  
	PowerCapParams cappingParams;
	HANDLE hPowerCapThread;
	FILE *fpCap;   


#pragma endregion


#pragma region Partition Phase

protected:
	// Partition phase variables
	struct PartitionThreadParams
	{
		class PSMJ* _this;

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

	struct FinalMergeParams
	{
		class PSMJ* _this;

		DWORD keyPosition;		// Attribute key of this table    

		HANDLE *hFanIn;
		HANDLE	hFanOut; 

		OVERLAPPEDEX *overlapRead; // Overlap structure for reading source table file  
		OVERLAPPEDEX overlapWrite; // Overlap structure for write run file  
		DoubleBuffer **dbcRead;  // Input buffer 
		DoubleBuffer *dbcWrite;    // Output buffer

		PagePtr *runPage; // Pointer to run page
		Buffer   runPageBuffer; // Buffer for run page   
		Buffer  *mergePageBuffer; // Buffer for in memory merge 
	};

protected:
	PartitionThreadParams *m_PartitionParams;
	FinalMergeParams	  *m_FinalMergeParams;

	static DWORD WINAPI PartitionPhase_Ex(LPVOID lpParam);
	DWORD WINAPI PartitionPhase_Func(LPVOID lpParam); 

	RC PartitionPhase_Execute(BOOL isR);  
	RC PartitionPhase_CheckEnoughMemory(); 
	RC PartitionPhase_Initialize(BOOL isR);
	RC PartitionPhase_PartitionTableR();
	RC PartitionPhase_PartitionTableS(); 
	RC PartitionPhase_Read(LPVOID lpParam, DWORD bufferIndex);
	RC PartitionPhase_Write(LPVOID lpParam);
	RC PartitionPhase_Merge(LPVOID lpParam); //merge read buffer in memory
	RC PartitionPhase_Sort(LPVOID lpParam, DWORD bufferIndex); 
	RC PartitionPhase_GetNextTuple(LPVOID lpParam, RECORD *&record, INT index);
	RC PartitionPhase_GetFanInPath(LPWSTR &fanInPath, INT threadID);  
	RC PartitionPhase_CreateNewRun(LPVOID lpParam);
	RC PartitionPhase_TerminateRun(LPVOID lpParam, DWORD tupleCount); 

	RC PartitionPhase_FinalMerge(std::queue<FANS*> &runQueue, const DWORD keyPos); //merge all sorted run on disk to single run
	RC PartitionPhase_FinalMerge_Read(DWORD runIdx); 
	RC PartitionPhase_FinalMerge_GetNextRecord(RECORD *&recordPtr, INT runIdx);
#pragma endregion
	 
#pragma region Join Phase

#pragma region "Join Plan 1"

	// struct for join thread params without barrier
	typedef struct JoinPlan1ThreadParams
	{
		PSMJ*	_this; 
		queue<FANS*> R_FanIns; // list fanIn of R each thread
		DWORD   R_FanInCount;
		DWORD   S_FanInCount; 
		DWORD   R_Key;
		DWORD   S_Key; 

		Buffer  readBufferR; // read buffer for R relation
		PagePtr *pageR;
		Buffer  pageBufferR;

		Buffer  readBufferS; // read buffer for S relation
		PagePtr *pageS;
		Buffer  pageBufferS;

		//////////////////////////////////////////////////////////////////////////
		RECORD * tupleR;
		RECORD * tupleS; 
		RECORD * tupleRS; 

		//////////////////////////////////////////////////////////////////////////
		// Write buffer
		LPWSTR joinFilePath; 
		HANDLE hWrite;
		//Buffer writeBuffer;

		OVERLAPPEDEX overlapWrite; // Overlap structure for write run file  
		DoubleBuffer *dbcWrite;    // Output buffer

		PagePtr *pageWrite;
		Buffer pageWriteBuffer;  
	}JoinPlan1ThreadParams;

protected: 
	static DWORD WINAPI JoinPhase_Plan1_Ex(LPVOID lpParam);
	DWORD WINAPI JoinPhase_Plan1_Func(LPVOID lpParam);  

	RC JoinPhase_Plan1_Initialize(); 
	RC JoinPhase_Plan1_Join(LPVOID lpParam);  
	RC JoinPhase_Plan1_GetNextTuple(Buffer *bufferPtr, Buffer &pageBuffer, RECORD *recordPtr, DWORD keyPos);
	RC JoinPhase_Plan1_SentOutput(LPVOID lpParam, RECORD *recordPtr);
#pragma endregion "Join Plan 1"

#pragma region "Join Plan 2"
	// Struct for shared S run
	typedef struct ShareDataS
	{
		HANDLE   *hFile; 
		DWORD  *lowestKey;			// lowest key value in run
		DWORD  *highestKey;			// highest key value in run
		DWORD     totalCount;		    //total run count
	} ShareDataS;

	// struct for join thread params with barrier implement
	typedef struct JoinPlan2ThreadParams
	{
		PSMJ*	_this; 
		queue<FANS*> R_FanIns; // list fanIn of R each thread
		DWORD   R_FanInCount;
		DWORD   S_FanInCount;
		DWORD   R_AvgFanInCount; // use for barrier avoid deadlock 
		DWORD   S_AvgFanInCount; // use for barrier avoid deadlock 
		DWORD   R_Key;
		DWORD   S_Key; 

		Buffer  readBufferR; // read buffer for R relation
		PagePtr *pageR;
		Buffer  pageBufferR;

		Buffer  readBufferS; // read buffer for S relation
		PagePtr *pageS;
		Buffer  pageBufferS;
		//////////////////////////////////////////////////////////////////////////
		RECORD * tupleR;
		RECORD * tupleS; 
		RECORD * tupleRS; 

		//////////////////////////////////////////////////////////////////////////
		// Write buffer
		LPWSTR joinFilePath; 
		HANDLE hWrite;
		//Buffer writeBuffer;

		OVERLAPPEDEX overlapWrite; // Overlap structure for write run file  
		DoubleBuffer *dbcWrite;    // Output buffer

		PagePtr *pageWrite;
		Buffer pageWriteBuffer; 

	}JoinPlan2ThreadParams;
protected: 
	ShareDataS		shareS; 

	Barrier *barrier1;  
	Barrier *barrier2; 
	Barrier *barrier3; 
	Barrier *barrier4;

	static DWORD WINAPI JoinPhase_Plan2_Ex(LPVOID lpParam); 
	DWORD WINAPI JoinPhase_Plan2_Func(LPVOID lpParam); // Parallel version with barrier implemention

	RC JoinPhase_Plan2_Initialize();  // for barrier implement
	RC JoinPhase_Plan2_Join(LPVOID lpParam, Buffer r_BufferData, Buffer s_BufferData, DWORD r_KeyPos, DWORD s_KeyPos);
	RC JoinPhase_Plan2_GetNextTuple(const Buffer *bufferPtr, Buffer &pageBuffer, RECORD *&recordPtr, DWORD &currentPageIndex, DWORD &currentTupleIndex, const DWORD keyPos);
	RC JoinPhase_Plan2_SentOutput(LPVOID lpParam, RECORD *recordPtr);
	VOID JoinPhase_Plan2_GetSrunPath(FANS *&sFanIn, BOOL &status);
#pragma endregion "Join Plan 2"

#pragma region "Join Plan 3"

	// Decription Partiton both R, S, then use multi-way merge to merge R, S to single run
	// Use merge join to merge R, S in parallel
	// Logical partition R, S in multiple part when merge finish 
	// then invoke multipthread to join

	// Range partition on single sorted-run

	typedef struct RangePartition
	{
		DWORD			Idx;
		LARGE_INTEGER	fileOffsetStart; // start address to read
		LARGE_INTEGER	fileOffsetEnd;  // end address to read
		DWORD			lowestKey;  // the lowest key in buffer
		DWORD		highestKey; // the highest key in buffer
	}RangePartition;

	typedef struct JoinPlan3ThreadParams
	{
		PSMJ*	_this;  

		RangePartition *PartitionR;
		RangePartition *PartitionS;

		DWORD   R_FanInCount;
		DWORD   S_FanInCount;

		DWORD   R_Key;
		DWORD   S_Key; 

		DWORD	RunSize; 

		DWORD	ReadSkipCountS; // number of fanIn S have been skipt
		DWORD	ReadCountS; // number of fanIn S have been read

		Buffer  readBufferR; // read buffer for R relation
		PagePtr *pageR;
		Buffer	 pageBufferR;

		Buffer   readBufferS; // read buffer for S relation
		PagePtr *pageS;
		Buffer  pageBufferS;
		//////////////////////////////////////////////////////////////////////////
		RECORD * tupleR;
		RECORD * tupleS; 
		RECORD * tupleRS; 

		//////////////////////////////////////////////////////////////////////////
		// Write buffer
		LPWSTR joinFilePath; 
		HANDLE hWrite;  
		OVERLAPPEDEX	 overlapWrite; // Overlap structure for write run file  
		DoubleBuffer	 *dbcWrite;    // Output buffer

		PagePtr			 *pageWrite;
		Buffer			 pageWriteBuffer; 

	}JoinPlan3ThreadParams;

protected:  

	static DWORD WINAPI JoinPhase_Plan3_Ex(LPVOID lpParam);
	DWORD WINAPI JoinPhase_Plan3_Func(LPVOID lpParam); 

	RC JoinPhase_Plan3_Initialize();  // for barrier implement
	RC JoinPhase_Plan3_Join(LPVOID lpParam);
	RC JoinPhase_Plan3_GetNextTuple(Buffer *bufferPtr, Buffer &pageBuffer, RECORD *recordPtr, DWORD keyPos);
	RC JoinPhase_Plan3_SentOutput(LPVOID lpParam, RECORD *recordPtr); 


#pragma endregion "Join Plan 3"


	// Join variables
protected:
	JoinPlan1ThreadParams *m_JoinPlan1ThreadParams;  
	JoinPlan2ThreadParams *m_JoinPlan2ThreadParams; // join thread params in barrier version  
	JoinPlan3ThreadParams *m_JoinPlan3ThreadParams;

	vector<RangePartition> m_RangePartition;
	vector<RangePartition> m_RRangePartition;
	vector<RangePartition> m_SRangePartition;
	UINT64 m_TotalJoinCount; 
	// Share read handle
	HANDLE hR;
	HANDLE hS;

	RC JoinPhase_CheckEnoughMemory(); 
	RC JoinPhase_Execute();  

	RC   JoinPhase_GetFanOutPath(LPWSTR &fanPath, INT threadID);
	VOID JoinPhase_MakeJoinRecord(RECORD *&joinRecord, RECORD *leftRecord, RECORD *rightRecord); 
	DWORD JoinPhase_GetTotalJoinCount();
#pragma endregion Join Phase


#pragma region Report

	struct PsmjReport
	{
		DOUBLE TotalCpuTime[2];
		DOUBLE PartitionCpuTimeR[2];
		DOUBLE PartitionCpuTimeS[2];

		DOUBLE MergeCpuTimeR[2];
		DOUBLE MergeCpuTimeS[2];

		DOUBLE JoinCpuTime[2];

		UINT64 SourceTableSizeR;
		UINT64 SourceTableSizeS; 

		UINT64 PartitionExecTimeR;
		UINT64 PartitionExecTimeS; 

		UINT64 MergeExecTimeR;
		UINT64 MergeExecTimeS; 

		DWORD  PartitionNumR;
		DWORD  PartitionNumS;

		DWORD  PartitionThreadNum;
		DWORD  JoinThreadNum;

		DWORD  RunSize;
		UINT64 JoinExecTime; 
		UINT64 TotalExecTime;
		DWORD BufferPoolSize;

		DWORD SortReadBufferSize;
		DWORD SortWriteBufferSize;

		DWORD MergeReadBufferSizeR;
		DWORD MergeWriteBufferSizeR;

		DWORD MergeReadBufferSizeS;
		DWORD MergeWriteBufferSizeS;

		DWORD JoinReadBufferSize;
		DWORD JoinWriteBufferSize;
	};

protected:
	PsmjReport rp;
	VOID WriteReport();

#pragma endregion Report



public:
	PSMJ(const PSMJ_PARAMS vParams);
	~PSMJ(); 

	RC Execute(); 

protected:
	PSMJ_PARAMS m_PsmjParams; // input params
	PEMS_PARAMS m_PemsParams; // input partition params
	BOOL isR;
	PageHelpers2 *utl;
	BufferPool bufferPool;

	DWORD m_InputBufferNum; 
	DWORD m_MemoryEachThreadSize; 
	DWORD m_WorkerThreadNum;
	HANDLE *m_hWorkerThread; 

	HANDLE	m_hSourceTable;    

	LONG  m_FanInIdx ;  // Index for naming run file
	std::queue<FANS*> m_FanIns;    
	std::queue<FANS*> m_FanInWillDelete;  
	DWORD m_RunSize;
	DWORD R_FanInCount;
	DWORD S_FanInCount; 

	std::queue<FANS*> m_R;  
	std::queue<FANS*> m_S;   
	std::queue<FANS*> m_runtimeS;  // in barrier version, refresh after each R iteration

	CriticalSection m_CS; 


	//////////////////////////////////////////////////////////////////////////

	UINT64 GetFanSize(HANDLE hFile);


};

