//
// Name: PSMJRP.cpp   
// Author: hieunt
// Description: Parallel sort-merge join module use replacement selection method
//

#pragma once

#include "DataTypes.h"
#include "CriticalSection.h"
#include "PageHelpers2.h"
#include "DoubleBuffer.h" 
#include "LoserTree.h"  
#include "Loggers.h"

class PSMJRP
{
	typedef struct RangePartition
	{
		DWORD			Idx;
		LARGE_INTEGER	fileOffsetStart; // start address to read
		LARGE_INTEGER	fileOffsetEnd;  // end address to read
		DWORD			lowestKey;  // the lowest key in buffer
		DWORD			highestKey; // the highest key in buffer
	}RangePartition;

	typedef struct PartitionPhaseParams
	{
		class PSMJRP* _this;
		DWORD        currentMark;  /* Determine tuple belongs to current run or next run */
		DWORD		 keyPos;

		Buffer		heapBuffer;
		DWORD		heapSize;

		OVERLAPPEDEX overlapRead;  // use for read relation larger than 4GB
		Buffer		 readBuffer;  
		PagePtr     *readPagePtr;  
		Buffer       readPageBuffer;  

		DWORD        dwBytesWritten;

		HANDLE		 hWrite;
		OVERLAPPEDEX overlapWrite; // Overlap structure for write run file
		LPWSTR		 fanPath;
		Buffer		 writeBuffer; 
		PagePtr     *writePagePtr;  
		Buffer       writePageBuffer;   

		std::queue<FANS*> fanIns;
	} PartitionPhaseParams;

	typedef struct MergePhaseParams
	{  
		class PSMJRP* _this;

		DWORD keyPos;		// Attribute key of this table    

		HANDLE *hFanIn;
		HANDLE	hFanOut;  

		OVERLAPPEDEX *overlapRead; // Overlap structure for reading source table file  
		OVERLAPPEDEX overlapWrite; // Overlap structure for write run file  
		DoubleBuffer **dbcRead;  // Input buffer 
		DoubleBuffer *dbcWrite;    // Output buffer

		PagePtr *runPage; // Pointer to run page
		Buffer   runPageBuffer; // Buffer for run page   
		Buffer  *mergePageBuffer; // Buffer for in memory merge 
	}MergePhaseParams;


	typedef struct JoinPhaseParams
	{
		PSMJRP*	_this;  

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

	}JoinPhaseParams;

	PSMJRP_PARAMS m_PsmjRpParams;
	PartitionPhaseParams *m_PartitionParams;
	MergePhaseParams	  *m_FinalMergeParams;
	JoinPhaseParams	  *m_JoinParams;

	DWORD m_WorkerThreadNum;
	HANDLE *m_hWorkerThread;  
	HANDLE	m_hSourceTable;  
	HANDLE hR;
	HANDLE hS;
	LONG m_FanIndex; 
	DWORD m_PartitionSize;
	UINT64 m_TotalJoinCount; 
	std::queue<FANS*> m_FanIns;
	CriticalSection m_CS; 
	BufferPool bufferPool; 
	PageHelpers2 *utl; /* Pointer to utilites */
	BOOL isR;
	std::queue<FANS*> m_R;  
	std::queue<FANS*> m_S; 

	vector<RangePartition> m_RangePartition;
	vector<RangePartition> m_RRangePartition;
	vector<RangePartition> m_SRangePartition;
public:
	PSMJRP(const PSMJRP_PARAMS vParams);
	~PSMJRP(void);

	RC Execute(); 
	RC SortTableR();
	RC SortTableS();
	RC JoinTable();

	static DWORD WINAPI PartitionPhase_Ex(LPVOID lpParam); 
	DWORD WINAPI PartitionPhase_Func(LPVOID lpParams);  
	 

	RC PartitionPhase_SentToOutput(LPVOID lpParams, RECORD *recordPtr, DWORD &pageCount);
	RC PartitionPhase_CreateNewRun(LPVOID lpParams);
	RC PartitionPhase_SaveFanIn(LPVOID lpParams, DWORD lowestKey, DWORD highestKey, DWORD tupleCount, DWORD pageCount);
	RC PartitionPhase_Read(LPVOID lpParams);
	RC PartitionPhase_GetNextRecord(LPVOID lpParams, RECORD *&recordPtr);

	RC MergePhase_Merge(std::queue<FANS*> &runQueue, const DWORD keyPos);
	RC MergePhase_Read(DWORD runIdx);
	RC MergePhase_GetNextRecord(RECORD *&recordPtr, INT runIdx);

	static DWORD WINAPI JoinPhase_Ex(LPVOID lpParam);
	DWORD WINAPI JoinPhase_Func(LPVOID lpParam);
	RC JoinPhase_Initialize();
	RC JoinPhase_Join(LPVOID lpParam);
	RC JoinPhase_GetNextTuple(Buffer *bufferPtr, Buffer &pageBuffer, RECORD *recordPtr, DWORD keyPos);
	VOID JoinPhase_MakeJoinRecord(RECORD *&joinRecord, RECORD *leftRecord, RECORD *rightRecord) ;
	RC JoinPhase_SentOutput(LPVOID lpParam, RECORD *recordPtr);
	RC JoinPhase_GetFanOutPath(LPWSTR &fanPath, INT threadID);

	RC  CopyRecord(RECORD *des, RECORD *&src);
	RC  GetFanPath(LPWSTR &fanPath);
	UINT64 GetFanSize(HANDLE hFile);
	INT TreeParent(INT i);
	INT TreeLeft(INT i);
	INT TreeRight (INT i);
	RC MinHeapify(RECORD **rec, INT i, INT heapSize);
	RC MaxHeapify(RECORD **rec, INT i, INT heapSize);
	RC BuildMinHeap(RECORD **rec, INT heapSize);
	RC BuildMaxHeap(RECORD **rec, INT heapSize);  



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
		DWORD HeapSizeR;
		DWORD HeapSizeS;
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
};

