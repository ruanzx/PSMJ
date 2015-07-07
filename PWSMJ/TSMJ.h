#pragma once

// Tradition Sort Merge Join,
// Partition Phase use Replacement Selection
// Input two table, if tables are not sorted, sort them
// No overlapped IO and double buffer

#include "DataTypes.h"
#include "PageHelpers2.h"
#include "DoubleBuffer.h" 
#include "LoserTree.h"  

class TSMJ
{
protected:
	typedef struct ExsRpPartitionPhase
	{
		DWORD        currentMark;  /* Determine tuple belongs to current run or next run */
		
		OVERLAPPEDEX *overlapRead;  // use for read relation larger than 4GB
		Buffer		 readBuffer;  
		PagePtr     *readPagePtr;  
		Buffer       readPageBuffer;  

		DWORD        dwBytesWritten;
		Buffer		 writeBuffer; 
		PagePtr     *writePagePtr;  
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
	typedef struct RELATION
	{ 
		HANDLE hFile; 
		DWORD keyPos;    
		DoubleBuffer *dbcRead;
		OVERLAPPEDEX overlapRead; 
		PagePtr *probePage;
		Buffer   probePageBuffer; // Buffer for run page  
	} RELATION;

	typedef struct RELATION_JOIN
	{  
		HANDLE hFile;   
		DoubleBuffer *dbcWrite;
		OVERLAPPEDEX overlapWrite;  
		PagePtr *runPage;
		Buffer runPageBuffer; // Buffer for run page  
	} RELATION_JOIN;

	struct SmjReport
	{
		DOUBLE CpuTime[2];
		UINT64 SourceTableSize_R, SourceTableSize_S; 
		UINT64 PartitionTime_R, PartitionTime_S, PartitionTotalTime; 
		DWORD  PartitionNum_R, PartitionNum_S; 
		DWORD  MergePass_R, MergePass_S; 
		UINT64 MergeTime_R, MergeTime_S, MergeTotalTime;  
		DWORD  HeapSize;
		UINT64 JoinTime; 
		UINT64 TotalTime;
		DWORD BufferPoolSize;
		DWORD SortReadBufferSize, SortWriteBufferSize; 
		DWORD MergeReadBufferSize, MergeWriteBufferSize; 
		DWORD JoinReadBufferSize, JoinWriteBufferSize; 

		SmjReport(){
			CpuTime[0] = 0;CpuTime[1] = 0;
			SourceTableSize_R = 0; SourceTableSize_S = 0; 
			PartitionTime_R = 0; PartitionTime_S = 0; 
			PartitionTotalTime = 0;
			PartitionNum_R = 0; PartitionNum_S = 0;
			MergePass_R = 1; MergePass_S = 1;
			MergeTime_R = 0; MergeTime_S = 0;
			MergeTotalTime = 0; 
			HeapSize = 0;
			JoinTime = 0; 
			TotalTime = 0;
			BufferPoolSize = 0;
			SortReadBufferSize = 0; SortWriteBufferSize = 0;
			MergeReadBufferSize = 0; MergeWriteBufferSize = 0;
			JoinReadBufferSize = 0; JoinWriteBufferSize = 0;
		}
	};
protected:
	const SMJ2_PARAMS m_SmjParams;
	RP_PARAMS m_RpParams; // input params  
	LONG m_FanIndex; 
	std::queue<FANS*> m_FanIns;
	SmjReport rp;
	StopWatch stwTotalTime, stwJoinTime, stwPartitionTime, stwMergeTime;
protected:
	RELATION *R;
	RELATION *S;
	RELATION_JOIN *RS;
	LPWSTR m_JoinFilePath;

protected:
	PageHelpers2 *utl; /* Pointer to utilites */
	LPWSTR m_FanPath; 
	BOOL isR;
	Buffer m_HeapBuffer;
	HANDLE hInputFile; /* Handle for source table */
	HANDLE hOutputFile; /* Handle for write fanIn from heapsort output */
	ExsRpPartitionPhase partitionParams;
	ExsRpMergePhase mergeParams;
	std::queue<FANS*> m_FanInWillDelete;   

	DWORD m_HeapSize;
	BufferPool bufferPool; 

protected: 
	RC SMJ_CheckEnoughMemory(); 
	RC SMJ_Join();
	RC SMJ_SentOutput(RECORD *joinRecord);
	RC SMJ_Read(RELATION *&rel); 
	RC SMJ_Write(); 
	RC SMJ_GetNextRecord(RELATION *&rel, RECORD *&record, BOOL first);
	VOID SMJ_MakeNewRecord(RECORD *&joinRecord, RECORD *leftRecord, RECORD *rightRecord);
	VOID SMJ_GetJoinFilePath(LPWSTR &joinFilePath, LPWSTR rNameNoExt, LPWSTR sNameNoExt);
	RC SMJ_Initialize(const FANS *fanR, const FANS *fanS);
	LPWSTR SMJ_GetJoinFile();
	//////////////////////////////////////////////////////////////////////////
	RC RP_CalculateHeapSize(); 
	RC RP_Initialize(); 
	RC RP_Execute(); 
	DWORD RP_GetHeapSize() const;
	//////////////////////////////////////////////////////////////////////////
	RC RP_PartitionPhase(); 
	RC RP_PartitionPhase_Read(); 
	RC RP_PartitionPhase_Write(); 
	RC RP_PartitionPhase_GetNextRecord(RECORD *&record); 
	RC RP_PartitionPhase_CreateNewRun(); 
	RC RP_PartitionPhase_SentToOutput(RECORD *recordPtr, DWORD64 &pageCount); 
	RC RP_PartitionPhase_SaveFanIn(DWORD64 lowestKey, DWORD64 highestKey, DWORD64 tupleCount, DWORD64 pageCount);
	//////////////////////////////////////////////////////////////////////////
	RC RP_MergePhase();
	RC RP_MergePhase_Read(DWORD index);
	RC RP_MergePhase_Write();
	RC RP_MergePhase_GetNextRecord(RECORD *&record, INT index);
	//////////////////////////////////////////////////////////////////////////
	RC RP_GetFanPath();  
	UINT64 RP_GetFanSize(HANDLE hFile);  
	RC CopyRecord(RECORD *des, RECORD *&src);
	//////////////////////////////////////////////////////////////////////////
	INT TreeParent(INT i);
	INT TreeLeft(INT i);
	INT TreeRight (INT i);
	RC MinHeapify(RECORD **rec, INT i, INT heapSize);
	RC MaxHeapify(RECORD **rec, INT i, INT heapSize);
	RC BuildMinHeap(RECORD **rec, INT heapSize);
	RC BuildMaxHeap(RECORD **rec, INT heapSize);  

	RC WriteReport();
public:
	TSMJ(const SMJ2_PARAMS vParams);
	~TSMJ();

	RC Execute();

};

