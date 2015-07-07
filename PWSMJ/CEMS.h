#pragma once 
// 
// Name: CEMS.h :  header file 
// Author: hieunt
// Description: Cache external merge sort implementation
//
 
// Cache external merge sort, using Replacement Selection for sorting
// Advantage: exploit SSD random read, all merge operation are executed on SSD
// Phase 1: Read as much as memory can save into, create full run into SSD space
//          When SSD is full, merge these runs into HDD
//          When read file table complete, all run are created on HDD, each run size are equal SSD space
//          Move to Phase 2
// Phase 2: Copy each part of each run into SSD, this operation is sequency read
//          When SSD is full, start to merge back into HDD
//          Stop when only one run in HDD left.

#include "DataTypes.h"
#include "PageHelpers2.h"
#include "LoserTree.h"  
#include "StopWatch.h"

class CEMS 
{ 
protected:
	typedef struct CemsReport
	{
		DWORD HeapSize;
		DWORD HeapBufferSize;
		DWORD SsdReadBufferSize;
		DWORD SsdWriteBufferSize;
		DWORD HddReadBufferSize;
		DWORD HddWriteBufferSize;
		DWORD BufferPoolSize;
		DWORD SsdStorageSize;
		UINT64 TableSize;
		UINT64 FanInSize; // estimated
		UINT64 TotalTime[2]; 
		DOUBLE CpuTime[2];
	};
protected:
	LPWSTR m_SsdFanInPath;   // Path of current SSD run
	HANDLE hSsdRunFile; /* Handle for write to SSD, fanIn from heapsort output */ 
	OVERLAPPEDEX **overlapReadSsd;  // overlap struct for read from SSD
	OVERLAPPEDEX **overlapCopyHddToSsd;  // Overlap struct for copy run file from HDD to SSD

	Buffer *ssdReadBuffer;  // ssd read fanIn buffer  
	Buffer ssdWriteBuffer;  // buffer for write to SSD
	PagePtr **ssdMergePage; /* merge page on SSD */
	Buffer *ssdMergePageBuffer; // buffer for merge page on SSD

	PagePtr* ssdPage; 
	Buffer ssdPageBuffer; 
	 
protected:
	LPWSTR m_HddFanInPath;   // Path of current HHD run
	HANDLE hHddRunFile; // Handle for write to HDD

	OVERLAPPEDEX *overlapReadHdd;  // overlap struct for read from HDD 
	OVERLAPPEDEX *overlapWriteHdd;  // overlap struct for write to HDD

	Buffer hddReadBuffer; // Read input buffer from HDD
	Buffer hddWriteBuffer; // Buffer for write to HDD

	PagePtr* hddPage; 
	Buffer hddPageBuffer; 
private: 
	const CEMS_PARAMS m_CemsParams; 
	BufferPool bufferPool; // memory pool
	HANDLE hTableFile; /* Handle for source table */

	PageHelpers2 *utl; /* Pointer to utilites */
	LONG m_FanIndex; // Naming run file 

	std::queue<FANS*> m_FanInSsd; // runtime fanIn created on SSD
	std::queue<FANS*> m_FanInHdd; // runtime fanIn created on HDD

	Buffer heapBuffer; // buffer for heap sort
	DWORD m_MaxFanInInOneStep; // Based on memory size
	DWORD m_HeapSize; // Based on memory size

	/* Replacement selection */
	DWORD m_CurrentMark; /* determine tuple belongs to current run or next run */
protected:
	CemsReport rp;
	StopWatch stwTotalTime;
	BOOL isGetFanInSize;
public:
	CEMS(CEMS_PARAMS vParams);
	~CEMS();

	RC Execute(); 
	RC Estimate(); 
	RC Phase1_Initialize();  
	RC Phase1_GetHeapSize(DWORD &maxFanInNum, DWORD &heapSize);  
	RC Phase1_GetNextTuple(HANDLE hFile, OVERLAPPEDEX *overlapEx, Buffer &SsdReadBuffer, Buffer &SsdMergePageBuffer, RECORD *recordPtr);  
	RC Phase1_TerminateRun(DWORD64 &totalPage, DWORD64 &currentSsdRunPageCount);
	RC Phase1_SentOutput(CHAR *tupleData, DWORD64 &totalTuple, DWORD64 &totalPage, DWORD64 &currentSsdRunPageCount); 
	RC Phase1_MergeRunsOnSsdWriteToHdd(); 
	RC Phase1_GetNextTupleFromHDD(RECORD *&recordPtr); 
	RC Phase1_ReadFromHdd();  
	RC Phase1_WriteToSsd();

	RC Phase2_MergeRunsOnHdd(); 
	RC Phase2_CopyFileFromHddToSsd(HANDLE hHddFile, FANS *&newSsdFanIn, OVERLAPPEDEX *overlapRead, Buffer &copyBuffer, DWORD64 maxRunSize);
	RC Phase2_GetNextTuple(HANDLE hHddFile, HANDLE &hSsdFile, FANS *&SsdFanInFile, const DWORD treeIndex, RECORD *recordPtr, DWORD SsdRunSize); 

	RC CreateSsdRun();
	RC CreateHddRun();
	RC SaveSsdFanIn(DWORD64 lowestKey, DWORD64 highestKey, DWORD64 totalTuple, DWORD64 totalPage, LONGLONG fileSize);
	RC SaveHddFanIn(DWORD64 lowestKey, DWORD64 highestKey, DWORD64 totalTuple, DWORD64 totalPage, LONGLONG fileSize); 

	RC WriteSsdFanInToHdd(HANDLE hFile);
	RC ReadSsdFanIn(HANDLE hFile, Buffer &SsdReadBuffer, OVERLAPPEDEX *overlapEx); 

	RC CopyRecord(RECORD *desRecordPtr, RECORD *&srcRecordPtr);

	RC GetSsdFanInPath(LPWSTR &fanInPath);
	RC GetHddFanInPath(LPWSTR &fanInPath); 

	RC WriteReport();

	UINT64 GetFanSize(HANDLE hFile);
	/* Replacement selection utilites function */ 
	INT TreeParent(INT i);
	INT TreeLeft(INT i);
	INT TreeRight (INT i);
	RC MinHeapify(RECORD **rec, INT i, INT heapSize);
	RC MaxHeapify(RECORD **rec, INT i, INT heapSize);
	RC BuildMinHeap(RECORD **rec, INT heapSize);
	RC BuildMaxHeap(RECORD **rec, INT heapSize); 
};
