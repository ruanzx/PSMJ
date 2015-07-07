// 
// Name: ExsMergePhase.cpp : implementation file 
// Author: hieunt
// Description: Implementation of merge phase in parallel external merge sort
//				Merge all run created, if not enough memory, then multiple merge step is needed
//
#pragma once
 

#include "DataTypes.h"
#include "DoubleBuffer.h"
#include "PageHelpers2.h"
#include "LoserTree.h"    

struct ExsMergeThreadParams
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

class ExsMergePhase
{  
	const PEMS_PARAMS m_Params; 
	std::queue<FANS*> m_FanIns;    
	std::queue<FANS*> m_FanInWillDelete;   
	ExsMergeThreadParams *m_ThreadParams; 
	DWORD    m_MergePassNum; 
	LPWSTR   m_FanOutName;
	FANS*    m_FanOut; 
	LONG     m_FanOutIndex ;  // Index for naming run file

	DWORD m_AvailableMemorySize; 

	PageHelpers2 *utl;
	BufferPool bufferPool;
	RC MergePhase_Initialize();  
	RC MergePhase_Read(LPVOID lpParam, DWORD f);  
	RC MergePhase_Write(LPVOID lpParam); 
	RC MergePhase_Merge(LPVOID lpParam); 
	RC MergePhase_GetFanOutPath(LPWSTR &fanOutName, INT threadID);  
	RC MergePhase_GetNextTuple(LPVOID lpParam, RECORD *&record, INT index);  
	UINT64 GetFanSize(HANDLE hFile);

private:
	/* Private variable */
	RC rc;   
	DWORD m_MaxFanIn;
public:
	ExsMergePhase(PEMS_PARAMS vParams, std::queue<FANS*> vFanIns, DWORD vAvailableMemorySize);
	~ExsMergePhase(); 
	RC MergePhase_Execute();
	RC MergePhase_CheckEnoughMemory(); 
	FANS* GetFinalFanOut() { return m_FanOut; } 
};
