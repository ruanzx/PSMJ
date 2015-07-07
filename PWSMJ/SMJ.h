#pragma once

// Classic Sort Merge Join
// Input: Require Two relations have already sorted


#include "DataTypes.h"
#include "DoubleBuffer.h"
#include "PageHelpers.h"    
class SMJ
{     
protected: 
	typedef struct RELATION
	{ 
		HANDLE hFile; 
		DWORD keyPos;    
		DoubleBuffer *dbcRead;
		OVERLAPPEDEX ovlRead; 
		PagePtr *probePage;
		Buffer   probePageBuffer; // Buffer for run page  
	} RELATION;

	typedef struct RELATION_JOIN
	{  
		HANDLE hFile;   
		DoubleBuffer *dbcWrite;
		OVERLAPPEDEX ovlWrite;  
		PagePtr *runPage;
		Buffer runPageBuffer; // Buffer for run page  
	} RELATION_JOIN;

protected:
	const SMJ_PARAMS m_Params; 

	RELATION *R;
	RELATION *S;
	RELATION_JOIN *RS;
	PageHelpers *utl;

	LPWSTR m_JoinFilePath;
	RC rc; 

protected:
	RC SMJ_SentOutput(RECORD *joinRecord);
	RC SMJ_Read(RELATION *&rel); 
	RC SMJ_Write(); 
	RC SMJ_GetNextRecord(RELATION *&rel, RECORD *&record, BOOL first);
	VOID SMJ_MakeNewRecord(RECORD *&joinRecord, RECORD *leftRecord, RECORD *rightRecord);
	VOID SMJ_GetJoinFilePath(LPWSTR &joinFilePath, LPWSTR rNameNoExt, LPWSTR sNameNoExt);
public:
	SMJ(const SMJ_PARAMS vParams);
	~SMJ(); 

	RC Execute();
	RC SMJ_Initialize();
	LPWSTR SMJ_GetJoinFile();
};
 