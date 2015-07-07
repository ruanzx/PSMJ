// 
// Name: BNLJ.h : header file 
// Author: hieunt
// Description: Block nested loop join implementation
//

#pragma once

#include "DataTypes.h"
#include "DoubleBuffer.h"
#include "PageHelpers.h"

class BNLJ  
{  
private:    
	BNLJ_PARAMS m_Params;
	PageHelpers *utl;
	UINT64 totalJoin;

	HANDLE hFileSmall; // customer
	DoubleBuffer *dbcReadSmall;
	OVERLAPPEDEX overlapReadSmall;

	HANDLE hFileBig; // order
	DoubleBuffer *dbcReadBig;
	OVERLAPPEDEX overlapReadBig;
	 
	PagePtr *hashBuildPage;
	Buffer   hashBuildPageBuffer;
	Buffer   hashTableBuffer;

	PagePtr *probePage;
	Buffer   probePageBuffer;
	 
	HANDLE hFileOut;
	DoubleBuffer *dbcWrite;
	OVERLAPPEDEX overlapWrite;
	PagePtr *writePage;
	Buffer   writePageBuffer;
	 
	HashTable hashTable;
	 
	 
	RECORD *probeRecord;
	RECORD *bigRelationRecord;
	RECORD *smallRelationRecord;
	RECORD *joinRecord;

	//////////////////////////////////////////////////////////////////////////
	RC Initialize(); 
	VOID CreateJoinRecord(RECORD *&joinRecord, RECORD *leftRecord, RECORD *rightRecord); 
	VOID HashBuild();
	VOID HashProbe(); 
	VOID GetJoinFilePath(LPWSTR &joinFilePath);
	RC SentOutput(RECORD *joinRecord); 
	RC ReadSmall();
	RC ReadBig();
	RC Write();  
public:    
	BNLJ(const BNLJ_PARAMS vParams);
	~BNLJ(); 
	RC Execute(); 
};
 