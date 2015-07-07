
// 
// Name: LoserTree.cpp : implementation file  
// Author: hieunt
// Description: Heap tree for merging fan-in files
//

#pragma once

#include "DataTypes.h"

class LoserTree  
{  
	UINT	  m_TreeSize;
	INT		 *m_LoserTree; // index array
	RECORD  **m_TreeNodes; //tupleKey & tupleData array  
public:    
	LoserTree(UINT treeSize);
	~LoserTree();
	VOID  CreateLoserTree();
	VOID  AddNewNode(RECORD *record, UINT pos);
	VOID  GetMinRecord(RECORD *&record);
	VOID  GetMinRecord(RECORD *&record, INT &index);
	INT   GetMinRecordIndex();
	VOID  Adjust(INT index);  
};
 