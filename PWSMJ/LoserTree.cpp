
// 
// Name: LoserTree.cpp : implementation file  
// Author: hieunt
// Description: Heap tree for merging fan-in files
//

#include "stdafx.h"
#include "LoserTree.h"

/// <summary>
/// Initializes a new instance of the <see cref="LoserTree"/> class.
/// </summary>
/// <param name="treeSize">Size of the tree.</param>
LoserTree::LoserTree(UINT treeSize)
{    
	m_TreeSize = treeSize;
	m_LoserTree = new int[m_TreeSize];
	m_TreeNodes = new RECORD*[m_TreeSize+1]; /// 1 for MAX element

	for(DWORD i=0; i<=m_TreeSize; i++)
	{
		m_TreeNodes[i] = new RECORD();
	} 
} 

/// <summary>
/// Finalizes an instance of the <see cref="LoserTree"/> class.
/// </summary>
LoserTree::~LoserTree()
{  
	delete [] m_LoserTree;
	delete [] m_TreeNodes;  
}  

/// <summary>
/// Adds new record into the tree node at position.
/// </summary>
/// <param name="record">The record pointer.</param>
/// <param name="pos">The position.</param>
VOID LoserTree::AddNewNode(RECORD *record, UINT pos)
{
	m_TreeNodes[pos]->key = record->key; 
	m_TreeNodes[pos]->data = record->data;
	//strcpy(	m_TreeNodes[pos]->tupleData, record->tupleData);
	m_TreeNodes[pos]->length = record->length; 
}

/// <summary>
/// Gets the record with the minimum index in tree
/// </summary>
/// <param name="record">The record pointer.</param>
VOID LoserTree::GetMinRecord(RECORD *&record)
{
	record = m_TreeNodes[ m_LoserTree[0] ]; 
}

/// <summary>
/// Gets the minimum key record.
/// </summary>
/// <param name="record">The record.</param>
/// <param name="index">The index.</param>
VOID LoserTree::GetMinRecord(RECORD *&record, INT &index)
{
	index = m_LoserTree[0]; 
	record = m_TreeNodes[m_LoserTree[0]]; 
}

/// <summary>
/// Gets the minimum index of the record.
/// </summary>
/// <returns></returns>
INT  LoserTree::GetMinRecordIndex()
{ 
	return m_LoserTree[0];
}

/// <summary>
/// Adjusts the tree.
/// </summary>
/// <param name="index">The index.</param>
VOID LoserTree::Adjust(INT index)
{
	INT t = (index + m_TreeSize) / 2;	 
	while(t>0)
	{
		if( m_TreeNodes[index]->key > m_TreeNodes[m_LoserTree[t]]->key ) 
		{ 
			std::swap(index, m_LoserTree[t]); 
		}
		t=t/2;
	} 
	m_LoserTree[0]=index; 
}  

/// <summary>
/// Creates the loser tree.
/// </summary>
VOID LoserTree::CreateLoserTree()
{
	// loserTree[0] is the index of minimum record in treeNodes
	m_TreeNodes[m_TreeSize]->key=0;

	for(INT i=0; i < m_TreeSize; i++)
	{
		m_LoserTree[i] = m_TreeSize; 
	}

	for(INT i=m_TreeSize-1; i>=0; i--)
	{ 
		Adjust( i );
	} 
} 