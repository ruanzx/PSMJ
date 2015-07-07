// 
// Name: DoubleBuffer.cpp : implementation file 
// Author: hieunt
// Description: Double buffer implementation
//

#include "stdafx.h"

#include "DoubleBuffer.h"

DoubleBuffer::DoubleBuffer(DWORD bufferSize)
{    
	//InitializeCriticalSection(&csProducer);  
	//InitializeCriticalSection(&csConsumer);  

	InitializeCriticalSectionAndSpinCount(&csProducer, CS_SPIN_COUNT);
	InitializeCriticalSectionAndSpinCount(&csConsumer, CS_SPIN_COUNT);

	back = 0;
	front = back ^ 1; 

	bFirstProduce = TRUE;    

	buffer[0].size = bufferSize;
	buffer[0].currentSize = 0;    
	buffer[0].currentPageIndex = 0; 
	buffer[0].pageCount = 0;   
	buffer[0].tupleCount = 0;   
	buffer[0].isSort = FALSE;  
	buffer[0].isFullMaxValue = FALSE;  

	buffer[1].size = bufferSize;
	buffer[1].currentSize = 0; 
	buffer[1].currentPageIndex = 0; 
	buffer[1].pageCount = 0;  
	buffer[1].tupleCount = 0;     
	buffer[1].isSort = FALSE;  
	buffer[1].isFullMaxValue = FALSE;  
} 

DoubleBuffer::~DoubleBuffer()
{  
	DeleteCriticalSection(&csProducer);  
	DeleteCriticalSection(&csConsumer); 

	// TODO: delete buffer, mem leak
}  

VOID DoubleBuffer::SwapBuffers()
{
	LockProducer();
	LockConsumer();  
	// Swap the buffer index 
	back ^= 1;  
	front = back ^ 1;

	UnLockConsumer();
	UnLockProducer();  
}

VOID DoubleBuffer::LockProducer()
{ 
	EnterCriticalSection(&csProducer); 
} 

VOID DoubleBuffer::UnLockProducer()
{
	LeaveCriticalSection(&csProducer);   
}  

VOID DoubleBuffer::LockConsumer()
{ 
	EnterCriticalSection(&csConsumer); 
} 

VOID DoubleBuffer::UnLockConsumer()
{
	LeaveCriticalSection(&csConsumer);  
}   