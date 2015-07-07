// 
// Name: DoubleBuffer.cpp : implementation file 
// Author: hieunt
// Description: Double buffer implementation
//

#pragma once

// Macro for quick access 
#define BACK_BUFFER(dbc) ((dbc)->buffer[(dbc)->back])
#define FRONT_BUFFER(dbc) ((dbc)->buffer[(dbc)->back ^ 1])   

#include "DataTypes.h"

class DoubleBuffer  
{  
private:   
	CRITICAL_SECTION csProducer;  // Locker for write into buffer
	CRITICAL_SECTION csConsumer;   // Locker for consume buffer
public:   
	volatile DWORD front; // front buffer
	volatile DWORD back; //  back buffer   
	volatile BOOL bFirstProduce;  // first time produce into buffer

	struct Buffer buffer[2];  

	DoubleBuffer(DWORD bufferSize);
	~DoubleBuffer();

	VOID SwapBuffers();

	VOID LockProducer();
	VOID UnLockProducer();

	VOID LockConsumer();
	VOID UnLockConsumer(); 
};
 