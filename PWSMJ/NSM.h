// 
// Name: NSM.cpp : implementation file 
// Author: hieunt
// Description: Making the N-ary struct page
//

#pragma once 

#include "PageHelpers.h"

class NSM
{   
	const NSM_PARAMS m_Params;
	CHAR *tuple;
	Buffer runPageBuffer;
	PagePtr* runPage; 
	CHAR *m_InputFilePath;
	CHAR *m_OutputFilePath; 

	FILE *fpRead; 
	FILE *fpWrite;  

	PageHelpers *utl; 

public:
	NSM(const NSM_PARAMS vParams);
	~NSM(); 

	RC Create(); 
	 
};
 