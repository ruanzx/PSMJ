
// 
// Name: NSM.cpp : implementation file 
// Author: hieunt
// Description: Making the N-ary struct page
//

#include "stdafx.h"
#include "NSM.h"  

/// <summary>
/// Initializes a new instance of the <see cref="NSM"/> class.
/// </summary>
/// <param name="vParams">The NSM parameters.</param>
NSM::NSM(const NSM_PARAMS vParams) : m_Params(vParams)
{
	tuple = new CHAR[TUPLE_SIZE];
	utl = new PageHelpers();
	m_InputFilePath = new CHAR[MAX_PATH];
	m_OutputFilePath = new CHAR[MAX_PATH];

	strcpy(m_InputFilePath, ws2s(vParams.SORT_FILE_PATH));

	LPWSTR temp = new TCHAR[MAX_PATH];
	swprintf(temp, MAX_PATH, L"%s%s.dat", vParams.WORK_SPACE_PATH, vParams.FILE_NAME_NO_EXT); 
	strcpy(m_OutputFilePath, ws2s(temp));
	delete temp;

	fpRead=fopen(m_InputFilePath, "r"); 
	fpWrite=fopen(m_OutputFilePath, "w+b");

	utl->InitBuffer(runPageBuffer, SSD_PAGE_SIZE);
	utl->InitRunPage(runPage, runPageBuffer); 
}

/// <summary>
/// Finalizes an instance of the <see cref="NSM"/> class.
/// </summary>
NSM::~NSM()
{
	fclose(fpRead);
	fclose(fpWrite);  

	delete m_InputFilePath;
	delete m_OutputFilePath; 

	delete runPage;
	delete tuple;
	delete utl;
}


/// <summary>
/// Creates this instance.
/// </summary>
/// <returns></returns>
RC NSM::Create()
{   
	while(true)
	{     
		if(!utl->IsPageFull(runPage)) 
		{ 
			if(NULL!=fgets(tuple, TUPLE_SIZE, fpRead))
			{   
				utl->AddTupleToPage(runPage, tuple, runPageBuffer);  
			} 
			else
			{
				fwrite(runPage->page, sizeof(CHAR), SSD_PAGE_SIZE, fpWrite);
				break;
			}
		} 
		else
		{    
			fwrite(runPage->page, sizeof(CHAR), SSD_PAGE_SIZE, fpWrite);  
			utl->ResetRunPage(runPage, runPageBuffer); 
		}
	} 

	return SUCCESS;
}



