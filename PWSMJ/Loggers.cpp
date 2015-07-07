// 
// Name: Loggers.cpp : implementation file  
// Author: hieunt
// Description: Helpers function for log infos to files
//

#include "stdafx.h" 
#include "Loggers.h" 
 
Loggers::Loggers(const LPWSTR vWorkSpacePath) 
{      
	m_FilePath = new CHAR[MAX_PATH];   
	LPWSTR temp = new TCHAR[MAX_PATH];
	swprintf(temp, MAX_PATH, L"%s%s", vWorkSpacePath, L"CPU_Report.csv" );

	// convert file path to char  
	size_t  count = wcstombs(m_FilePath, temp, MAX_PATH); 
	delete temp;
	fp=fopen(m_FilePath, "w+b"); 
} 

Loggers::~Loggers()
{    
	delete m_FilePath;   
	fclose(fp);
}   

VOID Loggers::Write(LPCSTR pszFormat, ...) 
{ 
	m_csLock.Lock();
	va_list argList;
	va_start(argList, pszFormat); 

	//vsprintf(m_Content, pszFormat, argList);
	vsprintf_s(m_Content, MAX_CHAR_LENGTH, pszFormat, argList);

	va_end(argList);

	fprintf(fp, m_Content);
	m_csLock.UnLock();
} 
   