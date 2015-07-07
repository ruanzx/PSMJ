// 
// Name: Loggers.h : implementation file  
// Author: hieunt
// Description: Helpers function for log infos to files
//

#pragma once
#include "CriticalSection.h"
#define MAX_CHAR_LENGTH 20*1024
 
class Loggers  
{  
private:  
	FILE *fp;   
	CHAR *m_FilePath;   
	CHAR m_Content[MAX_CHAR_LENGTH];
	CriticalSection m_csLock;
public:    
	Loggers(const LPWSTR vWorkSpacePath);
	~Loggers();   

	VOID Write(LPCSTR pszFormat, ...); 
};
 