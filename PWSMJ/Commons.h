// 
// Name: Commons.cpp
// Author: hieunt
// Description: Helpers function used in this application
//


#pragma once  

#include <CommCtrl.h>
#include <io.h> 


/* 
When the compiler sees a line like this:
   #pragma chMSG(Fix this later)

it outputs a line like this:

  c:\CD\CmnHdr.h(82):Fix this later

You can easily jump directly to this line and examine the surrounding code.
*/

#define chSTR2(x) #x
#define chSTR(x)  chSTR2(x)
#define chMSG(desc) message(__FILE__ "(" chSTR(__LINE__) "):" #desc)

// This macro returns TRUE if a number is between two others
#define chINRANGE(low, Num, high) (((low) <= (Num)) && ((Num) <= (high)))

// This macro evaluates to the number of bytes needed by a string.
#define chSIZEOFSTRING(psz)   ((lstrlen(psz) + 1) * sizeof(TCHAR))


/////////////////// chROUNDDOWN & chROUNDUP inline functions ////////////////// 
// This inline function rounds a value down to the nearest multiple
template <class TV, class TM>
TV chROUNDDOWN(TV Value, TM Multiple) {	return((Value / Multiple) * Multiple);}

// This inline function rounds a value up to the nearest multiple
template <class TV, class TM>
TV chROUNDUP(TV Value, TM Multiple) {return(chROUNDDOWN(Value, Multiple) + (((Value % Multiple) > 0) ? Multiple : 0));}


/////////////////////////// Quick MessageBox Macro //////////////////////////// 
inline void chMB(PCSTR szMsg) 
{
	char szTitle[MAX_PATH];
	GetModuleFileNameA(NULL, szTitle, _countof(szTitle));
	MessageBoxA(GetActiveWindow(), szMsg, szTitle, MB_OK);
}

inline void chMBE(LPWSTR szMsg) 
{  
	MessageBoxW(NULL, szMsg, L"Error", MB_OK);
}
   
#define chSTR2(x) #x
#define chSTR(x)  chSTR2(x)
#define chMSG(desc) message(__FILE__ "(" chSTR(__LINE__) "):" #desc)

#define RZX_ASSERT(Condition, Message)
#define RZX_ASSERTA(Condition, Message)
#define RZX_TRACE(String)
#define DEBUGMESSAGEBOX(Title, Text)
#define HEAPCHECK

static int gRefCount = 0;
static const DWORD DEBUG_MODE_NONE    = 0;
static const DWORD DEBUG_MODE_LOG     = 1;
static const DWORD DEBUG_MODE_MESSAGE = 2; 
static DWORD debugMode = DEBUG_MODE_LOG;

//handy defines
//-----------------------------------------------------------------------------
#define SAFE_RELEASE(p)     {if((p)){HEAPCHECK; gRefCount = (p)->Release(); (p)=NULL; HEAPCHECK;} }
#define SAFE_DELETE(p)      {if((p)){HEAPCHECK; delete (p);     (p)=NULL;HEAPCHECK; }}
#define SAFE_DELETE_ARRAY(p){if((p)){HEAPCHECK; delete[](p);    (p)=NULL;HEAPCHECK; }}
#define UNREFERENCED_PARAMETER(P) (P)


PTSTR LongToString(LONG lNum, PTSTR szBuf, DWORD chBufSize); // 
LPWSTR LongToString(LONG lNum,  PTSTR szPartern);
LPWSTR DoubleToString(DOUBLE dNum,  PTSTR szPartern);
LONG wcs2long(LPWSTR wsInput);
ULONG wcs2ulong(LPWSTR wsInput); 
DOUBLE wcs2double(LPWSTR wsInput); 
std::wstring itoc(const INT integer); // convert integer to wide/unicode ascii 
std::wstring ptoc(const LPVOID pPointer); // convert pointer to wide/unicode ascii 
std::wstring s2ws(const CHAR* stringArg); // convert char* to wide/unicode string 
LPWSTR c2ws(const CHAR* stringArg); // convert char* to wide/unicode string 
CHAR* ws2s(std::wstring string); // convert wide/unicode string to char 
CHAR* ws2s(LPWSTR string);// convert wide/unicode string to char 
std::wstring s2cs(const std::string &sourceString); // conversion from string to cstring, since UNICODE is defined, cString is a std::wstring

LPCTSTR GetErrorMessage(DWORD errorCode);
VOID PrintErrorMessage(DWORD errorCode);
VOID PrintErrorMessage(LPTSTR errorMessage, DWORD errorCode); 
VOID PrintErrorMessage(LPCTSTR errorMessage);
VOID ReportLastError();


VOID SleepMicrosecond(INT us);
VOID SleepMilisecond(INT ms);
VOID SleepSecond(INT s);
CHAR* GetTime();
UINT64 FileTime2UTC(const FILETIME* ftime);
DOUBLE GetCpuTime();  
void SetDebugMode(DWORD mode);
void DebugPrint(CString cstr);
BOOL DirectoryExist(LPWSTR pszPath);
DOUBLE JouleToWattHour(DOUBLE jouleNum);
DOUBLE JouleToMiliwattHour(DOUBLE jouleNum);