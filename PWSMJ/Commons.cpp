// 
// Name: Commons.cpp
// Author: hieunt
// Description: Helpers function used in this application
//

#include "stdafx.h"  
#include "Commons.h"


/// <summary>
/// This function accepts a number and converts it to a
/// string, inserting commas where appropriate.
/// </summary>
/// <param name="lNum">The input number.</param>
/// <param name="szBuf">The buffer pointer.</param>
/// <param name="chBufSize">Size of buffer.</param>
/// <returns></returns>
PTSTR LongToString(LONG lNum, PTSTR szBuf, DWORD chBufSize) 
{
	// Usage
	// TCHAR szBuf[50];
	// BigNumToString(111, szBuf, _countof(szBuf)) ;

	TCHAR szNum[100];
	StringCchPrintf(szNum, _countof(szNum), TEXT("%d"), lNum);
	NUMBERFMT nf;
	nf.NumDigits = 0;
	nf.LeadingZero = FALSE;
	nf.Grouping = 3;
	nf.lpDecimalSep = TEXT(".");
	nf.lpThousandSep = TEXT(",");
	nf.NegativeOrder = 0;
	GetNumberFormat(LOCALE_USER_DEFAULT, 0, szNum, &nf, szBuf, chBufSize);
	return(szBuf);
}

/// <summary>
/// Convert Long to string.
/// </summary>
/// <param name="lNum">The input number.</param>
/// <param name="szPartern">The partern pointer.</param>
/// <returns></returns>
LPWSTR LongToString(LONG lNum,  PTSTR szPartern)  
{  
	//Usage: LongToString(10, L"%d watt")
	LPWSTR szBuf = new TCHAR[100];
	//wsprintfW(szBuf, L"%d%s", lNum, szUnit); 
	wsprintfW(szBuf, szPartern, lNum ); 
	return(szBuf);
}

/// <summary>
/// Convert double to string.
/// </summary>
/// <param name="dNum">The input number.</param>
/// <param name="szPartern">The partern pointer.</param>
/// <returns></returns>
LPWSTR DoubleToString(DOUBLE dNum,  PTSTR szPartern)  
{  
	//Usage: DoubleToString(10, L"%d watt")
	LPWSTR szBuf = new TCHAR[100]; 
	swprintf(szBuf,100, szPartern, dNum); 
	return(szBuf);
}
 
/// <summary>
/// Convert wide string to unsigned long integer.
/// </summary>
/// <param name="wsInput">The ws input.</param>
/// <returns></returns>
LONG wcs2long(LPWSTR wsInput)
{ 
	//On success, the function returns the converted integral number as a long int value.
	//If no valid conversion could be performed, a zero value is returned (0L).
	LONG l1 = wcstol (wsInput, NULL, 0);
	//wprintf (L"Value: %ld\n", ul);
	return l1;
}
 
/// <summary>
/// Convert wide string to unsigned long integer.
/// </summary>
/// <param name="wsInput">The ws input.</param>
/// <returns></returns>
ULONG wcs2ulong(LPWSTR wsInput)
{ 
	//On success, the function returns the converted integral number as an unsigned long int value.
	//If no valid conversion could be performed, a zero value is returned.
	ULONG ul;
	ul = wcstoul (wsInput, NULL, 0);
	//wprintf (L"Value: %lu\n", ul);
	return ul;
}
 
/// <summary>
/// Convert wide string to double
/// </summary>
/// <param name="wsInput">The ws input.</param>
/// <returns></returns>
DOUBLE wcs2double(LPWSTR wsInput)
{ 
	DOUBLE d;
	//On success, the function returns the converted floating point number as a value of type double.
	//If no valid conversion could be performed, the function returns zero (0.0).
	d = wcstod (wsInput, NULL); 
	//wprintf (L"Value: %.2f\n", d);
	return d;
}

  
/// <summary>
/// Convert integer to wide/unicode ascii
/// </summary>
/// <param name="integer">The input integer.</param>
/// <returns></returns>
std::wstring itoc(const INT integer)
{
	wchar_t wcstring[1024];
	swprintf_s(&wcstring[0], 1024, L"%d", integer);
	std::wstring ws(wcstring);

	return ws;
}
  
/// <summary>
/// Convert pointer to wide/unicode ascii
/// </summary>
/// <param name="pPointer">The input pointer.</param>
/// <returns></returns>
std::wstring ptoc(const LPVOID pPointer)// convert pointer to wide/unicode ascii
{
	wchar_t wcstring[65];
	swprintf_s(wcstring,  L"%p", pPointer);

	return wcstring;
}

  
/// <summary>
/// Convert char* to wide/unicode string.
/// </summary>
/// <param name="stringArg">The string argument.</param>
/// <returns></returns>
std::wstring s2ws(const CHAR* stringArg)
{
	// compute the size of the buffer I need to allocate
	size_t numConvertedChars;
	mbstowcs_s(&numConvertedChars, NULL, 0, stringArg, _TRUNCATE);
	numConvertedChars++;  // +1 for null termination
	if(numConvertedChars>1024)
	{
		numConvertedChars = 1024;
	}

	// allocate the converted string and copy
	wchar_t *pWString = new wchar_t[numConvertedChars];
	mbstowcs_s(&numConvertedChars, pWString, numConvertedChars, stringArg, _TRUNCATE);
	std::wstring ws(pWString);
	delete [] pWString;
	return ws; 
}
  
/// <summary>
/// Convert char* to wide/unicode string.
/// </summary>
/// <param name="stringArg">The string argument.</param>
/// <returns></returns>
LPWSTR c2ws(const CHAR* stringArg)
{
	// compute the size of the buffer I need to allocate
	size_t numConvertedChars;
	mbstowcs_s(&numConvertedChars, NULL, 0, stringArg, _TRUNCATE);
	numConvertedChars++;  // +1 for null termination
	if(numConvertedChars>1024)
	{
		numConvertedChars = 1024;
	}

	// allocate the converted string and copy
	LPWSTR pWString = new TCHAR[numConvertedChars];
	mbstowcs_s(&numConvertedChars, pWString, numConvertedChars, stringArg, _TRUNCATE);

	return pWString; 
}
 
/// <summary>
/// Convert wide/unicode string to char.
/// </summary>
/// <param name="string">The string.</param>
/// <returns></returns>
CHAR* ws2s(std::wstring string)
{
	size_t numConverted, finalCount;

	// what size of buffer (in bytes) do we need to allocate for conversion?
	wcstombs_s(&numConverted, NULL, 0, string.c_str(), 1024);
	numConverted+=2; // for null termination
	CHAR *pBuffer = new CHAR[numConverted];

	// do the actual conversion
	wcstombs_s(&finalCount, pBuffer, numConverted, string.c_str(), 1024);

	return pBuffer;
}
 
/// <summary>
/// Convert wide/unicode string to char.
/// </summary>
/// <param name="string">The string.</param>
/// <returns></returns>
CHAR* ws2s(LPWSTR string)
{
	size_t numConverted, finalCount;

	// what size of buffer (in bytes) do we need to allocate for conversion?
	wcstombs_s(&numConverted, NULL, 0, string, 1024);
	numConverted+=2; // for null termination
	CHAR *pBuffer = new CHAR[numConverted];

	// do the actual conversion
	wcstombs_s(&finalCount, pBuffer, numConverted, string, 1024);

	return pBuffer;
}

 
/// <summary>
/// conversion from string to cstring
/// since UNICODE is defined, cString is a std::wstring
/// </summary>
/// <param name="sourceString">The source string.</param>
/// <returns></returns>
std::wstring s2cs(const std::string &sourceString)
{
	std::wstring destString;
	destString.assign(sourceString.begin(), sourceString.end());

	return destString;
}

/// <summary>
/// Gets the error message.
/// </summary>
/// <param name="error">The error code.</param>
/// <returns></returns>
LPCTSTR GetErrorMessage(DWORD error)  
{  
	LPVOID lpMsgBuf;

	FormatMessage(
		FORMAT_MESSAGE_ALLOCATE_BUFFER | 
		FORMAT_MESSAGE_FROM_SYSTEM |
		FORMAT_MESSAGE_IGNORE_INSERTS,
		NULL,
		error,
		MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
		(LPTSTR) &lpMsgBuf,
		0, NULL );

	return((LPCTSTR)lpMsgBuf);
}

/// <summary>
/// Prints the error message.
/// </summary>
/// <param name="errorCode">The error code.</param>
VOID PrintErrorMessage(DWORD errorCode) 
{  
	wprintf(L"[%d] %s\n\n", errorCode, GetErrorMessage(errorCode)); 
}

/// <summary>
/// Prints the error message.
/// </summary>
/// <param name="errorMessage">The error message.</param>
/// <param name="errorCode">The error code.</param>
VOID PrintErrorMessage(LPTSTR errorMessage, DWORD errorCode)
{  
	wprintf(L"%s [%d] %s\n\n", errorMessage, errorCode, GetErrorMessage(errorCode)); 
}

/// <summary>
/// Prints the error message.
/// </summary>
/// <param name="errorMessage">The error message.</param>
VOID PrintErrorMessage(LPCTSTR errorMessage) 
{
	wprintf(L"%s \n", errorMessage ); 
}

/// <summary>
/// Reports the last error.
/// </summary>
VOID ReportLastError()
{
	LPCTSTR pszCaption = _T("Windows SDK Error Report");
	DWORD dwError      = GetLastError();

	if(NOERROR == dwError)
	{
		MessageBox(NULL, _T("No error"), pszCaption, MB_OK);
	}
	else
	{
		const DWORD dwFormatControl = FORMAT_MESSAGE_ALLOCATE_BUFFER |
			FORMAT_MESSAGE_IGNORE_INSERTS |
			FORMAT_MESSAGE_FROM_SYSTEM;

		LPVOID pTextBuffer = NULL;
		DWORD dwCount = FormatMessage(dwFormatControl, 
			NULL, 
			dwError, 
			0, 
			(LPTSTR) &pTextBuffer, 
			0, 
			NULL);

		if(0 != dwCount)
		{
			MessageBox(NULL, (LPCWSTR)pTextBuffer, pszCaption, MB_OK|MB_ICONERROR);
			LocalFree(pTextBuffer);
		}
		else
		{
			MessageBox(NULL, _T("Unknown error"), pszCaption, MB_OK|MB_ICONERROR);
		}
	}
}

/// <summary>
/// Sleeps in the microsecond.
/// </summary>
/// <param name="us">The time to sleep in us.</param>
VOID SleepMicrosecond(INT us)
{
	UINT64 t1 = 0, t2 = 0, freq = 0;
	UINT64 wait_tick;
	QueryPerformanceFrequency((LARGE_INTEGER *) &freq);
	wait_tick = freq * us / 1000000ULL;
	QueryPerformanceCounter((LARGE_INTEGER *) &t1);
	do 
	{
		QueryPerformanceCounter((LARGE_INTEGER *) &t2);
		YieldProcessor();
	}
	while ((t2-t1) < wait_tick);
} 

/// <summary>
/// Sleeps the milisecond.
/// </summary>
/// <param name="ms">The time to sleep in ms.</param>
VOID SleepMilisecond(INT ms)
{
	Sleep(ms); 
}


/// <summary>
/// Sleeps the second.
/// </summary>
/// <param name="s">The time to sleep in s.</param>
VOID SleepSecond(INT s)
{ 
	Sleep(s*1000); 
}

CHAR* GetTime()
{  
	time_t rawtime;
	struct tm *timeinfo; 
	time (&rawtime);
	timeinfo = localtime(&rawtime); 

	static char result[20];
	sprintf(result, "%d:%.2d:%.2d:%.2d:%.2d:%.2d\0",
		1900 + timeinfo->tm_year, 
		timeinfo->tm_mon,
		timeinfo->tm_mday,
		timeinfo->tm_hour,
		timeinfo->tm_min, 
		timeinfo->tm_sec 
		); 
	return result;
}

/// <summary>
/// Convert FileTime to UTC.
/// </summary>
/// <param name="ftime">The file time.</param>
/// <returns></returns>
UINT64 FileTime2UTC(const FILETIME* ftime)
{
	LARGE_INTEGER li;
	ASSERT(ftime);
	li.LowPart = ftime->dwLowDateTime;
	li.HighPart = ftime->dwHighDateTime;
	return li.QuadPart;
}


/// <summary>
/// Returns the amount of CPU time used by the current process,
/// in seconds, or -1.0 if an error occurred. 
/// </summary>
/// <returns></returns>
DOUBLE GetCpuTime()
{ 
	/* Windows -------------------------------------------------- */
	FILETIME createTime;
	FILETIME exitTime;
	FILETIME kernelTime;
	FILETIME userTime;
	if (GetProcessTimes(GetCurrentProcess(), &createTime, &exitTime, &kernelTime, &userTime) != -1)
	{
		SYSTEMTIME userSystemTime;
		if ( FileTimeToSystemTime( &userTime, &userSystemTime ) != -1 )
			return (DOUBLE)userSystemTime.wHour * 3600.0 +
			(DOUBLE)userSystemTime.wMinute * 60.0 +
			(DOUBLE)userSystemTime.wSecond +
			(DOUBLE)userSystemTime.wMilliseconds / 1000.0;
	} 

#if defined(CLOCKS_PER_SEC)
	{
		clock_t cl = clock( );
		if ( cl != (clock_t)-1 )
			return (DOUBLE)cl / (DOUBLE)CLOCKS_PER_SEC;
	}
#endif 

	return -1;		/* Failed. */
}


/// <summary>
/// Sets the debug mode.
/// </summary>
/// <param name="mode">The mode.</param>
void SetDebugMode(DWORD mode)
{
	if(mode <= DEBUG_MODE_MESSAGE)
	{
		debugMode = mode;
	}
	else
	{
		debugMode = DEBUG_MODE_NONE;
	}
}
 
/// <summary>
/// Print the debug string.
/// </summary>
/// <param name="cstr">The CSTR.</param>
void DebugPrint(CString cstr)
{
	//http://msdn.microsoft.com/zh-cn/library/z5hh6ee9.aspx

	if(debugMode == DEBUG_MODE_NONE)
	{
		return ;
	}

	static int flag = TRUE;
	static TCHAR file[MAX_PATH];
	static DWORD first = GetTickCount();
	CString output;

	output.Format(_T("%08d "), GetTickCount() - first);
	output += cstr;
	output.Append(_T("\n"));
	output.Replace(_T("\r"), _T(""));

	if(flag)
	{
		TCHAR* ptrEnd;
		::GetModuleFileName(NULL, file, MAX_PATH);
		if((ptrEnd = _tcsrchr(file, '.')) != NULL )
		{
			*ptrEnd = '\0';
			_tcscat_s(file, MAX_PATH, _T(".log"));
		}
		DeleteFile(file);
		flag = FALSE;
	}
	  
	FILE *fp;
	_tfopen_s(&fp, file, _T("ac"));
	_ftprintf(fp, _T("%s"), output);
	fflush(fp);
	fclose(fp);

	if(debugMode == DEBUG_MODE_MESSAGE)
	{
		AfxMessageBox(output);
	}
}

/// <summary>
/// Check directories exist.
/// </summary>
/// <param name="pszPath">The PSZ path.</param>
/// <returns></returns>
BOOL DirectoryExist(LPWSTR pszPath)
{
	DWORD attribs = GetFileAttributes(pszPath);
	if (attribs == INVALID_FILE_ATTRIBUTES) {
		return FALSE;
	}

	if (attribs & FILE_ATTRIBUTE_DIRECTORY)
		return TRUE;

	return FALSE;
}

/// <summary>
/// Convert Joules to watt per hour.
/// </summary>
/// <param name="jouleNum">The joule number.</param>
/// <returns></returns>
DOUBLE JouleToWattHour(DOUBLE jouleNum)
{
	// http://www.asknumbers.com/EnergyWorkConversion.aspx
	// 1 joule = 0.00027777777778 watt hour
	return jouleNum * (0.00027777777778);
}

/// <summary>
/// Convert Joules to miliwatt per hour.
/// </summary>
/// <param name="jouleNum">The joule number.</param>
/// <returns></returns>
DOUBLE JouleToMiliwattHour(DOUBLE jouleNum)
{
	// http://www.asknumbers.com/EnergyWorkConversion.aspx
	// 1 joule = 0.00027777777778 watt hour
	return jouleNum * (0.00027777777778) * 1000;
}