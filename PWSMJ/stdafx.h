
// stdafx.h : include file for standard system include files,
// or project specific include files that are used frequently,
// but are changed infrequently

#pragma once

// Always compiler using Unicode.
#ifndef UNICODE
	#define UNICODE
#endif
// When using Unicode Windows functions, use Unicode C-Runtime functions too.
#ifndef _UNICODE
	#define _UNICODE
#endif


#ifndef VC_EXTRALEAN
	#define VC_EXTRALEAN            // Exclude rarely-used stuff from Windows headers
#endif

#include "targetver.h"

#define _ATL_CSTRING_EXPLICIT_CONSTRUCTORS      // some CString constructors will be explicit

// turns off MFC's hiding of some common and often safely ignored warning messages
#define _AFX_ALL_WARNINGS

// Allow code to compile cleanly at warning level 4 ///////////////  
#pragma warning(disable:4001) // nonstandard extension 'single line comment' was used  
#pragma warning(disable:4100) // unreferenced formal parameter
#pragma warning(disable:4699) // Note: Creating precompiled header 
#pragma warning(disable:4710) // function not inlined
#pragma warning(disable:4514) // unreferenced inline function has been removed
#pragma warning(disable:4512) // assignment operator could not be generated
#pragma warning(disable:4245) // conversion from 'LONGLONG' to 'ULONGLONG', signed/unsigned mismatch
#pragma warning(disable:4312) // 'type cast' : conversion from 'LONG' to 'HINSTANCE' of greater size
#pragma warning(disable:4244) // 'argument' : conversion from 'LPARAM' to 'LONG', possible loss of data
#pragma warning(disable:4995) // 'wsprintf': name was marked as #pragma deprecated
#pragma warning(disable:4146) // unary minus operator applied to unsigned type, result still unsigned
#pragma warning(disable:4267) // 'argument' : conversion from 'size_t' to 'int', possible loss of data 
#pragma warning(disable:4201) // nonstandard extension used : nameless struct/union
#pragma warning(disable:4005) // The macro identifier is defined twice. The compiler uses the second macro definition.  
#pragma warning(disable:4018) // signed/unsigned mismatch  
#pragma warning(disable:4293) // shift count negative or too big, undefined behavior  
  
#include <afxwin.h>         // MFC core and standard components
#include <afxext.h>         // MFC extensions


#include <afxdisp.h>        // MFC Automation classes



#ifndef _AFX_NO_OLE_SUPPORT
	#include <afxdtctl.h>           // MFC support for Internet Explorer 4 Common Controls
#endif
#ifndef _AFX_NO_AFXCMN_SUPPORT
	#include <afxcmn.h>             // MFC support for Windows Common Controls
#endif // _AFX_NO_AFXCMN_SUPPORT

#include <afxcontrolbars.h>     // MFC support for ribbons and control bars



#include <vector>
#include <queue>
#include <ctime>
#include <iostream>
#include <fstream>
#include <StrSafe.h> // StringCchPrintf function

#include "resource.h"

#include "DataTypes.h" 
#include "Exceptions.h"
#include "Commons.h" 
#include "DialogHelpers.h"
#include "StopWatch.h"
#include "Mutex.h"
#include "WaitInfo.h"

#define _WIN32_DCOM // get CPU info using WMI

 

using namespace std;


#ifdef _UNICODE
	#if defined _M_IX86
		#pragma comment(linker,"/manifestdependency:\"type='win32' name='Microsoft.Windows.Common-Controls' version='6.0.0.0' processorArchitecture='x86' publicKeyToken='6595b64144ccf1df' language='*'\"")
	#elif defined _M_X64
		#pragma comment(linker,"/manifestdependency:\"type='win32' name='Microsoft.Windows.Common-Controls' version='6.0.0.0' processorArchitecture='amd64' publicKeyToken='6595b64144ccf1df' language='*'\"")
	#else
		#pragma comment(linker,"/manifestdependency:\"type='win32' name='Microsoft.Windows.Common-Controls' version='6.0.0.0' processorArchitecture='*' publicKeyToken='6595b64144ccf1df' language='*'\"")
	#endif
#endif


