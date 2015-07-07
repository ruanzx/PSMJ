
// 
// Name: StopWatch.h : implementation file 
// Author: hieunt
// Description: Helpers for analizing
//


#include "stdafx.h"
#include "StopWatch.h"

/// <summary>
/// Initializes a new instance of the <see cref="StopWatch"/> class.
/// </summary>
StopWatch::StopWatch() 
{ 
	QueryPerformanceFrequency(&m_liPerfFreq); 
	Start(); 
}
 
/// <summary>
/// Finalizes an instance of the <see cref="StopWatch"/> class.
/// </summary>
StopWatch::~StopWatch() 
{  
}

VOID StopWatch::Start() 
{ 
	QueryPerformanceCounter(&m_liPerfStart);
}

// Returns # of milliseconds since Start was called
LONGLONG StopWatch::NowInSeconds() const 
{ 
	LARGE_INTEGER liPerfNow;
	QueryPerformanceCounter(&liPerfNow);
	return((liPerfNow.QuadPart - m_liPerfStart.QuadPart) / m_liPerfFreq.QuadPart);
}

// Returns # of milliseconds since Start was called
LONGLONG StopWatch::NowInMilliseconds() const 
{ 
	LARGE_INTEGER liPerfNow;
	QueryPerformanceCounter(&liPerfNow);
	return(((liPerfNow.QuadPart - m_liPerfStart.QuadPart) * 1000) / m_liPerfFreq.QuadPart);
}

// Returns # of microseconds
LONGLONG StopWatch::NowInMicroseconds() const 
{ 
	// since Start was called
	LARGE_INTEGER liPerfNow;
	QueryPerformanceCounter(&liPerfNow);
	return(((liPerfNow.QuadPart - m_liPerfStart.QuadPart) * 1000000) / m_liPerfFreq.QuadPart);
}