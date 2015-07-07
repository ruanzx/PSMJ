

// 
// Name: StopWatch.h : implementation file 
// Author: hieunt
// Description: Helpers for analizing
//

#pragma once
 
class StopWatch 
{
public:
	StopWatch();
	~StopWatch();
	VOID Start();
	LONGLONG NowInMilliseconds() const;
	LONGLONG NowInMicroseconds() const; 
	LONGLONG NowInSeconds() const;
private:
	LARGE_INTEGER m_liPerfFreq; // Counts per second
	LARGE_INTEGER m_liPerfStart; // Starting count
};
  