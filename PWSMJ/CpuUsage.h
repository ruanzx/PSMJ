/////////////////////////////////////////////////////////////////////////////////////////////////
// Determine CPU usage of current process
// http://www.philosophicalgeek.com/2009/01/03/determine-cpu-usage-of-current-process-c-and-c/
// Add GetCpuTime function
/////////////////////////////////////////////////////////////////////////////////////////////////

#pragma once
#include <windows.h>

class CpuUsage
{
public:
	CpuUsage(void);
	
	void  Calculate();
	double GetCpuTime();
	double GetTotalCpuTime();
		short GetCpuUsage();
private:
	ULONGLONG SubtractTimes(const FILETIME& ftA, const FILETIME& ftB);
	bool EnoughTimePassed();
	inline bool IsFirstRun() const { return (m_dwLastRun == 0); }
	
	//system total times
	FILETIME m_ftPrevSysKernel;
	FILETIME m_ftPrevSysUser;

	//process times
	FILETIME m_ftPrevProcKernel;
	FILETIME m_ftPrevProcUser;

	short m_nCpuUsage;
	double m_nCpuTimeTotal;
	double m_nCpuTimeDiff;
	double m_nCpuTime[2]; // struct for prev and current cpu time

	ULONGLONG m_dwLastRun;
	
	volatile LONG m_lRunCount;
};
