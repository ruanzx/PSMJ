
// 
// Name: CpuWorkload.cpp : implementation file 
// Author: hieunt
// Description: Stress CPU with many workload
// Make CPU run with max frequency by add many work as you want
#pragma once
  


struct ThreadInfo
{
	class CpuWorkload* _this; 
	DOUBLE ElapsedTime;
	INT ThreadID;
};

class CpuWorkload
{

	DWORD WINAPI Run(LPVOID p);
	static DWORD WINAPI RunEx(LPVOID lpParam);

	HANDLE*    m_WorkerThreads;
	ThreadInfo*	m_ThreadInfo;

	volatile INT                m_NumWorkerThreads;
	volatile BOOL               m_KillWorkerThreads;
	volatile BOOL               m_IsStart;
	INT64                      m_CPUWorkDoneSinceLastCheck;
	INT							m_MaxWorkedThreadCount;
	HANDLE						m_ThreadWait;
	INT							m_JobLimit;
	

public:
	CpuWorkload();
	~CpuWorkload(); 

	VOID Start();
	VOID Stop();
	VOID SetNumThreads(FLOAT ThreadCount);
	VOID SetJobLimit(FLOAT Jobs);
	INT GetMaxNumThreads(); 
	INT GetWorkDone();
	INT Reset();
};