 
// 
// Name: CpuWorkload.cpp : implementation file 
// Author: hieunt
// Description: Stress CPU with many workload
//

#include "stdafx.h"
#include "CpuWorkload.h"

#define ARR_LEN 2048

/// <summary>
/// Initializes a new instance of the <see cref="CpuWorkload"/> class.
/// </summary>
CpuWorkload::CpuWorkload()
{
	m_NumWorkerThreads	= 1;
	m_KillWorkerThreads = FALSE;
	m_CPUWorkDoneSinceLastCheck = 0;
	m_JobLimit = 100000;
	m_IsStart = FALSE;
	SYSTEM_INFO sysinfo;
	GetSystemInfo( &sysinfo ); 
	m_MaxWorkedThreadCount = max(1, sysinfo.dwNumberOfProcessors);
}

/// <summary>
/// Starts this instance.
/// </summary>
VOID CpuWorkload::Start()
{
	m_IsStart = TRUE;
	
	m_WorkerThreads = new HANDLE[m_MaxWorkedThreadCount];
	m_ThreadInfo = new ThreadInfo[m_MaxWorkedThreadCount];

	m_ThreadWait = CreateEvent( 
		NULL,               // default security attributes
		TRUE,               // manual-reset event
		FALSE,              // initial state is nonsignaled
		TEXT("WriteEvent")  // object name
		); 


	for( int i = 0; i < m_MaxWorkedThreadCount; i++ )
	{
		m_ThreadInfo[i]._this = this;
		m_ThreadInfo[i].ThreadID = i;
		m_WorkerThreads[i] = CreateThread(NULL, ARR_LEN * 8 + 256 * 1024, (LPTHREAD_START_ROUTINE)RunEx, (LPVOID)&(m_ThreadInfo[i]), CREATE_SUSPENDED, NULL);

		SetThreadPriority( m_WorkerThreads[i], THREAD_PRIORITY_LOWEST );
		ResumeThread( m_WorkerThreads[i] );
	}
}

/// <summary>
/// Stops this instance.
/// </summary>
VOID CpuWorkload::Stop()
{
	m_KillWorkerThreads = TRUE;
	
	WaitForMultipleObjects(m_MaxWorkedThreadCount, m_WorkerThreads, TRUE, 1000);
	//delete [] m_WorkerThreads;
	//delete [] m_ThreadInfo;
	m_IsStart = FALSE;
	m_KillWorkerThreads=FALSE;
}

/// <summary>
/// Finalizes an instance of the <see cref="CpuWorkload"/> class.
/// </summary>
CpuWorkload::~CpuWorkload()
{ 
	if(m_KillWorkerThreads==FALSE)
	{
		m_KillWorkerThreads = TRUE;
		if(m_IsStart==TRUE)
		{
			WaitForMultipleObjects( m_MaxWorkedThreadCount, m_WorkerThreads, TRUE, 1000 );
			delete [] m_WorkerThreads;
			delete [] m_ThreadInfo;
		} 
	} 
}

DWORD WINAPI CpuWorkload::RunEx(LPVOID lpParam)
{
	ThreadInfo* pThreadInfo = (ThreadInfo*)(lpParam);
	pThreadInfo->_this->Run((LPVOID)(pThreadInfo));
	return 0;
}

DWORD WINAPI CpuWorkload::Run(LPVOID p)
{
	ThreadInfo* pThreadInfo = (ThreadInfo*)p;
	int MyID = (int)pThreadInfo->ThreadID;

	FLOAT m_Arr[2][ARR_LEN];

	for (int i=0;i<ARR_LEN;i++)
	{
		m_Arr[0][i] = rand() / (float)RAND_MAX;
		m_Arr[1][i] = rand() / (float)RAND_MAX;
	}

	pThreadInfo->ElapsedTime = 0;

	while( !m_KillWorkerThreads )
	{
		INT64 CurrentWorkProcessed = InterlockedOr64(&m_CPUWorkDoneSinceLastCheck,0);

		if(m_JobLimit!=-1)
		{
			if(CurrentWorkProcessed>(m_JobLimit-m_NumWorkerThreads))
			{
				ResetEvent(m_ThreadWait) ;
				WaitForSingleObject( m_ThreadWait, 1000 );
			}
		}		
		if( MyID >= m_NumWorkerThreads )
		{
			// No job for us? Relax.
			Sleep( 10 );
			continue;            
		}

		// Right! Do some number crunching.
		double avg = 0.0;
		for (int i=0;i<ARR_LEN-1;i++)
		{
			avg += m_Arr[1][i];
			m_Arr[0][i] = (float)fmod( pow( (double)m_Arr[1][i]*10.0, (double)m_Arr[1][i+1]+0.5 ), 1.0 );
		}
		m_Arr[0][ARR_LEN-1] = (float)(avg / (double)(ARR_LEN-1));

		avg = 0.0;
		for (int i=0;i<ARR_LEN-1;i++)
		{
			avg += m_Arr[0][i];
			m_Arr[1][i] = (float)fmod( pow( (double)m_Arr[0][i+1]*10.0, (double)m_Arr[0][i]+0.5 ), 1.0 );
		}
		m_Arr[1][ARR_LEN-1] = (float)(avg / (double)(ARR_LEN-1));

		// Aaand just repeat one more time
		avg = 0.0;
		for (int i=0;i<ARR_LEN-1;i++)
		{
			avg += m_Arr[1][i];
			m_Arr[0][i] = (float)fmod( pow( (double)m_Arr[1][i]*10.0, (double)m_Arr[1][i+1]+0.5 ), 1.0 );
		}
		m_Arr[0][ARR_LEN-1] = (float)(avg / (double)(ARR_LEN-1));

		avg = 0.0;
		for (int i=0;i<ARR_LEN-1;i++)
		{
			avg += m_Arr[0][i];
			m_Arr[1][i] = (float)fmod( pow( (double)m_Arr[0][i+1]*10.0, (double)m_Arr[0][i]+0.5 ), 1.0 );
		}
		m_Arr[1][ARR_LEN-1] = (float)(avg / (double)(ARR_LEN-1));


		InterlockedIncrement64( &m_CPUWorkDoneSinceLastCheck );
	}
	return 0;
}

INT CpuWorkload::GetWorkDone()
{
	INT64 workDone = 0;
	workDone = ::InterlockedExchange64( &m_CPUWorkDoneSinceLastCheck, 0 );

	return (INT)workDone;
}

INT CpuWorkload::Reset()
{
	SetEvent(m_ThreadWait);
	return 0;
}

VOID CpuWorkload::SetNumThreads(FLOAT ThreadCount)
{ 
	m_NumWorkerThreads = (INT)ThreadCount;
}

VOID CpuWorkload::SetJobLimit(FLOAT Jobs) 
{
	m_JobLimit = (INT)Jobs;
}

INT CpuWorkload::GetMaxNumThreads()
{ 
	return m_MaxWorkedThreadCount;
}
