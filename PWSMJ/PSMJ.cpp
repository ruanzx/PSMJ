
// 
// Name: PSMJ.cpp : implementation file 
// Author: hieunt
// Description:  Parallel Sort merge join
//				 Partition Source table File to Multiple part
//				 Join these run part in parallel
//


#include "stdafx.h"
#include "PSMJ.h"
#include <windows.h>
#include <timeapi.h>
#pragma comment(lib, "Winmm.lib") // timeBeginPeriod, timeEndPeriod

// Global variable use for power capping
extern Loggers g_Logger;
extern volatile DOUBLE g_ProcessUsage; // Process cpu utilizion
extern volatile DOUBLE g_ProcessPower; // Current process power
extern volatile DOUBLE g_PackagePower; // Processor package power
extern volatile DOUBLE g_CapPower; // Process cap power
extern volatile DWORD g_TimeBase; // Cap interval time
 
/// <summary>
/// Initializes a new instance of the <see cref="PSMJ"/> class.
/// </summary>
/// <param name="vParams">The v parameters.</param>
PSMJ::PSMJ(const PSMJ_PARAMS vParams) : m_PsmjParams(vParams)
{

	// https://msdn.microsoft.com/en-us/library/windows/desktop/dd757624(v=vs.85).aspx
	// Minimum timer resolution, in milliseconds, for the application or device driver. 
	// A lower value specifies a higher (more accurate) resolution.

	timeBeginPeriod(1); // highest resolution

	m_WorkerThreadNum = m_PsmjParams.THREAD_NUM;  

	m_TotalJoinCount = 0;

	utl = new PageHelpers2(); 

	m_FanInIdx = 0;    
	m_RunSize = 0; 
	isR = TRUE;
	// thread power capping exit?
	bQuitCapping = FALSE;
}
 
/// <summary>
/// Finalizes an instance of the <see cref="PSMJ"/> class.
/// </summary>
PSMJ::~PSMJ()
{
	// Must match each call to timeBeginPeriod with a call to timeEndPeriod, 
	// specifying the same minimum resolution in both calls.
	timeEndPeriod(1);

	delete utl;
} 
 
/// <summary>
/// Entry point, Executes this instance.
/// </summary>
/// <returns></returns>
RC PSMJ::Execute()
{
	RC rc;

	if(m_PsmjParams.USE_POWER_CAP==TRUE)
	{
		CHAR *capPath = new CHAR[MAX_PATH];   
		LPWSTR tempPath = new TCHAR[MAX_PATH]; 
		swprintf(tempPath, MAX_PATH, L"%s%s", m_PsmjParams.WORK_SPACE_PATH, L"power_capping.csv" );  
		size_t  count = wcstombs(capPath, tempPath, MAX_PATH);   

		fpCap=fopen(capPath, "w+b"); 

		delete tempPath;
		delete capPath; 
	}

	//////////////////////////////////////////////////////////////////////////


	//////////////////////////////////////////////////////////////////////////
	m_PemsParams.WORK_SPACE_PATH = m_PsmjParams.WORK_SPACE_PATH;
	m_PemsParams.BUFFER_POOL_SIZE = m_PsmjParams.BUFFER_POOL_SIZE;
	m_PemsParams.SORT_READ_BUFFER_SIZE = m_PsmjParams.SORT_READ_BUFFER_SIZE;
	m_PemsParams.SORT_WRITE_BUFFER_SIZE = m_PsmjParams.SORT_WRITE_BUFFER_SIZE;

	//#pragma chMSG(Fix this later merge READ & WRITE buffers at 128K & 2M)
	m_PemsParams.MERGE_READ_BUFFER_SIZE =  32 * SSD_PAGE_SIZE;
	m_PemsParams.MERGE_WRITE_BUFFER_SIZE = 512 * SSD_PAGE_SIZE;
	m_PemsParams.THREAD_NUM = m_PsmjParams.THREAD_NUM;

	SYSTEM_INFO sysinfo;
	GetSystemInfo(&sysinfo);
	if(m_PemsParams.THREAD_NUM==0)
		m_WorkerThreadNum = sysinfo.dwNumberOfProcessors; 

	//////////////////////////////////////////////////////////////////////////
	rc = PartitionPhase_CheckEnoughMemory();
	if(rc!=SUCCESS) { return rc; }

	g_Logger.Write("PSMJ Start...\n");

	bufferPool.size = m_PemsParams.BUFFER_POOL_SIZE; 
	bufferPool.currentSize = 0;

	// Creates a private heap object that can be used by the calling process. 
	// The function reserves space in the virtual address space of the process and allocates physical storage 
	// for a specified initial portion of this block.
	// C++ cannot allocate more than 400MB with new[] in MFC
	// http://msdn.microsoft.com/en-us/library/aa366599.aspx

	//HEAP_GENERATE_EXCEPTIONS: The system raises an exception to indicate failure (for example, an out-of-memory condition) for calls to HeapAlloc and HeapReAlloc instead of returning NULL.
	//HEAP_NO_SERIALIZE
	// NOTE: Must enable  Linker / System / Enable Large Address
	// https://msdn.microsoft.com/en-us/library/wz223b1z.aspx
	HANDLE hHeap = HeapCreate(0, 0, 0); //bufferPool.size // 0.0.0
	bufferPool.data  = (CHAR*)HeapAlloc(hHeap, 0, bufferPool.size);

	if(NOERROR != GetLastError())
	{
		ReportLastError();
		HeapDestroy(hHeap);
		return ERR_CANNOT_INITIAL_BUFFER_POOL;
	} 

	StopWatch stwTotalTime, stwJoinTime;
	rp.BufferPoolSize = m_PsmjParams.BUFFER_POOL_SIZE;
	rp.SortReadBufferSize = m_PsmjParams.SORT_READ_BUFFER_SIZE;
	rp.SortWriteBufferSize = m_PsmjParams.SORT_WRITE_BUFFER_SIZE;
	rp.MergeReadBufferSizeR = 0;
	rp.MergeWriteBufferSizeR = 0;
	rp.MergeReadBufferSizeS = 0;
	rp.MergeWriteBufferSizeS = 0;
	rp.JoinReadBufferSize = 0;
	rp.JoinWriteBufferSize = 0;  
	rp.TotalCpuTime[0] = GetCpuTime(); 
	rp.MergeCpuTimeR[0] = 0; rp.MergeCpuTimeR[1] = 0;
	rp.MergeCpuTimeS[0] = 0; rp.MergeCpuTimeS[1] = 0;
	rp.MergeExecTimeR = 0; rp.MergeExecTimeS = 0;

	//////////////////////////////////////////////////////////////////////////
	stwTotalTime.Start();
	//////////////////////////////////////////////////////////////////////////
	rc = PartitionPhase_PartitionTableR();
	if(rc!=SUCCESS) { return rc; }

	//////////////////////////////////////////////////////////////////////////
	rc = PartitionPhase_PartitionTableS();
	if(rc!=SUCCESS) { return rc; }

	//////////////////////////////////////////////////////////////////////////

	//rp.JoinReadBufferSize = rp.RunSize;
	//rp.JoinWriteBufferSize = 0;

	//////////////////////////////////////////////////////////////////////////
	stwJoinTime.Start();
	rp.JoinCpuTime[0] = GetCpuTime();
	//////////////////////////////////////////////////////////////////////////
	g_Logger.Write("Join Start...\n");
	rc = JoinPhase_Execute();
	if(rc!=SUCCESS) { return rc; }
	//////////////////////////////////////////////////////////////////////////
	rp.JoinCpuTime[1] = rp.TotalCpuTime[1] = GetCpuTime();
	//rp.TotalCpuTime[1] = GetCpuTime();  
	rp.JoinExecTime = stwJoinTime.NowInMilliseconds();

	rp.TotalExecTime = stwTotalTime.NowInMilliseconds();
	//////////////////////////////////////////////////////////////////////////

	WriteReport();

	if(m_PsmjParams.USE_POWER_CAP==TRUE)
	{ 
		fclose(fpCap); 
	}


	//////////////////////////////////////////////////////////////////////////
	g_Logger.Write("PSMJ End...\n");

	//////////////////////////////////////////////////////////////////////////
	BOOL bRet = HeapFree(hHeap, 0, bufferPool.data);
	bRet = HeapDestroy(hHeap);
	//////////////////////////////////////////////////////////////////////////
	return SUCCESS;
}

/// <summary>
/// Writes the report to disk.
/// </summary>
VOID PSMJ::WriteReport()
{ 
	FILE *fp;   
	CHAR *reportFilePath = new CHAR[MAX_PATH];   
	LPWSTR tempReportPath = new TCHAR[MAX_PATH];

	swprintf(tempReportPath, MAX_PATH, L"%s%s", m_PsmjParams.WORK_SPACE_PATH, L"PSMJ_Report.csv" ); 

	// convert file path to char  
	size_t  count = wcstombs(reportFilePath, tempReportPath, MAX_PATH); 

	// open file log to write
	fp=fopen(reportFilePath, "w+b"); 

	CHAR *reportContent = new CHAR[2048]; 

	sprintf(reportContent, "%s,%d,%d\n", "Memory Size", rp.BufferPoolSize,  rp.BufferPoolSize / (SSD_PAGE_SIZE * 256));  fprintf(fp, reportContent);
	sprintf(reportContent, "%s,%d\n", "THREAD Num", m_PsmjParams.THREAD_NUM);  fprintf(fp, reportContent);

	//////////////////////////////////////////////////////////////////////////
	sprintf(reportContent, "%s\n", "Partition Phase");  fprintf(fp, reportContent); 
	sprintf(reportContent, "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n", "Table","Relation Size","Thread Num","Run Size","Partition Num","Read Buffer Size(sort)","Write Buffer Size(sort)","Read Buffer Size (merge)","Write Buffer Size (merge)","Partition Time(ms)","CPU Time Parition", "Merge Time", "CPU Time Merge"); fprintf(fp, reportContent);
	sprintf(reportContent, "%s,%lld,%d,%d,%d,%d,%d,%d,%d,%lld,%.f,%lld,%.f\n",   "R"  ,rp.SourceTableSizeR, rp.PartitionThreadNum, rp.RunSize, rp.PartitionNumR, rp.SortReadBufferSize,  rp.SortWriteBufferSize, rp.MergeReadBufferSizeR,rp.MergeWriteBufferSizeR,  rp.PartitionExecTimeR, rp.PartitionCpuTimeR[1] - rp.PartitionCpuTimeR[0], rp.MergeExecTimeR, rp.MergeCpuTimeR[1] - rp.MergeCpuTimeR[0]);  fprintf(fp, reportContent);
	sprintf(reportContent, "%s,%lld,%d,%d,%d,%d,%d,%d,%d,%lld,%.f,%lld,%.f\n",   "S"  ,rp.SourceTableSizeS, rp.PartitionThreadNum, rp.RunSize, rp.PartitionNumS, rp.SortReadBufferSize,  rp.SortWriteBufferSize, rp.MergeReadBufferSizeS,rp.MergeWriteBufferSizeS,  rp.PartitionExecTimeS, rp.PartitionCpuTimeS[1] - rp.PartitionCpuTimeS[0], rp.MergeExecTimeS, rp.MergeCpuTimeS[1] - rp.MergeCpuTimeS[0]);  fprintf(fp, reportContent);

	//////////////////////////////////////////////////////////////////////////
	sprintf(reportContent, "%s\n", "Join Phase");  fprintf(fp, reportContent);
	sprintf(reportContent, "%s,%s,%s,%s,%s,  %s\n",   "Thread Num",       "Run Size","Read Buffer Size(join)","Write Buffer Size(join)","Join Time(ms)","CPU Time");  fprintf(fp, reportContent);
	rp.JoinReadBufferSize = rp.RunSize;
	sprintf(reportContent, "%d,%d,%d,%d,%lld,%.f\n",   rp.JoinThreadNum, rp.RunSize, rp.JoinReadBufferSize, rp.JoinWriteBufferSize, rp.JoinExecTime, rp.JoinCpuTime[1] - rp.JoinCpuTime[0]);  fprintf(fp, reportContent);

	//////////////////////////////////////////////////////////////////////////
	sprintf(reportContent, "%s, %d  \n", "Total Join Count", m_TotalJoinCount);  fprintf(fp, reportContent);
	sprintf(reportContent, "%s, %d  \n", "Total Partition Time", rp.PartitionExecTimeR+rp.PartitionExecTimeS+rp.MergeExecTimeR+rp.MergeExecTimeS);  fprintf(fp, reportContent);
	sprintf(reportContent, "%s, %d  \n", "Total Join Time", rp.JoinExecTime);  fprintf(fp, reportContent);
	sprintf(reportContent, "%s, %lld\n", "Total Exec Time", rp.TotalExecTime);  fprintf(fp, reportContent); 
	sprintf(reportContent, "%s, %.f \n", "Total Cpu Time", rp.TotalCpuTime[1] - rp.TotalCpuTime[0] ); fprintf(fp, reportContent);

	// Close file
	fclose(fp);

	delete reportFilePath;
	delete tempReportPath; 
	delete reportContent;
}

 
/// <summary>
/// Waits the other threads. Barrier use sync with other thread
/// </summary>
/// <param name="barrierPtr">The barrier PTR.</param>
/// <param name="bReset">Need reset ?</param>
VOID PSMJ::WaitOtherThreads(Barrier *barrierPtr,  BOOL bReset)
{ 
	barrierPtr->cs.Lock();   

	if (0 == InterlockedDecrement(&(barrierPtr->ThreadWaiting))) 
	{ 
		// are we the last ones to arrive?
		// at this point, all the other threads are blocking on the semaphore
		// so we can manipulate shared structures without having to worry
		// about conflicts

		//wprintf(L"---Thread %d is the last to arrive, releasing synchronization barrier [%d]\n", threadID, barrierIndex);
		//wprintf(L"===========================================\n");

		// Reset thread waiting counter
		barrierPtr->ThreadWaiting = barrierPtr->ThreadsCount;  

		// Reset queue S run to original
		if(bReset) {m_runtimeS = m_S;}

		ReleaseSemaphore(barrierPtr->hSemaphore, barrierPtr->ThreadsCount - 1, NULL); // "ThreadsCount - 1" because this last thread will not block on semaphore

		barrierPtr->cs.UnLock(); 
	}
	else 
	{ 
		// nope, there are other threads still working on the iteration so let's wait 
		barrierPtr->cs.UnLock(); // need to unlock or will have deadlock
		//wprintf(L"---Thread %d is waiting on synchronization barrier [%d]\n", threadID, barrierIndex);
		WaitForSingleObject(barrierPtr->hSemaphore, INFINITE); // note that return value should be validated at this point ;) 
	}
}


//////////////////////////////////////////////////////////////////////////
//
// PARTITION PHASE
//	
//////////////////////////////////////////////////////////////////////////

#pragma region "Partition Phase"

/// <summary>
/// Wrapper for sort phase function.
/// </summary>
/// <param name="lpParam">The thread parameter.</param>
/// <returns></returns>
DWORD WINAPI PSMJ::PartitionPhase_Ex(LPVOID lpParam)
{
	PartitionThreadParams* p = (PartitionThreadParams*)(lpParam);
	p->_this->PartitionPhase_Func((void *)(p));
	return 0;
}

/// <summary>
/// Sort phase function.
/// </summary>
/// <param name="lpParam">The thread parameter.</param>
/// <returns></returns>
DWORD WINAPI PSMJ::PartitionPhase_Func(LPVOID lpParam)
{ 
	PartitionThreadParams *p = (PartitionThreadParams *)lpParam;
	DoubleBuffer *dbcWrite = p->dbcWrite;    

	RC rc;

	DWORD inputBufferCount = p->inputBufferCount; 

	p->inputBufferIndex = 0;

	// 1st read into BACK input buffer, then swap BACK to FRONT
	for(DWORD i=0; i<inputBufferCount; i++)
	{
		p->dbcRead[i]->LockProducer();

		rc = PartitionPhase_Read(p, i);   

		// Wait for Read complete  
		if(rc == SUCCESS)
		{
			GetOverlappedResult(p->hInputFile, &p->overlapRead.overlap, &p->overlapRead.dwBytesReadWritten, TRUE);   
			utl->ComputeBuffer(BACK_BUFFER(p->dbcRead[i]), p->overlapRead.dwBytesReadWritten);
		}
		p->inputBufferIndex++;  
		p->overlapRead.chunkIndex++;

		p->dbcRead[i]->UnLockProducer(); 
		p->dbcRead[i]->SwapBuffers();   

		if(p->dbcRead[i]->bFirstProduce==TRUE)
			p->dbcRead[i]->bFirstProduce=FALSE;
	}

	// Reset buffer index
	p->inputBufferIndex = 0; 

	BOOL bContinue  = TRUE;

	DWORD fanIn = inputBufferCount; 
	DWORD chunkMerged = 0;

	// Produce into BACK output buffer
	while(bContinue)
	{   
		// prefetch read, sort FRONT input buffer
		for(DWORD i=0; i<inputBufferCount; i++)
		{ 
			p->dbcRead[i]->LockProducer();
			rc = PartitionPhase_Read(p, i); // read + sort FRONT, current BACK is not sort  

			PartitionPhase_Sort(p, i);

			if(rc==SUCCESS) // Wait for Read complete 
			{  
				GetOverlappedResult(p->hInputFile, &p->overlapRead.overlap, &p->overlapRead.dwBytesReadWritten, TRUE);   
				utl->ComputeBuffer(BACK_BUFFER(p->dbcRead[i]),  p->overlapRead.dwBytesReadWritten); 
			}
			p->inputBufferIndex++;
			p->overlapRead.chunkIndex++;
			p->dbcRead[i]->UnLockProducer(); 
		}  

		PartitionPhase_Merge(p); // in-memory merg, merge k input-buffer

		chunkMerged += fanIn; 

		for(DWORD i = 0; i < inputBufferCount; i++) 
		{ 
			p->dbcRead[i]->SwapBuffers();   
		}  

		p->inputBufferIndex = 0; 

		if(chunkMerged > p->overlapRead.totalChunk)
		{
			bContinue = FALSE; 
		} 
	}    //  while bContinue  

	//// Cleaning, release memory 
	m_CS.Lock();
	while(p->fanIns.size() > 0)
	{
		FANS *fan = new FANS();
		fan = p->fanIns.back();
		p->fanIns.pop_back();

		m_FanIns.push(fan);  
	} 
	m_CS.UnLock(); 

	return 0;
}

/// <summary>
/// Check enough memory for sort phase.
/// </summary>
/// <returns></returns>
RC PSMJ::PartitionPhase_CheckEnoughMemory()
{
	// Estimate memory 
	DOUBLE m_TotalMemory = m_PemsParams.BUFFER_POOL_SIZE; 
	m_MemoryEachThreadSize = m_TotalMemory / m_WorkerThreadNum;

	// Caculate ouput sort buffer
	DWORD memoryForWriteSortRun = 0;
	memoryForWriteSortRun = (m_PemsParams.SORT_WRITE_BUFFER_SIZE * 2) * m_WorkerThreadNum;

	m_TotalMemory = m_TotalMemory - memoryForWriteSortRun;

	if( m_TotalMemory <= 0)
	{
		ShowMB(L"Not enough momory, Sort Write Buffer is too big");
		return ERR_NOT_ENOUGH_MEMORY; 
	}

	// Calculate inputBuffer num with current size  
	DWORD maxQuickSortItem = (m_PemsParams.SORT_READ_BUFFER_SIZE / 4096) * 40; // The maximum tuple for quick sort 

	DWORD memoryForQuickSort = 0;
	memoryForQuickSort = (maxQuickSortItem * sizeof(UINT64)) + (maxQuickSortItem * TUPLE_SIZE * sizeof(CHAR)) + SSD_PAGE_SIZE;
	memoryForQuickSort = memoryForQuickSort * m_WorkerThreadNum; 

	m_TotalMemory = m_TotalMemory - memoryForQuickSort;

	if( m_TotalMemory <= 0)
	{
		ShowMB(L"Not enough memory for quick sort");
		return ERR_NOT_ENOUGH_MEMORY; 
	}

	// (LoserKey + LoserTreeData + MergeBuffer + ReadInputBuffer*2) * NumberOfThread
	DWORD memoryForLoserTreeTuple = sizeof(UINT64) + (TUPLE_SIZE * sizeof(CHAR));
	DWORD memoryForMergePage = SSD_PAGE_SIZE;
	DWORD memoryForSortRun = m_PemsParams.SORT_READ_BUFFER_SIZE * 2;

	m_InputBufferNum = m_TotalMemory / ((memoryForLoserTreeTuple+memoryForMergePage+memoryForSortRun) * m_WorkerThreadNum);

	if( m_InputBufferNum == 0)
	{
		ShowMB(L"Not enough memory\nSort Read Buffer Size is too big\nNumber of Input buffer is zero");
		return ERR_NOT_ENOUGH_MEMORY; 
	}

	m_TotalMemory = m_TotalMemory - (m_InputBufferNum * m_WorkerThreadNum);

	if( m_TotalMemory <= 0 )
	{ 
		ShowMB(L"Not enough memory for %d thread\nIncrease pool size or choose other buffer size");
		return ERR_NOT_ENOUGH_MEMORY; 
	}   

	rp.RunSize = m_PemsParams.SORT_READ_BUFFER_SIZE * m_InputBufferNum;
	m_RunSize = m_PemsParams.SORT_READ_BUFFER_SIZE * m_InputBufferNum;
	return SUCCESS; 
}

/// <summary>
/// Initialize sort phase.
/// </summary>
/// <param name="isR">Current table is small table?</param>
/// <returns></returns>
RC PSMJ::PartitionPhase_Initialize(BOOL isR)
{    
	RC rc;   

	m_hWorkerThread = new HANDLE[m_WorkerThreadNum];
	m_PartitionParams = new PartitionThreadParams[m_WorkerThreadNum]; 

	rp.PartitionThreadNum = m_WorkerThreadNum;

	m_hSourceTable=CreateFile(
		(LPCWSTR)m_PemsParams.SORT_FILE_PATH, // file to open
		GENERIC_READ,			// open for reading
		FILE_SHARE_READ,        // share for reading
		NULL,					// default security
		OPEN_EXISTING,			// existing file only
		FILE_FLAG_OVERLAPPED | FILE_FLAG_NO_BUFFERING,	// overlapped operation //| FILE_FLAG_NO_BUFFERING
		NULL);					// no attr. template

	if (INVALID_HANDLE_VALUE==m_hSourceTable) 
	{   
		ShowMB(L"Cannot open file %s\r\n", m_PemsParams.SORT_FILE_PATH);
		return ERR_CANNOT_CREATE_HANDLE; 
	} 

	LARGE_INTEGER  liSourceFileSize = {0}; 

	if (!GetFileSizeEx(m_hSourceTable, &liSourceFileSize))
	{       
		ShowMB(L"Cannot get size of file %s\r\n", m_PemsParams.SORT_FILE_PATH);
		return ERR_CANNOT_GET_FILE_SIZE;
	} 

	if(isR)
		rp.SourceTableSizeR = liSourceFileSize.QuadPart;
	else 
		rp.SourceTableSizeS = liSourceFileSize.QuadPart;

	DWORD chunkSize = m_PemsParams.SORT_READ_BUFFER_SIZE; 
	UINT64 totalChunk = chROUNDUP(liSourceFileSize.QuadPart, chunkSize) / chunkSize;

	// Init default value
	for (DWORD i = 0; i < m_WorkerThreadNum; i++)
	{
		m_PartitionParams[i]._this = this; ///////////// Importance
		m_PartitionParams[i].threadID = i; 
		m_PartitionParams[i].keyPosition = m_PemsParams.KEY_POS;  
		m_PartitionParams[i].tempFanIn = new FANS(); 
		m_PartitionParams[i].hInputFile = m_hSourceTable; 

		m_PartitionParams[i].inputBufferCount = m_InputBufferNum;

		// để xem đang đọc từ cái buffer nào
		m_PartitionParams[i].inputBufferIndex = 0;     

		m_PartitionParams[i].dbcRead = new DoubleBuffer*[m_InputBufferNum];  
		for (DWORD j=0; j<m_InputBufferNum; j++)
		{
			// Front input buffer
			m_PartitionParams[i].dbcRead[j] = new DoubleBuffer(m_PemsParams.SORT_READ_BUFFER_SIZE); 
			rc = utl->InitBuffer(m_PartitionParams[i].dbcRead[j]->buffer[0], m_PemsParams.SORT_READ_BUFFER_SIZE, &bufferPool);
			if(rc!=SUCCESS) {return rc;}
			rc = utl->InitBuffer(m_PartitionParams[i].dbcRead[j]->buffer[1], m_PemsParams.SORT_READ_BUFFER_SIZE, &bufferPool);  
			if(rc!=SUCCESS) {return rc;}
		} 

		////////////////////////////////////////////////////////////////////////// 
		m_PartitionParams[i].dbcWrite = new DoubleBuffer(m_PemsParams.SORT_WRITE_BUFFER_SIZE);    
		rc = utl->InitBuffer(m_PartitionParams[i].dbcWrite->buffer[0], m_PemsParams.SORT_WRITE_BUFFER_SIZE, &bufferPool);
		if(rc!=SUCCESS) {return rc;}

		rc = utl->InitBuffer(m_PartitionParams[i].dbcWrite->buffer[1], m_PemsParams.SORT_WRITE_BUFFER_SIZE, &bufferPool); 
		if(rc!=SUCCESS) {return rc;}
		//////////////////////////////////////////////////////////////////////////

		// Init run buffer
		rc = utl->InitBuffer(m_PartitionParams[i].runPageBuffer, SSD_PAGE_SIZE, &bufferPool); 
		if(rc!=SUCCESS) {return rc;}

		//////////////////////////////////////////////////////////////////////////
		// Init runpage 
		rc = utl->InitRunPage(m_PartitionParams[i].runPage, m_PartitionParams[i].runPageBuffer); 
		if(rc!=SUCCESS) {return rc;}
		//////////////////////////////////////////////////////////////////////////
		//Working space buffer 
		//////////////////////////////////////////////////////////////////////////
#pragma chMSG(How many quicksort items)
		DWORD maxQuickSortItem = (m_PemsParams.SORT_READ_BUFFER_SIZE / 4096) * 40; // temporary assume one page have maximum 40 tuples
		DWORD sortBufferSize = maxQuickSortItem * sizeof(UINT64);

		// temp record for quicksort
		//m_PartitionParams[i].sortRecordPtr = new RECORD();

		// Quick sort page buffer
		rc = utl->InitBuffer(m_PartitionParams[i].quickSortPageBuffer, SSD_PAGE_SIZE, &bufferPool);  
		if(rc!=SUCCESS) {return rc;}

		rc = utl->InitBuffer(m_PartitionParams[i].quickSortDataBuffer, maxQuickSortItem * TUPLE_SIZE * sizeof(CHAR), &bufferPool);
		if(rc!=SUCCESS) {return rc;}
		// Quick sort data buffer  
		m_PartitionParams[i].quickSort = new QuickSort(maxQuickSortItem, m_PartitionParams[i].quickSortDataBuffer);

		// Init in memory merge buffer
		m_PartitionParams[i].memoryMergeBuffer = new Buffer[m_InputBufferNum];
		for (DWORD j = 0; j < m_InputBufferNum; j++)
		{
			rc = utl->InitBuffer(m_PartitionParams[i].memoryMergeBuffer[j], SSD_PAGE_SIZE, &bufferPool); 
			if(rc!=SUCCESS) {return rc;}
		}

		m_PartitionParams[i].overlapRead.dwBytesToReadWrite = m_PemsParams.SORT_READ_BUFFER_SIZE;  
		m_PartitionParams[i].overlapRead.dwBytesReadWritten = 0;  
		m_PartitionParams[i].overlapRead.startChunk = 0;  
		m_PartitionParams[i].overlapRead.chunkIndex = 0;  
		m_PartitionParams[i].overlapRead.endChunk = 0;  
		m_PartitionParams[i].overlapRead.totalChunk = 0; 
		m_PartitionParams[i].overlapRead.overlap.Offset = 0; 
		m_PartitionParams[i].overlapRead.overlap.OffsetHigh = 0; 

		m_PartitionParams[i].overlapWrite.dwBytesToReadWrite = m_PemsParams.SORT_WRITE_BUFFER_SIZE; 
		m_PartitionParams[i].overlapWrite.dwBytesReadWritten = 0; 
		m_PartitionParams[i].overlapWrite.overlap.Offset = 0;
		m_PartitionParams[i].overlapWrite.overlap.OffsetHigh = 0;
	} 

	// allocate chunk to each thread, make them equal works, the last thread may be less than others
	UINT64 temp = totalChunk;
	while (temp > 0)
	{
		for (DWORD i = 0; i < m_WorkerThreadNum; i++)
		{
			m_PartitionParams[i].overlapRead.totalChunk++;
			temp--;
			if(temp==0) { break; }
		}  
	}
	temp = totalChunk;
	DWORD startChunk = 0;
	while (temp > 0)
	{
		for (DWORD i = 0; i < m_WorkerThreadNum; i++)
		{ 
			m_PartitionParams[i].overlapRead.startChunk = startChunk; 
			m_PartitionParams[i].overlapRead.chunkIndex = startChunk; 
			startChunk += m_PartitionParams[i].overlapRead.totalChunk;
			m_PartitionParams[i].overlapRead.endChunk = startChunk - 1;  
			temp -= m_PartitionParams[i].overlapRead.totalChunk;
			if(temp==0) { break; }
		}  	
	}

	return SUCCESS;
}

/// <summary>
/// Execute sort phase.
/// </summary>
/// <param name="isR">Current table is small table?</param>
/// <returns></returns>
RC PSMJ::PartitionPhase_Execute(BOOL isR)
{ 
	if(SUCCESS!=PartitionPhase_Initialize(isR))
	{ 
		ShowMB(L"Cannot Initialize ExsPartitionPhase");
		return ERR_CANNOT_INITIAL_MEMORY;
	}

	for(DWORD i = 0; i < m_WorkerThreadNum; i++ )
	{  
		m_hWorkerThread[i] = CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)PartitionPhase_Ex, (LPVOID)&(m_PartitionParams[i]), CREATE_SUSPENDED, NULL);

		SetThreadPriority(m_hWorkerThread[i], THREAD_PRIORITY_NORMAL);
		ResumeThread(m_hWorkerThread[i]); 
	}

	//////////////////////////////////////////////////////////////////////////
	// Power capping monitor
	////////////////////////////////////////////////////////////////////////// 
	if(m_PsmjParams.USE_POWER_CAP==TRUE)
	{
		cappingParams._this = this;
		bQuitCapping = FALSE; 

		hPowerCapThread = CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)PowerCapEx, (LPVOID)&(cappingParams), CREATE_SUSPENDED, NULL);

		// Set monitor thread to higher priority
		SetThreadPriority(hPowerCapThread, THREAD_PRIORITY_TIME_CRITICAL);
		ResumeThread(hPowerCapThread); 
	}

	// Wait for partition thread exit
	WaitForMultipleObjects(m_WorkerThreadNum, m_hWorkerThread, TRUE, INFINITE); 

	// Wait for capping thread exit
	//////////////////////////////////////////////////////////////////////////
	if(m_PsmjParams.USE_POWER_CAP==TRUE)
	{
		bQuitCapping = TRUE;
		WaitForSingleObject(hPowerCapThread, INFINITE);
		CloseHandle(hPowerCapThread); 
	}

	// Cleaning
	for(DWORD i = 0; i < m_WorkerThreadNum; i++ )
	{
		CloseHandle(m_hWorkerThread[i]);
	}

	CloseHandle(m_hSourceTable);

	return SUCCESS;
}

/// <summary>
/// Partitions the small table.
/// </summary>
/// <returns></returns>
RC PSMJ::PartitionPhase_PartitionTableR()
{
	RC rc;
	isR = TRUE;
	// Partition R
	bufferPool.currentSize = 0;
	m_RangePartition.clear();
	//////////////////////////////////////////////////////////////////////////  
	m_PemsParams.SORT_FILE_PATH = m_PsmjParams.RELATION_R_PATH;
	m_PemsParams.FILE_NAME_NO_EXT = m_PsmjParams.RELATION_R_NO_EXT;
	m_PemsParams.KEY_POS = m_PsmjParams.R_KEY_POS;
	//////////////////////////////////////////////////////////////////////////

	StopWatch  stwPartitionTime;
	stwPartitionTime.Start();

	rp.PartitionCpuTimeR[0] = GetCpuTime(); 
	rc = PartitionPhase_Execute(TRUE);  
	if(rc!=SUCCESS) { return rc; }

	rp.PartitionCpuTimeR[1] = GetCpuTime(); 
	rp.PartitionExecTimeR = stwPartitionTime.NowInMilliseconds(); 
	rp.PartitionNumR = m_FanIns.size();

	while(m_FanIns.size() > 0)
	{
		FANS *_fan = new FANS();
		_fan = m_FanIns.front(); 
		m_FanIns.pop();

		m_R.push(_fan);  
	}  

	//////////////////////////////////////////////////////////////////////////

	if( m_PsmjParams.PLAN_FOR_JOIN == JOIN_PLAN::PLAN_3 )
	{ 
		StopWatch  stwMergeTime;
		stwMergeTime.Start();

		rp.MergeCpuTimeR[0] = GetCpuTime();  
		rc = PartitionPhase_FinalMerge(m_R, m_PemsParams.KEY_POS);
		if(rc!=SUCCESS) {return rc;}

		rp.MergeCpuTimeR[1] = GetCpuTime(); 
		rp.MergeExecTimeR = stwMergeTime.NowInMilliseconds(); 

		/*
		FILE* fso=fopen("C:\\R_Range.csv","w+b");
		CHAR *debugContent = new CHAR[1024];
		for(UINT rIdx=0; rIdx < m_RangePartition.size(); rIdx++)
		{   
		sprintf(debugContent, "%d, %lld, %lld, %lld, %lld\n", 
		rIdx,
		m_RangePartition[rIdx].fileOffsetStart.QuadPart, 
		m_RangePartition[rIdx].fileOffsetEnd.QuadPart,
		m_RangePartition[rIdx].lowestKey,
		m_RangePartition[rIdx].highestKey);  
		fprintf(fso, debugContent);  
		}  
		delete debugContent;
		fclose(fso); 
		*/

		m_RRangePartition = m_RangePartition;
		m_RangePartition.clear();


	}

	return SUCCESS;
}

/// <summary>
/// Partitions the large table.
/// </summary>
/// <returns></returns>
RC PSMJ::PartitionPhase_PartitionTableS()
{
	g_Logger.Write("Partition S Start...\n");
	RC rc;
	isR = FALSE;
	// Partition S
	bufferPool.currentSize = 0;
	m_RangePartition.clear();
	////////////////////////////////////////////////////////////////////////// 
	m_PemsParams.SORT_FILE_PATH = m_PsmjParams.RELATION_S_PATH;
	m_PemsParams.FILE_NAME_NO_EXT = m_PsmjParams.RELATION_S_NO_EXT;
	m_PemsParams.KEY_POS = m_PsmjParams.S_KEY_POS;
	//////////////////////////////////////////////////////////////////////////

	StopWatch  stwPartitionTime;
	stwPartitionTime.Start(); 
	rp.PartitionCpuTimeS[0] = GetCpuTime(); 

	rc = PartitionPhase_Execute(FALSE); 
	if(rc!=SUCCESS) { return rc; }

	rp.PartitionCpuTimeS[1] = GetCpuTime(); 
	rp.PartitionExecTimeS = stwPartitionTime.NowInMilliseconds(); 
	rp.PartitionNumS = m_FanIns.size();

	while(m_FanIns.size() > 0)
	{
		FANS *fan = new FANS();
		fan = m_FanIns.front(); // First in first out
		m_FanIns.pop();

		m_S.push(fan);  
	} 

	if(m_PsmjParams.PLAN_FOR_JOIN == JOIN_PLAN::PLAN_3)
	{
		StopWatch  stwMergeTime;
		stwMergeTime.Start(); 
		rp.MergeCpuTimeS[0] = GetCpuTime();

		rc = PartitionPhase_FinalMerge(m_S, m_PemsParams.KEY_POS);
		if(rc!=SUCCESS) {return rc;}

		rp.MergeCpuTimeS[1] = GetCpuTime(); 
		rp.MergeExecTimeS = stwMergeTime.NowInMilliseconds(); 

		/*
		FILE* fso=fopen("C:\\S_Range.csv","w+b");
		CHAR *debugContent = new CHAR[1024];
		for(UINT sIdx=0; sIdx < m_RangePartition.size(); sIdx++)
		{   
		sprintf(debugContent, "%d, %lld, %lld, %lld, %lld\n", 
		sIdx,
		m_RangePartition[sIdx].fileOffsetStart.QuadPart, 
		m_RangePartition[sIdx].fileOffsetEnd.QuadPart,
		m_RangePartition[sIdx].lowestKey,
		m_RangePartition[sIdx].highestKey);  
		fprintf(fso, debugContent);  
		}   
		delete debugContent;
		fclose(fso); 
		*/
		m_SRangePartition = m_RangePartition; 
		m_RangePartition.clear();
	}

	if(m_PsmjParams.PLAN_FOR_JOIN == JOIN_PLAN::PLAN_4)
	{
		rp.MergeCpuTimeS[0] = rp.MergeCpuTimeS[1] = 0;
		rp.MergeExecTimeS = 0;
	}

	return SUCCESS;
}

/// <summary>
/// Read from disk to buffer.
/// </summary>
/// <param name="lpParam">The thread parameter.</param>
/// <param name="bufferIndex">Index of the buffer.</param>
/// <returns></returns>
RC PSMJ::PartitionPhase_Read(LPVOID lpParam, DWORD bufferIndex)
{
	PartitionThreadParams *p = (PartitionThreadParams *)lpParam;

	// If current thread chunk to be read is over chunk has been allocated we mark them with MAX page
	if ( p->overlapRead.chunkIndex > p->overlapRead.endChunk )  
	{   
		utl->ResetBuffer(BACK_BUFFER(p->dbcRead[bufferIndex]));
		// Add max page to buffer
		utl->AddPageToBuffer(BACK_BUFFER(p->dbcRead[bufferIndex]), NULL, SSD_PAGE_SIZE); 

		BACK_BUFFER(p->dbcRead[bufferIndex]).currentPageIndex = 0;
		BACK_BUFFER(p->dbcRead[bufferIndex]).currentSize = SSD_PAGE_SIZE;
		BACK_BUFFER(p->dbcRead[bufferIndex]).pageCount = 1;
		BACK_BUFFER(p->dbcRead[bufferIndex]).tupleCount = utl->GetTupleNumInMaxPage(); 
		BACK_BUFFER(p->dbcRead[bufferIndex]).isSort = TRUE;
		BACK_BUFFER(p->dbcRead[bufferIndex]).isFullMaxValue = TRUE;
		return ERR_END_OF_FILE;
	}  

	p->overlapRead.fileSize.QuadPart = p->overlapRead.chunkIndex * m_PemsParams.SORT_READ_BUFFER_SIZE; 
	p->overlapRead.overlap.Offset = p->overlapRead.fileSize.LowPart;
	p->overlapRead.overlap.OffsetHigh = p->overlapRead.fileSize.HighPart;  

	// Attempt an asynchronous read operation. 
	ReadFile(p->hInputFile, BACK_BUFFER(p->dbcRead[bufferIndex]).data, p->overlapRead.dwBytesToReadWrite, &p->overlapRead.dwBytesReadWritten, &p->overlapRead.overlap);   

	return SUCCESS;
}
 
/// <summary>
/// Merge mini-runs in memory.
/// </summary>
/// <param name="lpParam">The lp parameter.</param>
/// <returns></returns>
RC PSMJ::PartitionPhase_Merge(LPVOID lpParam)
{ 
	PartitionThreadParams *p = (PartitionThreadParams *)lpParam;
	DoubleBuffer *dbcWrite = p->dbcWrite;
	DoubleBuffer **dbcRead = p->dbcRead;

	DWORD k = p->inputBufferCount; 
	LoserTree lsTree(k);
	RECORD *recordPtr = new RECORD();  


	// Prepare data for loser tree
	for(DWORD i = 0; i < k; i++) 
	{     
		FRONT_BUFFER(dbcRead[i]).currentPageIndex = 0;    
		utl->ResetBuffer(p->memoryMergeBuffer[i]); 
		utl->GetPageInfo(FRONT_BUFFER(dbcRead[i]).data, p->memoryMergeBuffer[i], 0, SSD_PAGE_SIZE); //pageIndex = 0  
		PartitionPhase_GetNextTuple(p, recordPtr, i); 
		lsTree.AddNewNode(recordPtr, i);    
	}    

	lsTree.CreateLoserTree();  

	dbcWrite->bFirstProduce = TRUE;
	utl->ResetBuffer(BACK_BUFFER(dbcWrite));
	utl->ResetBuffer(FRONT_BUFFER(dbcWrite));

	utl->ResetRunPage(p->runPage, p->runPageBuffer);  

	dbcWrite->LockProducer();

	DWORD tupleCount = 0;
	INT lsIdx = 0; // index in loser tree point to minimum record 
	BOOL bFirstWrite = TRUE;
	BOOL bLowestIsGet = FALSE;  

	PartitionPhase_CreateNewRun(p);

	while(TRUE)  
	{   
		lsTree.GetMinRecord(recordPtr, lsIdx); // index = loserTree[0]

		if(recordPtr->key==MAX) { break; }

		if(bLowestIsGet==FALSE)
		{
			p->tempFanIn->lowestKey = recordPtr->key;
			bLowestIsGet = TRUE;
		}
		p->tempFanIn->highestKey = recordPtr->key;

		p->runPage->consumed = FALSE;

		if(utl->IsPageFull(p->runPage))
		{  
			utl->AddPageToBuffer(BACK_BUFFER(dbcWrite),  p->runPage->page, SSD_PAGE_SIZE); 
			p->runPage->consumed = TRUE;

			// Reset page về ban đầu, bắt đầu ghép 1 page mới  
			utl->ResetRunPage(p->runPage, p->runPageBuffer);

			BACK_BUFFER(dbcWrite).currentPageIndex++;
			BACK_BUFFER(dbcWrite).pageCount++;
		} 

		if(utl->IsBufferFull(BACK_BUFFER(dbcWrite))) // write buffer is full
		{    
			dbcWrite->UnLockProducer();   

			if(dbcWrite->bFirstProduce) // Back Buffer is full, check is it the first product
			{ 
				dbcWrite->bFirstProduce=FALSE;  
				dbcWrite->SwapBuffers();   
				dbcWrite->LockConsumer();    
				PartitionPhase_Write(p); // Execute async write FRONT, continue merge in to BACK 
			}
			else
			{    
				//Wait for write complete   
				GetOverlappedResult(p->hFanOut, &p->overlapWrite.overlap, &p->overlapWrite.dwBytesReadWritten, TRUE) ;  

				p->overlapWrite.fileSize.QuadPart += p->overlapWrite.dwBytesReadWritten;  
				p->overlapWrite.overlap.Offset = p->overlapWrite.fileSize.LowPart;
				p->overlapWrite.overlap.OffsetHigh = p->overlapWrite.fileSize.HighPart;  
				p->overlapWrite.dwBytesReadWritten = 0;

				p->tempFanIn->pageCount += FRONT_BUFFER(dbcWrite).pageCount;
				p->tempFanIn->tupleCount += FRONT_BUFFER(dbcWrite).tupleCount;

				utl->ResetBuffer(FRONT_BUFFER(dbcWrite));  
				dbcWrite->UnLockConsumer(); 

				dbcWrite->SwapBuffers();  

				dbcWrite->LockConsumer();  
				PartitionPhase_Write(p);  // Execute async write FRONT, continue merge in to BACK
			}  
			dbcWrite->LockProducer();  
		} 

		utl->AddTupleToPage(p->runPage, recordPtr->data, p->runPageBuffer);  
		BACK_BUFFER(dbcWrite).tupleCount++;
		tupleCount++;

		PartitionPhase_GetNextTuple(p, recordPtr, lsIdx); 

		lsTree.AddNewNode(recordPtr, lsIdx);// Add this tuple to loser tree  
		lsTree.Adjust(lsIdx); //Shrink loser tree 
	}   // end while

	//Wait until current write process complete 
	GetOverlappedResult(p->hFanOut, &p->overlapWrite.overlap, &p->overlapWrite.dwBytesReadWritten, TRUE); 
	if(p->overlapWrite.dwBytesReadWritten > 0)
	{
		if(FRONT_BUFFER(dbcWrite).currentSize > 0)
		{
			p->overlapWrite.fileSize.QuadPart += p->overlapWrite.dwBytesReadWritten;  
			p->overlapWrite.overlap.Offset = p->overlapWrite.fileSize.LowPart;
			p->overlapWrite.overlap.OffsetHigh = p->overlapWrite.fileSize.HighPart;  

			p->tempFanIn->pageCount += FRONT_BUFFER(dbcWrite).pageCount;
			p->tempFanIn->tupleCount += FRONT_BUFFER(dbcWrite).tupleCount;

			p->overlapWrite.dwBytesReadWritten = 0;

			utl->ResetBuffer(FRONT_BUFFER(dbcWrite));  
		}
	}

	//////////////////////////////////////////////////////////////////////////

	// If the last page has not consumed
	if((p->runPage->consumed==FALSE) && (utl->IsBufferFull(BACK_BUFFER(dbcWrite))==FALSE))
	{ 
		if(!utl->IsEmptyPage(p->runPage))
		{
			utl->AddPageToBuffer(BACK_BUFFER(dbcWrite),  p->runPage->page, SSD_PAGE_SIZE);  
			p->runPage->consumed = TRUE; 
			BACK_BUFFER(dbcWrite).currentPageIndex++;
			BACK_BUFFER(dbcWrite).pageCount++;    
		}
		else
		{
			//int fuck=0;
		}
	}

	dbcWrite->UnLockProducer();

	dbcWrite->UnLockConsumer();

	if(BACK_BUFFER(dbcWrite).currentSize > 0)
	{    
		//write completed , swap buffer
		dbcWrite->SwapBuffers();   
		// Write the last run to disk  
		dbcWrite->LockConsumer(); 

		PartitionPhase_Write(p);

		GetOverlappedResult(p->hFanOut, &p->overlapWrite.overlap, &p->overlapWrite.dwBytesReadWritten, TRUE);

		p->overlapWrite.fileSize.QuadPart += p->overlapWrite.dwBytesReadWritten;  
		p->overlapWrite.overlap.Offset = p->overlapWrite.fileSize.LowPart;
		p->overlapWrite.overlap.OffsetHigh = p->overlapWrite.fileSize.HighPart;  

		p->tempFanIn->pageCount += FRONT_BUFFER(dbcWrite).pageCount;
		p->tempFanIn->tupleCount += FRONT_BUFFER(dbcWrite).tupleCount;

		utl->ResetBuffer(FRONT_BUFFER(dbcWrite)); 
		dbcWrite->UnLockConsumer();
	}

	// Terminate current run
	PartitionPhase_TerminateRun(p, tupleCount);

	//Reset read buffer
	for(DWORD i = 0; i < k; i++) 
	{   
		utl->ResetBuffer(FRONT_BUFFER(dbcRead[i])); 
	}     


	return SUCCESS;
}  


/// <summary>
/// PGet next tuple from mini-runs.
/// </summary>
/// <param name="lpParam">The thread parameter.</param>
/// <param name="record">The record pointer.</param>
/// <param name="index">The index of mini-runs.</param>
/// <returns></returns>
RC PSMJ::PartitionPhase_GetNextTuple(LPVOID lpParam, RECORD *&record, INT index)
{
	PartitionThreadParams *p = (PartitionThreadParams *)lpParam;
	DoubleBuffer *dbcRead = p->dbcRead[index];

	int tupleIndex = p->memoryMergeBuffer[index].currentTupleIndex; 
	if(tupleIndex <= p->memoryMergeBuffer[index].tupleCount)
	{ 
		utl->GetTupleInfo(record, tupleIndex, p->memoryMergeBuffer[index].data, SSD_PAGE_SIZE,  p->keyPosition);

		tupleIndex++;
	}
	else
	{
		// Reset merge buffer index
		tupleIndex = 1; 
		utl->ResetBuffer(p->memoryMergeBuffer[index]);

		// Get new page from input buffer
		FRONT_BUFFER(dbcRead).currentPageIndex++; 

		if(FRONT_BUFFER(dbcRead).currentPageIndex < FRONT_BUFFER(dbcRead).pageCount)
		{     
			utl->GetPageInfo(FRONT_BUFFER(dbcRead).data, p->memoryMergeBuffer[index], FRONT_BUFFER(dbcRead).currentPageIndex, SSD_PAGE_SIZE);

			utl->GetTupleInfo(record, tupleIndex, p->memoryMergeBuffer[index].data, SSD_PAGE_SIZE,  p->keyPosition);

			tupleIndex++; 
		}
		else
		{   
			// Input buffer read complete
			utl->ResetBuffer(FRONT_BUFFER(dbcRead));
			utl->SetMaxTuple(record); 
		}  
	}  

	// update new position of tuple
	p->memoryMergeBuffer[index].currentTupleIndex = tupleIndex;

	return SUCCESS;
}

/// <summary>
/// Terminate current run.
/// </summary>
/// <param name="lpParam">The thread parameter.</param>
/// <param name="tupleCount">The tuple count.</param>
/// <returns></returns>
RC  PSMJ::PartitionPhase_TerminateRun(LPVOID lpParam, DWORD tupleCount)
{
	PartitionThreadParams *p = (PartitionThreadParams *)lpParam;
	DoubleBuffer *dbcWrite = p->dbcWrite;  

	if(tupleCount!=0)
	{
		FANS *_fan = new FANS();  
		wcscpy(_fan->fileName,  p->tempFanIn->fileName); 
		_fan->threadID = p->threadID;
		_fan->pageCount = p->tempFanIn->pageCount; 
		_fan->tupleCount = p->tempFanIn->tupleCount;  
		_fan->fileSize.QuadPart = p->overlapWrite.fileSize.QuadPart;  
		_fan->lowestKey = p->tempFanIn->lowestKey;
		_fan->highestKey = p->tempFanIn->highestKey; 
		p->fanIns.push_back(_fan); 

		// The destination file size is a multiple of the page size. Open the
		// file WITH buffering to shrink its size to the source file's size.
		SetFilePointerEx(p->hFanOut, p->overlapWrite.fileSize, NULL, FILE_BEGIN); 
		SetEndOfFile(p->hFanOut); 

		// Close this file
		CloseHandle(p->hFanOut);  
	}
	else
	{
		// fuck pipeline
		CloseHandle(p->hFanOut);  
		DeleteFile(p->tempFanIn->fileName);
	} 

	// Reset temp value
	p->overlapWrite.overlap.Offset = 0;
	p->overlapWrite.overlap.OffsetHigh = 0;  
	p->overlapWrite.fileSize.QuadPart = 0;  

	p->tempFanIn->pageCount = 0;
	p->tempFanIn->tupleCount = 0;
	p->tempFanIn->lowestKey = 0;
	p->tempFanIn->highestKey = 0; 

	return SUCCESS;
}

/// <summary>
/// Write buffer to disk.
/// </summary>
/// <param name="lpParam">The thread parameter.</param>
/// <returns></returns>
RC PSMJ::PartitionPhase_Write(LPVOID lpParam) 
{
	PartitionThreadParams *p = (PartitionThreadParams *)lpParam;
	DoubleBuffer *dbcWrite = p->dbcWrite;  

	WriteFile(p->hFanOut, 
		FRONT_BUFFER(dbcWrite).data, 
		FRONT_BUFFER(dbcWrite).currentSize, 
		&p->overlapWrite.dwBytesReadWritten, 
		&p->overlapWrite.overlap); 			  

	return SUCCESS;
} 

/// <summary>
/// Sort mini-runs.
/// </summary>
/// <param name="lpParam">The thread parameter.</param>
/// <param name="bufferIndex">Index of the buffer.</param>
/// <returns></returns>
RC PSMJ::PartitionPhase_Sort(LPVOID lpParam, DWORD bufferIndex)  
{    
	PartitionThreadParams *p = (PartitionThreadParams *)lpParam; 
	DoubleBuffer *dbcRead  = p->dbcRead[bufferIndex]; 
	DWORD keyPos = p->keyPosition;
	QuickSort *quicksort = p->quickSort;

	if(FRONT_BUFFER(dbcRead).isSort==FALSE)
	{
		dbcRead->LockConsumer();  

		quicksort->Reset(); // reset quick sort size

		utl->ResetBuffer(p->quickSortPageBuffer);

		//#pragma chMSG(Memory leak)
		RECORD *recordPtr = new RECORD();

		DWORD qsIndex = 0;  // quick sort array index
		for(DWORD pageIndex=0; pageIndex < FRONT_BUFFER(dbcRead).pageCount; pageIndex++)
		{    
			utl->GetPageInfo(FRONT_BUFFER(dbcRead).data, p->quickSortPageBuffer, pageIndex, SSD_PAGE_SIZE);  

			for(DWORD tupleIndex=1; tupleIndex <= p->quickSortPageBuffer.tupleCount; tupleIndex++)
			{ 
				utl->GetTupleInfo(recordPtr, tupleIndex, p->quickSortPageBuffer.data, SSD_PAGE_SIZE, keyPos);

				quicksort->AddRecord(recordPtr, qsIndex); 
				qsIndex++; 
			}   
			utl->ResetBuffer(p->quickSortPageBuffer); 
		} 

		// In memory sort
		//////////////////////////////////////////////////////////////////////////
		quicksort->Sort();
		//////////////////////////////////////////////////////////////////////////
		//Save sort result to buffer
		utl->ResetBuffer(p->runPageBuffer);
		utl->ResetRunPage(p->runPage, p->runPageBuffer);  
		utl->ResetBuffer(FRONT_BUFFER(dbcRead));  

		p->runPage->consumed=TRUE;
		DWORD quicksortSize = quicksort->GetCurrentSize();
		for (DWORD q=0; q < quicksortSize; q++)
		{  
			p->runPage->consumed = FALSE; 

			recordPtr = quicksort->GetRecord(q);

			if(utl->IsPageFull(p->runPage))
			{  
				if(!utl->IsBufferFull(FRONT_BUFFER(dbcRead)))
				{
					utl->AddPageToBuffer(FRONT_BUFFER(dbcRead),  p->runPage->page, SSD_PAGE_SIZE);  
					FRONT_BUFFER(dbcRead).pageCount++;
					p->runPage->consumed=TRUE;  
				}
				else 
				{ 
					break; // Buffer full
				}

				utl->ResetRunPage(p->runPage, p->runPageBuffer);  
			}   
			utl->AddTupleToPage(p->runPage, recordPtr->data, p->runPageBuffer);  // Add this tuple to page  
		} 

		if( (p->runPage->consumed == FALSE) && (utl->IsBufferFull(FRONT_BUFFER(dbcRead))==FALSE) ) 
		{ 
			if(!utl->IsEmptyPage(p->runPage))
			{
				utl->AddPageToBuffer(FRONT_BUFFER(dbcRead), p->runPage->page, SSD_PAGE_SIZE);
				FRONT_BUFFER(dbcRead).pageCount++; 
			} 
		}   

		FRONT_BUFFER(dbcRead).tupleCount = quicksort->GetCurrentSize();   
		FRONT_BUFFER(dbcRead).isSort=TRUE;   

		dbcRead->UnLockConsumer(); 
	}

	return SUCCESS;
}   

/// <summary>
/// Create new run on disk.
/// </summary>
/// <param name="lpParam">The lp parameter.</param>
/// <returns></returns>
RC PSMJ::PartitionPhase_CreateNewRun(LPVOID lpParam)
{
	PartitionThreadParams *p = (PartitionThreadParams *)lpParam;

	PartitionPhase_GetFanInPath(p->tempFanIn->fileName, p->threadID);  

	p->hFanOut=CreateFile((LPCWSTR)p->tempFanIn->fileName,	 
		GENERIC_WRITE,			 
		0,						 
		NULL,					 
		CREATE_ALWAYS,			 
		FILE_FLAG_OVERLAPPED, 
		NULL);					// no attr. template

	if (INVALID_HANDLE_VALUE==p->hFanOut) 
	{  
		//myMessage.Add(L"Cannot create run %s\r\n", p->tempFanIn->fileName);
		return ERR_CANNOT_CREATE_HANDLE;
	}   

	// File systems extend files synchronously. Extend the destination file 
	// now so that I/Os execute asynchronously improving performance. 
	LARGE_INTEGER liTempFileSize = { 0 };
	liTempFileSize.QuadPart =  p->inputBufferCount * m_PemsParams.SORT_READ_BUFFER_SIZE;

	SetFilePointerEx(p->hFanOut, liTempFileSize, NULL, FILE_BEGIN);
	SetEndOfFile(p->hFanOut); 

	// Reset value
	p->overlapWrite.fileSize.QuadPart = 0; // Use for shrink fanout after write complete 
	p->overlapWrite.overlap.Offset = 0;
	p->overlapWrite.overlap.OffsetHigh = 0;  

	p->tempFanIn->pageCount = 0;
	p->tempFanIn->tupleCount = 0;
	p->tempFanIn->lowestKey = 0;
	p->tempFanIn->highestKey = 0;
	p->tempFanIn->fileSize.QuadPart = 0;

	return SUCCESS;
}
 
/// <summary>
/// Merge all sorted runs on diks to single sorted run.
/// </summary>
/// <param name="runQueue">The run queue.</param>
/// <param name="keyPos">The key position in table.</param>
/// <returns></returns>
RC PSMJ::PartitionPhase_FinalMerge(std::queue<FANS*> &runQueue, const DWORD keyPos)
{
	g_Logger.Write("Merge Start...\n");

	RC rc;
	m_FinalMergeParams = new FinalMergeParams();
	m_FinalMergeParams->_this = this;
	m_FinalMergeParams->keyPosition = keyPos;

	FinalMergeParams *p = m_FinalMergeParams; // shorter name

	// Reset buffer pool
	bufferPool.currentSize = 0;  

	DWORD k = runQueue.size(); 
	LoserTree lsTree(k); // Init loser tree
	RECORD *recordPtr = new RECORD();  
	std::queue<FANS*> fanInPendingDelete;    

	//#pragma chMSG(need to check memory here to merge in single merge pass)
	//////////////////////////////////////////////////////////////////////////
	// Fisrt check with 128K for read, if satisfy then use rest of memory for write
	DWORD readBufferSize = 0, writeBufferSize = 0;
	readBufferSize = m_PemsParams.MERGE_READ_BUFFER_SIZE; //128K

	DWORD memForPages = k*SSD_PAGE_SIZE + SSD_PAGE_SIZE;
	DWORD memForWork = bufferPool.size - memForPages;

	BOOL isOK = FALSE;
	while(!isOK)
	{
		DWORD memforRead = 0;
		for (UINT i=0; i<k; i++)
		{
			memforRead+=readBufferSize * 2;
		}

		if(memforRead > memForWork)
		{
			readBufferSize = (readBufferSize) / 2;
		}
		else
		{
			DWORD memFree = memForWork - memforRead;
			DWORD memforWrite = chROUNDDOWN( memFree / 2, SSD_PAGE_SIZE); 
			if(memforWrite < SSD_PAGE_SIZE * 256 * 8) // 8MB for write
			{
				readBufferSize = (readBufferSize) / 2;
				isOK = FALSE;

				if(readBufferSize < SSD_PAGE_SIZE)
				{
					// Not enough mem
					ShowMB(L"Memory for read fanIn smaller than 4096. Too many fanIn ->Exit\r\n");
					return ERR_NOT_ENOUGH_MEMORY;
				}
			}
			else
			{
				if(memforWrite > 4096 * 256 * 32) // 32MB
				{
					memforWrite = 4096 * 256 * 32; 
				}
				isOK = TRUE;
			}
			writeBufferSize = memforWrite;
		} 
	}

	m_PsmjParams.MERGE_READ_BUFFER_SIZE = readBufferSize;   
	m_PsmjParams.MERGE_WRITE_BUFFER_SIZE = writeBufferSize;
	//////////////////////////////////////////////////////////////////////////


	// Create handle to merge multiple Fan-In
	p->hFanIn = new HANDLE[k];
	p->overlapRead = new OVERLAPPEDEX[k];
	LARGE_INTEGER liFanOutSize = {0};
	for (DWORD runIdx=0; runIdx < k; runIdx++)
	{  
		FANS *_fanIn = runQueue.front(); 
		runQueue.pop();

		if(runQueue.size()==0)
		{
			int aaa=0;
		}
		fanInPendingDelete.push(_fanIn);

		p->hFanIn[runIdx] = CreateFile(
			(LPCWSTR)_fanIn->fileName, // file to open
			GENERIC_READ,			// open for reading
			0,						//  do not share for reading
			NULL,					// default security
			OPEN_EXISTING,			// existing file only
			FILE_FLAG_OVERLAPPED | FILE_FLAG_NO_BUFFERING,	// overlapped operation //| FILE_FLAG_NO_BUFFERING
			NULL);					// no attr. template

		DWORD err = GetLastError();

		if (INVALID_HANDLE_VALUE==p->hFanIn[runIdx]) 
		{   
			ShowMB(L"Final merge: Cannot open file %s\r\n", _fanIn->fileName);
			int a=0;
			return ERR_CANNOT_CREATE_HANDLE; 
		} 

		UINT64 fanInSize = GetFanSize(p->hFanIn[runIdx]); 
		liFanOutSize.QuadPart+= fanInSize;

		DWORD chunkSize = readBufferSize; 
		DWORD totalChunk = chROUNDUP(fanInSize, chunkSize) / chunkSize ;

		// Init overlapped struct for read Fan-In
		p->overlapRead[runIdx].dwBytesToReadWrite = readBufferSize;  
		p->overlapRead[runIdx].dwBytesReadWritten = 0;  
		p->overlapRead[runIdx].startChunk = 0;  
		p->overlapRead[runIdx].chunkIndex = 0;  
		p->overlapRead[runIdx].endChunk = totalChunk;  
		p->overlapRead[runIdx].totalChunk = totalChunk; 
		p->overlapRead[runIdx].overlap.Offset = 0; 
		p->overlapRead[runIdx].overlap.OffsetHigh = 0;  	
	}

	FANS *finalFan = new FANS(); 
	PartitionPhase_GetFanInPath(finalFan->fileName, 0);

	p->hFanOut = CreateFile(
		(LPCWSTR)finalFan->fileName, // file to open
		GENERIC_WRITE,			// open for write
		0,						// donot share
		NULL,					// default security
		CREATE_ALWAYS,			// create new file
		FILE_FLAG_OVERLAPPED | FILE_FLAG_NO_BUFFERING,	// overlapped operation //| FILE_FLAG_NO_BUFFERING
		NULL);					// no attr. template

	if (INVALID_HANDLE_VALUE==p->hFanOut) 
	{   
		ShowMB(L"Cannot open file %s\r\n", finalFan->fileName);
		return ERR_CANNOT_CREATE_HANDLE; 
	} 

	// Overlap struct for write Fan-Out
	p->overlapWrite.dwBytesToReadWrite = writeBufferSize; 
	p->overlapWrite.dwBytesReadWritten = 0; 
	p->overlapWrite.overlap.Offset = 0;
	p->overlapWrite.overlap.OffsetHigh = 0;

	if(isR)
	{
		rp.MergeReadBufferSizeR = readBufferSize;
		rp.MergeWriteBufferSizeR = writeBufferSize;
	}
	else
	{
		rp.MergeReadBufferSizeS = readBufferSize;
		rp.MergeWriteBufferSizeS = writeBufferSize;
	}

	// Init write buffer size
	p->dbcWrite = new DoubleBuffer(writeBufferSize); 
	rc = utl->InitBuffer(p->dbcWrite->buffer[0], writeBufferSize, &bufferPool);
	if(rc!=SUCCESS) {return rc;}
	rc = utl->InitBuffer(p->dbcWrite->buffer[1], writeBufferSize, &bufferPool);  
	if(rc!=SUCCESS) {return rc;}


	// Init run buffer
	rc = utl->InitBuffer(p->runPageBuffer, SSD_PAGE_SIZE, &bufferPool); 
	if(rc!=SUCCESS) {return rc;}

	//////////////////////////////////////////////////////////////////////////
	// Init runpage 
	rc = utl->InitRunPage(p->runPage, p->runPageBuffer); 
	if(rc!=SUCCESS) {return rc;}

	// Init read buffer and merge buffer


	p->dbcRead = new DoubleBuffer*[k]; 
	p->mergePageBuffer = new Buffer[k];
	for (DWORD runIdx=0; runIdx < k; runIdx++)
	{ 
		p->dbcRead[runIdx] = new DoubleBuffer(readBufferSize); 
		rc = utl->InitBuffer(p->dbcRead[runIdx]->buffer[0], readBufferSize, &bufferPool);
		if(rc!=SUCCESS) {return rc;}
		rc = utl->InitBuffer(p->dbcRead[runIdx]->buffer[1], readBufferSize, &bufferPool);  
		if(rc!=SUCCESS) {return rc;}

		// Init merge page buffer
		rc = utl->InitBuffer(p->mergePageBuffer[runIdx], SSD_PAGE_SIZE, &bufferPool);  
		if(rc!=SUCCESS) {return rc;} 
	} 


	// File systems extend files synchronously. Extend the destination file 
	// now so that I/Os execute asynchronously improving performance. 
	////////////////////////////////////////////////////////////////////////// 
	LARGE_INTEGER liDestSize = { 0 };
	liDestSize.QuadPart = chROUNDUP(liFanOutSize.QuadPart, writeBufferSize);

	SetFilePointerEx(p->hFanOut, liDestSize, NULL, FILE_BEGIN);
	SetEndOfFile(p->hFanOut); 

	// First read from disk to buffer
	//////////////////////////////////////////////////////////////////////////
	for(DWORD runIdx=0; runIdx < k; runIdx++) 
	{
		PartitionPhase_FinalMerge_Read(runIdx);     
	}


	// Prepare data for loser tree
	for(DWORD runIdx = 0; runIdx < k; runIdx++) 
	{     
		// Wait for read complete
		if(p->overlapRead[runIdx].chunkIndex < p->overlapRead[runIdx].totalChunk)
		{
			GetOverlappedResult(p->hFanIn[runIdx], 
				&p->overlapRead[runIdx].overlap,
				&p->overlapRead[runIdx].dwBytesReadWritten, 
				TRUE);  
		}

		p->overlapRead[runIdx].chunkIndex++; 
		utl->ComputeBuffer(BACK_BUFFER(p->dbcRead[runIdx]),  p->overlapRead[runIdx].dwBytesReadWritten);  

		utl->ResetBuffer(p->mergePageBuffer[runIdx]); 

		if(p->dbcRead[runIdx]->bFirstProduce==TRUE) { p->dbcRead[runIdx]->bFirstProduce=FALSE; } 

		utl->GetPageInfo(BACK_BUFFER(p->dbcRead[runIdx]).data, p->mergePageBuffer[runIdx], 0, SSD_PAGE_SIZE); 
		p->mergePageBuffer[runIdx].currentTupleIndex = 1;

		utl->GetTupleInfo(recordPtr, 
			p->mergePageBuffer[runIdx].currentTupleIndex, 
			p->mergePageBuffer[runIdx].data,
			SSD_PAGE_SIZE,
			p->keyPosition);  

		p->mergePageBuffer[runIdx].currentTupleIndex++;

		lsTree.AddNewNode(recordPtr, runIdx);  

		//p->dbcRead[i]->UnLockProducer();  

		p->dbcRead[runIdx]->SwapBuffers();  // swap read buffer

		PartitionPhase_FinalMerge_Read( runIdx );       
	}    

	lsTree.CreateLoserTree();  

	p->dbcWrite->bFirstProduce = TRUE;
	utl->ResetBuffer(BACK_BUFFER(p->dbcWrite));
	utl->ResetBuffer(FRONT_BUFFER(p->dbcWrite));

	utl->ResetRunPage(p->runPage, p->runPageBuffer);  

	p->dbcWrite->LockProducer();

	INT		lsIdx = 0; // index in loser tree point to minimum record 
	BOOL	bFirstWrite = TRUE;
	BOOL	bLowestIsGet = FALSE;  

	UINT64 partitionFileSize = 0;
	const DWORD numPageInPartititon = m_RunSize / SSD_PAGE_SIZE;

	UINT64 crrPageIdx = 0;
	UINT64 pageIdx = 0;

	BOOL lol = FALSE;
	DWORD lowestKey = 0;
	DWORD highestKey = 0;

	while(TRUE)  
	{   
		lsTree.GetMinRecord(recordPtr, lsIdx); // index = loserTree[0]

		if(recordPtr->key==MAX) { break; }

		if(bLowestIsGet==FALSE)
		{
			finalFan->lowestKey = recordPtr->key; 
			bLowestIsGet = TRUE;
		}

		if(lol==FALSE)
		{
			lowestKey = recordPtr->key; 
			lol = TRUE;
		}

		finalFan->highestKey = recordPtr->key;
		highestKey = recordPtr->key;

		p->runPage->consumed = FALSE;

		if(utl->IsPageFull(p->runPage))
		{  
			utl->AddPageToBuffer(BACK_BUFFER(p->dbcWrite),  p->runPage->page, SSD_PAGE_SIZE); 
			p->runPage->consumed = TRUE;

			// Reset page về ban đầu, bắt đầu ghép 1 page mới  
			utl->ResetRunPage(p->runPage, p->runPageBuffer);

			BACK_BUFFER(p->dbcWrite).currentPageIndex++;
			BACK_BUFFER(p->dbcWrite).pageCount++;

			crrPageIdx++;

			if(crrPageIdx>=numPageInPartititon)
			{
				RangePartition _myRange;
				_myRange.fileOffsetStart.QuadPart = pageIdx * SSD_PAGE_SIZE;
				_myRange.fileOffsetEnd.QuadPart = (pageIdx + crrPageIdx)  * SSD_PAGE_SIZE;
				_myRange.lowestKey = lowestKey;
				_myRange.highestKey = highestKey;

				m_RangePartition.push_back(_myRange);

				pageIdx += crrPageIdx;
				crrPageIdx = 0; 
				lol = FALSE;
			}
		} 

		if(utl->IsBufferFull(BACK_BUFFER(p->dbcWrite))) // write buffer is full
		{    
			p->dbcWrite->UnLockProducer();   

			if(p->dbcWrite->bFirstProduce) // Back Buffer is full, check is it the first product
			{ 
				p->dbcWrite->bFirstProduce=FALSE;  
				p->dbcWrite->SwapBuffers();  

				p->dbcWrite->LockConsumer();  
				WriteFile(p->hFanOut,  
					FRONT_BUFFER(p->dbcWrite).data,  
					FRONT_BUFFER(p->dbcWrite).currentSize,  
					&p->overlapWrite.dwBytesReadWritten,  
					&p->overlapWrite.overlap); 
			}
			else
			{    
				//Wait for write complete   
				GetOverlappedResult(p->hFanOut, 
					&p->overlapWrite.overlap, 
					&p->overlapWrite.dwBytesReadWritten, 
					TRUE) ;  

				p->overlapWrite.fileSize.QuadPart += p->overlapWrite.dwBytesReadWritten;  
				p->overlapWrite.overlap.Offset = p->overlapWrite.fileSize.LowPart;
				p->overlapWrite.overlap.OffsetHigh = p->overlapWrite.fileSize.HighPart;  
				p->overlapWrite.dwBytesReadWritten = 0;

				finalFan->pageCount += FRONT_BUFFER(p->dbcWrite).pageCount;
				finalFan->tupleCount += FRONT_BUFFER(p->dbcWrite).tupleCount;

				utl->ResetBuffer(FRONT_BUFFER(p->dbcWrite));  
				p->dbcWrite->UnLockConsumer(); 

				p->dbcWrite->SwapBuffers();  

				p->dbcWrite->LockConsumer();   
				WriteFile(p->hFanOut,  
					FRONT_BUFFER(p->dbcWrite).data,  
					FRONT_BUFFER(p->dbcWrite).currentSize,  
					&p->overlapWrite.dwBytesReadWritten,  
					&p->overlapWrite.overlap);
			}  
			p->dbcWrite->LockProducer();  
		} 

		utl->AddTupleToPage(p->runPage, recordPtr->data, p->runPageBuffer);  
		BACK_BUFFER(p->dbcWrite).tupleCount++;

		PartitionPhase_FinalMerge_GetNextRecord(recordPtr, lsIdx); 

		lsTree.AddNewNode(recordPtr, lsIdx);// Add this tuple to loser tree  
		lsTree.Adjust(lsIdx); //Shrink loser tree 
	}   // end while

	//Wait until current write process complete 
	GetOverlappedResult(p->hFanOut,
		&p->overlapWrite.overlap, 
		&p->overlapWrite.dwBytesReadWritten,
		TRUE); 

	if(p->overlapWrite.dwBytesReadWritten > 0)
	{
		if(FRONT_BUFFER(p->dbcWrite).currentSize > 0)
		{
			p->overlapWrite.fileSize.QuadPart += p->overlapWrite.dwBytesReadWritten;  
			p->overlapWrite.overlap.Offset = p->overlapWrite.fileSize.LowPart;
			p->overlapWrite.overlap.OffsetHigh = p->overlapWrite.fileSize.HighPart;  

			finalFan->pageCount += FRONT_BUFFER(p->dbcWrite).pageCount;
			finalFan->tupleCount += FRONT_BUFFER(p->dbcWrite).tupleCount;

			p->overlapWrite.dwBytesReadWritten = 0;

			utl->ResetBuffer(FRONT_BUFFER(p->dbcWrite));  
		}
	}


	RangePartition _myRangeLast;
	_myRangeLast.fileOffsetStart.QuadPart = pageIdx * SSD_PAGE_SIZE;
	_myRangeLast.fileOffsetEnd.QuadPart = (pageIdx + crrPageIdx)  * SSD_PAGE_SIZE;
	_myRangeLast.lowestKey = lowestKey;
	_myRangeLast.highestKey = highestKey;

	m_RangePartition.push_back(_myRangeLast);

	pageIdx += crrPageIdx;
	crrPageIdx = 0; 

	//////////////////////////////////////////////////////////////////////////

	// If the last page has not consumed
	if((p->runPage->consumed==FALSE) && (utl->IsBufferFull(BACK_BUFFER(p->dbcWrite))==FALSE))
	{ 
		if(!utl->IsEmptyPage(p->runPage))
		{
			utl->AddPageToBuffer(BACK_BUFFER(p->dbcWrite),  p->runPage->page, SSD_PAGE_SIZE);  
			p->runPage->consumed = TRUE; 
			BACK_BUFFER(p->dbcWrite).currentPageIndex++;
			BACK_BUFFER(p->dbcWrite).pageCount++;    
		}
		else
		{
			//int fuck=0;
		}
	}

	p->dbcWrite->UnLockProducer();

	p->dbcWrite->UnLockConsumer();

	if(BACK_BUFFER(p->dbcWrite).currentSize > 0)
	{    
		//write completed , swap buffer
		p->dbcWrite->SwapBuffers();   
		// Write the last run to disk  
		p->dbcWrite->LockConsumer();  

		WriteFile(p->hFanOut,  
			FRONT_BUFFER(p->dbcWrite).data,  
			FRONT_BUFFER(p->dbcWrite).currentSize,  
			&p->overlapWrite.dwBytesReadWritten,  
			&p->overlapWrite.overlap);

		GetOverlappedResult(p->hFanOut,
			&p->overlapWrite.overlap,
			&p->overlapWrite.dwBytesReadWritten, 
			TRUE);

		p->overlapWrite.fileSize.QuadPart += p->overlapWrite.dwBytesReadWritten;  
		p->overlapWrite.overlap.Offset = p->overlapWrite.fileSize.LowPart;
		p->overlapWrite.overlap.OffsetHigh = p->overlapWrite.fileSize.HighPart;  

		finalFan->pageCount += FRONT_BUFFER(p->dbcWrite).pageCount;
		finalFan->tupleCount += FRONT_BUFFER(p->dbcWrite).tupleCount;

		utl->ResetBuffer(FRONT_BUFFER(p->dbcWrite)); 
		p->dbcWrite->UnLockConsumer();
	}

	//Reset read buffer
	for(DWORD runIdx = 0; runIdx < k; runIdx++) 
	{   
		utl->ResetBuffer(FRONT_BUFFER(p->dbcRead[runIdx])); 
		CloseHandle(p->hFanIn[runIdx]);
	}     

	// Clean
	SetFilePointerEx(p->hFanOut, p->overlapWrite.fileSize, NULL, FILE_BEGIN); 
	SetEndOfFile(p->hFanOut); 

	CloseHandle(p->hFanOut);

	// Push final fan to queue
	runQueue.push(finalFan);

	//if(fanInPendingDelete.size() > 0)
	//{
	//	while (fanInPendingDelete.size() > 0)
	//	{
	//		FANS *_byeBye = fanInPendingDelete.front();
	//		fanInPendingDelete.pop();
	//		if(!DeleteFile(_byeBye->fileName))
	//		{
	//			ShowMB(L"Cannot delete file %s\r\n", _byeBye->fileName); 
	//		} 
	//	} 
	//}

	return SUCCESS; 
} 

/// <summary>
/// Read data on disk into buffer.
/// </summary>
/// <param name="runIdx">Index of the run.</param>
/// <returns></returns>
RC PSMJ::PartitionPhase_FinalMerge_Read(DWORD runIdx)
{    
	FinalMergeParams *p = m_FinalMergeParams;

	LARGE_INTEGER chunk = {0};
	chunk.QuadPart = p->overlapRead[runIdx].chunkIndex * m_PsmjParams.MERGE_READ_BUFFER_SIZE;   
	p->overlapRead[runIdx].overlap.Offset = chunk.LowPart;
	p->overlapRead[runIdx].overlap.OffsetHigh = chunk.HighPart;  

	// Attempt an asynchronous read operation.  
	ReadFile(p->hFanIn[runIdx], 
		BACK_BUFFER(p->dbcRead[runIdx]).data, 
		p->overlapRead[runIdx].dwBytesToReadWrite, 
		&p->overlapRead[runIdx].dwBytesReadWritten, 
		&p->overlapRead[runIdx].overlap);

	return SUCCESS; 
}


/// <summary>
/// Get next record in buffer.
/// </summary>
/// <param name="recordPtr">The record PTR.</param>
/// <param name="runIdx">Index of the run.</param>
/// <returns></returns>
RC PSMJ::PartitionPhase_FinalMerge_GetNextRecord(RECORD *&recordPtr, INT runIdx)
{    

	FinalMergeParams *p = m_FinalMergeParams;

	if(p->mergePageBuffer[runIdx].currentTupleIndex > p->mergePageBuffer[runIdx].tupleCount)  
	{
		// Read complete this page, need to fetch next page from disk  
		utl->ResetBuffer(p->mergePageBuffer[runIdx]);  
		FRONT_BUFFER(p->dbcRead[runIdx]).currentPageIndex++;   
		if(FRONT_BUFFER(p->dbcRead[runIdx]).currentPageIndex >= FRONT_BUFFER(p->dbcRead[runIdx]).pageCount)
		{    
			utl->ResetBuffer(FRONT_BUFFER(p->dbcRead[runIdx])); 

			// Waiting for reading in to back buffer completed, do not wait the last one
			if(p->overlapRead[runIdx].chunkIndex < p->overlapRead[runIdx].totalChunk)
				GetOverlappedResult(p->hFanIn[runIdx], 
				&p->overlapRead[runIdx].overlap, 
				&p->overlapRead[runIdx].dwBytesReadWritten, 
				TRUE); 

			p->overlapRead[runIdx].chunkIndex++;  
			if(p->overlapRead[runIdx].dwBytesReadWritten==0)
			{
				utl->ResetBuffer(BACK_BUFFER(p->dbcRead[runIdx]));
				utl->AddPageMAXToBuffer(p->mergePageBuffer[runIdx], SSD_PAGE_SIZE); 
				utl->AddPageToBuffer(BACK_BUFFER(p->dbcRead[runIdx]),
					p->mergePageBuffer[runIdx].data, 
					SSD_PAGE_SIZE); 

				p->mergePageBuffer[runIdx].isFullMaxValue = TRUE; 
				FRONT_BUFFER(p->dbcRead[runIdx]).currentPageIndex = 0;
			}
			else
			{
				utl->ComputeBuffer(BACK_BUFFER(p->dbcRead[runIdx]), 
					p->overlapRead[runIdx].dwBytesReadWritten);   
			}

			//Swap buffer to continue merge 
			p->dbcRead[runIdx]->SwapBuffers();  
			// read next chunk
			PartitionPhase_FinalMerge_Read(runIdx);   
		}   

		utl->GetPageInfo(FRONT_BUFFER(p->dbcRead[runIdx]).data, 
			p->mergePageBuffer[runIdx], 
			FRONT_BUFFER(p->dbcRead[runIdx]).currentPageIndex, 
			SSD_PAGE_SIZE);   

		p->mergePageBuffer[runIdx].currentTupleIndex=1; 
	}

	utl->GetTupleInfo(recordPtr,
		p->mergePageBuffer[runIdx].currentTupleIndex, 
		p->mergePageBuffer[runIdx].data, 
		SSD_PAGE_SIZE,
		m_PemsParams.KEY_POS);  

	p->mergePageBuffer[runIdx].currentTupleIndex++;

	return SUCCESS; 
}


/// <summary>
/// Get fan-in path.
/// </summary>
/// <param name="fanInName">Name of the fan in.</param>
/// <param name="threadID">The thread identifier.</param>
/// <returns></returns>
RC PSMJ::PartitionPhase_GetFanInPath(LPWSTR &fanInName, INT threadID)  
{    
	m_CS.Lock();
	swprintf(fanInName, MAX_PATH, L"%s%s_%d_%d.dat", m_PemsParams.WORK_SPACE_PATH, m_PemsParams.FILE_NAME_NO_EXT, threadID, m_FanInIdx);  
	//InterlockedExchangeAdd(&m_FanInIdx, 1);  
	m_FanInIdx++;
	m_CS.UnLock();
	return SUCCESS; 
} 

#pragma endregion

//////////////////////////////////////////////////////////////////////////
//
// JOIN PHASE
//
//////////////////////////////////////////////////////////////////////////

/// <summary>
/// Check enough memory for join phase.
/// </summary>
/// <returns></returns>
RC PSMJ::JoinPhase_CheckEnoughMemory()
{  
	return SUCCESS;
}


/// <summary>
/// Execute the join phase.
/// </summary>
/// <returns></returns>
RC PSMJ::JoinPhase_Execute()
{
	RC rc;
	rp.JoinThreadNum = m_WorkerThreadNum;
	if(m_PsmjParams.PLAN_FOR_JOIN == JOIN_PLAN::PLAN_1)
	{
		rc = JoinPhase_Plan1_Initialize();
		if(rc!=SUCCESS) { return rc; } 

		// Start working
		for(UINT i=0; i < m_WorkerThreadNum; i++ )
		{  
			m_hWorkerThread[i] = CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)JoinPhase_Plan1_Ex, (LPVOID)&(m_JoinPlan1ThreadParams[i]), CREATE_SUSPENDED, NULL); 
			SetThreadPriority(m_hWorkerThread[i], THREAD_PRIORITY_NORMAL);
			ResumeThread(m_hWorkerThread[i]); 
		}
	}
	else if(m_PsmjParams.PLAN_FOR_JOIN == JOIN_PLAN::PLAN_2)
	{
		rc = JoinPhase_Plan2_Initialize();
		if(rc!=SUCCESS) { return rc; } 

		// Start working
		for(UINT i=0; i < m_WorkerThreadNum; i++ )
		{  
			m_hWorkerThread[i] = CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)JoinPhase_Plan2_Ex, (LPVOID)&(m_JoinPlan2ThreadParams[i]), CREATE_SUSPENDED, NULL); 
			SetThreadPriority(m_hWorkerThread[i], THREAD_PRIORITY_NORMAL);
			ResumeThread(m_hWorkerThread[i]); 
		}
	}
	else if(m_PsmjParams.PLAN_FOR_JOIN == JOIN_PLAN::PLAN_3)
	{
		rc = JoinPhase_Plan3_Initialize();
		if(rc!=SUCCESS) { return rc; } 

		// Start working
		for(UINT i=0; i < m_WorkerThreadNum; i++ )
		{  
			m_hWorkerThread[i] = CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)JoinPhase_Plan3_Ex, (LPVOID)&(m_JoinPlan3ThreadParams[i]), CREATE_SUSPENDED, NULL); 
			SetThreadPriority(m_hWorkerThread[i], THREAD_PRIORITY_NORMAL);
			ResumeThread(m_hWorkerThread[i]); 
		}
	}
	else if(m_PsmjParams.PLAN_FOR_JOIN == JOIN_PLAN::PLAN_4)
	{

	}


	//////////////////////////////////////////////////////////////////////////
	// Power capping monitor
	////////////////////////////////////////////////////////////////////////// 
	if(m_PsmjParams.USE_POWER_CAP==TRUE)
	{
		cappingParams._this = this;
		bQuitCapping = FALSE; 

		hPowerCapThread = CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)PowerCapEx, (LPVOID)&(cappingParams), CREATE_SUSPENDED, NULL);

		// Set monitor thread to higher priority
		SetThreadPriority(hPowerCapThread, THREAD_PRIORITY_TIME_CRITICAL);
		ResumeThread(hPowerCapThread); 
	}

	// Wait for worker thread finish
	WaitForMultipleObjects(m_WorkerThreadNum, m_hWorkerThread, TRUE, INFINITE); 
	/*
	if(m_PsmjParams.PLAN_FOR_JOIN == JOIN_PLAN::PLAN_3)
	{
	FILE* fso=fopen("C:\\skip.csv","w+b");
	CHAR *debugContent = new CHAR[1024];
	for(UINT tIdx=0; tIdx < m_WorkerThreadNum; tIdx++)
	{   
	sprintf(debugContent, "%d, %d\n", m_JoinPlan3ThreadParams[tIdx].ReadSkipCountS, m_JoinPlan3ThreadParams[tIdx].ReadCountS);  fprintf(fso, debugContent);  
	} 

	delete debugContent;
	fclose(fso); 
	} 
	*/

	// Wait for capping thread exit
	//////////////////////////////////////////////////////////////////////////
	if(m_PsmjParams.USE_POWER_CAP==TRUE)
	{
		bQuitCapping = TRUE;
		WaitForSingleObject(hPowerCapThread, INFINITE);
		CloseHandle(hPowerCapThread); 
	}

	if(m_PsmjParams.PLAN_FOR_JOIN == JOIN_PLAN::PLAN_1)
	{
		for (UINT i=0; i < shareS.totalCount; i++)
		{
			CloseHandle(shareS.hFile[i]);
		}
	}
	else if(m_PsmjParams.PLAN_FOR_JOIN == JOIN_PLAN::PLAN_2)
	{

	} 
	else if(m_PsmjParams.PLAN_FOR_JOIN == JOIN_PLAN::PLAN_3)
	{
		CloseHandle(hR);
		CloseHandle(hS); 
	}


	// Cleaning
	for(DWORD i = 0; i < m_WorkerThreadNum; i++ )
	{
		CloseHandle(m_hWorkerThread[i]);
	}
	return SUCCESS;
}


//////////////////////////////////////////////////////////////////////////
//
// JOIN PHASE PLAN 1
// Each Ri join with all Si with no barrier sync
//////////////////////////////////////////////////////////////////////////

#pragma region "Join Phase Plan 1"  

/// <summary>
/// Initialize join phase.
/// </summary>
/// <returns></returns>
RC PSMJ::JoinPhase_Plan1_Initialize()
{
	RC rc; 
	// Reset buffer pool
	bufferPool.currentSize = 0;

	SYSTEM_INFO sysinfo;
	GetSystemInfo(&sysinfo);
	if(m_PsmjParams.THREAD_NUM==0)
	{
		m_WorkerThreadNum = sysinfo.dwNumberOfProcessors; 
	}

	// Share read S
	S_FanInCount = m_S.size(); 

	shareS.totalCount = S_FanInCount;
	shareS.hFile = new HANDLE[shareS.totalCount];
	shareS.lowestKey = new DWORD[shareS.totalCount];
	shareS.highestKey = new DWORD[shareS.totalCount];

	for(UINT sIndex=0; sIndex < shareS.totalCount; sIndex++)
	{
		FANS *_fanS = m_S.front(); 
		m_S.pop();

		shareS.hFile[sIndex] = CreateFile(
			(LPCWSTR)_fanS->fileName, // file to open
			GENERIC_READ,			// open for reading
			FILE_SHARE_READ,        // share for reading
			NULL,					// default security
			OPEN_EXISTING,			// existing file only
			FILE_ATTRIBUTE_NORMAL,	// overlapped operation //| FILE_FLAG_NO_BUFFERING
			NULL);		

		if(shareS.hFile[sIndex]==INVALID_HANDLE_VALUE)
		{
			ShowMB(L"Cannot create handle %s", _fanS->fileName);
			return ERR_CANNOT_CREATE_HANDLE;
		}

		shareS.lowestKey[sIndex] = _fanS->lowestKey;
		shareS.highestKey[sIndex] = _fanS->highestKey; 
	} 

	R_FanInCount = m_R.size();


	if(R_FanInCount < m_WorkerThreadNum) // dont need too many thread
	{
		m_WorkerThreadNum = R_FanInCount; 
	}

	// Save report
	rp.JoinThreadNum = m_WorkerThreadNum;

	m_hWorkerThread = new HANDLE[m_WorkerThreadNum]; 
	m_JoinPlan1ThreadParams = new JoinPlan1ThreadParams[m_WorkerThreadNum];

	for(UINT t=0; t < m_WorkerThreadNum; t++)
	{
		m_JoinPlan1ThreadParams[t].R_FanInCount = 0;
		m_JoinPlan1ThreadParams[t].S_FanInCount = S_FanInCount;  
	}  

	DWORD _Rcount = R_FanInCount; 

	while(_Rcount>0)
	{
		for(UINT t=0; t < m_WorkerThreadNum; t++)
		{ 
			m_JoinPlan1ThreadParams[t].R_FanInCount++;

			FANS *rFan = m_R.front();
			m_JoinPlan1ThreadParams[t].R_FanIns.push(rFan);
			m_R.pop();

			_Rcount--;
			if(_Rcount==0) { break; }
		} 
	}

	DWORD ReadBufferSize = m_RunSize;

	rp.JoinReadBufferSize = ReadBufferSize;

	for(UINT t=0; t < m_WorkerThreadNum; t++)
	{
		m_JoinPlan1ThreadParams[t]._this = this;
		////////////////////////////////////////////////////////////////////////// 
		m_JoinPlan1ThreadParams[t].tupleR = new RECORD();
		m_JoinPlan1ThreadParams[t].tupleS = new RECORD();
		m_JoinPlan1ThreadParams[t].tupleRS = new RECORD(TUPLE_SIZE*2);

		m_JoinPlan1ThreadParams[t].R_Key = m_PsmjParams.R_KEY_POS;
		m_JoinPlan1ThreadParams[t].S_Key = m_PsmjParams.S_KEY_POS; 

		// Init buffer
		// Read buffer for R
		rc = utl->InitBuffer(m_JoinPlan1ThreadParams[t].readBufferR, ReadBufferSize, &bufferPool);
		if(rc!=SUCCESS) {  return rc; } 
		rc = utl->InitBuffer(m_JoinPlan1ThreadParams[t].pageBufferR, SSD_PAGE_SIZE, &bufferPool);
		if(rc!=SUCCESS) {  return rc; } 
		rc = utl->InitRunPage(m_JoinPlan1ThreadParams[t].pageR, m_JoinPlan1ThreadParams[t].pageBufferR);
		if(rc!=SUCCESS) {  return rc; }

		// Read buffer for S
		rc = utl->InitBuffer(m_JoinPlan1ThreadParams[t].readBufferS, ReadBufferSize, &bufferPool);
		if(rc!=SUCCESS) {  return rc; }  
		rc = utl->InitBuffer(m_JoinPlan1ThreadParams[t].pageBufferS, SSD_PAGE_SIZE, &bufferPool);
		if(rc!=SUCCESS) {  return rc; }  
		rc = utl->InitRunPage(m_JoinPlan1ThreadParams[t].pageS, m_JoinPlan1ThreadParams[t].pageBufferS);
		if(rc!=SUCCESS) {  return rc; } 

		// Init write buffers & page 
		rc = utl->InitBuffer(m_JoinPlan1ThreadParams[t].pageWriteBuffer, SSD_PAGE_SIZE, &bufferPool);
		if(rc!=SUCCESS) {  return rc; }

		rc = utl->InitRunPage(m_JoinPlan1ThreadParams[t].pageWrite, m_JoinPlan1ThreadParams[t].pageWriteBuffer);
		if(rc!=SUCCESS) {  return rc; }

		// Variable for write join tuple to disk
		m_JoinPlan1ThreadParams[t].joinFilePath = new TCHAR[MAX_PATH];
		JoinPhase_GetFanOutPath(m_JoinPlan1ThreadParams[t].joinFilePath, t);

		m_JoinPlan1ThreadParams[t].hWrite = CreateFile(
			m_JoinPlan1ThreadParams[t].joinFilePath,	 
			GENERIC_WRITE,			 
			0,						 
			NULL,					 
			CREATE_ALWAYS,			 
			FILE_FLAG_OVERLAPPED, // overlapped write
			NULL);		

		if (INVALID_HANDLE_VALUE==m_JoinPlan1ThreadParams[t].hWrite) 
		{   
			ShowMB(L"Cannot open file %s\r\n", m_JoinPlan1ThreadParams[t].joinFilePath);
			return ERR_CANNOT_CREATE_HANDLE; 
		}  
	}

	// Calculate write buffer size
	DWORD memSizeFree = bufferPool.size - bufferPool.currentSize;
	if(memSizeFree==0) {return ERR_NOT_ENOUGH_MEMORY;}

	DWORD writeMemSize = chROUNDDOWN((memSizeFree / m_WorkerThreadNum) / 2, SSD_PAGE_SIZE); // divide for 2 because use double buffer

	rp.JoinWriteBufferSize = writeMemSize;

	if(writeMemSize <  SSD_PAGE_SIZE*256)
	{
		chMB("Alert::Memory for write below 1MB");
	}

	for(UINT t=0; t < m_WorkerThreadNum; t++)
	{
		//#pragma chMSG(Join Write buffer size is fixed as 4MB)

		m_JoinPlan1ThreadParams[t].dbcWrite = new DoubleBuffer(writeMemSize); 
		rc = utl->InitBuffer(m_JoinPlan1ThreadParams[t].dbcWrite->buffer[0], writeMemSize, &bufferPool);
		if(rc!=SUCCESS) {return rc;}
		rc = utl->InitBuffer(m_JoinPlan1ThreadParams[t].dbcWrite->buffer[1], writeMemSize, &bufferPool);  
		if(rc!=SUCCESS) {return rc;}

		//rc = utl->InitBuffer(m_JoinThreadParams[t].writeBuffer, writeMemSize, &bufferPool);
		//if(rc!=SUCCESS) {  return rc; }

		m_JoinPlan1ThreadParams[t].overlapWrite.dwBytesToReadWrite = writeMemSize; 
		m_JoinPlan1ThreadParams[t].overlapWrite.dwBytesReadWritten = 0;  
		m_JoinPlan1ThreadParams[t].overlapWrite.totalChunk = 0;
		m_JoinPlan1ThreadParams[t].overlapWrite.chunkIndex = 0;
		m_JoinPlan1ThreadParams[t].overlapWrite.startChunk = 0;
		m_JoinPlan1ThreadParams[t].overlapWrite.endChunk = 0;
		m_JoinPlan1ThreadParams[t].overlapWrite.fileSize.QuadPart = 0; 
		m_JoinPlan1ThreadParams[t].overlapWrite.overlap.Offset = 0;
		m_JoinPlan1ThreadParams[t].overlapWrite.overlap.OffsetHigh = 0; 
	}


	return SUCCESS;
}

/// <summary>
/// Wrapper for join thread function.
/// </summary>
/// <param name="lpParam">The thread parameter.</param>
/// <returns></returns>
DWORD WINAPI PSMJ::JoinPhase_Plan1_Ex(LPVOID lpParam)
{ 
	JoinPlan1ThreadParams* p = (JoinPlan1ThreadParams*)lpParam;
	p->_this->JoinPhase_Plan1_Func((LPVOID)(p)); // no barrier version 
	return 0;
}
 
/// <summary>
/// Join thread function.
/// Join execute without barrier, dont need any synchronize
/// </summary>
/// <param name="lpParam">The thread parameter.</param>
/// <returns></returns>
DWORD WINAPI PSMJ::JoinPhase_Plan1_Func(LPVOID lpParam)
{  
	JoinPlan1ThreadParams* p = (JoinPlan1ThreadParams*)lpParam;

	// Do join operation 
	for(UINT rIdx=0; rIdx < p->R_FanInCount; rIdx++)
	{
		// Read R 
		utl->ResetBuffer(p->readBufferR);

		FANS *rFan = p->R_FanIns.front();
		p->R_FanIns.pop();

		DWORD dwBytesReadR = 0;
		HANDLE hR = CreateFile(
			(LPCWSTR)rFan->fileName, // file to open
			GENERIC_READ,			// open for reading
			0,						// dont share  
			NULL,					// default security
			OPEN_EXISTING,			// existing file only
			FILE_ATTRIBUTE_NORMAL,	// overlapped operation //| FILE_FLAG_NO_BUFFERING
			NULL);		

		if(hR==INVALID_HANDLE_VALUE)
		{
			ShowMB(L"Cannot create handle %s", rFan->fileName);
			return ERR_CANNOT_CREATE_HANDLE;
		}

		ReadFile(hR, p->readBufferR.data, p->readBufferR.size, &dwBytesReadR, NULL); 

		utl->ComputeBuffer(p->readBufferR, dwBytesReadR);

		// We dont need this handle anymore
		CloseHandle(hR);

		if(dwBytesReadR > 0)
		{
			// Join with all S
			for(int sIdx=0; sIdx < shareS.totalCount; sIdx++)
			{ 
				// TODO: Check for key R and S are in range or not?
				if((rFan->lowestKey > shareS.highestKey[sIdx]) || (shareS.lowestKey[sIdx] > rFan->highestKey))
				{ 
					continue;
				}

				// Read S  
				utl->ResetBuffer(p->readBufferS);

				DWORD dwBytesReadS = 0;
				m_CS.Lock();
				if(SetFilePointer(shareS.hFile[sIdx], 0, 0, FILE_BEGIN)==INVALID_SET_FILE_POINTER)
				{ 
					// What's the hell
				} 

				ReadFile(shareS.hFile[sIdx], p->readBufferS.data, p->readBufferS.size, &dwBytesReadS, NULL);
				m_CS.UnLock();

				utl->ComputeBuffer(p->readBufferS, dwBytesReadS);

				JoinPhase_Plan1_Join(p); 
			}   
		} // dwBytesReadR > 0 
	} // for rIndex

	//Wait until current write process complete 
	GetOverlappedResult(p->hWrite, &p->overlapWrite.overlap, &p->overlapWrite.dwBytesReadWritten, TRUE); 
	if(p->overlapWrite.dwBytesReadWritten > 0)
	{
		if(FRONT_BUFFER(p->dbcWrite).currentSize > 0)
		{
			p->overlapWrite.fileSize.QuadPart += p->overlapWrite.dwBytesReadWritten;  
			p->overlapWrite.overlap.Offset = p->overlapWrite.fileSize.LowPart;
			p->overlapWrite.overlap.OffsetHigh = p->overlapWrite.fileSize.HighPart;  
			p->overlapWrite.dwBytesReadWritten = 0; 
			utl->ResetBuffer(FRONT_BUFFER(p->dbcWrite));  
		}
	}
	p->dbcWrite->UnLockConsumer();

	//////////////////////////////////////////////////////////////////////////

	if(BACK_BUFFER(p->dbcWrite).currentSize > 0)
	{    
		WriteFile(p->hWrite, 
			BACK_BUFFER(p->dbcWrite).data, 
			BACK_BUFFER(p->dbcWrite).currentSize, 
			&p->overlapWrite.dwBytesReadWritten, 
			&p->overlapWrite.overlap);  

		GetOverlappedResult(p->hWrite, &p->overlapWrite.overlap, &p->overlapWrite.dwBytesReadWritten, TRUE);

		p->overlapWrite.fileSize.QuadPart += p->overlapWrite.dwBytesReadWritten;  
		p->overlapWrite.overlap.Offset = p->overlapWrite.fileSize.LowPart;
		p->overlapWrite.overlap.OffsetHigh = p->overlapWrite.fileSize.HighPart;  
		p->overlapWrite.dwBytesReadWritten = 0;  
		utl->ResetBuffer(BACK_BUFFER(p->dbcWrite));  
	}


	// Cleaning
	delete p->joinFilePath;
	CloseHandle(p->hWrite);

	return 0;
}

/// <summary>
/// Joins operation.
/// </summary>
/// <param name="lpParam">The thread parameter.</param>
/// <returns></returns>
RC PSMJ::JoinPhase_Plan1_Join(LPVOID lpParam)
{
	JoinPlan1ThreadParams* p = (JoinPlan1ThreadParams*)lpParam;

	p->readBufferR.currentPageIndex = 0;
	utl->GetPageInfo(p->readBufferR.data, p->pageBufferR, p->readBufferR.currentPageIndex,  SSD_PAGE_SIZE);  

	p->readBufferS.currentPageIndex = 0;
	utl->GetPageInfo(p->readBufferS.data, p->pageBufferS, p->readBufferS.currentPageIndex,  SSD_PAGE_SIZE);  

	p->pageBufferR.currentTupleIndex = 1;
	utl->GetTupleInfo(p->tupleR, p->pageBufferR.currentTupleIndex, p->pageBufferR.data, SSD_PAGE_SIZE, p->R_Key);  
	p->pageBufferR.currentTupleIndex++;

	p->pageBufferS.currentTupleIndex = 1;
	utl->GetTupleInfo(p->tupleS, p->pageBufferS.currentTupleIndex, p->pageBufferS.data, SSD_PAGE_SIZE, p->S_Key);  
	p->pageBufferS.currentTupleIndex++;

	// This approach is wasting time to read S
	// Because with one R need to read all S many time while other thread already read that S

	while((p->tupleR->key!=MAX) && (p->tupleS->key!=MAX))
	{  
		// while left is less than right, move left up
		while (p->tupleR->key < p->tupleS->key) 
		{
			JoinPhase_Plan1_GetNextTuple(&p->readBufferR, p->pageBufferR, p->tupleR, p->R_Key); 
			if (p->tupleR->key == MAX)  { break; }
		} 

		// if done, no more joins, break
		if (p->tupleR->key == MAX)  { break; } 

		// while left is greater than right, move right up
		while (p->tupleR->key > p->tupleS->key) 
		{
			JoinPhase_Plan1_GetNextTuple(&p->readBufferS, p->pageBufferS, p->tupleS, p->S_Key); 
			if (p->tupleS->key == MAX) { break; }
		} 

		// if done, no more joins, break
		if (p->tupleS->key == MAX) {  break;  }

		// while the two are equal, segment equal
		while (p->tupleR->key == p->tupleS->key) 
		{   
			JoinPhase_MakeJoinRecord(p->tupleRS, p->tupleR, p->tupleS);

			// Send this join tuple to BACK write buffer 
			JoinPhase_Plan1_SentOutput(p, p->tupleRS); 

			// Get next S tuple 
			JoinPhase_Plan1_GetNextTuple(&p->readBufferS, p->pageBufferS, p->tupleS, p->S_Key);

			while (p->tupleS->key == p->tupleR->key) 
			{  
				JoinPhase_MakeJoinRecord(p->tupleRS, p->tupleR, p->tupleS);

				// Save this to Output buffer
				JoinPhase_Plan1_SentOutput(p, p->tupleRS); 

				// Get next S tuple
				JoinPhase_Plan1_GetNextTuple(&p->readBufferS, p->pageBufferS, p->tupleS, p->S_Key); 
				if (p->tupleS->key == MAX)  { break; }
			}

			// Get next R tuple
			JoinPhase_Plan1_GetNextTuple(&p->readBufferR, p->pageBufferR, p->tupleR, p->R_Key);   

			if (p->tupleR->key == MAX)  {  break;  }
		}

		// Get next S tuple
		JoinPhase_Plan1_GetNextTuple(&p->readBufferS, p->pageBufferS, p->tupleS, p->S_Key);   
	}


	return SUCCESS;
}


/// <summary>
/// Get next tuple in buffer.
/// </summary>
/// <param name="bufferPtr">The buffer PTR.</param>
/// <param name="pageBuffer">The page buffer.</param>
/// <param name="recordPtr">The record PTR.</param>
/// <param name="keyPos">The key position in table.</param>
/// <returns></returns>
RC PSMJ::JoinPhase_Plan1_GetNextTuple(Buffer *bufferPtr, Buffer &pageBuffer, RECORD *recordPtr, DWORD keyPos)
{   
	if(pageBuffer.currentTupleIndex > pageBuffer.tupleCount)
	{ 
		bufferPtr->currentPageIndex++;   
		if(bufferPtr->currentPageIndex >= bufferPtr->pageCount)
		{     
			// Buffer empty
			utl->ResetBuffer(pageBuffer);  
			utl->SetMaxTuple(recordPtr); 
			return SUCCESS;
		}

		utl->GetPageInfo(bufferPtr->data, pageBuffer, bufferPtr->currentPageIndex,  SSD_PAGE_SIZE);  
		pageBuffer.currentTupleIndex=1; 
	}

	utl->GetTupleInfo(recordPtr, pageBuffer.currentTupleIndex, pageBuffer.data, SSD_PAGE_SIZE, keyPos);  
	pageBuffer.currentTupleIndex++; 

	return SUCCESS;
} 


/// <summary>
/// Sent join tuple to buffer, if buffer then write to disk.
/// </summary>
/// <param name="lpParam">The thread parameter.</param>
/// <param name="recordPtr">The record PTR.</param>
/// <returns></returns>
RC PSMJ::JoinPhase_Plan1_SentOutput(LPVOID lpParam, RECORD *recordPtr)
{
	JoinPlan1ThreadParams* p = (JoinPlan1ThreadParams*)lpParam;

	////////////////////////////////////////////////////////////////////////// 
	// With barrier version, need write buffer large enough for reducing waiting time
	// Need concern about atomic
	// At this moment, simulate join count only

	InterlockedExchangeAdd(&m_TotalJoinCount, 1); 

	if(utl->IsJoinPageFull(p->pageWrite))
	{   
		utl->AddPageToBuffer(BACK_BUFFER(p->dbcWrite), p->pageWrite->page, SSD_PAGE_SIZE); //4 
		//BACK_BUFFER(p->dbcWrite).currentPageIndex++; 
		BACK_BUFFER(p->dbcWrite).pageCount++;
		utl->ResetRunPage(p->pageWrite, p->pageWriteBuffer);  
		p->pageWrite->consumed = TRUE; 
	}  

	if(utl->IsBufferFull(BACK_BUFFER(p->dbcWrite)))  
	{     
		if(p->dbcWrite->bFirstProduce==TRUE) // Back Buffer is full, check is it the first product
		{ 
			p->dbcWrite->bFirstProduce=FALSE;  
			p->dbcWrite->SwapBuffers();   

			p->dbcWrite->LockConsumer();  // lock no lai

			WriteFile(p->hWrite, 
				FRONT_BUFFER(p->dbcWrite).data, 
				FRONT_BUFFER(p->dbcWrite).currentSize, 
				&p->overlapWrite.dwBytesReadWritten, 
				&p->overlapWrite.overlap); 	
		}
		else
		{    
			//Wait for write complete   
			GetOverlappedResult(p->hWrite, &p->overlapWrite.overlap, &p->overlapWrite.dwBytesReadWritten, TRUE) ;  

			p->overlapWrite.fileSize.QuadPart += p->overlapWrite.dwBytesReadWritten;  
			p->overlapWrite.overlap.Offset = p->overlapWrite.fileSize.LowPart;
			p->overlapWrite.overlap.OffsetHigh = p->overlapWrite.fileSize.HighPart;  
			p->overlapWrite.dwBytesReadWritten = 0;

			utl->ResetBuffer(FRONT_BUFFER(p->dbcWrite));  // reset for next data

			p->dbcWrite->UnLockConsumer(); 

			p->dbcWrite->SwapBuffers();  

			p->dbcWrite->LockConsumer();  
			WriteFile(p->hWrite, 
				FRONT_BUFFER(p->dbcWrite).data, 
				FRONT_BUFFER(p->dbcWrite).currentSize, 
				&p->overlapWrite.dwBytesReadWritten, 
				&p->overlapWrite.overlap); 
		}   
	}

	utl->AddTupleToPage(p->pageWrite, recordPtr, p->pageWriteBuffer);   // Add this tuples to page   
	p->pageWrite->consumed = FALSE;
	return SUCCESS; 
}

#pragma endregion 

//////////////////////////////////////////////////////////////////////////
//
// JOIN PHASE PLAN 2
// Each Ri join with all Si with barrier sync
//////////////////////////////////////////////////////////////////////////

#pragma region "Join Phase Plan 2" 

/// <summary>
/// Initialize join phase.
/// </summary>
/// <returns></returns>
RC PSMJ::JoinPhase_Plan2_Initialize()
{ 
	RC rc;

	// Reset buffer pool
	bufferPool.currentSize = 0;

	SYSTEM_INFO sysinfo;
	GetSystemInfo(&sysinfo);
	if(m_PsmjParams.THREAD_NUM==0)
	{
		m_WorkerThreadNum = sysinfo.dwNumberOfProcessors; 
	}

	// Share read S
	S_FanInCount = m_S.size(); 

	// Init runtime S fanIn in barrier version
	m_runtimeS = m_S; // m_runtimeS may change when the last thread arrive barrier

	R_FanInCount = m_R.size();
	DWORD _Rcount = R_FanInCount; 

	if(R_FanInCount < m_WorkerThreadNum) // dont need too many thread
	{
		m_WorkerThreadNum = R_FanInCount; 
	}

	rp.JoinThreadNum = m_WorkerThreadNum;

	m_hWorkerThread = new HANDLE[m_WorkerThreadNum]; 
	m_JoinPlan2ThreadParams = new JoinPlan2ThreadParams[m_WorkerThreadNum];

	// Calculate average r,s fanIn count for avoiding barrier deadlock
	DWORD rCountAvg = 0, sCountAvg = 0;
	rCountAvg = chROUNDUP(R_FanInCount, m_WorkerThreadNum) / m_WorkerThreadNum;
	sCountAvg = chROUNDUP(S_FanInCount, m_WorkerThreadNum) / m_WorkerThreadNum;

	// Init barrier
	barrier1 = new Barrier(m_WorkerThreadNum);  
	barrier2 = new Barrier(m_WorkerThreadNum); 
	barrier3 = new Barrier(m_WorkerThreadNum); 
	barrier4 = new Barrier(m_WorkerThreadNum); 

	for(UINT tIdx=0; tIdx < m_WorkerThreadNum; tIdx++)
	{
		m_JoinPlan2ThreadParams[tIdx].R_FanInCount = 0;
		m_JoinPlan2ThreadParams[tIdx].S_FanInCount = S_FanInCount; 
		m_JoinPlan2ThreadParams[tIdx].R_AvgFanInCount = rCountAvg;
		m_JoinPlan2ThreadParams[tIdx].S_AvgFanInCount = sCountAvg;
	}  

	// Arrange R fanIn to worker thread 
	std::queue<FANS*> tempFanInOfR = m_R; // temp 
	while(_Rcount>0)
	{
		for(UINT tIdx=0; tIdx < m_WorkerThreadNum; tIdx++)
		{ 
			m_JoinPlan2ThreadParams[tIdx].R_FanInCount++;

			FANS *rFan = tempFanInOfR.front();
			m_JoinPlan2ThreadParams[tIdx].R_FanIns.push(rFan);
			tempFanInOfR.pop();

			_Rcount--;

			if(_Rcount==0) { break; }
		} 
	}

	DWORD ReadBufferSize = m_RunSize;

	for(UINT tIdx=0; tIdx < m_WorkerThreadNum; tIdx++)
	{
		m_JoinPlan2ThreadParams[tIdx]._this = this;
		//////////////////////////////////////////////////////////////////////////
		//m_JoinThreadParams[t].hR = new HANDLE[m_JoinThreadParams[t].R_FanInCount];
		m_JoinPlan2ThreadParams[tIdx].tupleR = new RECORD();
		m_JoinPlan2ThreadParams[tIdx].tupleS = new RECORD();
		m_JoinPlan2ThreadParams[tIdx].tupleRS = new RECORD(TUPLE_SIZE*2);

		m_JoinPlan2ThreadParams[tIdx].R_Key = m_PsmjParams.R_KEY_POS;
		m_JoinPlan2ThreadParams[tIdx].S_Key = m_PsmjParams.S_KEY_POS; 

		// Init buffer
		// Read buffer for R
		rc = utl->InitBuffer(m_JoinPlan2ThreadParams[tIdx].readBufferR, ReadBufferSize, &bufferPool);
		if(rc!=SUCCESS) {  return rc; } 
		rc = utl->InitBuffer(m_JoinPlan2ThreadParams[tIdx].pageBufferR, SSD_PAGE_SIZE, &bufferPool);
		if(rc!=SUCCESS) {  return rc; } 
		rc = utl->InitRunPage(m_JoinPlan2ThreadParams[tIdx].pageR, m_JoinPlan2ThreadParams[tIdx].pageBufferR);
		if(rc!=SUCCESS) {  return rc; }

		// Read buffer for S
		rc = utl->InitBuffer(m_JoinPlan2ThreadParams[tIdx].readBufferS, ReadBufferSize, &bufferPool);
		if(rc!=SUCCESS) {  return rc; }  
		rc = utl->InitBuffer(m_JoinPlan2ThreadParams[tIdx].pageBufferS, SSD_PAGE_SIZE, &bufferPool);
		if(rc!=SUCCESS) {  return rc; }  
		rc = utl->InitRunPage(m_JoinPlan2ThreadParams[tIdx].pageS, m_JoinPlan2ThreadParams[tIdx].pageBufferS);
		if(rc!=SUCCESS) {  return rc; } 

		// Init write buffers 
		rc = utl->InitBuffer(m_JoinPlan2ThreadParams[tIdx].pageWriteBuffer, SSD_PAGE_SIZE, &bufferPool);
		if(rc!=SUCCESS) {  return rc; }

		rc = utl->InitRunPage(m_JoinPlan2ThreadParams[tIdx].pageWrite, m_JoinPlan2ThreadParams[tIdx].pageWriteBuffer);
		if(rc!=SUCCESS) {  return rc; }

		// Variable for write join tuple to disk
		m_JoinPlan2ThreadParams[tIdx].joinFilePath = new TCHAR[MAX_PATH];
		JoinPhase_GetFanOutPath(m_JoinPlan2ThreadParams[tIdx].joinFilePath, tIdx);

		m_JoinPlan2ThreadParams[tIdx].hWrite = CreateFile(
			m_JoinPlan2ThreadParams[tIdx].joinFilePath,	 
			GENERIC_WRITE,			 
			0,						 
			NULL,					 
			CREATE_ALWAYS,			 
			FILE_FLAG_OVERLAPPED, 
			NULL);		

		if (INVALID_HANDLE_VALUE==m_JoinPlan2ThreadParams[tIdx].hWrite) 
		{   
			ShowMB(L"Cannot open file %s\r\n", m_JoinPlan2ThreadParams[tIdx].joinFilePath);
			return ERR_CANNOT_CREATE_HANDLE; 
		}  
	}

	// Calculate write buffer size
	DWORD memSizeFree = bufferPool.size - bufferPool.currentSize;
	if(memSizeFree==0) {return ERR_NOT_ENOUGH_MEMORY;}

	DWORD writeMemSize = chROUNDDOWN((memSizeFree / m_WorkerThreadNum) /2, SSD_PAGE_SIZE); // must be > 0 and multiple of 4096, divide 2 because use double buffer
	rp.JoinWriteBufferSize = writeMemSize;

	if(writeMemSize <  SSD_PAGE_SIZE*256)
	{
		chMB("Alert::Memory for write below 1MB");
	}

	for(UINT tIdx=0; tIdx < m_WorkerThreadNum; tIdx++)
	{
		//#pragma chMSG(Join Write buffer size is fixed as 4MB)
		//rc = utl->InitBuffer(m_JoinThreadParamsBarrier[t].writeBuffer, writeMemSize, &bufferPool);
		//if(rc!=SUCCESS) {  return rc; }

		m_JoinPlan2ThreadParams[tIdx].dbcWrite = new DoubleBuffer(writeMemSize); 
		rc = utl->InitBuffer(m_JoinPlan2ThreadParams[tIdx].dbcWrite->buffer[0], writeMemSize, &bufferPool);
		if(rc!=SUCCESS) {return rc;}
		rc = utl->InitBuffer(m_JoinPlan2ThreadParams[tIdx].dbcWrite->buffer[1], writeMemSize, &bufferPool);  
		if(rc!=SUCCESS) {return rc;}

		m_JoinPlan2ThreadParams[tIdx].overlapWrite.dwBytesToReadWrite = writeMemSize; 
		m_JoinPlan2ThreadParams[tIdx].overlapWrite.dwBytesReadWritten = 0;  
		m_JoinPlan2ThreadParams[tIdx].overlapWrite.totalChunk = 0;
		m_JoinPlan2ThreadParams[tIdx].overlapWrite.chunkIndex = 0;
		m_JoinPlan2ThreadParams[tIdx].overlapWrite.startChunk = 0;
		m_JoinPlan2ThreadParams[tIdx].overlapWrite.endChunk = 0;
		m_JoinPlan2ThreadParams[tIdx].overlapWrite.fileSize.QuadPart = 0; 
		m_JoinPlan2ThreadParams[tIdx].overlapWrite.overlap.Offset = 0;
		m_JoinPlan2ThreadParams[tIdx].overlapWrite.overlap.OffsetHigh = 0; 
	}

	return SUCCESS;
}

/// <summary>
/// Wrapper for join thread function.
/// </summary>
/// <param name="lpParam">The thread parameter.</param>
/// <returns></returns>
DWORD WINAPI PSMJ::JoinPhase_Plan2_Ex(LPVOID lpParam)
{
	JoinPlan2ThreadParams* p = (JoinPlan2ThreadParams*)lpParam; 
	p->_this->JoinPhase_Plan2_Func((LPVOID)(p));  
	return 0;
}

/// <summary>
/// Join thread function.
/// </summary>
/// <param name="lpParam">The thread parameter.</param>
/// <returns></returns>
DWORD WINAPI PSMJ::JoinPhase_Plan2_Func(LPVOID lpParam)
{ 
	JoinPlan2ThreadParams* p = (JoinPlan2ThreadParams*)lpParam;
	FANS *sFanIn = new FANS();

	// Wait for all thread started 
	WaitOtherThreads(barrier1, FALSE);

	//// Do join operation 
	for(UINT rIdx=0; rIdx < p->R_AvgFanInCount; rIdx++)
	{
		// Read R 
		// TODO: check current Index
		DWORD dwBytesReadR = 0; 

		// Empty R buffer
		utl->ResetBuffer(p->readBufferR);

		if(rIdx < p->R_FanInCount) // check this index or deadlock
		{ 
			if(p->R_FanIns.size()>0)
			{
				FANS *rFan = p->R_FanIns.front();
				p->R_FanIns.pop();

				HANDLE hR = CreateFile(
					(LPCWSTR)rFan->fileName,  
					GENERIC_READ,			 
					FILE_SHARE_READ,         
					NULL,					 
					OPEN_EXISTING,			 
					FILE_ATTRIBUTE_NORMAL,	 
					NULL);		

				if(hR==INVALID_HANDLE_VALUE)
				{
					ShowMB(L"Cannot create handle %s", rFan->fileName); 
				} 

				// Read R fanIn content into buffer
				ReadFile(hR, p->readBufferR.data, p->readBufferR.size, &dwBytesReadR, NULL);  

				// Let's see what do we have in this buffer
				utl->ComputeBuffer(p->readBufferR, dwBytesReadR);
				p->readBufferR.lowestKey = rFan->lowestKey;
				p->readBufferR.highestKey = rFan->highestKey;

				// We don't need this handle any more
				CloseHandle(hR);
			} 
		}

		// Wait for all thread have been read R run
		WaitOtherThreads(barrier2, FALSE); 

		for(int sIdx=0; sIdx < p->S_AvgFanInCount; sIdx++)
		{
			// Read S 
			// TODO: check current Index
			//////////////////////////////////////////////////////////////////////////
			BOOL sStatus = FALSE;
			DWORD dwBytesReadS = 0;

			// Empty S buffer
			utl->ResetBuffer(p->readBufferS);

			// Get S fanIn from queue
			JoinPhase_Plan2_GetSrunPath(sFanIn, sStatus);
			if(sStatus==TRUE)
			{
				HANDLE hSFile = CreateFile(
					(LPCWSTR)sFanIn->fileName, // file to open
					GENERIC_READ,			// open for reading
					FILE_SHARE_READ,        // share for reading
					NULL,					// default security
					OPEN_EXISTING,			// existing file only
					FILE_ATTRIBUTE_NORMAL,	// overlapped operation //| FILE_FLAG_NO_BUFFERING
					NULL);	 

				// Read file content to buffer
				ReadFile(hSFile, p->readBufferS.data, p->readBufferS.size, &dwBytesReadS, NULL);

				// Calculate what do we have in this buffer?
				utl->ComputeBuffer(p->readBufferS, dwBytesReadS);
				p->readBufferS.lowestKey = sFanIn->lowestKey;
				p->readBufferS.highestKey = sFanIn->highestKey;

				// I don't need u anymore
				CloseHandle(hSFile);
			}
			//////////////////////////////////////////////////////////////////////////
			// Wait for all thread have been read S run
			WaitOtherThreads(barrier3, FALSE); 
			//////////////////////////////////////////////////////////////////////////

			// At this point, it's safe to read shared data from other thread,
			// Because READ ONLY thus the consistent is OK
			// Join current R with other threads buffer 
			// Read shared data from other threads
			for(int tIdx=0; tIdx < m_WorkerThreadNum; tIdx++)
			{   
				// join two buffer  
				// TODO: need to check buffer have data or not?
				JoinPhase_Plan2_Join(p, p->readBufferR, m_JoinPlan2ThreadParams[tIdx].readBufferS, p->R_Key, p->S_Key);
			} 

			//////////////////////////////////////////////////////////////////////////
			// Wait for other threads complete their works
			// We must do not change thread buffer when other thread still using it
			WaitOtherThreads(barrier4, FALSE); 
			//////////////////////////////////////////////////////////////////////////
		} // end for S run


		// Wait other threads complete their works then continue read R run
		// Reset S run to default (ONLY one Thread can access this value)
		WaitOtherThreads(barrier3, TRUE);
		//////////////////////////////////////////////////////////////////////////
	} //end for R run

	//// write the last data in buffer to disk
	//if(p->writeBuffer.currentSize>0)  
	//{  
	//	DWORD dwByteWritten = 0;
	//	WriteFile(p->hWrite, 
	//		p->writeBuffer.data, 
	//		p->writeBuffer.currentSize, 
	//		&dwByteWritten, 
	//		NULL);

	//	utl->ResetBuffer(p->writeBuffer);
	//}


	//Wait until current write process complete 
	GetOverlappedResult(p->hWrite, &p->overlapWrite.overlap, &p->overlapWrite.dwBytesReadWritten, TRUE); 
	if(p->overlapWrite.dwBytesReadWritten > 0)
	{
		if(FRONT_BUFFER(p->dbcWrite).currentSize > 0)
		{
			p->overlapWrite.fileSize.QuadPart += p->overlapWrite.dwBytesReadWritten;  
			p->overlapWrite.overlap.Offset = p->overlapWrite.fileSize.LowPart;
			p->overlapWrite.overlap.OffsetHigh = p->overlapWrite.fileSize.HighPart;  
			p->overlapWrite.dwBytesReadWritten = 0; 
			utl->ResetBuffer(FRONT_BUFFER(p->dbcWrite));  
		}
	}
	p->dbcWrite->UnLockConsumer();

	//////////////////////////////////////////////////////////////////////////

	if(BACK_BUFFER(p->dbcWrite).currentSize > 0)
	{    
		WriteFile(p->hWrite, 
			BACK_BUFFER(p->dbcWrite).data, 
			BACK_BUFFER(p->dbcWrite).currentSize, 
			&p->overlapWrite.dwBytesReadWritten, 
			&p->overlapWrite.overlap);  

		GetOverlappedResult(p->hWrite, &p->overlapWrite.overlap, &p->overlapWrite.dwBytesReadWritten, TRUE);

		p->overlapWrite.fileSize.QuadPart += p->overlapWrite.dwBytesReadWritten;  
		p->overlapWrite.overlap.Offset = p->overlapWrite.fileSize.LowPart;
		p->overlapWrite.overlap.OffsetHigh = p->overlapWrite.fileSize.HighPart;  
		p->overlapWrite.dwBytesReadWritten = 0;  
		utl->ResetBuffer(BACK_BUFFER(p->dbcWrite));  
	}

	// Wait for other thread before exit current thread
	WaitOtherThreads(barrier1, FALSE);

	// Cleanning
	CloseHandle(p->hWrite);
	delete p->joinFilePath; 

	return 0;
}

/// <summary>
/// Join operation.
/// </summary>
/// <param name="lpParam">The thread parameter.</param>
/// <param name="r_Buffer">The R buffer.</param>
/// <param name="s_Buffer">The S buffer.</param>
/// <param name="r_KeyPos">The R key position.</param>
/// <param name="s_KeyPos">The S key position.</param>
/// <returns></returns>
RC PSMJ::JoinPhase_Plan2_Join(LPVOID lpParam, const Buffer r_Buffer, const Buffer s_Buffer, const DWORD r_KeyPos,const DWORD s_KeyPos)
{ 

	// Check for buffers have data content or not
	// If anyone of them is empty, return immediately
	if((r_Buffer.currentSize==0) || (s_Buffer.currentSize==0))
	{
		return SUCCESS; 
	}

	// TODO: check the lowest value and highest value is in range for join or not
	// to do so, we will save many cpu instruction
	// but in barrier version, this may not helpful, 
	// because current thread still need to sync with other at barrier
	if(r_Buffer.lowestKey > s_Buffer.highestKey)
	{
		return SUCCESS; // don't need to join because not in range
	}

	if(s_Buffer.lowestKey > r_Buffer.highestKey)
	{
		return SUCCESS; // don't need to join because not in range
	}


	JoinPlan2ThreadParams* p = (JoinPlan2ThreadParams*)lpParam;

	DWORD r_currentPageIndex = 0, s_currentPageIndex=0;
	DWORD r_currentTupleIndex = 1, s_currentTupleIndex=1;
	utl->GetPageInfo(r_Buffer.data, p->pageBufferR, r_currentPageIndex,  SSD_PAGE_SIZE);   
	utl->GetPageInfo(s_Buffer.data, p->pageBufferS, s_currentPageIndex,  SSD_PAGE_SIZE);   

	utl->GetTupleInfo(p->tupleR, r_currentTupleIndex, p->pageBufferR.data, SSD_PAGE_SIZE, r_KeyPos);  
	r_currentTupleIndex++;

	utl->GetTupleInfo(p->tupleS, s_currentTupleIndex, p->pageBufferS.data, SSD_PAGE_SIZE, s_KeyPos);  
	s_currentTupleIndex++;

	while((p->tupleR->key!=MAX) && (p->tupleS->key!=MAX))
	{  
		// while left is less than right, move left up
		while (p->tupleR->key < p->tupleS->key) 
		{
			JoinPhase_Plan2_GetNextTuple(&r_Buffer, p->pageBufferR, p->tupleR, r_currentPageIndex, r_currentTupleIndex, r_KeyPos); 
			if (p->tupleR->key == MAX)  { break; }
		} 

		// if done, no more joins, break
		if (p->tupleR->key == MAX)  { break; } 

		// while left is greater than right, move right up
		while (p->tupleR->key > p->tupleS->key) 
		{
			JoinPhase_Plan2_GetNextTuple(&s_Buffer, p->pageBufferS, p->tupleS, s_currentPageIndex, s_currentTupleIndex, s_KeyPos); 
			if (p->tupleS->key == MAX) { break; }
		} 

		// if done, no more joins, break
		if (p->tupleS->key == MAX) {  break;  }

		// while the two are equal, segment equal
		while (p->tupleR->key == p->tupleS->key) 
		{   
			JoinPhase_MakeJoinRecord(p->tupleRS, p->tupleR, p->tupleS);

			// Send this join tuple to BACK write buffer 
			JoinPhase_Plan2_SentOutput(p, p->tupleRS); 

			// Get next S tuple  
			JoinPhase_Plan2_GetNextTuple(&s_Buffer, p->pageBufferS, p->tupleS, s_currentPageIndex, s_currentTupleIndex, s_KeyPos); 

			while (p->tupleS->key == p->tupleR->key) 
			{  
				JoinPhase_MakeJoinRecord(p->tupleRS, p->tupleR, p->tupleS);

				// Save this to Output buffer
				JoinPhase_Plan2_SentOutput(p, p->tupleRS); 

				// Get next S tuple  
				JoinPhase_Plan2_GetNextTuple(&s_Buffer, p->pageBufferS, p->tupleS, s_currentPageIndex, s_currentTupleIndex, s_KeyPos);
				if (p->tupleS->key == MAX)  { break; }
			}

			// Get next R tuple 
			JoinPhase_Plan2_GetNextTuple(&r_Buffer, p->pageBufferR, p->tupleR, r_currentPageIndex, r_currentTupleIndex, r_KeyPos); 
			if (p->tupleR->key == MAX)  {  break;  }
		}

		// Get next S tuple 
		JoinPhase_Plan2_GetNextTuple(&s_Buffer, p->pageBufferS, p->tupleS, s_currentPageIndex, s_currentTupleIndex, s_KeyPos);
	}

	return SUCCESS;
}

/// <summary>
/// Get next tuple from buffer.
/// </summary>
/// <param name="bufferPtr">The buffer PTR.</param>
/// <param name="pageBuffer">The page buffer.</param>
/// <param name="recordPtr">The record PTR.</param>
/// <param name="currentPageIndex">Index of the current page.</param>
/// <param name="currentTupleIndex">Index of the current tuple.</param>
/// <param name="keyPos">The key position.</param>
/// <returns></returns>
RC PSMJ::JoinPhase_Plan2_GetNextTuple(const Buffer *bufferPtr, Buffer &pageBuffer, RECORD *&recordPtr, DWORD &currentPageIndex, DWORD &currentTupleIndex, const DWORD keyPos)
{   
	// Use for barrier version
	// Do not change buffer data, because use for shared memory between threads

	if(currentTupleIndex > pageBuffer.tupleCount)
	{ 
		currentPageIndex++;   
		if(currentPageIndex >= bufferPtr->pageCount)
		{     
			// Buffer empty
			utl->ResetBuffer(pageBuffer);  
			utl->SetMaxTuple(recordPtr); 
			return SUCCESS;
		}

		utl->GetPageInfo(bufferPtr->data, pageBuffer, currentPageIndex,  SSD_PAGE_SIZE);  
		currentTupleIndex=1; 
	}

	utl->GetTupleInfo(recordPtr, currentTupleIndex, pageBuffer.data, SSD_PAGE_SIZE, keyPos);  
	currentTupleIndex++; 

	return SUCCESS;
} 

/// <summary>
/// Sent join tuple to output buffer, if buffer is full, then write buffer to disk.
/// </summary>
/// <param name="lpParam">The lp parameter.</param>
/// <param name="recordPtr">The record PTR.</param>
/// <returns></returns>
RC PSMJ::JoinPhase_Plan2_SentOutput(LPVOID lpParam, RECORD *recordPtr)
{
	JoinPlan2ThreadParams* p = (JoinPlan2ThreadParams*)lpParam;

	//////////////////////////////////////////////////////////////////////////
	// TODO: Write to disk 
	// With barrier version, need write buffer large enough for reducing waiting time
	// Need concern about atomic
	// At this moment, simulate join count only


	InterlockedExchangeAdd(&m_TotalJoinCount, 1); 

	if(utl->IsJoinPageFull(p->pageWrite))
	{   
		utl->AddPageToBuffer(BACK_BUFFER(p->dbcWrite), p->pageWrite->page, SSD_PAGE_SIZE); //4 
		//BACK_BUFFER(p->dbcWrite).currentPageIndex++; 
		BACK_BUFFER(p->dbcWrite).pageCount++;
		utl->ResetRunPage(p->pageWrite, p->pageWriteBuffer);  
		p->pageWrite->consumed = TRUE; 
	}  

	if(utl->IsBufferFull(BACK_BUFFER(p->dbcWrite)))  
	{     
		if(p->dbcWrite->bFirstProduce==TRUE) // Back Buffer is full, check is it the first product
		{ 
			p->dbcWrite->bFirstProduce=FALSE;  
			p->dbcWrite->SwapBuffers();   

			p->dbcWrite->LockConsumer();  // lock no lai

			WriteFile(p->hWrite, 
				FRONT_BUFFER(p->dbcWrite).data, 
				FRONT_BUFFER(p->dbcWrite).currentSize, 
				&p->overlapWrite.dwBytesReadWritten, 
				&p->overlapWrite.overlap); 	
		}
		else
		{    
			//Wait for write complete   
			GetOverlappedResult(p->hWrite, &p->overlapWrite.overlap, &p->overlapWrite.dwBytesReadWritten, TRUE) ;  

			p->overlapWrite.fileSize.QuadPart += p->overlapWrite.dwBytesReadWritten;  
			p->overlapWrite.overlap.Offset = p->overlapWrite.fileSize.LowPart;
			p->overlapWrite.overlap.OffsetHigh = p->overlapWrite.fileSize.HighPart;  
			p->overlapWrite.dwBytesReadWritten = 0;

			utl->ResetBuffer(FRONT_BUFFER(p->dbcWrite));  // reset for next data

			p->dbcWrite->UnLockConsumer(); 

			p->dbcWrite->SwapBuffers();  

			p->dbcWrite->LockConsumer();  
			WriteFile(p->hWrite, 
				FRONT_BUFFER(p->dbcWrite).data, 
				FRONT_BUFFER(p->dbcWrite).currentSize, 
				&p->overlapWrite.dwBytesReadWritten, 
				&p->overlapWrite.overlap); 
		}   
	}

	utl->AddTupleToPage(p->pageWrite, recordPtr, p->pageWriteBuffer);   // Add this tuples to page   
	p->pageWrite->consumed = FALSE;

	return SUCCESS; 


	/* No Double buffer write join data to disk
	InterlockedExchangeAdd(&m_TotalJoinCount, 1); 

	if(utl->IsJoinPageFull(p->pageWrite))
	{   
	utl->AddPageToBuffer(p->writeBuffer, p->pageWrite->page, SSD_PAGE_SIZE); //4 
	p->writeBuffer.currentPageIndex++; 
	utl->ResetRunPage(p->pageWrite, p->pageWriteBuffer);  
	p->pageWrite->consumed = TRUE; 
	}  

	if(utl->IsBufferFull(p->writeBuffer))  
	{  
	DWORD dwByteWritten = 0;
	WriteFile(p->hWrite, 
	p->writeBuffer.data, 
	p->writeBuffer.currentSize, 
	&dwByteWritten, 
	NULL);

	utl->ResetBuffer(p->writeBuffer);
	}

	utl->AddTupleToPage(p->pageWrite, recordPtr, p->pageWriteBuffer);   // Add this tuples to page   
	p->pageWrite->consumed = FALSE;
	return SUCCESS; 

	*/
}

/// <summary>
/// Get run path.
/// </summary>
/// <param name="sFanIn">The s fan in.</param>
/// <param name="status">The status.</param>
VOID PSMJ::JoinPhase_Plan2_GetSrunPath(FANS *&sFanIn, BOOL &status)
{
	// Get S file path 
	m_CS.Lock();

	status = FALSE; 
	if(m_runtimeS.size() > 0)
	{
		sFanIn = m_runtimeS.front(); // Get fan info from queue
		m_runtimeS.pop();
		status = TRUE; 
	}

	m_CS.UnLock();
}
#pragma endregion



//////////////////////////////////////////////////////////////////////////
//
// JOIN PHASE PLAN 3
// Merge all Ri and Si then execute join
//////////////////////////////////////////////////////////////////////////
#pragma region "Join Phase Plan 3" 

/// <summary>
/// Wrapper for join thread.
/// </summary>
/// <param name="lpParam">The thread parameter.</param>
/// <returns></returns>
DWORD WINAPI PSMJ::JoinPhase_Plan3_Ex(LPVOID lpParam)
{ 
	JoinPlan3ThreadParams* p = (JoinPlan3ThreadParams*)lpParam;
	p->_this->JoinPhase_Plan3_Func((LPVOID)(p)); 
	return 0;
}
 
/// <summary>
/// Join thread function.
/// </summary>
/// <param name="lpParam">The thread parameter.</param>
/// <returns></returns>
DWORD WINAPI PSMJ::JoinPhase_Plan3_Func(LPVOID lpParam)
{  
	JoinPlan3ThreadParams* p = (JoinPlan3ThreadParams*)lpParam; 

	DWORD dwBytesReadR = 0;
	DWORD dwBytesReadS = 0;
	OVERLAPPED overlapR = {0};
	OVERLAPPED overlapS = {0};

	// Do join operation 
	for(UINT rIdx=0; rIdx < p->R_FanInCount; rIdx++)
	{
		// Read R 
		utl->ResetBuffer(p->readBufferR); 

		overlapR.Offset = p->PartitionR[rIdx].fileOffsetStart.LowPart;
		overlapR.OffsetHigh = p->PartitionR[rIdx].fileOffsetStart.HighPart;
		ReadFile(hR, p->readBufferR.data, p->readBufferR.size, &dwBytesReadR, &overlapR); 
		GetOverlappedResult(hR, &overlapR, &dwBytesReadR, TRUE);
		utl->ComputeBuffer(p->readBufferR, dwBytesReadR);

		if(dwBytesReadR > 0)
		{
			// Join with all S
			for(UINT sIdx=0; sIdx < p->S_FanInCount; sIdx++)
			{ 
				// TODO: Check for key R and S are in range or not?
				if((p->PartitionR[rIdx].lowestKey > p->PartitionS[sIdx].highestKey) || ( p->PartitionS[sIdx].lowestKey > p->PartitionR[rIdx].highestKey))
				{ 
					// Skip this sIdx
					p->ReadSkipCountS++;
					continue;
				}

				// Read S  
				utl->ResetBuffer(p->readBufferS);

				overlapS.Offset = p->PartitionS[sIdx].fileOffsetStart.LowPart;
				overlapS.OffsetHigh = p->PartitionS[sIdx].fileOffsetStart.HighPart;
				ReadFile(hS, p->readBufferS.data, p->readBufferS.size, &dwBytesReadS, &overlapS);
				GetOverlappedResult(hS, &overlapS, &dwBytesReadS, TRUE); 
				utl->ComputeBuffer(p->readBufferS, dwBytesReadS);

				p->ReadCountS++;

				JoinPhase_Plan3_Join(p); 
			}   
		} // dwBytesReadR > 0 
	} // for rIndex

	//Wait until current write process complete 
	GetOverlappedResult(p->hWrite, &p->overlapWrite.overlap, &p->overlapWrite.dwBytesReadWritten, TRUE); 
	if(p->overlapWrite.dwBytesReadWritten > 0)
	{
		if(FRONT_BUFFER(p->dbcWrite).currentSize > 0)
		{
			p->overlapWrite.fileSize.QuadPart += p->overlapWrite.dwBytesReadWritten;  
			p->overlapWrite.overlap.Offset = p->overlapWrite.fileSize.LowPart;
			p->overlapWrite.overlap.OffsetHigh = p->overlapWrite.fileSize.HighPart;  
			p->overlapWrite.dwBytesReadWritten = 0; 
			utl->ResetBuffer(FRONT_BUFFER(p->dbcWrite));  
		}
	}
	p->dbcWrite->UnLockConsumer();

	//////////////////////////////////////////////////////////////////////////

	if(BACK_BUFFER(p->dbcWrite).currentSize > 0)
	{    
		WriteFile(p->hWrite, 
			BACK_BUFFER(p->dbcWrite).data, 
			BACK_BUFFER(p->dbcWrite).currentSize, 
			&p->overlapWrite.dwBytesReadWritten, 
			&p->overlapWrite.overlap);  

		GetOverlappedResult(p->hWrite, &p->overlapWrite.overlap, &p->overlapWrite.dwBytesReadWritten, TRUE);

		p->overlapWrite.fileSize.QuadPart += p->overlapWrite.dwBytesReadWritten;  
		p->overlapWrite.overlap.Offset = p->overlapWrite.fileSize.LowPart;
		p->overlapWrite.overlap.OffsetHigh = p->overlapWrite.fileSize.HighPart;  
		p->overlapWrite.dwBytesReadWritten = 0;  
		utl->ResetBuffer(BACK_BUFFER(p->dbcWrite));  
	}


	// Cleaning
	delete p->joinFilePath;
	CloseHandle(p->hWrite);

	return 0;
}


/// <summary>
/// Initialize join thread.
/// </summary>
/// <returns></returns>
RC PSMJ::JoinPhase_Plan3_Initialize()
{
	RC rc;
	// Reset buffer pool
	bufferPool.currentSize = 0;

	m_JoinPlan3ThreadParams = new JoinPlan3ThreadParams[m_WorkerThreadNum];

	FANS * finalFanInR = m_R.front();
	m_R.pop(); 

	FANS * finalFanInS = m_S.front();
	m_S.pop(); 

	hR = CreateFile(
		(LPCWSTR)finalFanInR->fileName, // file to open
		GENERIC_READ,			// open for reading
		FILE_SHARE_READ,        // share for reading
		NULL,					// default security
		OPEN_EXISTING,			// existing file only
		FILE_FLAG_OVERLAPPED,	// overlapped operation //| FILE_FLAG_NO_BUFFERING
		NULL);					// no attr. template

	if (INVALID_HANDLE_VALUE==hR) 
	{   
		ShowMB(L"Cannot open file %s\r\n", finalFanInR->fileName);
		return ERR_CANNOT_CREATE_HANDLE; 
	} 

	hS = CreateFile(
		(LPCWSTR)finalFanInS->fileName, // file to open
		GENERIC_READ,			// open for reading
		FILE_SHARE_READ,        // share for reading
		NULL,					// default security
		OPEN_EXISTING,			// existing file only
		FILE_FLAG_OVERLAPPED,	// overlapped operation //| FILE_FLAG_NO_BUFFERING
		NULL);					// no attr. template

	if (INVALID_HANDLE_VALUE==hS) 
	{   
		ShowMB(L"Cannot open file %s\r\n", finalFanInS->fileName);
		return ERR_CANNOT_CREATE_HANDLE; 
	} 

	LARGE_INTEGER  finalFanInSizeR = {0}; 
	LARGE_INTEGER  finalFanInSizeS = {0}; 

	if (!GetFileSizeEx(hR, &finalFanInSizeR))
	{       
		ShowMB(L"Cannot get size of file %s\r\n", finalFanInR->fileName);
		return ERR_CANNOT_GET_FILE_SIZE;
	} 

	if (!GetFileSizeEx(hS, &finalFanInSizeS))
	{       
		ShowMB(L"Cannot get size of file %s\r\n", finalFanInS->fileName);
		return ERR_CANNOT_GET_FILE_SIZE;
	} 


	// Allocate range partition
	DWORD chunkSize = m_RunSize; 
	UINT64 totalChunkR = chROUNDUP(finalFanInSizeR.QuadPart, chunkSize) / chunkSize;
	UINT64 totalChunkS = chROUNDUP(finalFanInSizeS.QuadPart, chunkSize) / chunkSize; 
	UINT64 _totalChunkR = totalChunkR; 

	for(int wIdx = 0; wIdx < m_WorkerThreadNum; wIdx++)
	{
		m_JoinPlan3ThreadParams[wIdx]._this = this;
		m_JoinPlan3ThreadParams[wIdx].RunSize = chunkSize; 
		m_JoinPlan3ThreadParams[wIdx].R_FanInCount = 0;
		m_JoinPlan3ThreadParams[wIdx].S_FanInCount = 0;
		m_JoinPlan3ThreadParams[wIdx].R_Key = m_PsmjParams.R_KEY_POS;
		m_JoinPlan3ThreadParams[wIdx].S_Key = m_PsmjParams.S_KEY_POS;

		m_JoinPlan3ThreadParams[wIdx].ReadSkipCountS = 0;
		m_JoinPlan3ThreadParams[wIdx].ReadCountS = 0;


	}

	while(_totalChunkR > 0)
	{
		for(UINT tIdx=0; tIdx < m_WorkerThreadNum; tIdx++)
		{ 
			m_JoinPlan3ThreadParams[tIdx].R_FanInCount++; 
			_totalChunkR--; 
			if(_totalChunkR==0) { break; }
		} 
	}  

	for(UINT tIdx=0; tIdx < m_WorkerThreadNum; tIdx++)
	{ 
		m_JoinPlan3ThreadParams[tIdx].S_FanInCount = totalChunkS;  
		m_JoinPlan3ThreadParams[tIdx].PartitionS = new RangePartition[m_JoinPlan3ThreadParams[tIdx].S_FanInCount];
	} 

	DWORD nIdx = 0;
	for(UINT tIdx=0; tIdx < m_WorkerThreadNum; tIdx++)
	{ 
		m_JoinPlan3ThreadParams[tIdx].PartitionR = new RangePartition[m_JoinPlan3ThreadParams[tIdx].R_FanInCount];
		for(UINT runIdx = 0; runIdx < m_JoinPlan3ThreadParams[tIdx].R_FanInCount; runIdx++)
		{
			m_JoinPlan3ThreadParams[tIdx].PartitionR[runIdx].Idx = nIdx; 
			m_JoinPlan3ThreadParams[tIdx].PartitionR[runIdx].lowestKey = m_RRangePartition[nIdx].lowestKey;
			m_JoinPlan3ThreadParams[tIdx].PartitionR[runIdx].highestKey = m_RRangePartition[nIdx].highestKey;
			m_JoinPlan3ThreadParams[tIdx].PartitionR[runIdx].fileOffsetStart = m_RRangePartition[nIdx].fileOffsetStart; 
			m_JoinPlan3ThreadParams[tIdx].PartitionR[runIdx].fileOffsetEnd = m_RRangePartition[nIdx].fileOffsetEnd; 
			nIdx++;
		} 
	}
	nIdx = 0;
	// Table S
	for(UINT runIdx = 0; runIdx < m_JoinPlan3ThreadParams[0].S_FanInCount; runIdx++)
	{ 
		m_JoinPlan3ThreadParams[0].PartitionS[runIdx].Idx = runIdx;
		m_JoinPlan3ThreadParams[0].PartitionS[runIdx].lowestKey = m_SRangePartition[nIdx].lowestKey;
		m_JoinPlan3ThreadParams[0].PartitionS[runIdx].highestKey = m_SRangePartition[nIdx].highestKey; 
		m_JoinPlan3ThreadParams[0].PartitionS[runIdx].fileOffsetStart = m_SRangePartition[nIdx].fileOffsetStart; 
		m_JoinPlan3ThreadParams[0].PartitionS[runIdx].fileOffsetEnd = m_SRangePartition[nIdx].fileOffsetEnd; 
		nIdx++;
	}
	for(UINT tIdx=0; tIdx < m_WorkerThreadNum; tIdx++)
	{ 
		if(tIdx!=0)
		{
			// Update other thread with thread 0 parameter
			for(UINT runIdx = 0; runIdx < m_JoinPlan3ThreadParams[tIdx].S_FanInCount; runIdx++)
			{
				m_JoinPlan3ThreadParams[tIdx].PartitionS[runIdx].Idx = m_JoinPlan3ThreadParams[0].PartitionS[runIdx].Idx; 
				m_JoinPlan3ThreadParams[tIdx].PartitionS[runIdx].lowestKey = m_JoinPlan3ThreadParams[0].PartitionS[runIdx].lowestKey; 
				m_JoinPlan3ThreadParams[tIdx].PartitionS[runIdx].highestKey = m_JoinPlan3ThreadParams[0].PartitionS[runIdx].highestKey;
				m_JoinPlan3ThreadParams[tIdx].PartitionS[runIdx].fileOffsetStart = m_JoinPlan3ThreadParams[0].PartitionS[runIdx].fileOffsetStart;
				m_JoinPlan3ThreadParams[tIdx].PartitionS[runIdx].fileOffsetEnd = m_JoinPlan3ThreadParams[0].PartitionS[runIdx].fileOffsetEnd;
			} 
		}
	}


	////==================Start DEBUG=======================================================================================================
	//// Check for the last fanIn is over file size or not
	//// Allocate offset
	//LARGE_INTEGER _tmpOffset = {0};
	//for(UINT tIdx=0; tIdx < m_WorkerThreadNum; tIdx++)
	//{ 
	//	m_JoinPlan3ThreadParams[tIdx].PartitionR = new RangePartition[m_JoinPlan3ThreadParams[tIdx].R_FanInCount];
	//	for(UINT runIdx = 0; runIdx < m_JoinPlan3ThreadParams[tIdx].R_FanInCount; runIdx++)
	//	{
	//		m_JoinPlan3ThreadParams[tIdx].PartitionR[runIdx].Idx = runIdx;
	//		m_JoinPlan3ThreadParams[tIdx].PartitionR[runIdx].lowestKey = 0;
	//		m_JoinPlan3ThreadParams[tIdx].PartitionR[runIdx].highestKey = 0;
	//		m_JoinPlan3ThreadParams[tIdx].PartitionR[runIdx].fileOffsetStart.QuadPart = _tmpOffset.QuadPart;
	//		_tmpOffset.QuadPart += chunkSize; 
	//		m_JoinPlan3ThreadParams[tIdx].PartitionR[runIdx].fileOffsetEnd.QuadPart = m_JoinPlan3ThreadParams[tIdx].PartitionR[runIdx].fileOffsetStart.QuadPart + chunkSize;
	//		if(_tmpOffset.QuadPart >= finalFanInSizeR.QuadPart)
	//		{
	//			m_JoinPlan3ThreadParams[tIdx].PartitionR[runIdx].fileOffsetEnd.QuadPart = finalFanInSizeR.QuadPart;
	//		} 
	//	} 
	//} 

	//_tmpOffset.QuadPart = 0;
	//for(UINT tIdx=0; tIdx < m_WorkerThreadNum; tIdx++)
	//{ 
	//	m_JoinPlan3ThreadParams[tIdx].PartitionS = new RangePartition[m_JoinPlan3ThreadParams[tIdx].S_FanInCount];
	//	for(UINT runIdx = 0; runIdx < m_JoinPlan3ThreadParams[tIdx].S_FanInCount; runIdx++)
	//	{
	//		m_JoinPlan3ThreadParams[tIdx].PartitionS[runIdx].Idx = runIdx;
	//		m_JoinPlan3ThreadParams[tIdx].PartitionS[runIdx].lowestKey = 0;
	//		m_JoinPlan3ThreadParams[tIdx].PartitionS[runIdx].highestKey = 0; 
	//		m_JoinPlan3ThreadParams[tIdx].PartitionS[runIdx].fileOffsetStart.QuadPart = _tmpOffset.QuadPart;
	//		_tmpOffset.QuadPart += chunkSize; 

	//		m_JoinPlan3ThreadParams[tIdx].PartitionS[runIdx].fileOffsetEnd.QuadPart = m_JoinPlan3ThreadParams[tIdx].PartitionS[runIdx].fileOffsetStart.QuadPart + chunkSize;
	//		if(_tmpOffset.QuadPart >= finalFanInSizeS.QuadPart)
	//		{
	//			m_JoinPlan3ThreadParams[tIdx].PartitionS[runIdx].fileOffsetEnd.QuadPart = finalFanInSizeS.QuadPart;
	//		} 
	//	} 
	//	_tmpOffset.QuadPart = 0;
	//} 


	////////////////////////////////////////////////////////////////////////////
	//// Compute range key in each partition, spend some I/O here
	//Buffer _myPageBuffer;
	//PageHeader *_pageHeader;
	//PageSlot *_tupleInfo;
	//RECORD *_myRecordPtr = new RECORD();
	//DWORD _myTupleCount = 0;
	//DWORD _dwByteRead = 0;

	//OVERLAPPED _myOverlap = {0}; 
	//LARGE_INTEGER _myOffset = {0};
	//_myOverlap.Offset = 0;
	//_myOverlap.OffsetHigh = 0;

	//rc = utl->InitBuffer(_myPageBuffer, SSD_PAGE_SIZE, &bufferPool);
	//if(rc!=SUCCESS) {  return rc; }   

	//for(UINT tIdx=0; tIdx < m_WorkerThreadNum; tIdx++)
	//{ 
	//	// Table R
	//	for(UINT runIdx = 0; runIdx < m_JoinPlan3ThreadParams[tIdx].R_FanInCount; runIdx++)
	//	{
	//		// Get lowest Key
	//		////////////////////////////////////////////////////////////////////////// 
	//		_myOffset.QuadPart = m_JoinPlan3ThreadParams[tIdx].PartitionR[runIdx].fileOffsetStart.QuadPart;
	//		_myOverlap.Offset = _myOffset.LowPart;
	//		_myOverlap.OffsetHigh = _myOffset.HighPart;

	//		ReadFile(hR, _myPageBuffer.data, SSD_PAGE_SIZE, &_dwByteRead, &_myOverlap); 
	//		GetOverlappedResult(hR, &_myOverlap, &_dwByteRead, TRUE); 

	//		_pageHeader = (PageHeader *)_myPageBuffer.data + 0 * SSD_PAGE_SIZE; 
	//		_myTupleCount = _pageHeader->totalTuple; 
	//		_tupleInfo = (PageSlot *)(_myPageBuffer.data + SSD_PAGE_SIZE - 1 * sizeof(PageSlot)); 
	//		_myRecordPtr->data = _myPageBuffer.data + ((_tupleInfo)->tupleOffset); 
	//		_myRecordPtr->length = (_tupleInfo)->tupleSize; 
	//		_myRecordPtr->offset = (_tupleInfo)->tupleOffset;

	//		utl->GetTupleKey(_myRecordPtr->data, _myRecordPtr->key, m_JoinPlan3ThreadParams[tIdx].R_Key);  
	//		utl->ResetBuffer(_myPageBuffer);

	//		m_JoinPlan3ThreadParams[tIdx].PartitionR[runIdx].lowestKey = _myRecordPtr->key; 

	//		// Get highest Key
	//		//////////////////////////////////////////////////////////////////////////
	//		_myOffset.QuadPart = m_JoinPlan3ThreadParams[tIdx].PartitionR[runIdx].fileOffsetEnd.QuadPart - SSD_PAGE_SIZE;
	//		_myOverlap.Offset = _myOffset.LowPart;
	//		_myOverlap.OffsetHigh = _myOffset.HighPart;

	//		ReadFile(hR, _myPageBuffer.data, SSD_PAGE_SIZE, &_dwByteRead, &_myOverlap); 
	//		GetOverlappedResult(hR, &_myOverlap, &_dwByteRead, TRUE); 

	//		_pageHeader = (PageHeader *)_myPageBuffer.data + 0 * SSD_PAGE_SIZE;  
	//		_myTupleCount = _pageHeader->totalTuple; 
	//		_tupleInfo = (PageSlot *)(_myPageBuffer.data + SSD_PAGE_SIZE - _myTupleCount * sizeof(PageSlot));  
	//		_myRecordPtr->data = _myPageBuffer.data + ((_tupleInfo)->tupleOffset); 
	//		_myRecordPtr->length = (_tupleInfo)->tupleSize; 
	//		_myRecordPtr->offset = (_tupleInfo)->tupleOffset;

	//		utl->GetTupleKey(_myRecordPtr->data, _myRecordPtr->key, m_JoinPlan3ThreadParams[tIdx].R_Key); 

	//		m_JoinPlan3ThreadParams[tIdx].PartitionR[runIdx].highestKey = _myRecordPtr->key;
	//		utl->ResetBuffer(_myPageBuffer);
	//	}  
	//}


	//// Table S
	//for(UINT runIdx = 0; runIdx < m_JoinPlan3ThreadParams[0].S_FanInCount; runIdx++)
	//{
	//	// Get lowest Key
	//	////////////////////////////////////////////////////////////////////////// 
	//	_myOffset.QuadPart = m_JoinPlan3ThreadParams[0].PartitionS[runIdx].fileOffsetStart.QuadPart;
	//	_myOverlap.Offset = _myOffset.LowPart;
	//	_myOverlap.OffsetHigh = _myOffset.HighPart;

	//	ReadFile(hS, _myPageBuffer.data, SSD_PAGE_SIZE, &_dwByteRead, &_myOverlap); 
	//	GetOverlappedResult(hS, &_myOverlap, &_dwByteRead, TRUE); 

	//	_pageHeader = (PageHeader *)_myPageBuffer.data + 0 * SSD_PAGE_SIZE; 
	//	_myTupleCount = _pageHeader->totalTuple; 
	//	_tupleInfo = (PageSlot *)(_myPageBuffer.data + SSD_PAGE_SIZE - 1 * sizeof(PageSlot)); 
	//	_myRecordPtr->data = _myPageBuffer.data + ((_tupleInfo)->tupleOffset); 
	//	_myRecordPtr->length = (_tupleInfo)->tupleSize; 
	//	_myRecordPtr->offset = (_tupleInfo)->tupleOffset;

	//	utl->GetTupleKey(_myRecordPtr->data, _myRecordPtr->key, m_JoinPlan3ThreadParams[0].S_Key);  
	//	utl->ResetBuffer(_myPageBuffer);

	//	m_JoinPlan3ThreadParams[0].PartitionS[runIdx].lowestKey = _myRecordPtr->key; 

	//	// Get highest Key
	//	//////////////////////////////////////////////////////////////////////////
	//	_myOffset.QuadPart = m_JoinPlan3ThreadParams[0].PartitionS[runIdx].fileOffsetEnd.QuadPart - SSD_PAGE_SIZE;
	//	_myOverlap.Offset = _myOffset.LowPart;
	//	_myOverlap.OffsetHigh = _myOffset.HighPart;

	//	ReadFile(hS, _myPageBuffer.data, SSD_PAGE_SIZE, &_dwByteRead, &_myOverlap); 
	//	GetOverlappedResult(hS, &_myOverlap, &_dwByteRead, TRUE); 

	//	_pageHeader = (PageHeader *)_myPageBuffer.data + 0 * SSD_PAGE_SIZE;  
	//	_myTupleCount = _pageHeader->totalTuple; 
	//	_tupleInfo = (PageSlot *)(_myPageBuffer.data + SSD_PAGE_SIZE - _myTupleCount * sizeof(PageSlot));  
	//	_myRecordPtr->data = _myPageBuffer.data + ((_tupleInfo)->tupleOffset); 
	//	_myRecordPtr->length = (_tupleInfo)->tupleSize; 
	//	_myRecordPtr->offset = (_tupleInfo)->tupleOffset;

	//	utl->GetTupleKey(_myRecordPtr->data, _myRecordPtr->key, m_JoinPlan3ThreadParams[0].S_Key); 

	//	m_JoinPlan3ThreadParams[0].PartitionS[runIdx].highestKey = _myRecordPtr->key;
	//	utl->ResetBuffer(_myPageBuffer);
	//}
	//for(UINT tIdx=0; tIdx < m_WorkerThreadNum; tIdx++)
	//{ 
	//	if(tIdx!=0)
	//	{
	//		// Update other thread with thread 0 parameter
	//		for(UINT runIdx = 0; runIdx < m_JoinPlan3ThreadParams[tIdx].S_FanInCount; runIdx++)
	//		{
	//			m_JoinPlan3ThreadParams[tIdx].PartitionS[runIdx].lowestKey = m_JoinPlan3ThreadParams[0].PartitionS[runIdx].lowestKey; 
	//			m_JoinPlan3ThreadParams[tIdx].PartitionS[runIdx].highestKey = m_JoinPlan3ThreadParams[0].PartitionS[runIdx].highestKey;
	//			m_JoinPlan3ThreadParams[tIdx].PartitionS[runIdx].fileOffsetStart = m_JoinPlan3ThreadParams[0].PartitionS[runIdx].fileOffsetStart;
	//			m_JoinPlan3ThreadParams[tIdx].PartitionS[runIdx].fileOffsetEnd = m_JoinPlan3ThreadParams[0].PartitionS[runIdx].fileOffsetEnd;
	//		} 
	//	}
	//}

	////==================End DEBUG=======================================================================================================


	// DEBUG
	//////////////////////////////////////////////////////////////////////////
	/*
	CHAR *debugContent = new CHAR[2048]; 
	FILE* fso=fopen("C:\\debug1.csv","w+b");

	for(UINT tIdx=0; tIdx < m_WorkerThreadNum; tIdx++)
	{   
	for(UINT runIdx = 0; runIdx < m_JoinPlan3ThreadParams[tIdx].R_FanInCount; runIdx++)
	{
	sprintf(debugContent, "%d,%lld,%lld,%lld,%lld\n",
	runIdx,
	m_JoinPlan3ThreadParams[tIdx].PartitionR[runIdx].fileOffsetStart.QuadPart,
	m_JoinPlan3ThreadParams[tIdx].PartitionR[runIdx].fileOffsetEnd.QuadPart,
	m_JoinPlan3ThreadParams[tIdx].PartitionR[runIdx].lowestKey,
	m_JoinPlan3ThreadParams[tIdx].PartitionR[runIdx].highestKey);  fprintf(fso, debugContent); 
	} 

	fprintf(fso, "\n"); 
	} 

	for(UINT tIdx=0; tIdx < m_WorkerThreadNum; tIdx++)
	{   
	for(UINT runIdx = 0; runIdx < m_JoinPlan3ThreadParams[tIdx].S_FanInCount; runIdx++)
	{
	sprintf(debugContent, "%d,%lld,%lld,%lld,%lld\n",
	runIdx,
	m_JoinPlan3ThreadParams[tIdx].PartitionS[runIdx].fileOffsetStart.QuadPart,
	m_JoinPlan3ThreadParams[tIdx].PartitionS[runIdx].fileOffsetEnd.QuadPart,
	m_JoinPlan3ThreadParams[tIdx].PartitionS[runIdx].lowestKey,
	m_JoinPlan3ThreadParams[tIdx].PartitionS[runIdx].highestKey);  fprintf(fso, debugContent); 
	} 

	fprintf(fso, "\n"); 
	} 

	delete debugContent;
	fclose(fso); 
	*/
	// Reset buffer pool
	bufferPool.currentSize = 0;

	//////////////////////////////////////////////////////////////////////////
	// Init buffer 
	for(UINT tIdx=0; tIdx < m_WorkerThreadNum; tIdx++)
	{  
		m_JoinPlan3ThreadParams[tIdx].tupleR = new RECORD();
		m_JoinPlan3ThreadParams[tIdx].tupleS = new RECORD();
		m_JoinPlan3ThreadParams[tIdx].tupleRS = new RECORD(TUPLE_SIZE*2); 

		// Init buffer
		// Read buffer for R
		rc = utl->InitBuffer(m_JoinPlan3ThreadParams[tIdx].readBufferR, chunkSize, &bufferPool);
		if(rc!=SUCCESS) {  return rc; } 
		rc = utl->InitBuffer(m_JoinPlan3ThreadParams[tIdx].pageBufferR, SSD_PAGE_SIZE, &bufferPool);
		if(rc!=SUCCESS) {  return rc; } 
		rc = utl->InitRunPage(m_JoinPlan3ThreadParams[tIdx].pageR, m_JoinPlan3ThreadParams[tIdx].pageBufferR);
		if(rc!=SUCCESS) {  return rc; }

		// Read buffer for S
		rc = utl->InitBuffer(m_JoinPlan3ThreadParams[tIdx].readBufferS, chunkSize, &bufferPool);
		if(rc!=SUCCESS) {  return rc; }  
		rc = utl->InitBuffer(m_JoinPlan3ThreadParams[tIdx].pageBufferS, SSD_PAGE_SIZE, &bufferPool);
		if(rc!=SUCCESS) {  return rc; }  
		rc = utl->InitRunPage(m_JoinPlan3ThreadParams[tIdx].pageS, m_JoinPlan3ThreadParams[tIdx].pageBufferS);
		if(rc!=SUCCESS) {  return rc; } 


		// Variable for write join tuple to disk
		m_JoinPlan3ThreadParams[tIdx].joinFilePath = new TCHAR[MAX_PATH];
		JoinPhase_GetFanOutPath(m_JoinPlan3ThreadParams[tIdx].joinFilePath, tIdx);

		m_JoinPlan3ThreadParams[tIdx].hWrite = CreateFile(
			m_JoinPlan3ThreadParams[tIdx].joinFilePath,	 
			GENERIC_WRITE,			 
			0,						 
			NULL,					 
			CREATE_ALWAYS,			 
			FILE_FLAG_OVERLAPPED, 
			NULL);		

		if (INVALID_HANDLE_VALUE==m_JoinPlan3ThreadParams[tIdx].hWrite) 
		{   
			ShowMB(L"Cannot open file %s\r\n", m_JoinPlan3ThreadParams[tIdx].joinFilePath);
			return ERR_CANNOT_CREATE_HANDLE; 
		}  

		// Init write buffers 
		rc = utl->InitBuffer(m_JoinPlan3ThreadParams[tIdx].pageWriteBuffer, SSD_PAGE_SIZE, &bufferPool);
		if(rc!=SUCCESS) {  return rc; }

		rc = utl->InitRunPage(m_JoinPlan3ThreadParams[tIdx].pageWrite, m_JoinPlan3ThreadParams[tIdx].pageWriteBuffer);
		if(rc!=SUCCESS) {  return rc; }
	}

	// Calculate write buffer size
	DWORD memSizeFree = bufferPool.size - bufferPool.currentSize;
	if(memSizeFree==0) {return ERR_NOT_ENOUGH_MEMORY;}

	// must be > 0 and multiple of 4096, divide 2 because use double buffer
	DWORD writeMemSize = chROUNDDOWN((memSizeFree / m_WorkerThreadNum) / 2, SSD_PAGE_SIZE);
	rp.JoinWriteBufferSize = writeMemSize;

	if(writeMemSize < SSD_PAGE_SIZE*256)
	{
		chMB("Alert::Memory for write below 1MB");
	}

	for(UINT tIdx=0; tIdx < m_WorkerThreadNum; tIdx++)
	{ 
		m_JoinPlan3ThreadParams[tIdx].dbcWrite = new DoubleBuffer(writeMemSize); 
		rc = utl->InitBuffer(m_JoinPlan3ThreadParams[tIdx].dbcWrite->buffer[0], writeMemSize, &bufferPool);
		if(rc!=SUCCESS) {return rc;}
		rc = utl->InitBuffer(m_JoinPlan3ThreadParams[tIdx].dbcWrite->buffer[1], writeMemSize, &bufferPool);  
		if(rc!=SUCCESS) {return rc;}

		m_JoinPlan3ThreadParams[tIdx].overlapWrite.dwBytesToReadWrite = writeMemSize; 
		m_JoinPlan3ThreadParams[tIdx].overlapWrite.dwBytesReadWritten = 0;  
		m_JoinPlan3ThreadParams[tIdx].overlapWrite.totalChunk = 0;
		m_JoinPlan3ThreadParams[tIdx].overlapWrite.chunkIndex = 0;
		m_JoinPlan3ThreadParams[tIdx].overlapWrite.startChunk = 0;
		m_JoinPlan3ThreadParams[tIdx].overlapWrite.endChunk = 0;
		m_JoinPlan3ThreadParams[tIdx].overlapWrite.fileSize.QuadPart = 0; 
		m_JoinPlan3ThreadParams[tIdx].overlapWrite.overlap.Offset = 0;
		m_JoinPlan3ThreadParams[tIdx].overlapWrite.overlap.OffsetHigh = 0; 
	}

	// It's OK, return

	return SUCCESS;
}

RC PSMJ::JoinPhase_Plan3_Join(LPVOID lpParam)
{
	RC rc;

	JoinPlan3ThreadParams* p = (JoinPlan3ThreadParams*)lpParam;

	p->readBufferR.currentPageIndex = 0;
	rc = utl->GetPageInfo(p->readBufferR.data, p->pageBufferR, p->readBufferR.currentPageIndex,  SSD_PAGE_SIZE);  

	p->readBufferS.currentPageIndex = 0;
	rc = utl->GetPageInfo(p->readBufferS.data, p->pageBufferS, p->readBufferS.currentPageIndex,  SSD_PAGE_SIZE);  

	p->pageBufferR.currentTupleIndex = 1;
	rc = utl->GetTupleInfo(p->tupleR, p->pageBufferR.currentTupleIndex, p->pageBufferR.data, SSD_PAGE_SIZE, p->R_Key);  
	p->pageBufferR.currentTupleIndex++;

	p->pageBufferS.currentTupleIndex = 1;
	rc = utl->GetTupleInfo(p->tupleS, p->pageBufferS.currentTupleIndex, p->pageBufferS.data, SSD_PAGE_SIZE, p->S_Key);  
	p->pageBufferS.currentTupleIndex++;

	while((p->tupleR->key!=MAX) && (p->tupleS->key!=MAX))
	{  
		// while left is less than right, move left up
		while (p->tupleR->key < p->tupleS->key) 
		{
			rc = JoinPhase_Plan3_GetNextTuple(&p->readBufferR, p->pageBufferR, p->tupleR, p->R_Key); 
			if (p->tupleR->key == MAX)  { break; }
		} 

		// if done, no more joins, break
		if (p->tupleR->key == MAX)  { break; } 

		// while left is greater than right, move right up
		while (p->tupleR->key > p->tupleS->key) 
		{
			rc = JoinPhase_Plan3_GetNextTuple(&p->readBufferS, p->pageBufferS, p->tupleS, p->S_Key); 
			if (p->tupleS->key == MAX) { break; }
		} 

		// if done, no more joins, break
		if (p->tupleS->key == MAX) {  break;  }

		// while the two are equal, segment equal
		while (p->tupleR->key == p->tupleS->key) 
		{   
			JoinPhase_MakeJoinRecord(p->tupleRS, p->tupleR, p->tupleS);

			// Send this join tuple to BACK write buffer 
			rc = JoinPhase_Plan3_SentOutput(p, p->tupleRS); 

			// Get next S tuple 
			rc = JoinPhase_Plan3_GetNextTuple(&p->readBufferS, p->pageBufferS, p->tupleS, p->S_Key);

			while (p->tupleS->key == p->tupleR->key) 
			{  
				JoinPhase_MakeJoinRecord(p->tupleRS, p->tupleR, p->tupleS);

				// Save this to Output buffer
				JoinPhase_Plan3_SentOutput(p, p->tupleRS); 

				// Get next S tuple
				rc = JoinPhase_Plan3_GetNextTuple(&p->readBufferS, p->pageBufferS, p->tupleS, p->S_Key); 
				if (p->tupleS->key == MAX)  { break; }
			}

			// Get next R tuple
			rc = JoinPhase_Plan3_GetNextTuple(&p->readBufferR, p->pageBufferR, p->tupleR, p->R_Key);   

			if (p->tupleR->key == MAX)  {  break;  }
		}

		// Get next S tuple
		rc = JoinPhase_Plan3_GetNextTuple(&p->readBufferS, p->pageBufferS, p->tupleS, p->S_Key);   
	}


	return SUCCESS;
}

RC PSMJ::JoinPhase_Plan3_GetNextTuple(Buffer *bufferPtr, Buffer &pageBuffer, RECORD *recordPtr, DWORD keyPos)
{
	RC rc;

	if(pageBuffer.currentTupleIndex > pageBuffer.tupleCount)
	{ 
		bufferPtr->currentPageIndex++;   
		if(bufferPtr->currentPageIndex >= bufferPtr->pageCount)
		{     
			// Buffer empty
			rc = utl->ResetBuffer(pageBuffer);  
			rc = utl->SetMaxTuple(recordPtr); 
			return SUCCESS;
		}

		rc = utl->GetPageInfo(bufferPtr->data, pageBuffer, bufferPtr->currentPageIndex,  SSD_PAGE_SIZE);  
		pageBuffer.currentTupleIndex=1; 
	}

	rc = utl->GetTupleInfo(recordPtr, pageBuffer.currentTupleIndex, pageBuffer.data, SSD_PAGE_SIZE, keyPos);  
	pageBuffer.currentTupleIndex++; 

	return SUCCESS; 
}

/// <summary>
/// Sent join tuple to output buffer.
/// </summary>
/// <param name="lpParam">The thread parameter.</param>
/// <param name="recordPtr">The record PTR.</param>
/// <returns></returns>
RC PSMJ::JoinPhase_Plan3_SentOutput(LPVOID lpParam, RECORD *recordPtr)
{
	RC rc;
	JoinPlan3ThreadParams* p = (JoinPlan3ThreadParams*)lpParam;

	InterlockedExchangeAdd(&m_TotalJoinCount, 1); 

	if(utl->IsJoinPageFull(p->pageWrite))
	{   
		rc = utl->AddPageToBuffer(BACK_BUFFER(p->dbcWrite), p->pageWrite->page, SSD_PAGE_SIZE); //4 
		//BACK_BUFFER(p->dbcWrite).currentPageIndex++; 
		BACK_BUFFER(p->dbcWrite).pageCount++;
		utl->ResetRunPage(p->pageWrite, p->pageWriteBuffer);  
		p->pageWrite->consumed = TRUE; 
	}  

	if(utl->IsBufferFull(BACK_BUFFER(p->dbcWrite)))  
	{     
		if(p->dbcWrite->bFirstProduce==TRUE) // Back Buffer is full, check is it the first product
		{ 
			p->dbcWrite->bFirstProduce=FALSE;  
			p->dbcWrite->SwapBuffers();   

			p->dbcWrite->LockConsumer();  // lock no lai

			WriteFile(p->hWrite, 
				FRONT_BUFFER(p->dbcWrite).data, 
				FRONT_BUFFER(p->dbcWrite).currentSize, 
				&p->overlapWrite.dwBytesReadWritten, 
				&p->overlapWrite.overlap); 	
		}
		else
		{    
			//Wait for write complete   
			GetOverlappedResult(p->hWrite, &p->overlapWrite.overlap, &p->overlapWrite.dwBytesReadWritten, TRUE) ;  

			p->overlapWrite.fileSize.QuadPart += p->overlapWrite.dwBytesReadWritten;  
			p->overlapWrite.overlap.Offset = p->overlapWrite.fileSize.LowPart;
			p->overlapWrite.overlap.OffsetHigh = p->overlapWrite.fileSize.HighPart;  
			p->overlapWrite.dwBytesReadWritten = 0;

			rc = utl->ResetBuffer(FRONT_BUFFER(p->dbcWrite));  // reset for next data

			p->dbcWrite->UnLockConsumer(); 

			p->dbcWrite->SwapBuffers();  

			p->dbcWrite->LockConsumer();  
			WriteFile(p->hWrite, 
				FRONT_BUFFER(p->dbcWrite).data, 
				FRONT_BUFFER(p->dbcWrite).currentSize, 
				&p->overlapWrite.dwBytesReadWritten, 
				&p->overlapWrite.overlap); 
		}   
	}

	rc = utl->AddTupleToPage(p->pageWrite, recordPtr, p->pageWriteBuffer);   // Add this tuples to page   
	p->pageWrite->consumed = FALSE;

	return SUCCESS;  
}

#pragma endregion "Join Phase Plan 3" 


//////////////////////////////////////////////////////////////////////////
// Some ultility function use for join

#pragma region "Join utilities"

/// <summary>
/// Make join record.
/// </summary>
/// <param name="joinRecord">The join record pointer.</param>
/// <param name="leftRecord">The left record pointer.</param>
/// <param name="rightRecord">The right record pointer.</param>
VOID PSMJ::JoinPhase_MakeJoinRecord(RECORD *&joinRecord, RECORD *leftRecord, RECORD *rightRecord) 
{
	joinRecord->key = leftRecord->key;
	joinRecord->length = leftRecord->length + rightRecord->length;
	memcpy(joinRecord->data, leftRecord->data, leftRecord->length);
	memcpy(joinRecord->data + leftRecord->length, rightRecord->data, rightRecord->length); 
}

/// <summary>
/// Get fan-out path.
/// </summary>
/// <param name="fanPath">The fan path.</param>
/// <param name="threadID">The thread identifier.</param>
/// <returns></returns>
RC PSMJ::JoinPhase_GetFanOutPath(LPWSTR &fanPath, INT threadID)
{
	swprintf(fanPath, MAX_PATH, L"%sjoined_%d.dat", m_PemsParams.WORK_SPACE_PATH, threadID);   
	return SUCCESS; 
}

/// <summary>
/// Get total join count.
/// </summary>
/// <returns></returns>
DWORD PSMJ::JoinPhase_GetTotalJoinCount()
{
	return m_TotalJoinCount;
}

#pragma endregion

/// <summary>
/// Gets the size of the fan-in.
/// </summary>
/// <param name="hFile">The fan-in file handle.</param>
/// <returns></returns>
UINT64 PSMJ::GetFanSize(HANDLE hFile)  
{
	LARGE_INTEGER liFileSize; 
	if (!GetFileSizeEx(hFile, &liFileSize))
	{      
		return 0;
	} 

	return liFileSize.QuadPart;
}



//////////////////////////////////////////////////////////////////////////
//
//  POWER CAPPING
//
////////////////////////////////////////////////////////////////////////// 
#pragma region  "Power capping"  

/// <summary>
/// Wrapper for power capping thread function.
/// </summary>
/// <param name="lpParam">The thread parameter.</param>
/// <returns></returns>
DWORD WINAPI PSMJ::PowerCapEx(LPVOID lpParam)
{
	PowerCapParams* p = (PowerCapParams*)(lpParam);
	p->_this->PowerCap((LPVOID)(p));
	return 0;
}

/// <summary>
/// Power capping thread function.
/// </summary>
/// <param name="lpParam">The thread parameter.</param>
/// <returns></returns>
DWORD WINAPI PSMJ::PowerCap(LPVOID lpParam)
{
	PowerCapParams *p = (PowerCapParams *)lpParam;

	if(g_CapPower==0)
	{
		chMB("Power capping value is not set");
		return 0;
	}

	DWORD timeBase = g_TimeBase; // 100ms
	DOUBLE powerOver = 0;
	DOUBLE powerSupply = 0;
	DOUBLE powerProcess = 0;
	DOUBLE ratio = 0;  

	DWORD timeSleep = 0;
	DWORD timeExecute = timeBase; 

	CHAR *capData = new CHAR[2048];
	sprintf(capData, "%s,%d\n", "Time Base", timeBase);  fprintf(fpCap, capData);
	sprintf(capData, "%s,%.2f\n", "Power Cap Value", g_CapPower);  fprintf(fpCap, capData);
	sprintf(capData, "%s\n", "Package Power, Process Usage, Process Power, Power Over, Power Supply, Ratio, Time Sleep, Time Execute");  fprintf(fpCap, capData);

	// wait for all worker thread do work
	Sleep(100);

	while( !bQuitCapping )  
	{    
		//powerProcess = (g_ProcessUsage / 100) * (g_PackagePower - CPU_IDLE_POWER_WATT);
		powerProcess = g_ProcessPower;
		if(powerProcess > g_CapPower)
		{
			powerOver = powerProcess - g_CapPower;
			if(powerOver > g_CapPower)
			{  
				ratio = 0.75; // decrease step by step
			}
			else
			{
				ratio = powerOver / powerProcess; 
			} 

			timeSleep = ratio * DOUBLE(timeBase); // milisecond
			timeExecute = timeBase - timeSleep;
			powerSupply = 0;
		}
		else
		{
			powerSupply = g_CapPower - powerProcess;
			ratio = powerSupply / g_CapPower; 
			timeExecute = ratio * DOUBLE(timeBase);
			timeSleep = timeBase - timeExecute;
			powerOver = 0; 
		}

		sprintf(capData, "%.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %d, %d\n", 
			g_PackagePower, 
			g_ProcessUsage, 
			powerProcess,
			powerOver,
			powerSupply,
			ratio, 
			timeSleep,
			timeExecute);  fprintf(fpCap, capData);

		if(chINRANGE(1, timeSleep, timeBase))
		{  
			for (int tIdx=0; tIdx < m_WorkerThreadNum;tIdx++)
			{
				SuspendThread(m_hWorkerThread[tIdx]);
			}

			Sleep(timeSleep);

			for (int tIdx=0; tIdx < m_WorkerThreadNum;tIdx++)
			{
				ResumeThread(m_hWorkerThread[tIdx]);
			} 
		} 

		// Sleep for program execute
		if(chINRANGE(1, timeExecute, timeBase))
		{  
			Sleep(timeExecute); 
		}   
	} 

	delete capData;

	return 0;
}

#pragma endregion  