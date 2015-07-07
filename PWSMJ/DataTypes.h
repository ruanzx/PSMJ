// 
// Name: ExsTypes.h : implementation file 
// Author: hieunt
// Description:  Globals variables for all operations
//

#pragma once

#include "stdafx.h"

#define DEFAULT_WORK_SPACE L"D:\\ExtSort\\" // modify this value for your own folder
#define DEFAULT_WORK_SPACE_HDD L"F:\\ExtSort\\" // modify this value for your own folder
#define DEFAULT_BUFFER_POOL_SIZE_MB 50  // buffer pool size default size in megabytes
#define DEFAULT_SSD_STORAGE_SIZE_MB 60  // SSD storage default size in megabytes, use in CEMS
#define CPU_POWER_CONSTRAIN_WATT 0  // process power consumed throttle value
#define CPU_IDLE_POWER_WATT 6.5  // process power consumed throttle value, on difference computer may have difference values

#define REFRESH_GUI_INTERVAL_MILISECOND 1000 // default interval refresh GUI
#define REFRESH_SAMPLE_INTERVAL_MILISECOND 100 // default interval refresh sample

#define MAXIMUM_TUPLE_IN_ONE_PAGE 45
#define CACHE_LINE_SIZE 64
#define CACHE_ALIGN __declspec(align(CACHE_LINE_SIZE)) 
#define CS_SPIN_COUNT 4000 // Double buffer crical section spin count

#define TUPLE_SIZE 250 // maximum tuple size
#define MAX 1000000000 // positive infinitive 
#define SSD_PAGE_SIZE 4096	  
#define HDD_PAGE_SIZE 4096  

#define FLAG_DEFAULT    0 
#define FLAG_NO_DATA    10 
#define FLAG_HAVE_DATA  20 

typedef enum OPERATIONS
{
	OP_PEMS,       // Parallel external merge sort use with SSD
	OP_NSM,        // Make NSM page layout
	OP_SMJ,        // Sort merge join
	OP_SMJ2,        // Sort & merge join using RP
	OP_CEMS,       // Combine SSD, HDD external merge sort
	OP_RP,         // Classic Replacement selection
	OP_BNLJ,       // Block nested loop join
	OP_HHJ,		   // Hybrid hash join
	OP_GHJ,		   // Grace Hash Join
	OP_PSMJ		   // Parallel Sort merge join
}OPERATIONS;
 
typedef enum JOIN_PLAN
{
	PLAN_1,       
	PLAN_2,      
	PLAN_3,        
	PLAN_4 
}JOIN_PLAN;

// Parallel Sort Merge Join input parameters
typedef struct SMJ2_PARAMS
{
	LPWSTR RELATION_R_PATH;
	LPWSTR RELATION_R_NO_EXT;
	LPWSTR RELATION_S_PATH;
	LPWSTR RELATION_S_NO_EXT;
	LPWSTR WORK_SPACE_PATH;
	DWORD R_KEY_POS;	 //
	DWORD S_KEY_POS;  
	DWORD SORT_READ_BUFFER_SIZE;	 //
	DWORD SORT_WRITE_BUFFER_SIZE; 
	DWORD MERGE_READ_BUFFER_SIZE;
	DWORD MERGE_WRITE_BUFFER_SIZE;
	DWORD JOIN_READ_BUFFER_SIZE;	 //
	DWORD JOIN_WRITE_BUFFER_SIZE; 
	DWORD BUFFER_POOL_SIZE;

	SMJ2_PARAMS() // constructor
	{
		RELATION_R_PATH = new TCHAR[MAX_PATH];
		RELATION_R_NO_EXT = new TCHAR[MAX_PATH];
		RELATION_S_PATH = new TCHAR[MAX_PATH];
		RELATION_S_NO_EXT = new TCHAR[MAX_PATH];   
		WORK_SPACE_PATH = new TCHAR[MAX_PATH]; 

		JOIN_READ_BUFFER_SIZE = 0;	 
		JOIN_READ_BUFFER_SIZE = 0; 

		BUFFER_POOL_SIZE = 0;
		R_KEY_POS = 0;	  
		S_KEY_POS = 0;   
	}

} SMJ2_PARAMS; 

/// <summary>
/// Parallel sort merge join parameters
/// </summary>
typedef struct PSMJ_PARAMS
{
	LPWSTR RELATION_R_PATH;
	LPWSTR RELATION_R_NO_EXT;
	DWORD R_KEY_POS;

	LPWSTR RELATION_S_PATH;
	LPWSTR RELATION_S_NO_EXT;
	DWORD S_KEY_POS; 

	LPWSTR WORK_SPACE_PATH;

	DWORD THREAD_NUM;  
	DWORD SORT_READ_BUFFER_SIZE;	 //
	DWORD SORT_WRITE_BUFFER_SIZE; 
	DWORD MERGE_READ_BUFFER_SIZE;
	DWORD MERGE_WRITE_BUFFER_SIZE;

	DWORD BUFFER_POOL_SIZE;    
	BOOL	USE_POWER_CAP; 

	DWORD PLAN_FOR_JOIN;

	PSMJ_PARAMS()
	{  
		WORK_SPACE_PATH = new TCHAR[MAX_PATH];   
		RELATION_R_PATH = new TCHAR[MAX_PATH];
		RELATION_R_NO_EXT = new TCHAR[MAX_PATH];
		RELATION_S_PATH = new TCHAR[MAX_PATH];
		RELATION_S_NO_EXT = new TCHAR[MAX_PATH];  

		BUFFER_POOL_SIZE = 0;  
		R_KEY_POS = 0;	  
		S_KEY_POS = 0;   
		THREAD_NUM = 0;

		USE_POWER_CAP = FALSE; 
	} 
} PSMJ_PARAMS;

 
/// <summary>
/// Parallel Sort Merge Join (Replacement Selection method) parameters
/// </summary>
typedef struct PSMJRP_PARAMS
{
	LPWSTR RELATION_R_PATH;
	LPWSTR RELATION_R_NO_EXT;
	LPWSTR RELATION_S_PATH;
	LPWSTR RELATION_S_NO_EXT;
	LPWSTR WORK_SPACE_PATH;
	DWORD THREAD_NUM;
	DWORD R_KEY_POS;	 //
	DWORD S_KEY_POS;  
	DWORD SORT_READ_BUFFER_SIZE;	 //
	DWORD SORT_WRITE_BUFFER_SIZE; 
	DWORD MERGE_READ_BUFFER_SIZE;
	DWORD MERGE_WRITE_BUFFER_SIZE;
	DWORD JOIN_READ_BUFFER_SIZE;	 //
	DWORD JOIN_WRITE_BUFFER_SIZE; 
	DWORD BUFFER_POOL_SIZE;

	PSMJRP_PARAMS() // constructor
	{
		RELATION_R_PATH = new TCHAR[MAX_PATH];
		RELATION_R_NO_EXT = new TCHAR[MAX_PATH];
		RELATION_S_PATH = new TCHAR[MAX_PATH];
		RELATION_S_NO_EXT = new TCHAR[MAX_PATH];   
		WORK_SPACE_PATH = new TCHAR[MAX_PATH]; 

		JOIN_READ_BUFFER_SIZE = 0;	 
		JOIN_READ_BUFFER_SIZE = 0; 
		THREAD_NUM = 0;
		BUFFER_POOL_SIZE = 0;
		R_KEY_POS = 0;	  
		S_KEY_POS = 0;   
	}

} PSMJRP_PARAMS; 
 
/// <summary>
/// Grace hash join input parameters
/// </summary>
typedef struct GHJ_PARAMS
{
	LPWSTR RELATION_R_PATH;
	LPWSTR RELATION_R_NO_EXT;
	LPWSTR RELATION_S_PATH;
	LPWSTR RELATION_S_NO_EXT;
	LPWSTR WORK_SPACE_PATH;
	DWORD R_KEY_POS;	 //
	DWORD S_KEY_POS;  

	DWORD BUCKET_SIZE;	 //
	DWORD READ_BUFFER_SIZE;	 //
	DWORD WRITE_BUFFER_SIZE; 
	DWORD BUFFER_POOL_SIZE;
	DWORD PROBE_SIZE_EVERY_TIME; //大表的读取粒度（单位：4k页）
	DOUBLE FUDGE_FACTOR; //散列表膨胀系数

	GHJ_PARAMS()
	{
		RELATION_R_PATH = new TCHAR[MAX_PATH];
		RELATION_R_NO_EXT = new TCHAR[MAX_PATH];
		RELATION_S_PATH = new TCHAR[MAX_PATH];
		RELATION_S_NO_EXT = new TCHAR[MAX_PATH];   
		WORK_SPACE_PATH = new TCHAR[MAX_PATH]; 

		BUCKET_SIZE = 0;	 
		WRITE_BUFFER_SIZE = 0; 
		BUFFER_POOL_SIZE = 0;
		PROBE_SIZE_EVERY_TIME = 1000;
		FUDGE_FACTOR = 1.2; 
		R_KEY_POS = 0;	  
		S_KEY_POS = 0;   
	} 
} GHJ_PARAMS;
 
/// <summary>
/// Hybrid Hash Join input parameters
/// </summary>
typedef struct HHJ_PARAMS
{
	LPWSTR RELATION_R_PATH;
	LPWSTR RELATION_R_NO_EXT;
	LPWSTR RELATION_S_PATH;
	LPWSTR RELATION_S_NO_EXT;
	LPWSTR WORK_SPACE_PATH;
	DWORD R_KEY_POS;	 //
	DWORD S_KEY_POS;  

	DWORD BUCKET_SIZE;	 //
	DWORD READ_BUFFER_SIZE;	 //
	DWORD WRITE_BUFFER_SIZE; 
	DWORD BUFFER_POOL_SIZE;
	DWORD PROBE_SIZE_EVERY_TIME; //大表的读取粒度（单位：4k页）
	DOUBLE FUDGE_FACTOR; //散列表膨胀系数

	HHJ_PARAMS()
	{
		RELATION_R_PATH = new TCHAR[MAX_PATH];
		RELATION_R_NO_EXT = new TCHAR[MAX_PATH];
		RELATION_S_PATH = new TCHAR[MAX_PATH];
		RELATION_S_NO_EXT = new TCHAR[MAX_PATH];   
		WORK_SPACE_PATH = new TCHAR[MAX_PATH]; 

		BUCKET_SIZE = 0;	 
		WRITE_BUFFER_SIZE = 0; 
		BUFFER_POOL_SIZE = 0;
		PROBE_SIZE_EVERY_TIME = 1000;
		FUDGE_FACTOR = 1.2; 
		R_KEY_POS = 0;	  
		S_KEY_POS = 0;   
	}

} HHJ_PARAMS;

 
/// <summary>
/// Block nested loop join input parameters
/// </summary>
typedef struct BNLJ_PARAMS
{
	LPWSTR RELATION_R_PATH;
	LPWSTR RELATION_R_NO_EXT;
	LPWSTR RELATION_S_PATH;
	LPWSTR RELATION_S_NO_EXT;
	LPWSTR WORK_SPACE_PATH;
	DWORD R_KEY_POS;	 //
	DWORD S_KEY_POS;  
	DWORD READ_BUFFER_SIZE;	 //
	DWORD WRITE_BUFFER_SIZE; 
	DWORD BUFFER_POOL_SIZE;
	DWORD PROBE_SIZE_EVERY_TIME; //大表的读取粒度（单位：4k页）
	DOUBLE FUDGE_FACTOR; //散列表膨胀系数
	BNLJ_PARAMS()
	{
		RELATION_R_PATH = new TCHAR[MAX_PATH];
		RELATION_R_NO_EXT = new TCHAR[MAX_PATH];
		RELATION_S_PATH = new TCHAR[MAX_PATH];
		RELATION_S_NO_EXT = new TCHAR[MAX_PATH];   
		WORK_SPACE_PATH = new TCHAR[MAX_PATH]; 

		READ_BUFFER_SIZE = 0;	 
		WRITE_BUFFER_SIZE = 0; 
		BUFFER_POOL_SIZE = 0;
		PROBE_SIZE_EVERY_TIME = 1000;
		FUDGE_FACTOR = 1.2; 
		R_KEY_POS = 0;	  
		S_KEY_POS = 0;   
	}

} BNLJ_PARAMS; 
  
/// <summary>
/// Classic Sort Merge Join parameters
/// </summary>
typedef struct SMJ_PARAMS
{
	LPWSTR RELATION_R_PATH;
	LPWSTR RELATION_R_NO_EXT;
	LPWSTR RELATION_S_PATH;
	LPWSTR RELATION_S_NO_EXT;
	LPWSTR WORK_SPACE_PATH;
	DWORD R_KEY_POS;	 //
	DWORD S_KEY_POS;  
	DWORD READ_BUFFER_SIZE;	 //
	DWORD WRITE_BUFFER_SIZE; 
	DWORD BUFFER_POOL_SIZE;
	SMJ_PARAMS()
	{
		RELATION_R_PATH = new TCHAR[MAX_PATH];
		RELATION_R_NO_EXT = new TCHAR[MAX_PATH];
		RELATION_S_PATH = new TCHAR[MAX_PATH];
		RELATION_S_NO_EXT = new TCHAR[MAX_PATH];   
		WORK_SPACE_PATH = new TCHAR[MAX_PATH]; 

		READ_BUFFER_SIZE = 0;	 
		WRITE_BUFFER_SIZE = 0; 
		BUFFER_POOL_SIZE = 0;
		R_KEY_POS = 0;	  
		S_KEY_POS = 0;   
	}

} SMJ_PARAMS; 
 
/// <summary>
/// Create page NSM input parameters
/// </summary>
typedef struct NSM_PARAMS
{
	LPWSTR SORT_FILE_PATH; 
	LPWSTR FILE_NAME_NO_EXT;
	LPWSTR WORK_SPACE_PATH;  

	NSM_PARAMS()
	{
		SORT_FILE_PATH = new TCHAR[MAX_PATH];
		FILE_NAME_NO_EXT = new TCHAR[MAX_PATH];
		WORK_SPACE_PATH = new TCHAR[MAX_PATH];  
	} 
} NSM_PARAMS; 

 
/// <summary>
/// Parallel external merge sort input parameters
/// </summary>
typedef struct PEMS_PARAMS
{
	LPWSTR SORT_FILE_PATH; 
	LPWSTR FILE_NAME_NO_EXT;
	LPWSTR WORK_SPACE_PATH; 

	DWORD SORT_READ_BUFFER_SIZE;	 //
	DWORD SORT_WRITE_BUFFER_SIZE; 
	DWORD MERGE_READ_BUFFER_SIZE;
	DWORD MERGE_WRITE_BUFFER_SIZE;

	DWORD KEY_POS;
	DWORD THREAD_NUM;

	DWORD BUFFER_POOL_SIZE;
	BOOL USE_SYNC_READ_WRITE_MODE; 
	BOOL USE_PARALLEL_MERGE; 
	BOOL USE_LOG_TO_FILE;
	BOOL USE_DELETE_AFTER_OPERATION;

	PEMS_PARAMS()
	{
		SORT_FILE_PATH = new TCHAR[MAX_PATH];
		FILE_NAME_NO_EXT = new TCHAR[MAX_PATH];
		WORK_SPACE_PATH = new TCHAR[MAX_PATH]; 

		SORT_READ_BUFFER_SIZE = 0;
		SORT_WRITE_BUFFER_SIZE = 0;
		MERGE_READ_BUFFER_SIZE = 0;
		MERGE_WRITE_BUFFER_SIZE = 0;
		KEY_POS = 0;

		BUFFER_POOL_SIZE = 0;
		THREAD_NUM = 0;
		USE_SYNC_READ_WRITE_MODE = 0;
		USE_PARALLEL_MERGE = 0;
		USE_LOG_TO_FILE = 0;
		USE_DELETE_AFTER_OPERATION = 0;
	}

} PEMS_PARAMS; 

 
/// <summary>
/// Cache External Merge Sort input parameters
/// </summary>
typedef struct CEMS_PARAMS
{
	LPWSTR SORT_FILE_PATH; 
	LPWSTR FILE_NAME_NO_EXT;
	LPWSTR SSD_WORK_SPACE_PATH;
	LPWSTR HDD_WORK_SPACE_PATH; 

	DWORD KEY_POS; 

	BOOL USE_LOG_TO_FILE;
	BOOL USE_DELETE_AFTER_OPERATION;

	DWORD BUFFER_POOL_SIZE;

	DWORD HEAP_SORT_MEMORY_SIZE;   // Memory use for sorting, use replacement selection, run size in best case is average 2*HeapSize*TupleSize

	DWORD SSD_READ_BUFFER_SIZE;
	DWORD SSD_WRITE_BUFFER_SIZE;    

	DWORD HDD_READ_BUFFER_SIZE;
	DWORD HDD_WRITE_BUFFER_SIZE;

	DWORD SSD_STORAGE_SIZE;                         

	CEMS_PARAMS()
	{
		SORT_FILE_PATH = new TCHAR[MAX_PATH];
		FILE_NAME_NO_EXT = new TCHAR[MAX_PATH];
		SSD_WORK_SPACE_PATH = new TCHAR[MAX_PATH];
		HDD_WORK_SPACE_PATH = new TCHAR[MAX_PATH];

		KEY_POS = 0;

		BUFFER_POOL_SIZE = 0; 
		SSD_READ_BUFFER_SIZE = 0; /* read from SSD buffer */ 
		SSD_WRITE_BUFFER_SIZE = 0; /* write to SSD buffer size, 4MB*/  
		HDD_READ_BUFFER_SIZE = 0; /* read from HDD buffer size */
		HDD_WRITE_BUFFER_SIZE = 0; /* write to HDD buffer size */  
		SSD_STORAGE_SIZE = 0; /* Maximum storage this SSD can hold, eg: 30MB */

		USE_LOG_TO_FILE = 0;
		USE_DELETE_AFTER_OPERATION = 0;
	}

} CEMS_PARAMS; 

 
/// <summary>
///  Replacement Selection sorting input parameters
/// </summary>
typedef struct RP_PARAMS
{
	LPWSTR SORT_FILE_PATH; 
	LPWSTR FILE_NAME_NO_EXT;
	LPWSTR WORK_SPACE_PATH; 

	DWORD SORT_READ_BUFFER_SIZE;	 
	DWORD SORT_WRITE_BUFFER_SIZE; 
	DWORD MERGE_READ_BUFFER_SIZE;
	DWORD MERGE_WRITE_BUFFER_SIZE;

	DWORD KEY_POS;
	DWORD BUFFER_POOL_SIZE;

	BOOL USE_SYNC_READ_WRITE_MODE; 
	BOOL USE_PARALLEL_MERGE; 
	BOOL USE_LOG_TO_FILE;
	BOOL USE_DELETE_AFTER_OPERATION;

	RP_PARAMS()
	{
		SORT_FILE_PATH = new TCHAR[MAX_PATH];
		FILE_NAME_NO_EXT = new TCHAR[MAX_PATH];
		WORK_SPACE_PATH = new TCHAR[MAX_PATH]; 

		SORT_READ_BUFFER_SIZE = 0;
		SORT_WRITE_BUFFER_SIZE = 0;
		MERGE_READ_BUFFER_SIZE = 0;
		MERGE_WRITE_BUFFER_SIZE = 0;
		KEY_POS = 0;

		BUFFER_POOL_SIZE = 0; 
		USE_SYNC_READ_WRITE_MODE = 0;
		USE_PARALLEL_MERGE = 0;
		USE_LOG_TO_FILE = 0;
		USE_DELETE_AFTER_OPERATION = 0;
	}

} RP_PARAMS;
 
/// <summary>
/// Tuple struct 
/// </summary>
typedef struct RECORD
{
	DWORD key;  // tuple key
	CHAR *data;  // tuple data
	INT offset; // indicate tuple position in buffer
	INT length;  // tuple length
	INT mark; /* Use in replacement selection, for determining this tuple belong to current run or next run */

	RECORD(): key(), data(), length()
	{ 
		key = 0;
		offset = 0;
		length = TUPLE_SIZE;
		data = new CHAR[TUPLE_SIZE];
	}

	// constructor 
	RECORD(UINT tupleSize) 
	{ 
		key = 0;
		offset = 0;
		length = tupleSize;
		data = new CHAR[tupleSize];
	}

	// constructor using with buffer pool
	RECORD(DWORD vKey, CHAR *vData) 
	{ 
		key = vKey; 
		offset = 0;
		data = vData;
		length = strlen(data);
	}


	// constructor using with buffer pool
	RECORD(DWORD vKey, CHAR *vData, INT vLength) 
	{ 
		key = vKey; 
		data = vData;
		length = vLength;
		offset = 0;
	}


} RECORD;

/// <summary>
/// Buffer struct
/// </summary>
typedef struct Buffer
{
	CHAR* data; 
	DWORD size; 
	DWORD startLocation; 
	DWORD freeLocation; 
	DWORD currentTupleIndex;
	DWORD currentSize; 
	DWORD currentPageIndex; 
	DWORD pageCount;
	DWORD tupleCount; 
	DWORD lowestKey;
	DWORD highestKey;
	BOOL isSort; // Is data sorted?
	BOOL isFullMaxValue;  // mark is buffer with MAX value
} Buffer;

typedef struct CACHE_ALIGN BufferPool
{
	DWORD size; 
	DWORD currentSize; 
	CHAR *data;  
} BufferPool;

typedef struct PageHeader
{
	DWORD totalTuple;
	DWORD slotLocation;
} PageHeader;

typedef struct PageSlot
{
	DWORD tupleSize;
	DWORD tupleOffset;
} PageSlot;

typedef struct PagePtr
{
	PageHeader *pageHeader; 
	PageSlot *pageSlot; 
	CHAR *tuple; 
	CHAR *page; 
	DWORD offset;
	DWORD freeSpace; // free space in page  
	BOOL  consumed;   // is this page write to buffer or not
} PagePtr;


typedef struct HashTable
{
	DWORD startLocation;//散列表在缓冲池起始地址
	DWORD size;
	DWORD currentSize;
	CHAR *data;//散列表的指针
	DWORD hashFn;   //散列表的取余函数
	INT *hashTupleCount;//为每个桶做标记

	// constructor
	HashTable() { size = 0; currentSize = 0; } 
}HashTable;

typedef struct HashTuple
{
	DWORD key;    //元组的连接键值
	DWORD offset;//元组在输入缓冲区的偏移地址
	HashTuple *next;

	HashTuple()
	{

	}

	~HashTuple()
	{

	}
}HashTuple;

typedef struct  FANS //CACHE_ALIGN
{
	LPWSTR fileName; // Path to this run on disk
	LARGE_INTEGER fileSize; // size of run created 
	DWORD threadID; // Thread ID
	DWORD pageCount; // Total page count
	DWORD tupleCount; // total tuple count
	DWORD lowestKey; // lowest key value in run
	DWORD highestKey; // highest key value in run
	BOOL isDeleted;
	BOOL isFinal; /* finally run or not?*/

	// constructor
	FANS()
	{
		fileName = new TCHAR[MAX_PATH]; 
		fileSize.QuadPart = 0;
		pageCount = 0;
		tupleCount = 0;
		lowestKey = 0;
		highestKey = 0;
		isFinal = FALSE;
		isDeleted = FALSE;
	} 
}FANS;


// Overlap structure
typedef struct OVERLAPPEDEX
{ 
	OVERLAPPED overlap;  // Overlap structure
	HANDLE hEvent; // Overlap event
	LARGE_INTEGER fileSize; // use to read/write file over 4GB
	DWORD dwBytesToReadWrite; // number of bytes to read/write into buffer
	DWORD dwBytesReadWritten;  // number of bytes have been read/written
	DWORD startChunk; // start read/write from whick chunk
	DWORD endChunk; // in multiple thread read/write, each thread must have this value
	DWORD totalChunk; // total chunk will be read/write
	DWORD chunkIndex; // split large file to multiple chunk, indicate current read/write chunk index


	// constructor
	OVERLAPPEDEX() 
	{
		dwBytesToReadWrite = 0;
		dwBytesReadWritten = 0;
		fileSize.QuadPart = 0;

		startChunk = 0;
		endChunk = 0;
		totalChunk = 0;
		chunkIndex = 0;

		hEvent = CreateEvent(NULL, FALSE, FALSE, NULL);   // create auto reset event, nonsignaled  
		overlap.hEvent = hEvent;
		overlap.Offset = 0;
		overlap.OffsetHigh = 0;
	}

	// destructor, close overlap event handle, prevent handle leak
	~OVERLAPPEDEX()
	{
		CloseHandle(hEvent);
	}

} OVERLAPPEDEX;


// global combobox structure value, use in GUI
typedef struct ComboBoxItem
{
	LPTSTR Name;
	DWORD Value; 
	ComboBoxItem() {}
	ComboBoxItem(LPTSTR vName, DWORD vValue) 
	{ 
		Name = vName; 
		Value = vValue; 
	}
}ComboBoxItem; 

//////////////////////////////////////////////////////////////////////////
///  WMI Struct
//////////////////////////////////////////////////////////////////////////

/************************************************************************/
/*  CPU PerformanceState                                                */
/************************************************************************/
typedef struct WMI_PerformanceState
{
	DWORD		Flags; /* Flag = 1 : Performance, Flag = 2: Throttle */
	DWORD		Frequency;
	DWORD		PercentFrequency;

	/* ProcessorBiosInfo */
	DWORD       Power;
	DWORD       BmLatency; 
	DWORD       Latency;
}WMI_PerformanceState; 

typedef struct WMI_PerformanceStates
{
	DWORD			 Count; /* Total P-state */
	DWORD			 Current; /* Current P-State */
	WMI_PerformanceState *State; 
}WMI_PerformanceStates;  

typedef struct WMI_ProcessorStatus
{
	BOOL				Active;
	DWORD				CurrentPerfState;
	LPWSTR				InstanceName;
	DWORD				LastRequestedThrottle;
	DWORD				LastTransitionResult;
	DWORD				LowestPerfState;
	DWORD			    ThrottleValue;
	BOOL			    UsingLegacyInterface;
	WMI_PerformanceStates	PerfStates;
	WMI_ProcessorStatus()
	{
		InstanceName = new TCHAR[1024];
	}
}WMI_ProcessorStatus; 

/************************************************************************/
/*  CPU Kernel Idle States                                              */
/************************************************************************/ 
typedef struct WMI_KernelIdleState  
{
	DWORD Context;
	DWORD DemotePercent;
	DWORD IdleHandler;
	DWORD Latency;
	DWORD Power;
	DWORD PromotePercent;
	DWORD Reserved;
	DWORD Reserved1;
	DWORD StateFlags;
	DWORD StateType;
	DWORD TimeCheck;
} WMI_KernelIdleState;

typedef struct WMI_KernelIdleStates 
{ 
	LPWSTR InstanceName;
	BOOL Active;
	DWORD Type;
	DWORD Count;
	DWORD TargetState;
	DWORD OldState;
	DWORD64 TargetProcessors;
	WMI_KernelIdleState *State; 
	WMI_KernelIdleStates()
	{
		InstanceName = new TCHAR[1024];
	}
}WMI_KernelIdleStates; 

typedef struct WMI_ProcessorBiosInfo 
{   
	DWORD Count;  
	WMI_PerformanceState *State;
}WMI_ProcessorBiosInfo;