// 
// Name: DiskMonitor.h : implementation file 
// Author: hieunt
// Description: Function working with disk, provide bandwitdth infos
//

#pragma once
#include <WinIoCtl.h>
#include <pdh.h>
#include "StopWatch.h"
//#include "CriticalSection.h"
#include "PageHelpers2.h"    

typedef struct DISK_STATES
{
	struct DiffCount 
	{
		INT64 ReadCount;
		INT64 WriteCount;
		INT64 SplitCount;
	}DiffCount;

	struct DiffBytes
	{
		INT64 BytesRead;
		INT64 BytesWritten;
	}DiffBytes;

	struct DiffTime 
	{
		INT64 QueryTime;
		INT64 ReadTime;
		INT64 WriteTime;
		INT64 IdleTime;
	}DiffTime;

	struct AvgTime 
	{
		INT64 ReadTime; 
		INT64 WriteTime;
	}AvgTime;

	struct AvgBytes
	{
		INT64 BytesRead; 
		INT64 BytesWritten;
	}AvgBytes;

	// Last time value
	INT64 ReadCount;
	INT64 WriteCount;
	INT64 SplitCount;

	INT64 QueryTime;
	INT64 ReadTime;
	INT64 WriteTime;
	INT64 IdleTime;

	INT64 BytesRead;
	INT64 BytesWritten;

}DISK_STATES;


typedef struct DISK_SPACE
{
	LPWSTR DiskName;
	DOUBLE TotalDiskSpace;
	DOUBLE FreeDiskSpace;
}DISK_SPACE;


typedef struct PROCESS_IO_COUNTER
{ 
	UINT64 ReadTransferCount[2];
	UINT64 WriteTransferCount[2];
	UINT64 ReadOperationCount[2];
	UINT64 WriteOperationCount[2]; 
	UINT64 TimeTransfer[2]; 
	//DWORD ReadTransferRate;
	//DWORD WriteTransferRate;

	PROCESS_IO_COUNTER()
	{
		TimeTransfer[0] = TimeTransfer[1] = 0;
		ReadTransferCount[0] = ReadTransferCount[1] = 0;
		WriteTransferCount[0] = WriteTransferCount[1] = 0;
		ReadOperationCount[0] = ReadOperationCount[1] = 0;
		WriteOperationCount[0] = WriteOperationCount[1] = 0;

		//ReadTransferRate = WriteTransferRate = 0;
	}
}PROCESS_IO_COUNTER;

class DiskMonitor 
{ 

public: 
	DISK_STATES diskStates;
	DiskMonitor(); 
	~DiskMonitor();

	BOOL GetDiskSpaceInfo();
	BOOL GetDriveGeometry(DISK_GEOMETRY *pdg);
	BOOL GetDrivePerformance();
	BOOL GetIOBytesUsage(UINT64 &readBytes, UINT64 &writeBytes);

	VOID GetProcessIoCounter (UINT64 &readTransferCount, UINT64 &writeTransferCount, DOUBLE &readTransferRate, DOUBLE &writeTransferRate, INT64 &timeTransfer, UINT64 &totalByteRead); 

	int GetDiskPerformance()
	{
		//////////////////
		// initail
		/////////////////
		PDH_STATUS status;
		HQUERY queryDiskTransfers = NULL, queryDiskReads = NULL, queryDiskWrites = NULL;
		HCOUNTER diskTransfersSec, diskReadsSec, diskWritesSec;
		PDH_FMT_COUNTERVALUE DisplayValue1, DisplayValue2, DisplayValue3;
		DWORD CounterType1, CounterType2, CounterType3;
		char diskTransfersSecBuffer[] = "\\PhysicalDisk(*)\\Disk Transfers/sec";
		char diskReadsSecBuffer[] = "\\PhysicalDisk(*)\\Disk Reads/sec";
		char diskWritesSecBuffer[] = "\\PhysicalDisk(*)\\Disk Writes/sec";

		 
		// Create a query.
		status = PdhOpenQuery(NULL, NULL, &queryDiskTransfers);
		if (status != ERROR_SUCCESS) 
		{
			wprintf(L"\nPdhOpenQuery1 failed with status 0x%x.", status);
			goto Cleanup;
		}
		// Add the selected counter to the query.
		status = PdhAddCounterA(queryDiskTransfers, diskTransfersSecBuffer, 0, &diskTransfersSec);
		if (status != ERROR_SUCCESS) 
		{
			wprintf(L"\nPdhAddCounter1 failed with status 0x%x.", status);
			goto Cleanup;
		}

		status = PdhCollectQueryData(queryDiskTransfers);
		if (status != ERROR_SUCCESS) 
		{
			wprintf(L"\nPdhCollectQueryData1 failed with 0x%x.\n", status);
			goto Cleanup;
		}
		 
		status = PdhOpenQuery(NULL, NULL, &queryDiskReads);
		if (status != ERROR_SUCCESS) 
		{
			wprintf(L"\nPdhOpenQuery2 failed with status 0x%x.", status);
			goto Cleanup;
		}
		// Add the selected counter to the query.
		status = PdhAddCounterA(queryDiskReads, diskReadsSecBuffer, 0, &diskReadsSec);
		if (status != ERROR_SUCCESS) 
		{
			wprintf(L"\nPdhAddCounter2 failed with status 0x%x.", status);
			goto Cleanup;
		}

		status = PdhCollectQueryData(queryDiskReads);
		if (status != ERROR_SUCCESS) 
		{
			wprintf(L"\nPdhCollectQueryData2 failed with 0x%x.\n", status);
			goto Cleanup;
		}
		 
		status = PdhOpenQuery(NULL, NULL, &queryDiskWrites);
		if (status != ERROR_SUCCESS) 
		{
			wprintf(L"\nPdhOpenQuery3 failed with status 0x%x.", status);
			goto Cleanup;
		}
		// Add the selected counter to the query.
		status = PdhAddCounterA(queryDiskWrites, diskWritesSecBuffer, 0, &diskWritesSec);
		if (status != ERROR_SUCCESS) 
		{
			wprintf(L"\nPdhAddCounter3 failed with status 0x%x.", status);
			goto Cleanup;
		}

		status = PdhCollectQueryData(queryDiskWrites);
		if (status != ERROR_SUCCESS) 
		{
			wprintf(L"\nPdhCollectQueryData3 failed with 0x%x.\n", status);
			goto Cleanup;
		}

		 
		int n = 5;
		while ( n-- > 0) 
		{
			Sleep(1000);

			//总IO流量
			status = PdhCollectQueryData(queryDiskTransfers);
			if (status != ERROR_SUCCESS) 
			{
				wprintf(L"\nPdhCollectQueryData1 failed with status 0x%x.", status);
			} 
			status = PdhGetFormattedCounterValue(diskTransfersSec,
				PDH_FMT_DOUBLE,//PDH_FMT_DOUBLE
				&CounterType1,
				&DisplayValue1);
			if (status != ERROR_SUCCESS) 
			{
				wprintf(L"\nPdhGetFormattedCounterValue1 failed with status 0x%x.", status);
				//goto Cleanup;
			}

			//读IO流量
			status = PdhCollectQueryData(queryDiskReads);
			if (status != ERROR_SUCCESS) 
			{
				wprintf(L"\nPdhCollectQueryData2 failed with status 0x%x.", status);
			} 

			status = PdhGetFormattedCounterValue(diskReadsSec,
				PDH_FMT_DOUBLE,//PDH_FMT_DOUBLE
				&CounterType2,
				&DisplayValue2);
			if (status != ERROR_SUCCESS) 
			{
				wprintf(L"\nPdhGetFormattedCounterValue2 failed with status 0x%x.", status);
				//goto Cleanup;
			}

			//写IO流量
			status = PdhCollectQueryData(queryDiskWrites);
			if (status != ERROR_SUCCESS) 
			{
				wprintf(L"\nPdhCollectQueryData3 failed with status 0x%x.", status);
			} 
			status = PdhGetFormattedCounterValue(diskWritesSec,
				PDH_FMT_DOUBLE,//PDH_FMT_DOUBLE
				&CounterType3,
				&DisplayValue3);
			if (status != ERROR_SUCCESS) 
			{
				wprintf(L"\nPdhGetFormattedCounterValue3 failed with status 0x%x.", status);
				//goto Cleanup;
			}

			//输出
			wprintf(L"Total: %6.2f MB/s, Read: %6.2f MB/s, Write: %6.2f MB/s\n", 
				DisplayValue1.doubleValue,
				DisplayValue2.doubleValue,
				DisplayValue3.doubleValue);
		}


Cleanup:
		if (queryDiskTransfers) 
		{
			PdhCloseQuery(queryDiskTransfers);
		}
		if (queryDiskReads) 
		{
			PdhCloseQuery(queryDiskReads);
		}
		if (queryDiskWrites) 
		{
			PdhCloseQuery(queryDiskWrites);
		}
		return -1;
	}

private:
	BOOL ExamineDrivePerformance (DISK_PERFORMANCE *dp); 



private:
	/* Variables */ 
	HANDLE hDevice;  // handle to the drive to be examined 
	DISK_PERFORMANCE diskPerformanceBefore;
	DISK_PERFORMANCE diskPerformanceAfter;
	vector<DISK_SPACE> DiskInfo; 
	StopWatch   stw; // measure transfer rate time
	//IO_COUNTERS ioCounter;
	PROCESS_IO_COUNTER processIoCounter;
	//CriticalSection sync;
	int a;
		PageHelpers2 *utl1;
};
