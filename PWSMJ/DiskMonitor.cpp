// 
// Name: DiskMonitor.cpp : implementation file 
// Author: hieunt
// Description: Function working with disk, provide bandwitdth infos
//

#include "stdafx.h" 
#include "DiskMonitor.h"

DiskMonitor::DiskMonitor()
{ 
	stw.Start();
	a = 0;
 utl1 = new PageHelpers2();
	hDevice = CreateFile(L"\\\\.\\PhysicalDrive0",  // drive to open
		0,                // no access to the drive
		FILE_SHARE_READ | // share mode
		FILE_SHARE_WRITE, 
		NULL,             // default security attributes
		OPEN_EXISTING,    // disposition
		0, //FILE_FLAG_OVERLAPPED ,                // file attributes
		NULL);            // do not copy file attributes

	diskStates.ReadCount = 0;
	diskStates.WriteCount = 0;
	diskStates.SplitCount = 0;
	diskStates.DiffCount.ReadCount = 0;
	diskStates.DiffCount.WriteCount = 0;
	diskStates.DiffCount.SplitCount = 0;
	diskStates.BytesRead = 0;
	diskStates.BytesWritten = 0;
	diskStates.DiffBytes.BytesRead = 0;
	diskStates.DiffBytes.BytesWritten = 0;
	diskStates.ReadTime = 0;
	diskStates.WriteTime = 0;
	diskStates.QueryTime = 0;
	diskStates.IdleTime = 0;
	diskStates.DiffTime.ReadTime = 0;
	diskStates.DiffTime.WriteTime = 0;
	diskStates.DiffTime.QueryTime = 0;
	diskStates.DiffTime.IdleTime = 0; 
	diskStates.AvgTime.ReadTime = 0;
	diskStates.AvgTime.WriteTime = 0;
	diskStates.AvgBytes.BytesRead = 0;
	diskStates.AvgBytes.BytesWritten = 0;
	ExamineDrivePerformance(&diskPerformanceAfter);

	// Save state
	diskPerformanceBefore =	diskPerformanceAfter;
}

DiskMonitor::~DiskMonitor()
{
	CloseHandle(hDevice);
}

BOOL DiskMonitor::GetDiskSpaceInfo()
{
	DWORD diskNumber = 0;
	DWORD diskDrivers;
	if(!(diskDrivers = GetLogicalDrives()))
	{
		//Log::Error("Get device driver error");
		return FALSE;
	}

	DiskInfo.clear();

	DOUBLE totalDiskSpace = 0;
	DOUBLE freeDiskSpace = 0;
	char diskName[16] = {'A',':'};
	for(int i = 0; i < 26; i++)
	{
		int bitMask = 0x01;
		bitMask = bitMask << i;
		if(diskDrivers & bitMask)    // there is a bit
		{
			diskName[0] = 'A' + i;
			LPWSTR diskNameW = c2ws(diskName);
			UINT diskType = GetDriveTypeW(diskNameW);
			if(diskType == DRIVE_FIXED)
			{
				DISK_SPACE disk;
				disk.DiskName = diskNameW;

				UINT64  totalBytes;
				UINT64  freeBytes;
				if(GetDiskFreeSpaceExW(diskNameW, NULL, (PULARGE_INTEGER)&totalBytes, (PULARGE_INTEGER)&freeBytes))
				{
					disk.TotalDiskSpace = (DOUBLE)totalBytes/1024/1024;
					disk.FreeDiskSpace = (DOUBLE)freeBytes/1024/1024;
				}
				DiskInfo.push_back(disk);
			}
		}
	} 

	return TRUE;
}

BOOL DiskMonitor::GetDriveGeometry(DISK_GEOMETRY *pdg)
{
	HANDLE hDevice;               // handle to the drive to be examined 
	BOOL bResult;                 // results flag
	DWORD junk;                   // discard results

	hDevice = CreateFile(L"\\\\.\\PhysicalDrive0",  // drive to open
		0,                // no access to the drive
		FILE_SHARE_READ | // share mode
		FILE_SHARE_WRITE, 
		NULL,             // default security attributes
		OPEN_EXISTING,    // disposition
		0,                // file attributes
		NULL);            // do not copy file attributes

	if (hDevice == INVALID_HANDLE_VALUE) // cannot open the drive
	{
		//wprintf(L"CreateFile() failed!\n");
		return (FALSE);
	}

	bResult = DeviceIoControl(hDevice,  // device to be queried
		IOCTL_DISK_GET_DRIVE_GEOMETRY,  // operation to perform
		NULL, 0, // no input buffer
		pdg, sizeof(*pdg),     // output buffer
		&junk,                 // # bytes returned
		(LPOVERLAPPED) NULL);  // synchronous I/O

	CloseHandle(hDevice);

	return (bResult);
}

BOOL DiskMonitor::GetDrivePerformance()
{
	if (hDevice == INVALID_HANDLE_VALUE) // cannot open the drive
	{ 
		return (FALSE);
	}

	ExamineDrivePerformance(&diskPerformanceAfter);

	diskStates.ReadCount = diskPerformanceAfter.ReadCount;
	diskStates.WriteCount = diskPerformanceAfter.WriteCount;
	diskStates.SplitCount = diskPerformanceAfter.SplitCount;
	diskStates.DiffCount.ReadCount = diskPerformanceAfter.ReadCount - diskPerformanceBefore.ReadCount;
	diskStates.DiffCount.ReadCount = diskPerformanceAfter.ReadCount - diskPerformanceBefore.ReadCount;
	diskStates.DiffCount.SplitCount = diskPerformanceAfter.SplitCount - diskPerformanceBefore.SplitCount;

	diskStates.BytesRead = diskPerformanceAfter.BytesRead.QuadPart;
	diskStates.BytesWritten = diskPerformanceAfter.BytesWritten.QuadPart;
	diskStates.DiffBytes.BytesRead = diskPerformanceAfter.BytesRead.QuadPart - diskPerformanceBefore.BytesRead.QuadPart;
	diskStates.DiffBytes.BytesWritten = diskPerformanceAfter.BytesWritten.QuadPart - diskPerformanceBefore.BytesWritten.QuadPart;

	diskStates.ReadTime = diskPerformanceAfter.ReadTime.QuadPart;
	diskStates.WriteTime = diskPerformanceAfter.WriteTime.QuadPart;
	diskStates.QueryTime = diskPerformanceAfter.QueryTime.QuadPart;
	diskStates.IdleTime = diskPerformanceAfter.IdleTime.QuadPart;
	diskStates.DiffTime.ReadTime = diskPerformanceAfter.ReadTime.QuadPart - diskPerformanceBefore.ReadTime.QuadPart;
	diskStates.DiffTime.WriteTime = diskPerformanceAfter.WriteTime.QuadPart - diskPerformanceBefore.WriteTime.QuadPart;
	diskStates.DiffTime.QueryTime = diskPerformanceAfter.QueryTime.QuadPart - diskPerformanceBefore.QueryTime.QuadPart;
	diskStates.DiffTime.IdleTime = diskPerformanceAfter.IdleTime.QuadPart - diskPerformanceBefore.IdleTime.QuadPart;

	if(diskStates.ReadCount==0)
	{
		diskStates.AvgTime.ReadTime = -1;
		diskStates.AvgBytes.BytesRead = 0;
	}
	else
	{
		diskStates.AvgTime.ReadTime = DOUBLE(diskStates.DiffTime.ReadTime / 10000) / DOUBLE(diskStates.ReadCount);
		//if(diskStates.DiffTime.ReadTime < 0) diskStates.DiffTime.ReadTime = 1;
		//if(diskStates.DiffBytes.BytesRead < 0) diskStates.DiffBytes.BytesRead = 0;
		INT64 ReadTime = diskStates.DiffTime.ReadTime <= 0 ? 1 : diskStates.DiffTime.ReadTime;
		DOUBLE BytesRead = diskStates.DiffBytes.BytesRead <= 0 ? 0 : diskStates.DiffBytes.BytesRead;

		diskStates.AvgBytes.BytesRead = DOUBLE(BytesRead)/ DOUBLE(ReadTime / 10000);
		diskStates.AvgBytes.BytesRead = diskStates.AvgBytes.BytesRead < 0 ? 0 : diskStates.AvgBytes.BytesRead;
	}
	if(diskStates.WriteCount==0)
	{
		diskStates.AvgTime.WriteTime = -1;
		diskStates.AvgBytes.BytesWritten = 0;
	}
	else
	{
		diskStates.AvgTime.WriteTime = DOUBLE(diskStates.DiffTime.WriteTime / 10000) / DOUBLE(diskStates.WriteCount);
		//if(diskStates.DiffTime.WriteTime < 0) diskStates.DiffTime.WriteTime = 1; 
		INT64 WriteTime = diskStates.DiffTime.WriteTime <= 0 ? 1 : diskStates.DiffTime.WriteTime;
		DOUBLE BytesWritten = diskStates.DiffBytes.BytesWritten <= 0 ? 0 : diskStates.DiffBytes.BytesWritten;
		diskStates.AvgBytes.BytesWritten =BytesWritten / DOUBLE(WriteTime / 10000);
		diskStates.AvgBytes.BytesWritten = diskStates.AvgBytes.BytesWritten < 0 ? 0 : diskStates.AvgBytes.BytesWritten;
	}
	diskPerformanceBefore = diskPerformanceAfter;

	return TRUE;
}

BOOL DiskMonitor::ExamineDrivePerformance (DISK_PERFORMANCE *dp)
{

	BOOL bResult;                 // results flag
	DWORD junk;                   // discard results

	LPOVERLAPPED lpOverlapped = {0};
	bResult = DeviceIoControl(
		hDevice,            // handle to device
		IOCTL_DISK_PERFORMANCE,      // dwIoControlCode
		NULL,                        // lpInBuffer
		0,                           // nInBufferSize
		dp,        // output buffer
		sizeof(*dp),      // size of output buffer
		&junk,   // number of bytes returned
		NULL //(LPOVERLAPPED) lpOverlapped  // OVERLAPPED structure
		);

	return (bResult);
}

VOID  DiskMonitor::GetProcessIoCounter(UINT64 &readTransferCount, UINT64 &writeTransferCount, DOUBLE &readTransferRate, DOUBLE &writeTransferRate, INT64 &timeTransfer,  UINT64 &totalByteRead)
{
	//sync.Lock();
	// Update new counter
	// Correct only with one thread execute
	IO_COUNTERS ioCounter;
	if(GetProcessIoCounters(GetCurrentProcess(), &ioCounter))
	{
		processIoCounter.TimeTransfer[1] = stw.NowInMilliseconds();

		processIoCounter.ReadTransferCount[1] = ioCounter.ReadTransferCount;
		processIoCounter.WriteTransferCount[1] = ioCounter.WriteTransferCount;
		processIoCounter.ReadOperationCount[1] = ioCounter.ReadOperationCount;
		processIoCounter.WriteOperationCount[1] = ioCounter.WriteOperationCount;
	 
		// Calculate read / write tranfer
		INT64 timeEslapse = 0; // unit in second
		if(processIoCounter.TimeTransfer[1] >= processIoCounter.TimeTransfer[0])
			timeEslapse = processIoCounter.TimeTransfer[1] - processIoCounter.TimeTransfer[0];
		else
			timeEslapse = processIoCounter.TimeTransfer[0] - processIoCounter.TimeTransfer[1];

		timeTransfer = timeEslapse; // in ms


		readTransferCount = processIoCounter.ReadTransferCount[1] - processIoCounter.ReadTransferCount[0];
		writeTransferCount = processIoCounter.WriteTransferCount[1] - processIoCounter.WriteTransferCount[0]; 
		totalByteRead = ioCounter.ReadTransferCount;

		DOUBLE timeEslapseInSecond = DOUBLE(timeEslapse) / 1000;
		if(timeEslapse!=0)
		{
			readTransferRate = DOUBLE (readTransferCount / (1024 * 1024)) / timeEslapseInSecond;
			writeTransferRate = DOUBLE (writeTransferCount / (1024 * 1024)) / timeEslapseInSecond;
		}
		else
		{
			readTransferRate = writeTransferRate = 0;
		}

		// Save current values to previous
		processIoCounter.ReadTransferCount[0] = processIoCounter.ReadTransferCount[1];
		processIoCounter.WriteTransferCount[0] = processIoCounter.WriteTransferCount[1];
		processIoCounter.ReadOperationCount[0] = processIoCounter.ReadOperationCount[1] ;
		processIoCounter.WriteOperationCount[0] = processIoCounter.WriteOperationCount[1]; 
		processIoCounter.TimeTransfer[0] = processIoCounter.TimeTransfer[1]; 
	}
	else
	{ 
		readTransferCount = 0;
		writeTransferCount = 0; 
		totalByteRead = 0;

		chMB("aaaa");
		//utl1->DebugToFile("Fuck", a++, 4096);

		//processIoCounter.ReadTransferCount[0] = processIoCounter.ReadTransferCount[1] = 0;
		//processIoCounter.WriteTransferCount[0] = processIoCounter.WriteTransferCount[1] = 0;
		//processIoCounter.ReadOperationCount[0] = processIoCounter.ReadOperationCount[1] = 0;
		//processIoCounter.WriteOperationCount[0] = processIoCounter.WriteOperationCount[1] = 0; 
	}
	//sync.UnLock();
}

BOOL DiskMonitor::GetIOBytesUsage(UINT64 &readBytes, UINT64 &writeBytes)
{ 
	//if(GetProcessIoCounters(GetCurrentProcess(), &ioCounter))
	//{  
	//	readBytes = ioCounter.ReadTransferCount;
	//	writeBytes = ioCounter.WriteTransferCount;
		//ioCounter.ReadOperationCount
		//ioCounter.WriteOperationCount 
	//	return TRUE;
	//}

	//TODO You can call GetProcessIoCounters to get overall disk I/O data per process - you'll need to keep track of deltas and converting to time-based rate yourself.
	// This API will tell you total number of I/O operations as well as total bytes.
	return FALSE;
}