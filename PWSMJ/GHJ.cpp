// 
// Name: GHJ.cpp : implementation file  
// Description: Implementation of Grace Hash Join 
//

#include "stdafx.h"
#include "GHJ.h"

GHJ::GHJ(const GHJ_PARAMS vParams) : m_Params(vParams)
{
	m_AvailableMemorySize = m_Params.BUFFER_POOL_SIZE / SSD_PAGE_SIZE; // Unit in page
	m_BucketSize = m_Params.BUCKET_SIZE / SSD_PAGE_SIZE;
	m_ReadBufferSize = m_Params.READ_BUFFER_SIZE / SSD_PAGE_SIZE;
	m_JoinCount = 0;
	m_JoinTuple  = new RECORD(TUPLE_SIZE * 2);
	m_R_Tuple = new RECORD(TUPLE_SIZE);
	m_S_Tuple = new RECORD(TUPLE_SIZE); 
}

GHJ::~GHJ()
{
	delete m_JoinTuple;
	delete m_R_Tuple;
	delete m_S_Tuple;
}

RC GHJ::Initialize()
{ 
	HANDLE hR = CreateFile(
		(LPCWSTR)m_Params.RELATION_R_PATH, // file to open
		GENERIC_READ,			// open for reading
		FILE_SHARE_READ,        // share for reading
		NULL,					// default security
		OPEN_EXISTING,			// existing file only
		FILE_ATTRIBUTE_NORMAL,	// overlapped operation //| FILE_FLAG_NO_BUFFERING
		NULL);					// no attr. template 

	//FILE_FLAG_NO_BUFFERING
	//http://msdn.microsoft.com/en-us/library/windows/desktop/cc644950(v=vs.85).aspx

	if (INVALID_HANDLE_VALUE==hR) 
	{  
		return ERR_CANNOT_CREATE_HANDLE;
	} 

	LARGE_INTEGER *liFileSize = new LARGE_INTEGER();  
	if (!GetFileSizeEx(hR, liFileSize))
	{       
		return ERR_CANNOT_GET_FILE_SIZE; 
	} 

	CloseHandle(hR);

	m_R_FileSize = chROUNDUP(liFileSize->QuadPart, SSD_PAGE_SIZE) / SSD_PAGE_SIZE;


	//考虑FUDGE_FACTOR倍增系数
	m_PartitionNum = (int)(m_R_FileSize*m_Params.FUDGE_FACTOR / (m_AvailableMemorySize)+1);//-PROBE_SIZE_EVERY_TIME
	//当分区数为8整除时数据集分布可能有异常
	if(0==m_PartitionNum % 8)
	{
		m_PartitionNum = m_PartitionNum + 1;//nbatch 被3整除时 s表第一分区为0 避开
	}

	m_FileHandle = new HANDLE[m_PartitionNum]; 

	if(SUCCESS!=InitBufferPool(&m_Pool, m_Params.BUFFER_POOL_SIZE))
	{ 
		ShowMB(L"Cannot init buffer pool");
		return ERR_CANNOT_INITIAL_BUFFER_POOL;
	}

	if(((DWORD)(m_AvailableMemorySize - m_ReadBufferSize) % m_PartitionNum)==0)
	{
		m_BucketSize=(m_AvailableMemorySize  - m_ReadBufferSize) / m_PartitionNum;
	}
	else
	{
		m_ReadBufferSize+=DWORD(m_AvailableMemorySize - m_ReadBufferSize) % m_PartitionNum;
		m_BucketSize=(m_AvailableMemorySize - m_ReadBufferSize) / m_PartitionNum;
	}

	if(m_BucketSize<=0)
	{
		ShowMB(L"Not enough memory for bucket");
		return ERR_NOT_ENOUGH_MEMORY;
	}

	// Init read buffer size
	InitBuffer(&m_InBuffer, m_Params.READ_BUFFER_SIZE, &m_Pool);

	m_BucketBuffer = new Buffer[m_PartitionNum];
	for(UINT partitionIndex=0; partitionIndex < m_PartitionNum; partitionIndex++)
	{
		if(SUCCESS!=InitBuffer(&m_BucketBuffer[partitionIndex], m_BucketSize * SSD_PAGE_SIZE,  &m_Pool))
		{
			ShowMB(L"Bufpool size is too small for all the buckets!"); 
			return ERR_NOT_ENOUGH_MEMORY;
		}
	}

	//对桶装配过程中需要的全局变量初始化
	m_BucketPage= new PagePtr[m_PartitionNum];	//每个桶当前页 
	for(UINT partitionIndex=0; partitionIndex < m_PartitionNum; partitionIndex++)
	{     
		InitRunPage(&m_BucketPage[partitionIndex], &m_BucketBuffer[partitionIndex]); 
	}

	return SUCCESS;
}

//连接阶段
RC GHJ::HashJoin(BufferPool *bufferPool)
{
	//缓冲池重新初始化
	m_JoinCount=0;
	bufferPool->currentSize = 0; 

	HashTable hashTable;
	PageHeader *pageHeader;//用于操作读进来的页以获得页头等信息

	LPWSTR bucketPath_R = new TCHAR[MAX_PATH]; //桶文件名
	LPWSTR bucketPath_S = new TCHAR[MAX_PATH];
	LARGE_INTEGER liFileSize;

	//Init out page buffer 
	if(SUCCESS!=InitBuffer(&m_OutPageBuffer,SSD_PAGE_SIZE, &m_Pool))
	{
		ShowMB(L"Not enough memory for output page buffer");
		return ERR_NOT_ENOUGH_MEMORY;
	} 

	//Init out page 
	InitRunPage(&m_OutPage, &m_OutPageBuffer);  

	// Init write buffer size 
	if(SUCCESS!=InitBuffer(&m_OutBuffer, m_Params.WRITE_BUFFER_SIZE, &m_Pool))
	{
		ShowMB(L"Not enough memory for output buffer");
		return ERR_NOT_ENOUGH_MEMORY;
	}

	LPWSTR outputFileName = new TCHAR[MAX_PATH];
	swprintf_s(outputFileName, MAX_PATH, L"%sGHJ_%s_%s.dat", m_Params.WORK_SPACE_PATH, m_Params.RELATION_R_NO_EXT, m_Params.RELATION_S_NO_EXT);

	m_hOutFile=CreateFile(
		(LPCWSTR)outputFileName,		// file to write
		GENERIC_WRITE,			// open for writing
		0,						// Do not share
		NULL,					// default security
		CREATE_ALWAYS,			 
		FILE_ATTRIBUTE_NORMAL,	// FILE_FLAG_OVERLAPPED
		NULL);				 

	//FILE_FLAG_NO_BUFFERING
	//http://msdn.microsoft.com/en-us/library/windows/desktop/cc644950(v=vs.85).aspx

	if (INVALID_HANDLE_VALUE==m_hOutFile) 
	{  
		ShowMB(L"Cannot create handle %s", outputFileName);
		return ERR_CANNOT_CREATE_HANDLE;
	} 

	//Init read buffer
	InitBuffer(&m_InBuffer, 0, bufferPool);

	for(UINT partitionIndex=0; partitionIndex < m_PartitionNum; partitionIndex++)
	{       
		swprintf_s(bucketPath_R, MAX_PATH, L"%s%d%s.tmp", m_Params.WORK_SPACE_PATH, partitionIndex, m_Params.RELATION_R_NO_EXT); //生成桶文件名
		swprintf_s(bucketPath_S, MAX_PATH, L"%s%d%s.tmp", m_Params.WORK_SPACE_PATH, partitionIndex, m_Params.RELATION_S_NO_EXT);

		HANDLE hBucketR=CreateFile(bucketPath_R, GENERIC_READ|GENERIC_WRITE, 0, NULL, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
		HANDLE hBucketS=CreateFile(bucketPath_S, GENERIC_READ|GENERIC_WRITE, 0, NULL, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);

		if(hBucketR==INVALID_HANDLE_VALUE || hBucketS==INVALID_HANDLE_VALUE)
		{ 
			ShowMB(L"Cannot create bucket handle %s\n or %s", bucketPath_R, bucketPath_S);
			return ERR_CANNOT_CREATE_HANDLE;
		} 

		GetFileSizeEx(hBucketR, &liFileSize);
		if (!GetFileSizeEx(hBucketR, &liFileSize))
		{       
			ShowMB(L"Cannot get file size %s", bucketPath_R);
			return ERR_CANNOT_GET_FILE_SIZE; 
		} 

		// Init input buffer 
		m_InBuffer.size = liFileSize.QuadPart;
		bufferPool->currentSize+=m_InBuffer.size;

		DWORD tupleCount = 0;
		//分区文件读入输入缓冲区
		while(TRUE)
		{   
			DWORD dwBytesRead = 0;
			ReadFile(hBucketR,
				m_InBuffer.data,
				m_InBuffer.size,
				&dwBytesRead, 
				NULL);

			if(dwBytesRead==0)
			{
				break;
			}

			DWORD pageCount = 0;
			if(dwBytesRead%SSD_PAGE_SIZE==0)
				pageCount = dwBytesRead/SSD_PAGE_SIZE;
			else
				pageCount = dwBytesRead/SSD_PAGE_SIZE + 1;

			m_InBuffer.currentSize+=dwBytesRead;
			m_InBuffer.freeLocation+=dwBytesRead;  
			m_InBuffer.pageCount += pageCount;

			for(UINT pageIndex=0; pageIndex < pageCount; pageIndex++)
			{
				pageHeader=(PageHeader*)(m_InBuffer.data + pageIndex * SSD_PAGE_SIZE);
				tupleCount+=pageHeader->totalTuple;
			} 
		}	
		m_InBuffer.tupleCount+=tupleCount;

		//初始化散列表信息
		hashTable.startLocation = bufferPool->currentSize;
		hashTable.data = bufferPool->data + hashTable.startLocation;

		if(tupleCount%2==0)
			hashTable.hashFn=tupleCount+1;
		else
			hashTable.hashFn=tupleCount;

		hashTable.size = hashTable.hashFn * sizeof(HashTuple);

		//构建散列表
		HashBuild(&m_InBuffer, &hashTable);
		bufferPool->currentSize+=hashTable.size;

		//为大表建立读取缓存
		m_ProbeBuffer.startLocation = bufferPool->currentSize;
		m_ProbeBuffer.freeLocation = m_ProbeBuffer.startLocation;
		m_ProbeBuffer.data = bufferPool->data + m_ProbeBuffer.startLocation;
		DWORD probeBufferSize = chROUNDDOWN((bufferPool->size - bufferPool->currentSize), SSD_PAGE_SIZE) / SSD_PAGE_SIZE; 
		m_ProbeBuffer.size = probeBufferSize * SSD_PAGE_SIZE; 	
		m_ProbeBuffer.currentSize = 0;
		m_ProbeBuffer.pageCount = 0;
		m_ProbeBuffer.tupleCount = 0; 
		bufferPool->currentSize += m_ProbeBuffer.size;

		if(m_ProbeBuffer.size==0)
		{ 
			ShowMB(L"Exceeds available memory limit");
			return ERR_NOT_ENOUGH_MEMORY;
		}

		PagePtr probePage;
		while(TRUE)
		{	
			DWORD dwBytesRead = 0; 
			ReadFile(hBucketS,
				m_ProbeBuffer.data, 
				m_ProbeBuffer.size, 
				&dwBytesRead,
				NULL);

			if(dwBytesRead==0)
			{
				break;
			}

			DWORD pageCount = 0;
			if(dwBytesRead%SSD_PAGE_SIZE==0)
				pageCount = dwBytesRead/SSD_PAGE_SIZE;
			else
				pageCount = dwBytesRead/SSD_PAGE_SIZE + 1;

			for(UINT pageIndex=0; pageIndex < pageCount; pageIndex++)
			{
				probePage.page = m_ProbeBuffer.data + pageIndex*SSD_PAGE_SIZE;
				HashProbe(&probePage, &hashTable, &m_InBuffer);
			}
		}

		CloseHandle(hBucketR);
		CloseHandle(hBucketS); 

		ResetBuffer(&m_InBuffer);
	} 

	if(m_OutBuffer.currentSize > 0)
	{
		DWORD dwBytesWritten = 0; 
		WriteFile(m_hOutFile, 
			m_OutBuffer.data, 
			m_OutBuffer.currentSize,
			&dwBytesWritten,
			NULL); 
	}

	CloseHandle(m_hOutFile);

	delete outputFileName; 

	return SUCCESS;
}

RC GHJ::Execute()
{
	RC rc = Initialize();
	if(rc!=SUCCESS)
	{
		return rc;
	} 

	DOUBLE cpuTimeBefore = 0;
	DOUBLE cpuTimeAfter = 0;
	DOUBLE cpuTime = 0;
	StopWatch stwTotalTime;
	StopWatch stwJoinTime;
	StopWatch stwPartitionTime;
	UINT64 totalTime = 0;
	UINT64 partitionTime = 0;
	UINT64 joinTime = 0;

	stwTotalTime.Start();
	stwPartitionTime.Start();
	cpuTimeBefore = GetCpuTime();

	PartitionTable(m_Params.RELATION_R_PATH, m_Params.R_KEY_POS, &m_InBuffer, m_BucketBuffer, m_FileHandle);

	/*************************************************************************/ 

	//再次对桶装配过程中需要的全局变量初始化 
	for(UINT partitionIndex=0; partitionIndex < m_PartitionNum; partitionIndex++)
	{   
		ResetPage(&m_BucketPage[partitionIndex], &m_BucketBuffer[partitionIndex]); 
	}

	PartitionTable(m_Params.RELATION_S_PATH, m_Params.S_KEY_POS, &m_InBuffer, m_BucketBuffer, m_FileHandle);

	partitionTime = stwPartitionTime.NowInMilliseconds();
	stwJoinTime.Start();

	HashJoin(&m_Pool);


	cpuTimeAfter = GetCpuTime(); 
	totalTime = stwTotalTime.NowInMilliseconds();
	joinTime = stwJoinTime.NowInMilliseconds();
	cpuTime = cpuTimeAfter - cpuTimeBefore;


	FILE *fp;   
	CHAR *reportFilePath = new CHAR[MAX_PATH];   
	LPWSTR tempReportPath = new TCHAR[MAX_PATH];
	swprintf(tempReportPath, MAX_PATH, L"%s%s", m_Params.WORK_SPACE_PATH, L"GHJ_Report.csv" ); 
	// convert file path to char  
	size_t  count = wcstombs(reportFilePath, tempReportPath, MAX_PATH); 

	CHAR *reportTitle = "Relation Size,Memory Size,Bucket Size,Partition,Read Buffer Size,Write Buffer Size,Total Execute Time(ms),Partition Time(ms),Join Time(ms),CPU Time\n";
	CHAR *reportContent = new CHAR[1024];
	sprintf(reportContent, "%d,%d,%.f,%d,%d,%d,%lld,%lld,%lld,%.f", 
		m_R_FileSize, 
		m_Params.BUFFER_POOL_SIZE/SSD_PAGE_SIZE, 
		m_BucketSize, 
		m_PartitionNum, 
		m_Params.READ_BUFFER_SIZE/SSD_PAGE_SIZE,
		m_Params.WRITE_BUFFER_SIZE/SSD_PAGE_SIZE, 
		totalTime, 
		partitionTime, 
		joinTime, 
		cpuTime);
	fp=fopen(reportFilePath, "w+b"); 
	fprintf(fp, reportTitle);
	fprintf(fp, reportContent);
	fclose(fp);

	delete reportFilePath;
	delete tempReportPath; 
	delete reportContent;

	//连接结束后 删除所有的临时文件 
	LPWSTR tempBucketName = new TCHAR[MAX_PATH];
	for(UINT partitionIndex=0;partitionIndex < m_PartitionNum;partitionIndex++)
	{
		swprintf_s(tempBucketName, MAX_PATH, L"%s%d%s.tmp", m_Params.WORK_SPACE_PATH, partitionIndex, m_Params.RELATION_R_NO_EXT);
		DeleteFile(tempBucketName);

		swprintf_s(tempBucketName, MAX_PATH, L"%s%d%s.tmp", m_Params.WORK_SPACE_PATH, partitionIndex, m_Params.RELATION_S_NO_EXT);
		DeleteFile(tempBucketName); 
	}

	delete tempBucketName;
	delete m_Pool.data;
	CloseHandle(m_hOutFile);

	ShowMB(L"GHJ done"); 

	return SUCCESS;
}

//读取文件进行分区
RC  GHJ::PartitionTable(const LPWSTR tableName, const DWORD keyPos, Buffer *inBuffer, Buffer *bucketBuffer, HANDLE *&hFile)
{    	 
	HANDLE hTable=CreateFile(
		tableName,
		GENERIC_READ,
		0,
		NULL,
		OPEN_EXISTING,
		FILE_ATTRIBUTE_NORMAL,
		NULL);

	if(hTable==INVALID_HANDLE_VALUE)
	{
		ShowMB(L"Cannot create handle file %s", tableName);
		return ERR_CANNOT_CREATE_HANDLE; 
	}

	LPWSTR tempFilePath = new TCHAR[MAX_PATH]; 
	LPWSTR fileNameNoExt = new TCHAR[MAX_PATH]; 
	_wsplitpath(tableName, NULL, NULL, fileNameNoExt, NULL); 

	for(UINT partitionIndex=0; partitionIndex < m_PartitionNum; partitionIndex++)
	{
		swprintf_s(tempFilePath, MAX_PATH, L"%s%d%s.tmp", m_Params.WORK_SPACE_PATH, partitionIndex, fileNameNoExt); //生成对应分区的文件名

		hFile[partitionIndex]=CreateFile(
			tempFilePath,
			GENERIC_WRITE,
			0,
			NULL,
			CREATE_ALWAYS,
			FILE_ATTRIBUTE_NORMAL,
			NULL); //用于写操作

		if(hFile[partitionIndex]==INVALID_HANDLE_VALUE)
		{
			ShowMB(L"Cannot create handle file %s", tempFilePath);
			return ERR_CANNOT_CREATE_HANDLE; 
		}
	}

	while(TRUE) //当文件指针未指向文件尾
	{
		DWORD dwBytesRead = 0;
		ReadFile(hTable,
			inBuffer->data,
			inBuffer->size,
			&dwBytesRead,
			NULL);

		inBuffer->pageCount = dwBytesRead / SSD_PAGE_SIZE;  
		inBuffer->freeLocation+=dwBytesRead;
		inBuffer->currentSize+=dwBytesRead;

		if(dwBytesRead==0)
		{
			ResetBuffer(inBuffer);
			break;
		}

		for(UINT pageIndex=0; pageIndex < inBuffer->pageCount; pageIndex++)	//以页为单位处理输入缓冲区
		{
			ProcessPage(pageIndex, inBuffer, bucketBuffer, hFile, keyPos);
		}

		//清空输入缓冲区
		inBuffer->freeLocation = inBuffer->startLocation;
		inBuffer->currentSize = 0;
		inBuffer->pageCount = 0;
		inBuffer->tupleCount = 0; 
	}	

	//对每个桶剩余的页面写回分区对应的临时文件  

	for(UINT partitionIndex=0; partitionIndex < m_PartitionNum; partitionIndex++)
	{ 
		DWORD dwBytesWritten = 0;
		DWORD dwBytesToWrite = (bucketBuffer[partitionIndex].currentPageIndex+1) * SSD_PAGE_SIZE; 
		WriteFile(hFile[partitionIndex],
			(LPVOID)bucketBuffer[partitionIndex].data, 
			dwBytesToWrite,
			&dwBytesWritten,
			NULL);

		//置零输出缓存
		bucketBuffer[partitionIndex].freeLocation=bucketBuffer[partitionIndex].startLocation;
		bucketBuffer[partitionIndex].currentPageIndex = 0;
		bucketBuffer[partitionIndex].currentTupleIndex = 0;
		bucketBuffer[partitionIndex].currentSize = 0;
		bucketBuffer[partitionIndex].pageCount = 0;
		bucketBuffer[partitionIndex].tupleCount = 0;
	}

	for(UINT partitionIndex=0; partitionIndex < m_PartitionNum; partitionIndex++)
	{
		CloseHandle(hFile[partitionIndex]);
	}


	CloseHandle(hTable);
	delete tempFilePath;
	delete fileNameNoExt; 

	return SUCCESS;
}	

//对输入缓存的当前页进行分区操作
VOID GHJ::ProcessPage(const INT pageIndex, Buffer *inBuffer, Buffer *bucketBuffer, HANDLE *&hFile, const DWORD keyPos)
{
	PagePtr	pagePtr;//输入缓冲区的当前页
	DWORD tupleCount=0;//输入缓冲区当前页中元组的数量
	DWORD tupleKey; //元组的连接键值
	DWORD hashKey;//计算出相应的桶序号

	DWORD dwBytesRead=1;//
	DWORD dwBytesWritten=1;//

	HashTuple *hashTuple = new HashTuple();
	pagePtr.page = inBuffer->data + SSD_PAGE_SIZE*pageIndex;
	pagePtr.pageSlot = (PageSlot*)(pagePtr.page + SSD_PAGE_SIZE - sizeof(PageSlot));  
	pagePtr.pageHeader = (PageHeader *)pagePtr.page;
	pagePtr.tuple = pagePtr.page + sizeof(PageHeader);

	tupleCount = pagePtr.pageHeader->totalTuple;

	//依次读取输入缓冲的page中的每条记录

	for(UINT tupleIndex=0; tupleIndex < tupleCount; tupleIndex++)
	{
		tupleKey = GetKey(pagePtr.page+(pagePtr.pageSlot-tupleIndex)->tupleOffset, keyPos);//从第一个槽获得第一条元组的信息并计算出键值
		hashKey = tupleKey % m_PartitionNum;

		if(m_BucketPage[hashKey].freeSpace > 258)	//桶的当前页未满
		{
			UpdateBucket(tupleIndex, pagePtr, m_BucketPage, hashKey);
			bucketBuffer[hashKey].tupleCount++;
		}
		else//桶当前页已满
		{ 
			//桶缓冲区还有剩余的空间

			if(bucketBuffer[hashKey].size - bucketBuffer[hashKey].currentSize > 0)
			{
				//Add page to buffer 
				RefreshBucket(hashKey, m_BucketPage, bucketBuffer);
				//将输入缓冲区的元组继续装配到新的当前页
				UpdateBucket(tupleIndex,pagePtr,m_BucketPage,hashKey);
				bucketBuffer[hashKey].currentPageIndex++; 
				bucketBuffer[hashKey].pageCount++; 
				bucketBuffer[hashKey].currentSize+=SSD_PAGE_SIZE;
				bucketBuffer[hashKey].freeLocation+=SSD_PAGE_SIZE;//桶的剩余空间减掉一页
			}
			else//该桶已经没有剩余的页
			{  
				WriteFile(hFile[hashKey], 
					bucketBuffer[hashKey].data, 
					bucketBuffer[hashKey].currentSize,
					&dwBytesWritten,
					NULL);

				ResetBuffer(&bucketBuffer[hashKey]);

				RefreshBucket(hashKey, m_BucketPage, bucketBuffer);

				//从输入缓冲区复制进输出缓冲区，并更新该输出缓冲区的相关状态
				UpdateBucket(tupleIndex, pagePtr, m_BucketPage, hashKey);
				bucketBuffer[hashKey].tupleCount++;
			}
		}
	}
} 


//从输入缓冲区复制进输出缓冲区，并更新该输出缓冲区的相关状态
VOID GHJ::UpdateBucket(const DWORD tupleIndex,const PagePtr pagePtr, PagePtr *bucketPage, const DWORD hashKey)
{	
	strncpy(bucketPage[hashKey].tuple, pagePtr.page+(pagePtr.pageSlot - tupleIndex)->tupleOffset, (pagePtr.pageSlot-tupleIndex)->tupleSize);
	//装配
	bucketPage[hashKey].pageSlot--;
	bucketPage[hashKey].pageSlot->tupleSize=(pagePtr.pageSlot-tupleIndex)->tupleSize;
	bucketPage[hashKey].pageSlot->tupleOffset=bucketPage[hashKey].offset;
	bucketPage[hashKey].offset+=(pagePtr.pageSlot-tupleIndex)->tupleSize;
	bucketPage[hashKey].freeSpace = bucketPage[hashKey].freeSpace - (pagePtr.pageSlot-tupleIndex)->tupleSize-sizeof(PageSlot);
	bucketPage[hashKey].tuple+=(pagePtr.pageSlot-tupleIndex)->tupleSize;
	bucketPage[hashKey].pageHeader->totalTuple++;
	bucketPage[hashKey].pageHeader->slotLocation-=sizeof(PageSlot);	
}

//将桶的新一页当作当前页，当前页的辅助信息恢复到初始状态
VOID GHJ::RefreshBucket(const DWORD hashKey, PagePtr *bucketPage, Buffer *bucketBuffer)
{
	bucketPage[hashKey].offset = 8;
	bucketPage[hashKey].freeSpace = SSD_PAGE_SIZE-8;
	bucketPage[hashKey].page = bucketBuffer[hashKey].data + SSD_PAGE_SIZE * bucketBuffer[hashKey].currentPageIndex;
	bucketPage[hashKey].pageHeader=(PageHeader *)bucketPage[hashKey].page;
	bucketPage[hashKey].tuple=bucketPage[hashKey].page+sizeof(PageHeader);
	bucketPage[hashKey].pageSlot=(PageSlot *)(bucketPage[hashKey].page + SSD_PAGE_SIZE);  
	bucketPage[hashKey].pageHeader->totalTuple = 0;
	bucketPage[hashKey].pageHeader->slotLocation = SSD_PAGE_SIZE;
}

VOID GHJ::HashBuild(Buffer *inBuffer, HashTable *hashTable)
{
	PagePtr pagePtr;//每页为单位进行处理
	HashTuple *hashTuple;
	DWORD tupleCount;
	DWORD joinKey, hashValue;
	hashTable->hashTupleCount = new INT[hashTable->hashFn];

	//初始散列表每个桶的标记为0
	for(UINT i=0; i < hashTable->hashFn; i++)
		hashTable->hashTupleCount[i]=0;

	for(UINT pageIndex=0; pageIndex < inBuffer->pageCount; pageIndex++)
	{
		pagePtr.page=inBuffer->data + pageIndex * SSD_PAGE_SIZE;
		pagePtr.pageHeader=(PageHeader*)pagePtr.page;
		pagePtr.pageSlot=(PageSlot*)(pagePtr.page+SSD_PAGE_SIZE-sizeof(PageSlot)); 
		pagePtr.tuple=pagePtr.page+sizeof(PageHeader);
		tupleCount=pagePtr.pageHeader->totalTuple;

		for(UINT tupleIndex=0; tupleIndex < tupleCount; tupleIndex++)
		{
			joinKey = GetKey(pagePtr.page+(pagePtr.pageSlot-tupleIndex)->tupleOffset, m_Params.R_KEY_POS);
			hashValue = joinKey % hashTable->hashFn;
			//散列表没发生冲突
			if(hashTable->hashTupleCount[hashValue]==0)
			{
				hashTuple=(HashTuple *)(hashTable->data + hashValue*sizeof(HashTuple));
				hashTuple->key=joinKey;
				//此处需将页内偏移转化成输入缓冲区的偏移地址
				hashTuple->offset = SSD_PAGE_SIZE*pageIndex+(pagePtr.pageSlot-tupleIndex)->tupleOffset;
				hashTuple->next = NULL;
				hashTable->hashTupleCount[hashValue]++;
			}
			//冲突发生
			else
			{
				hashTuple=(HashTuple *)(hashTable->data + hashTable->size);
				hashTuple->key=joinKey;
				hashTuple->offset=SSD_PAGE_SIZE*pageIndex+(pagePtr.pageSlot-tupleIndex)->tupleOffset;
				hashTuple->next=NULL;
				hashTable->size+=sizeof(HashTuple);
				HashTuple *p=(HashTuple *)(hashTable->data + hashValue*sizeof(HashTuple));
				while(p->next!=NULL)
					p=p->next;
				p->next=hashTuple;
			}
		}
	}
}

VOID GHJ::HashProbe(PagePtr *probePage, HashTable *hashTable, Buffer *inBuffer)
{
	probePage->pageHeader=(PageHeader*)probePage->page;
	probePage->pageSlot=(PageSlot*)(probePage->page + SSD_PAGE_SIZE-sizeof(PageSlot)); 
	probePage->tuple=probePage->page + sizeof(PageHeader);

	DWORD tupleCount=probePage->pageHeader->totalTuple;

	//对探测页中的元组按条探测散列表
	for(UINT tupleIndex=0; tupleIndex < tupleCount; tupleIndex++)
	{
		DWORD joinKey = GetKey(probePage->page+(probePage->pageSlot-tupleIndex)->tupleOffset, m_Params.S_KEY_POS);
		DWORD hashValue = joinKey % hashTable->hashFn;
		if(hashTable->hashTupleCount[hashValue]!=0)//如果散列桶被标记,搜索链表进行连接
		{
			HashTuple *p=(HashTuple *)(hashTable->data + hashValue*sizeof(HashTuple));
			while(p!=NULL)
			{
				if(p->key==joinKey)
				{ 
					//做连接操作/.......  
					m_R_Tuple->data = p->offset + inBuffer->data;
					m_R_Tuple->length = strlen(m_R_Tuple->data)+1;
					m_S_Tuple->data = probePage->page + (probePage->pageSlot-tupleIndex)->tupleOffset;
					m_S_Tuple->length = strlen(m_S_Tuple->data)+1;

					strncpy(m_JoinTuple->data, m_R_Tuple->data, m_R_Tuple->length);
					strncpy(m_JoinTuple->data + m_R_Tuple->length, m_S_Tuple->data, m_S_Tuple->length);
					m_JoinTuple->length = m_R_Tuple->length + m_S_Tuple->length;

					SentOutput(m_JoinTuple);

					m_JoinCount++;	
				}
				p=p->next;
			}
		}
	} 
} 

//获取元组连接键值
DWORD GHJ::GetKey(CHAR * tuple, DWORD keyPos) 
{
	CHAR * keyFieldStartPtr;
	DWORD returnKey;
	if(keyPos==1)//连接字段为表的第一个字段
	{
		keyFieldStartPtr=tuple;
		while(*tuple!='|')
			tuple++;
		*tuple='\0';//keyFieldEndPosition
	}
	else//连接字段为表的第二个字段
	{
		//找出第一个字段的结束标识
		while(*tuple!='|')
			tuple++;
		tuple++;
		//now it's the start position of the second position
		keyFieldStartPtr=tuple;
		//find the end position of the second field
		while(*tuple!='|')
			tuple++;
		*tuple='\0';//keyFieldEndPosition
	}
	returnKey=atoi(keyFieldStartPtr);  
	//recover the tuple to its original form
	*tuple='|';

	return returnKey;
} 

RC GHJ::SentOutput(RECORD * tuple)
{
	if(m_OutPage.freeSpace > 508) // page not full 
	{ 
		AddTupleToPage(tuple, &m_OutPage, &m_OutPageBuffer);   
	}
	else // page is full
	{  
		if(m_OutBuffer.currentSize <= m_OutBuffer.size) // output buffer full
		{  
			AddPageToBuffer(&m_OutPage, &m_OutBuffer); 

			ResetPage(&m_OutPage, &m_OutPageBuffer); 
			AddTupleToPage(tuple, &m_OutPage, &m_OutPageBuffer); 
		}
		else//该桶已经没有剩余的页
		{ 
			DWORD dwBytesToWrite = m_OutBuffer.currentSize;
			DWORD dwBytesWritten = 0;
			WriteFile(m_hOutFile, 
				m_OutBuffer.data, 
				dwBytesToWrite,
				&dwBytesWritten,
				NULL);


			ResetBuffer(&m_OutBuffer); 
			AddPageToBuffer(&m_OutPage, &m_OutBuffer);

			ResetPage(&m_OutPage, &m_OutPageBuffer);
			AddTupleToPage(tuple, &m_OutPage, &m_OutPageBuffer);  

			m_OutBuffer.tupleCount++;
		}
		m_OutBuffer.pageCount++;

	}
	return SUCCESS;
}  