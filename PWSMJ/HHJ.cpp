// 
// Name: HHJ.cpp : implementation file  
// Description: Implementation of Hybrid Hash Join 
//				The hybrid hash join algorithm is a refinement of the grace hash join 
//				which takes advantage of more available memory. During the partitioning phase, 
//				the hybrid hash join uses the available memory for two purposes:
//				    1. To hold the current output buffer page for each of the k partitions
//					2. To hold an entire partition in-memory, known as "partition 0"
//				Because partition 0 is never written to or read from disk, the hybrid hash join 
//				typically performs fewer I/O operations than the grace hash join. 
//				Note that this algorithm is memory-sensitive, because there are two competing demands
//				for memory (the hash table for partition 0, and the output buffers for the remaining partitions). 
//				Choosing too large a hash table might cause the algorithm to recurse 
//				because one of the non-zero partitions is too large to fit into memory.
//

#include "stdafx.h"
#include "HHJ.h"
 
/// <summary>
/// Initializes a new instance of the <see cref="HHJ"/> class.
/// </summary>
/// <param name="vParams">The HHJ parameters.</param>
HHJ::HHJ(const HHJ_PARAMS vParams) : m_Params(vParams)
{
	m_AvailableMemorySize = m_Params.BUFFER_POOL_SIZE / SSD_PAGE_SIZE; // Unit in page
	m_BucketSize = m_Params.BUCKET_SIZE / SSD_PAGE_SIZE;
	m_JoinCount = 0;
	utl=new PageHelpers;
}

HHJ::~HHJ()
{
	delete utl;
}

/// <summary>
/// Initializes this instance.
/// </summary>
/// <returns></returns>
RC HHJ::Initialize()
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
	DOUBLE maxBucketSize = (m_AvailableMemorySize * m_AvailableMemorySize) / (4 * m_Params.FUDGE_FACTOR * m_R_FileSize); 
	if(m_BucketSize > maxBucketSize)
	{
		m_BucketSize = maxBucketSize;  
	}

	// Calculate partition number 
	DOUBLE temp = (m_AvailableMemorySize * m_AvailableMemorySize) - (4 * m_Params.FUDGE_FACTOR * m_R_FileSize);
	anchorBucketSize = (m_AvailableMemorySize + sqrt(temp)) / (2 * m_Params.FUDGE_FACTOR);
	m_PartitionNum = m_R_FileSize/anchorBucketSize + 1; 
	m_HashTableSize = m_AvailableMemorySize - anchorBucketSize - (m_PartitionNum * m_BucketSize) - (m_Params.READ_BUFFER_SIZE/SSD_PAGE_SIZE);

	if(m_HashTableSize <= 0)
	{
		ShowMB(L"Not enough memory for hash table");
		return ERR_NOT_ENOUGH_MEMORY;
	}

	m_Pool.size = m_Params.BUFFER_POOL_SIZE;  // in bytes
	m_Pool.currentSize = 0;
	m_Pool.data = new CHAR[m_Pool.size];

	if(NULL==m_Pool.data)
	{ 
		ShowMB(L"Cannot init buffer pool");
		return ERR_CANNOT_INITIAL_BUFFER_POOL;
	}

	m_BucketBuffer = new Buffer[m_PartitionNum];

	// Init read buffer size
	if(m_Pool.currentSize < m_Pool.size) 
	{
		m_InBuffer.startLocation = m_Pool.currentSize;
		m_InBuffer.freeLocation = m_InBuffer.startLocation;
		m_InBuffer.data = m_Pool.data + m_InBuffer.startLocation;
		m_InBuffer.size = m_Params.READ_BUFFER_SIZE;
		m_InBuffer.currentSize = 0; //缓冲区空闲区间也从起始地址开始
		m_InBuffer.currentPageIndex = 0;
		m_InBuffer.currentTupleIndex = 0;
		m_InBuffer.pageCount = 0;
		m_InBuffer.tupleCount = 0; 
		m_InBuffer.isSort = FALSE;
		m_InBuffer.isFullMaxValue = FALSE;

		m_Pool.currentSize += m_InBuffer.size;
	}
	else
	{
		ShowMB(L"Not enough memory for input buffer"); 
		return ERR_NOT_ENOUGH_MEMORY;
	}

	//初始化工作区 
	m_PartitionFirstBuffer.startLocation = m_Pool.currentSize;
	m_PartitionFirstBuffer.freeLocation = m_PartitionFirstBuffer.startLocation;
	m_PartitionFirstBuffer.data = m_Pool.data + m_PartitionFirstBuffer.startLocation;
	m_PartitionFirstBuffer.size = anchorBucketSize * SSD_PAGE_SIZE;
	m_PartitionFirstBuffer.currentSize = 0;
	m_PartitionFirstBuffer.currentPageIndex = 0;
	m_PartitionFirstBuffer.currentTupleIndex = 0;
	m_PartitionFirstBuffer.pageCount = 0;
	m_PartitionFirstBuffer.tupleCount = 0;

	m_Pool.currentSize += m_PartitionFirstBuffer.size;

	//初始化第一个分区的散列表
	m_HashTable.startLocation = m_Pool.currentSize;
	m_HashTable.data = m_Pool.data + m_HashTable.startLocation;
	m_HashTable.size = m_HashTableSize * SSD_PAGE_SIZE;
	m_HashTable.hashFn = m_HashTable.size / sizeof(HashTuple);
	m_Pool.currentSize += m_HashTable.size;

	m_HashTable.hashTupleCount = new INT[m_HashTable.hashFn]; 

	for(UINT i=0; i < m_HashTable.hashFn; i++)
		m_HashTable.hashTupleCount[i] = 0;


	//初始化桶的输出缓存  注意第一个不用再分配空间
	for(UINT partitionIndex=1; partitionIndex < m_PartitionNum; partitionIndex++)
	{
		if(m_Pool.currentSize < m_Pool.size)
		{
			m_BucketBuffer[partitionIndex].startLocation = m_Pool.currentSize;//每个桶缓冲区起始位置为缓冲池当前空闲区间的起始位置
			m_BucketBuffer[partitionIndex].freeLocation = m_BucketBuffer[partitionIndex].startLocation;
			m_BucketBuffer[partitionIndex].data = m_Pool.data + m_BucketBuffer[partitionIndex].startLocation;
			m_BucketBuffer[partitionIndex].size = m_BucketSize * SSD_PAGE_SIZE;	 	//BUCKET_SIZE
			m_BucketBuffer[partitionIndex].currentSize = 0;
			m_BucketBuffer[partitionIndex].currentPageIndex = 0;
			m_BucketBuffer[partitionIndex].currentTupleIndex = 0; 
			m_BucketBuffer[partitionIndex].pageCount = 0; 
			m_BucketBuffer[partitionIndex].tupleCount = 0; 
			m_Pool.currentSize += m_BucketBuffer[partitionIndex].size;
		}
		else
		{
			ShowMB(L"Bufpool size is too small for all the buckets!"); 
			return ERR_NOT_ENOUGH_MEMORY;
		}
	}

	//对桶装配过程中需要的全局变量初始化
	m_BucketPage= new PagePtr[m_PartitionNum];	//每个桶当前页 
	for(UINT partitionIndex=1; partitionIndex < m_PartitionNum; partitionIndex++)
	{    
		m_BucketPage[partitionIndex].page = m_BucketBuffer[partitionIndex].data + m_BucketBuffer[partitionIndex].currentPageIndex * SSD_PAGE_SIZE;//当前页的起始地址
		m_BucketPage[partitionIndex].pageHeader = (PageHeader *) m_BucketPage[partitionIndex].page;

		//初始化当前页包含的数据结构
		m_BucketPage[partitionIndex].tuple=m_BucketPage[partitionIndex].page + sizeof(PageHeader);
		m_BucketPage[partitionIndex].pageSlot=(PageSlot *)(m_BucketPage[partitionIndex].page + SSD_PAGE_SIZE);  
		m_BucketPage[partitionIndex].pageHeader->totalTuple = 0;
		m_BucketPage[partitionIndex].pageHeader->slotLocation = SSD_PAGE_SIZE;

		m_BucketPage[partitionIndex].offset = 8;
		m_BucketPage[partitionIndex].freeSpace = SSD_PAGE_SIZE-8; //桶当前页剩余空间 
		m_BucketPage[partitionIndex].consumed = FALSE;
	}

	m_FileHandle = new HANDLE[m_PartitionNum]; 

	R_PartitionTable(&m_InBuffer, m_BucketBuffer, m_FileHandle);

	//再次对桶装配过程中需要的全局变量初始化 
	for(UINT partitionIndex=1; partitionIndex < m_PartitionNum; partitionIndex++)
	{   
		m_BucketPage[partitionIndex].offset = 8;
		m_BucketPage[partitionIndex].freeSpace = SSD_PAGE_SIZE-8; //桶当前页剩余空间 
		m_BucketBuffer[partitionIndex].currentPageIndex = 0;//桶当前页的页号
		m_BucketPage[partitionIndex].page=m_BucketBuffer[partitionIndex].data + (m_BucketBuffer[partitionIndex].currentPageIndex * SSD_PAGE_SIZE);//当前页的起始地址
		m_BucketPage[partitionIndex].pageHeader=(PageHeader *) m_BucketPage[partitionIndex].page;
		//初始化当前页包含的数据结构
		m_BucketPage[partitionIndex].tuple=m_BucketPage[partitionIndex].page + sizeof(PageHeader);
		m_BucketPage[partitionIndex].pageSlot=(PageSlot *)(m_BucketPage[partitionIndex].page + SSD_PAGE_SIZE);  
		m_BucketPage[partitionIndex].pageHeader->totalTuple = 0;
		m_BucketPage[partitionIndex].pageHeader->slotLocation = SSD_PAGE_SIZE;
	}

	S_PartitionTable(&m_InBuffer, m_BucketBuffer, m_FileHandle);

	Buffer probebuffer;//大表输入缓冲区
	//调用连接模块完成连接
	Join(&m_Pool,&m_InBuffer, &probebuffer);

	return SUCCESS;
}

//连接阶段
RC HHJ::Join(BufferPool *bufpool, Buffer *inBuffer,Buffer *probeBuffer)
{
	//缓冲池重新初始化
	m_JoinCount=0;
	bufpool->currentSize = 0;
	//FILE *fr,*fs;
	HashTable hashTable;
	PageHeader *pageHeader;//用于操作读进来的页以获得页头等信息

	LPWSTR bucketPath_R = new TCHAR[MAX_PATH]; //桶文件名
	LPWSTR bucketPath_S = new TCHAR[MAX_PATH];
	LARGE_INTEGER liFileSize;

	for(UINT partitionIndex=1; partitionIndex < m_PartitionNum; partitionIndex++)
	{    
		DWORD dwBytesRead_R=1;
		DWORD dwBytesRead_S=1; 
		DWORD totalTuple=0;

		inBuffer->startLocation = bufpool->currentSize;
		inBuffer->data=bufpool->data + inBuffer->startLocation;
		inBuffer->size=0;//////////////////////
		inBuffer->freeLocation=inBuffer->startLocation;	
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

		//分区文件读入输入缓冲区
		while(dwBytesRead_R)
		{   
			ReadFile(hBucketR,
				(LPVOID)(inBuffer->data+inBuffer->freeLocation - inBuffer->startLocation),
				liFileSize.QuadPart,
				&dwBytesRead_R, 
				NULL);

			inBuffer->size+=dwBytesRead_R;
			inBuffer->freeLocation+=dwBytesRead_R;
			inBuffer->currentSize+=dwBytesRead_R;
			UINT pageCount = dwBytesRead_R / SSD_PAGE_SIZE;
			inBuffer->pageCount += pageCount;

			bufpool->currentSize+=dwBytesRead_R;

			for(UINT pageIndex=0;pageIndex < pageCount; pageIndex++)
			{
				pageHeader=(PageHeader*)(inBuffer->data + SSD_PAGE_SIZE*pageIndex);
				totalTuple+=pageHeader->totalTuple;
			}
		}

		//初始化散列表信息
		hashTable.startLocation = bufpool->currentSize;
		hashTable.data = bufpool->data + hashTable.startLocation;
		if(totalTuple % 2==0)
			hashTable.hashFn=totalTuple+1;
		else
			hashTable.hashFn=totalTuple;

		hashTable.size = hashTable.hashFn * sizeof(HashTuple);

		//构建散列表
		HashBuild(inBuffer, &hashTable);
		bufpool->currentSize+=hashTable.size;

		//cout<<"小表第"<<i<<"个文件散列表已建立完毕，共"<<totaltuple<<"条记录，生成散列"<<hashtable.size/sizeof(HashTuple)<<"条."<<endl;

		//为大表建立读取缓存
		probeBuffer->startLocation = bufpool->currentSize;
		probeBuffer->data = bufpool->data + probeBuffer->startLocation;
		DWORD probeBufferSize=(bufpool->size - inBuffer->size - hashTable.size)/SSD_PAGE_SIZE;
		//cout<<"探测缓存大小为："<<bufsize<<"页"<<endl;
		probeBuffer->size=probeBufferSize * SSD_PAGE_SIZE ; 		//probebuffer->bufsize = PROBE_SIZE_EVERY_TIME*4096;
		probeBuffer->freeLocation = probeBuffer->startLocation;

		if((inBuffer->size + hashTable.size + probeBuffer->size) > bufpool->size)
		{ 
			ShowMB(L"Exceeds available memory limit");
			return ERR_NOT_ENOUGH_MEMORY;
		}

		PagePtr probePage;
		while(dwBytesRead_S)
		{	
			ReadFile(hBucketS,
				(LPVOID)(probeBuffer->data + probeBuffer->freeLocation - probeBuffer->startLocation), 
				probeBuffer->size, 
				&dwBytesRead_S,
				NULL);

			for(UINT pageIndex=0; pageIndex < dwBytesRead_S/SSD_PAGE_SIZE; pageIndex++)
			{
				probePage.page=probeBuffer->data + pageIndex*SSD_PAGE_SIZE;
				HashProbe(&probePage, &hashTable, inBuffer);
			}
		}

		CloseHandle(hBucketR);
		CloseHandle(hBucketS);
		//每2个分区对应连接做完之后 缓冲区需要再次清空 再进行下一轮对应分区的连接
		bufpool->currentSize=0;
	}

	return SUCCESS;
}

RC HHJ::Execute()
{
	RC rc = Initialize();
	if(rc!=SUCCESS)
	{
		return rc;
	}

	return SUCCESS;
}

//读取文件进行分区
RC  HHJ::R_PartitionTable(Buffer *inBuffer, Buffer *bucketBuffer, HANDLE *&hFile)
{    	
	DWORD dwBytesRead=1;//判断是否读到文件尾
	DWORD dwBytesWritten=1;//用于写

	HANDLE hR=CreateFile(
		m_Params.RELATION_R_PATH,
		GENERIC_READ,
		0,
		NULL,
		OPEN_EXISTING,
		FILE_FLAG_NO_BUFFERING,
		NULL);

	if(hR==INVALID_HANDLE_VALUE)
	{
		ShowMB(L"Cannot create handle file %s", m_Params.RELATION_R_PATH);
		return ERR_CANNOT_CREATE_HANDLE; 
	}

	LPWSTR tempFilePath = new TCHAR[MAX_PATH]; 

	for(UINT partitionIndex=1; partitionIndex < m_PartitionNum; partitionIndex++)
	{
		swprintf_s(tempFilePath, MAX_PATH, L"%s%d%s.tmp", m_Params.WORK_SPACE_PATH, partitionIndex, m_Params.RELATION_R_NO_EXT); //生成对应分区的文件名
		int a=0;
		hFile[partitionIndex]=CreateFile(
			tempFilePath,
			GENERIC_WRITE|GENERIC_READ,
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

	delete tempFilePath;

	//初始化第一个分区的当前页
	m_PartitionFirstPage.page = m_PartitionFirstBuffer.data + m_PartitionFirstBuffer.currentPageIndex * SSD_PAGE_SIZE;
	m_PartitionFirstPage.pageHeader=(PageHeader *)m_PartitionFirstPage.page;
	m_PartitionFirstPage.pageSlot=(PageSlot*)(m_PartitionFirstPage.page + SSD_PAGE_SIZE);
	m_PartitionFirstPage.tuple=m_PartitionFirstPage.page + sizeof(PageHeader);
	m_PartitionFirstPage.pageHeader->slotLocation = SSD_PAGE_SIZE;
	m_PartitionFirstPage.pageHeader->totalTuple = 0;
	m_PartitionFirstPage.freeSpace = SSD_PAGE_SIZE-8;
	m_PartitionFirstPage.offset = 8;

	while(dwBytesRead) //当文件指针未指向文件尾
	{
		ReadFile(hR,
			(LPVOID)(inBuffer->data + inBuffer->freeLocation - inBuffer->startLocation),
			inBuffer->size,
			&dwBytesRead,
			NULL) ;//读入输入缓冲区的大小

		inBuffer->pageCount = dwBytesRead / SSD_PAGE_SIZE; //输入缓冲区中已存放的页数
		inBuffer->freeLocation+=dwBytesRead;//空闲剩余区间减少相应页面数
		inBuffer->currentSize+=dwBytesRead;

		for(UINT pageIndex=0;pageIndex < inBuffer->pageCount; pageIndex++)	//以页为单位处理输入缓冲区
		{
			R_ProcessPage(pageIndex, inBuffer, bucketBuffer, hFile);
		}

		//清空输入缓冲区
		inBuffer->freeLocation = inBuffer->startLocation;
		inBuffer->currentSize = 0;
		inBuffer->pageCount = 0;
		inBuffer->tupleCount = 0; 
	}	

	//对每个桶剩余的页面写回分区对应的临时文件  
	for(UINT partitionIndex=1; partitionIndex<m_PartitionNum; partitionIndex++)
	{ 
		WriteFile(hFile[partitionIndex],
			(LPVOID)bucketBuffer[partitionIndex].data, 
			(bucketBuffer[partitionIndex].currentPageIndex+1) * SSD_PAGE_SIZE,
			&dwBytesWritten,
			NULL);

		//置零输出缓存
		bucketBuffer[partitionIndex].freeLocation=bucketBuffer[partitionIndex].startLocation;
	}

	for(UINT partitionIndex=1; partitionIndex < m_PartitionNum; partitionIndex++)
		CloseHandle(hFile[partitionIndex]);

	CloseHandle(hR);
	return SUCCESS;
}	


//对输入缓存的当前页进行分区操作
VOID HHJ::R_ProcessPage(INT pageIndex, Buffer *inBuffer, Buffer *bucketBuffer, HANDLE *&hFile)
{
	PagePtr	pagePtr;//输入缓冲区的当前页
	DWORD tupleCount=0;//输入缓冲区当前页中元组的数量
	DWORD tupleKey; //元组的连接键值
	DWORD hashKey;//计算出相应的桶序号
	DWORD joinHashKey;

	DWORD dwBytesRead=0;//
	DWORD dwBytesWritten;//

	HashTuple *hashTuple;
	pagePtr.page = inBuffer->data + SSD_PAGE_SIZE*pageIndex;
	pagePtr.pageSlot = (PageSlot*)(pagePtr.page + SSD_PAGE_SIZE - sizeof(PageSlot));  
	pagePtr.pageHeader = (PageHeader *)pagePtr.page;
	pagePtr.tuple = pagePtr.page + sizeof(PageHeader);

	tupleCount = pagePtr.pageHeader->totalTuple;

	//依次读取输入缓冲的page中的每条记录

	for(UINT tupleIndex=0; tupleIndex < tupleCount; tupleIndex++)
	{
		tupleKey = GetKey(pagePtr.page+(pagePtr.pageSlot-tupleIndex)->tupleOffset, m_Params.R_KEY_POS);//从第一个槽获得第一条元组的信息并计算出键值

		hashKey = tupleKey % m_PartitionNum;
		//如果不属于第一个分区
		if(hashKey!=0)
		{
			if(m_BucketPage[hashKey].freeSpace > 258)	//桶的当前页未满
			{
				UpdateBucket(tupleIndex, pagePtr, m_BucketPage, hashKey);
				bucketBuffer[hashKey].tupleCount++;
			}
			else//桶当前页已满
			{
				bucketBuffer[hashKey].freeLocation+=SSD_PAGE_SIZE;//桶的剩余空间减掉一页
				bucketBuffer[hashKey].currentPageIndex++; 
				bucketBuffer[hashKey].pageCount++; 
				bucketBuffer[hashKey].currentSize+=SSD_PAGE_SIZE;
				//桶缓冲区还有剩余的空间
				if((bucketBuffer[hashKey].startLocation + bucketBuffer[hashKey].size - bucketBuffer[hashKey].freeLocation)>=SSD_PAGE_SIZE)
				{
					RefreshBucket(hashKey, m_BucketPage, bucketBuffer);
					//将输入缓冲区的元组继续装配到新的当前页
					UpdateBucket(tupleIndex,pagePtr,m_BucketPage,hashKey);
				}
				else//该桶已经没有剩余的页
				{ 
					DWORD dwBytesToWrite = bucketBuffer[hashKey].currentPageIndex * SSD_PAGE_SIZE;
					  
					WriteFile(hFile[hashKey], 
						bucketBuffer[hashKey].data, 
						dwBytesToWrite,
						&dwBytesWritten,
						NULL);

					//utl->DebugToFile(bucketBuffer[hashKey].data, 1, dwBytesToWrite);

					//清空该桶
					bucketBuffer[hashKey].freeLocation = bucketBuffer[hashKey].startLocation;
					bucketBuffer[hashKey].currentPageIndex = 0; 
					bucketBuffer[hashKey].pageCount = 0; 
					bucketBuffer[hashKey].currentSize = 0;
					bucketBuffer[hashKey].tupleCount = 0; 

					RefreshBucket(hashKey, m_BucketPage, bucketBuffer);

					//从输入缓冲区复制进输出缓冲区，并更新该输出缓冲区的相关状态
					UpdateBucket(tupleIndex, pagePtr, m_BucketPage, hashKey);
					bucketBuffer[hashKey].tupleCount++;
				}
			}
		}
		//hashkey为0表示为第一个分区
		else
		{
			//将第一个分区的元组放在内存
			//当前页空间能容纳一条元组进行装配
			if(m_PartitionFirstPage.freeSpace > 258)
			{
				strncpy(m_PartitionFirstPage.tuple, pagePtr.page+(pagePtr.pageSlot-tupleIndex)->tupleOffset,(pagePtr.pageSlot-tupleIndex)->tupleSize);
				m_PartitionFirstBuffer.tupleCount++;
				//装配
				m_PartitionFirstPage.pageSlot--;
				m_PartitionFirstPage.pageSlot->tupleSize=(pagePtr.pageSlot-tupleIndex)->tupleSize;
				m_PartitionFirstPage.pageSlot->tupleOffset = m_PartitionFirstPage.offset;
				m_PartitionFirstPage.offset+=(pagePtr.pageSlot-tupleIndex)->tupleSize;
				m_PartitionFirstPage.freeSpace = m_PartitionFirstPage.freeSpace - (pagePtr.pageSlot-tupleIndex)->tupleSize - sizeof(PageSlot);
				m_PartitionFirstPage.tuple+=(pagePtr.pageSlot-tupleIndex)->tupleSize;
				m_PartitionFirstPage.pageHeader->totalTuple++;
				m_PartitionFirstPage.pageHeader->slotLocation-=sizeof(PageSlot);	 
			}
			//当前页空间不足
			else
			{
				//当前页页号加1
				m_PartitionFirstBuffer.currentPageIndex++;
				m_PartitionFirstBuffer.pageCount++;
				m_PartitionFirstBuffer.currentSize+=SSD_PAGE_SIZE;

				//当前页辅助变量恢复初始状态
				m_PartitionFirstPage.freeSpace = SSD_PAGE_SIZE-8;
				m_PartitionFirstPage.offset = 8;
				//更新当前页状态
				m_PartitionFirstPage.page=m_PartitionFirstBuffer.data + m_PartitionFirstBuffer.currentPageIndex*SSD_PAGE_SIZE;
				m_PartitionFirstPage.pageHeader=(PageHeader *)m_PartitionFirstPage.page;
				m_PartitionFirstPage.pageSlot=(PageSlot*)(m_PartitionFirstPage.page + SSD_PAGE_SIZE);
				m_PartitionFirstPage.tuple=m_PartitionFirstPage.page+sizeof(PageHeader);
				m_PartitionFirstPage.pageHeader->slotLocation = SSD_PAGE_SIZE;
				m_PartitionFirstPage.pageHeader->totalTuple=0;
				//接下来继续完成装配
				//////////////////////////////////////////////////////////////////////////
				strncpy(m_PartitionFirstPage.tuple, pagePtr.page+(pagePtr.pageSlot-tupleIndex)->tupleOffset,(pagePtr.pageSlot-tupleIndex)->tupleSize);
				m_PartitionFirstBuffer.tupleCount++;
				//装配
				m_PartitionFirstPage.pageSlot--;
				m_PartitionFirstPage.pageSlot->tupleSize = (pagePtr.pageSlot-tupleIndex)->tupleSize;
				m_PartitionFirstPage.pageSlot->tupleOffset = m_PartitionFirstPage.offset;
				m_PartitionFirstPage.offset +=(pagePtr.pageSlot-tupleIndex)->tupleSize;
				m_PartitionFirstPage.freeSpace = m_PartitionFirstPage.freeSpace - (pagePtr.pageSlot-tupleIndex)->tupleSize-sizeof(PageSlot);
				m_PartitionFirstPage.tuple+=(pagePtr.pageSlot-tupleIndex)->tupleSize;
				m_PartitionFirstPage.pageHeader->totalTuple++;
				m_PartitionFirstPage.pageHeader->slotLocation-=sizeof(PageSlot);	

			}
			//同时还需要构建第一个分区的散列表
			joinHashKey = tupleKey % m_HashTable.hashFn;
			//散列表该没发生冲突,直接插入
			if(m_HashTable.hashTupleCount[joinHashKey]==0)
			{
				hashTuple=(HashTuple *)(m_HashTable.data + joinHashKey * sizeof(HashTuple));
				hashTuple->key = tupleKey;

				//此处需将页内偏移转化成输入缓冲区的偏移地址 
				hashTuple->offset = m_PartitionFirstBuffer.currentPageIndex * SSD_PAGE_SIZE + m_PartitionFirstPage.pageSlot->tupleOffset;
				hashTuple->next=NULL;
				m_HashTable.hashTupleCount[joinHashKey]++;
			}
			else //冲突发生
			{
				hashTuple = new HashTuple;//TODO
				hashTuple->key = tupleKey;
				hashTuple->offset = m_PartitionFirstBuffer.currentPageIndex*SSD_PAGE_SIZE + m_PartitionFirstPage.pageSlot->tupleOffset;
				hashTuple->next=NULL;

				HashTuple *p=(HashTuple *)(m_HashTable.data + joinHashKey*sizeof(HashTuple));
				while(p->next!=NULL)
					p=p->next;
				p->next=hashTuple;
			}
		}
	}
}


RC HHJ::S_PartitionTable(Buffer * inBuffer, Buffer *bucketBuffer, HANDLE *&hFile)
{    
	DWORD dwBytesRead=1;//判断是否读到文件尾
	DWORD dwBytesWritten=1;//用于写

	HANDLE hS=CreateFile(
		m_Params.RELATION_S_PATH,
		GENERIC_READ,
		0,
		NULL,
		OPEN_EXISTING,
		FILE_FLAG_NO_BUFFERING,
		NULL);

	if(hS==INVALID_HANDLE_VALUE)
	{
		ShowMB(L"Cannot create handle file %s", m_Params.RELATION_S_PATH);
		return ERR_CANNOT_CREATE_HANDLE; 
	}

	LPWSTR tempFilePath = new TCHAR[MAX_PATH];

	for(UINT partitionIndex=1; partitionIndex < m_PartitionNum; partitionIndex++)
	{
		swprintf_s(tempFilePath, MAX_PATH, L"%s%d%s.tmp", m_Params.WORK_SPACE_PATH, partitionIndex, m_Params.RELATION_S_NO_EXT); //生成对应分区的文件名

		hFile[partitionIndex]=CreateFile(
			tempFilePath,
			GENERIC_WRITE|GENERIC_READ,
			0,
			NULL,
			OPEN_ALWAYS,
			FILE_ATTRIBUTE_NORMAL,
			NULL); //用于写操作

		if(hFile[partitionIndex]==INVALID_HANDLE_VALUE)
		{
			ShowMB(L"Cannot create handle file %s", tempFilePath);
			return ERR_CANNOT_CREATE_HANDLE;
		}
	}

	delete tempFilePath;

	while(dwBytesRead) //当文件指针未指向文件尾
	{
		ReadFile(hS,
			(LPVOID)(inBuffer->data + inBuffer->freeLocation - inBuffer->startLocation),
			inBuffer->size,
			&dwBytesRead,
			NULL) ;//读入输入缓冲区的大小

		//判断读入的实际大小，更新输入缓冲区内实际存放的页面数记录 
		inBuffer->freeLocation+=dwBytesRead;//空闲剩余区间减少相应页面数
		inBuffer->pageCount = dwBytesRead / SSD_PAGE_SIZE;
		inBuffer->currentSize = dwBytesRead;

		for(UINT pageIndex=0; pageIndex < inBuffer->pageCount; pageIndex++)	//以页为单位处理输入缓冲区
		{
			S_ProcessPage(pageIndex, inBuffer, bucketBuffer, hFile);
		}

		//清空输入缓冲区
		inBuffer->freeLocation=inBuffer->startLocation; 
		inBuffer->pageCount = 0;
		inBuffer->tupleCount = 0;
		inBuffer->currentPageIndex = 0;
		inBuffer->currentSize = 0;
	}	

	//对每个桶剩余的页面写回分区对应的临时文件  
	for(UINT j=1; j<m_PartitionNum; j++)
	{
		WriteFile(hFile[j],
			(LPVOID)bucketBuffer[j].data,
			(bucketBuffer[j].currentPageIndex+1) * SSD_PAGE_SIZE,
			&dwBytesWritten,
			NULL);

		//置零输出缓存
		bucketBuffer[j].freeLocation = bucketBuffer[j].startLocation;
	}

	for(UINT partitionIndex=1; partitionIndex < m_PartitionNum; partitionIndex++)
	{
		CloseHandle(hFile[partitionIndex]);
	}

	CloseHandle(hS);

	return SUCCESS;
}	


VOID HHJ::S_ProcessPage(INT pageIndex, Buffer *inBuffer, Buffer *bucketBuffer, HANDLE *&hFile)
{
	PagePtr	pagePtr;//输入缓冲区的当前页
	DWORD tupleCount=0;//输入缓冲区当前页中元组的数量
	DWORD tupleKey; //元组的连接键值
	DWORD hashKey;//计算出相应的桶序号
	DWORD joinHashKey;
	CHAR *R_TupleData;
	CHAR *S_TupleData;
	DWORD dwBytesRead=1;//
	DWORD dwBytesWritten=1;//

	//	HashTuple *hashtuple;
	pagePtr.page=inBuffer->data + SSD_PAGE_SIZE * pageIndex;
	pagePtr.pageSlot=(PageSlot*)(pagePtr.page+SSD_PAGE_SIZE - sizeof(PageSlot)); 
	pagePtr.pageHeader=(PageHeader *)pagePtr.page;
	pagePtr.tuple=pagePtr.page + sizeof(PageHeader);
	tupleCount=pagePtr.pageHeader->totalTuple;
	//依次读取输入缓冲的page中的每条记录
	for(UINT tupleIndex=0; tupleIndex<tupleCount; tupleIndex++)
	{
		tupleKey = GetKey(pagePtr.page+(pagePtr.pageSlot-tupleIndex)->tupleOffset, m_Params.S_KEY_POS);//从第一个槽获得第一条元组的信息并计算出键值
		hashKey = tupleKey % m_PartitionNum;
		//如果不属于第一个分区
		if(hashKey!=0)
		{
			if(m_BucketPage[hashKey].freeSpace > 258)	//桶的当前页未满
			{
				UpdateBucket(tupleIndex,pagePtr,m_BucketPage,hashKey);
			}
			else//桶当前页已满
			{
				bucketBuffer[hashKey].freeLocation+=SSD_PAGE_SIZE;//桶的剩余空间减掉一页
				bucketBuffer[hashKey].currentPageIndex++; 

				//桶缓冲区还有剩余的空间
				if((bucketBuffer[hashKey].startLocation+bucketBuffer[hashKey].size - bucketBuffer[hashKey].freeLocation)>=SSD_PAGE_SIZE)
				{
					RefreshBucket(hashKey, m_BucketPage, bucketBuffer);
					//将输入缓冲区的元组继续装配到新的当前页
					UpdateBucket(tupleIndex,pagePtr,m_BucketPage,hashKey);
				}
				else//该桶已经没有剩余的页
				{ 
					WriteFile(hFile[hashKey],
						(LPVOID)bucketBuffer[hashKey].data, 
						bucketBuffer[hashKey].currentPageIndex * SSD_PAGE_SIZE,
						&dwBytesWritten,
						NULL);

					//清空该桶
					bucketBuffer[hashKey].freeLocation = bucketBuffer[hashKey].startLocation;
					bucketBuffer[hashKey].currentPageIndex = 0;
					RefreshBucket(hashKey, m_BucketPage, bucketBuffer);
					//从输入缓冲区复制进输出缓冲区，并更新该输出缓冲区的相关状态
					UpdateBucket(tupleIndex,pagePtr,m_BucketPage,hashKey);
				}
			}
		}
		//hashkey为0表示第一个分区,大表的第一个分区直接和小表在内存的分区做连接做连接
		else
		{
			joinHashKey = tupleKey % m_HashTable.hashFn;
			if(m_HashTable.hashTupleCount[joinHashKey]!=0)//如果散列桶被标记,搜索链表进行连接
			{
				HashTuple *p=(HashTuple *)(m_HashTable.data + joinHashKey*sizeof(HashTuple));
				while(p!=NULL)
				{
					if(p->key==tupleKey)
					{ 
						//做连接操作/.......
						S_TupleData = pagePtr.page + (pagePtr.pageSlot-tupleIndex)->tupleOffset;
						R_TupleData = p->offset + m_PartitionFirstBuffer.data;

						m_JoinCount++;	
					}
					p=p->next;
				}
			}
		}
	} 
}

//从输入缓冲区复制进输出缓冲区，并更新该输出缓冲区的相关状态
VOID HHJ::UpdateBucket(DWORD tupleIndex, PagePtr pagePtr, PagePtr* bucketPage, DWORD hashKey)
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
VOID HHJ::RefreshBucket(DWORD hashKey, PagePtr* bucketPage, Buffer *bucketBuffer)
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

VOID HHJ::HashBuild(Buffer *inBuffer, HashTable *hashTable)
{
	PagePtr pagePtr;//每页为单位进行处理
	HashTuple *hashTuple;
	DWORD tupleCount;
	DWORD joinKey, hashValue;
	hashTable->hashTupleCount = new INT[hashTable->hashFn];

	//初始散列表每个桶的标记为0
	for(UINT i=0; i < hashTable->hashFn; i++)
		hashTable->hashTupleCount[i]=0;

	for(UINT pageIndex=0; pageIndex<inBuffer->pageCount; pageIndex++)
	{
		pagePtr.page=inBuffer->data+pageIndex*SSD_PAGE_SIZE;
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

VOID HHJ::HashProbe(PagePtr *probePage, HashTable *hashTable, Buffer *inBuffer)
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

					m_JoinCount++;	
				}
				p=p->next;
			}
		}
	} 
} 

//获取元组连接键值
DWORD HHJ::GetKey(CHAR * tuple, DWORD keyPos) 
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