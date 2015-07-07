#include "stdafx.h"
#include "WMI.h"


WMI::WMI(void)
{
	isInitWmi = TRUE; 
	isReady = FALSE;
	rc = SUCCESS;

	if(!cService.IsServiceRunning(_T("Winmgmt")))
	{
		DebugPrint(_T("Waiting... Winmgmt (Windows Management Instrumentation )"));
		isInitWmi = cService.EasyStart(_T("Winmgmt")); 
	}

	if(!isInitWmi)
	{
		DebugPrint(_T("Cannot start services"));
		rc = ERR_WMI_FAILED_TO_INITIALIZE_COM_LIBRARY;
	}
	else
	{
		hres = S_OK;

		// Set general COM security levels
		hres = CoInitializeSecurity(
			NULL, 
			-1,                          // COM authentication
			NULL,                        // Authentication services
			NULL,                        // Reserved
			RPC_C_AUTHN_LEVEL_DEFAULT,   // Default authentication 
			RPC_C_IMP_LEVEL_IMPERSONATE, // Default Impersonation  
			NULL,                        // Authentication info
			EOAC_NONE,                   // Additional capabilities 
			NULL                         // Reserved
			); 

		if (FAILED(hres))
		{ 
			DebugPrint(_T("WMI::GetProcessorBiosInfo(): Failed to initialize security"));   
			rc = ERR_WMI_FAILED_TO_INITIALIZE_SECURITY; // Program has failed.
		}
		else
		{
			isReady = TRUE;
		} 
	} 
}

WMI::~WMI(void)
{
	CoUninitialize();
}
 
/// <summary>
/// Gets the p-state table.
/// </summary>
/// <param name="table">The table.</param>
/// <returns></returns>
RC WMI::GetPstateTable(WMI_ProcessorStatus &table)
{
	RC rc; 
	rc = GetCpuPStates();
	if(rc!=SUCCESS) { return rc; }

	table = m_PStates;

	return SUCCESS;
}


/// <summary>
/// Gets the c-state table.
/// </summary>
/// <param name="table">The table.</param>
/// <returns></returns>
RC WMI::GetCstateTable(WMI_KernelIdleStates &table)
{
	RC rc; 
	rc = GetCpuCStates();
	if(rc!=SUCCESS) { return rc; }
	table = m_CStates;

	return SUCCESS;
}

/// <summary>
/// Gets the processor bios information table.
/// </summary>
/// <param name="table">The table.</param>
/// <returns></returns>
RC WMI::GetProcessorBiosInfoTable(WMI_ProcessorBiosInfo &table)
{
	RC rc;
	rc = GetProcessorBiosInfo();
	if(rc!=SUCCESS) { return rc; }
	table = m_PBI;
	return SUCCESS;
}

/// <summary>
/// Gets the processor bios information.
/// </summary>
/// <returns></returns>
RC WMI::GetProcessorBiosInfo()
{
	// http://msdn.microsoft.com/en-us/library/aa390423(v=vs.85).aspx

	if(!isInitWmi) return rc;
	if(!isReady) return rc;


	hres = S_OK; 


	// Obtain the initial locator to WMI -------------------------

	IWbemLocator *pLocator = NULL;

	hres = CoCreateInstance(
		CLSID_WbemLocator,             
		0, 
		CLSCTX_INPROC_SERVER, 
		IID_IWbemLocator, (LPVOID *) &pLocator);

	if (FAILED(hres))
	{ 
		DebugPrint(_T("WMI::GetProcessorBiosInfo(): Failed to create IWbemLocator object"));   
		return ERR_WMI_FAILED_TO_CREATE_IWBEMLOCATOR_OBJECT;   // Program has failed.
	}

	// Connect to WMI through the IWbemLocator::ConnectServer method 
	IWbemServices *pService = NULL;

	// Connect to the root\cimv2 namespace with
	// the current user and obtain pointer pSvc
	// to make IWbemServices calls.
	hres = pLocator->ConnectServer(
		_bstr_t(L"ROOT\\WMI"),   // Object path of WMI namespace
		NULL,                    // User name. NULL = current user
		NULL,                    // User password. NULL = current
		0,                       // Locale. NULL indicates current
		NULL,                    // Security flags.
		0,                       // Authority (for example, Kerberos)
		0,                       // Context object 
		&pService                // pointer to IWbemServices proxy
		);

	if (FAILED(hres))
	{ 
		DebugPrint(_T("WMI::GetProcessorBiosInfo(): Could not connect WMI namespace ROOT|WMI ")); 
		//pService->Release();
		pLocator->Release();     
		return ERR_WMI_COULD_NOT_CONNECT_WMI_NAMESPACE;  // Program has failed.
	}

	// Set security levels on the proxy -------------------------

	hres = CoSetProxyBlanket(
		pService,                    // Indicates the proxy to set
		RPC_C_AUTHN_WINNT,           // RPC_C_AUTHN_xxx
		RPC_C_AUTHZ_NONE,            // RPC_C_AUTHZ_xxx
		NULL,                        // Server principal name 
		RPC_C_AUTHN_LEVEL_CALL,      // RPC_C_AUTHN_LEVEL_xxx 
		RPC_C_IMP_LEVEL_IMPERSONATE, // RPC_C_IMP_LEVEL_xxx
		NULL,                        // client identity
		EOAC_NONE                    // proxy capabilities 
		);

	if (FAILED(hres))
	{
		//cout << "Could not set proxy blanket. Error code = 0x"  << hex << hres << endl;
		pService->Release();
		pLocator->Release();     
		return ERR_WMI_COULD_NOT_SET_PROXY_BLANKET;    // Program has failed.
	}

	// Use the IWbemServices pointer to make requests of WMI ---- 
	/************************************************************************/
	/*  Get P-States                                                        */
	/************************************************************************/
	IEnumWbemClassObject* pEnumerator = NULL;
	hres = pService->ExecQuery(
		bstr_t("WQL"), 
		bstr_t("SELECT * FROM ProcessorBiosInfo"), // ProcessorBiosInfo
		WBEM_FLAG_FORWARD_ONLY | WBEM_FLAG_RETURN_IMMEDIATELY, 
		NULL,
		&pEnumerator);

	if (FAILED(hres))
	{ 
		DebugPrint(_T("WMI::GetProcessorBiosInfo(): Query for ProcessorBiosInfo failed")); 
		pService->Release();
		pLocator->Release(); 
		return ERR_WMI_QUERY_FAILED;   // Program has failed.
	}

	/************************************************************************/  
	/* Step 7: -------------------------------------------------------------*/
	/* Get the data from the query in step 6 -------------------------------*/
	/* http://msdn.microsoft.com/en-us/library/aa394373(v=vs.85).aspx       */
	/************************************************************************/
	IWbemClassObject *pclsObj = NULL;
	ULONG uReturn = 0; 

	DWORD instanceCount = 0;
	while (pEnumerator)
	{
		HRESULT hr = pEnumerator->Next(WBEM_INFINITE, 1, &pclsObj, &uReturn);

		/* Checking */
		if(0 == uReturn)  break; 
		instanceCount++;

		VARIANT vtProp; 
		_variant_t vtPfProp; 
		hr = pclsObj->Get(L"Pss", 0, &vtPfProp, 0, 0); 
		IUnknown* pclsPerfStatesObj = vtPfProp; 
		hr = pclsPerfStatesObj->QueryInterface( IID_IWbemClassObject, reinterpret_cast< void** >( &pclsObj ) );

		/* Get PerfStates properties */ 
		hr = pclsObj->Get( L"Count", 0, &vtProp, NULL, NULL ); 
		m_PBI.Count = vtProp.intVal;
		//wcout << " Count : " <<   vtProp.intVal << endl;  
		VariantClear(&vtProp); 

		/* Get CPU P-states */
		hr = pclsObj->Get( L"State", 0, &vtProp, NULL, NULL );
		m_PBI.State = new WMI_PerformanceState[m_PBI.Count]; 
		SAFEARRAY* psaPStates = vtProp.parray; 
		LONG lower, upper;  
		SafeArrayGetLBound(psaPStates, 1, &lower);
		SafeArrayGetUBound(psaPStates, 1, &upper);
		IWbemClassObject *pclsStateObj = NULL;
		VARIANT vtState;
		for (long i = lower; i <= upper; i++) 
		{
			/* Get State object from array */
			hres = SafeArrayGetElement(psaPStates, &i, &pclsStateObj);

			pclsStateObj->Get( L"BmLatency", 0, &vtState, NULL, NULL );
			m_PBI.State[i].BmLatency = vtState.intVal; 
			VariantClear(&vtState); 

			pclsStateObj->Get( L"Latency", 0, &vtState, NULL, NULL );
			m_PBI.State[i].Latency = vtState.intVal; 
			VariantClear(&vtState); 

			pclsStateObj->Get( L"Frequency", 0, &vtState, NULL, NULL );
			m_PBI.State[i].Frequency = vtState.intVal; 
			VariantClear(&vtState); 

			pclsStateObj->Get( L"Power", 0, &vtState, NULL, NULL );
			m_PBI.State[i].Power = vtState.intVal; 
			VariantClear(&vtState); 
		}

		/* Cleaning */
		VariantClear(&vtProp); 
		VariantClear(&vtPfProp);
		SafeArrayDestroy(psaPStates); 
		pclsStateObj->Release();
		pclsObj->Release();

		/* Get information of 1 core Only */
		break;
	}

	// Cleanup
	// ========
	pEnumerator->Release(); 

	if(!pclsObj)  pclsObj->Release(); 

	pService->Release();
	pLocator->Release(); 

	if(instanceCount==0) return ERR_WMI_NOT_FOUND_ANY_INSTANCE;

	return SUCCESS;
}



/// <summary>
/// Gets the cpu c-states.
/// </summary>
/// <returns></returns>
RC WMI::GetCpuCStates()
{
	// http://msdn.microsoft.com/en-us/library/aa390423(v=vs.85).aspx

	if(!isInitWmi) return rc; 
	if(!isReady) return rc; 

	hres = S_OK;

	// Obtain the initial locator to WMI -------------------------

	IWbemLocator *pLocator = NULL;

	hres = CoCreateInstance(
		CLSID_WbemLocator,             
		0, 
		CLSCTX_INPROC_SERVER, 
		IID_IWbemLocator, (LPVOID *) &pLocator);

	if (FAILED(hres))
	{ 
		DebugPrint(_T("WMI::GetCpuCStates(): Failed to create IWbemLocator object")); 
		return ERR_WMI_FAILED_TO_CREATE_IWBEMLOCATOR_OBJECT;                 // Program has failed.
	}

	// Connect to WMI through the IWbemLocator::ConnectServer method 
	IWbemServices *pService = NULL;

	// Connect to the root\cimv2 namespace with
	// the current user and obtain pointer pSvc
	// to make IWbemServices calls.
	hres = pLocator->ConnectServer(
		_bstr_t(L"ROOT\\WMI"), // Object path of WMI namespace
		NULL,                    // User name. NULL = current user
		NULL,                    // User password. NULL = current
		0,                       // Locale. NULL indicates current
		NULL,                    // Security flags.
		0,                       // Authority (for example, Kerberos)
		0,                       // Context object 
		&pService                    // pointer to IWbemServices proxy
		);

	if (FAILED(hres))
	{ 
		DebugPrint(_T("WMI::GetCpuCStates(): Could not connect WMI namespace ROOT|WMI"));
		pLocator->Release();      
		return ERR_WMI_COULD_NOT_CONNECT_WMI_NAMESPACE;  // Program has failed.
	}

	// Set security levels on the proxy ------------------------- 
	hres = CoSetProxyBlanket(
		pService,                    // Indicates the proxy to set
		RPC_C_AUTHN_WINNT,           // RPC_C_AUTHN_xxx
		RPC_C_AUTHZ_NONE,            // RPC_C_AUTHZ_xxx
		NULL,                        // Server principal name 
		RPC_C_AUTHN_LEVEL_CALL,      // RPC_C_AUTHN_LEVEL_xxx 
		RPC_C_IMP_LEVEL_IMPERSONATE, // RPC_C_IMP_LEVEL_xxx
		NULL,                        // client identity
		EOAC_NONE                    // proxy capabilities 
		);

	if (FAILED(hres))
	{ 
		DebugPrint(_T("WMI::GetCpuCStates(): Could not set proxy blanket"));
		pService->Release();
		pLocator->Release();     
		return ERR_WMI_COULD_NOT_SET_PROXY_BLANKET;    // Program has failed.
	} 

	// Use the IWbemServices pointer to make requests of WMI ---- 

	/************************************************************************/  
	/* Step 7: -------------------------------------------------------------*/
	/* Get the data from the query in step 6 -------------------------------*/
	/* http://msdn.microsoft.com/en-us/library/aa394373(v=vs.85).aspx       */
	/************************************************************************/
	IWbemClassObject *pclsProcessorStatusObj = NULL;
	ULONG uReturn = 0; 


	/************************************************************************/
	/* Get C-States                                                         */
	/************************************************************************/ 
	IEnumWbemClassObject* pKernelIdleStatesEnumerator = NULL;
	hres = pService->ExecQuery(
		bstr_t("WQL"), 
		bstr_t("SELECT * FROM KernelIdleStates"), // ProcessorStatus 
		WBEM_FLAG_FORWARD_ONLY | WBEM_FLAG_RETURN_IMMEDIATELY, 
		NULL,
		&pKernelIdleStatesEnumerator);

	if (FAILED(hres))
	{ 
		DebugPrint(_T("WMI::GetCpuCStates(): Query for KernelIdleStates failed"));
		pService->Release();
		pLocator->Release(); 
		return ERR_WMI_QUERY_FAILED;   // Program has failed.
	}

	IWbemClassObject *pclsKernelIdleStatesObj = NULL;
	DWORD instanceCount = 0;

	while (pKernelIdleStatesEnumerator)
	{
		HRESULT hr = pKernelIdleStatesEnumerator->Next(WBEM_INFINITE, 1, &pclsKernelIdleStatesObj, &uReturn);

		/* Checking */
		if(0 == uReturn)  break; 
		instanceCount++;

		VARIANT vtProp;
		VariantClear(&vtProp);
		hr = pclsKernelIdleStatesObj->Get(L"InstanceName", 0, &vtProp, 0, 0);
		m_CStates.InstanceName =(LPWSTR) vtProp.bstrVal;
		//wcout << " InstanceName : " <<   vtProp.bstrVal << endl; 
		VariantClear(&vtProp);

		hr = pclsKernelIdleStatesObj->Get(L"Active", 0, &vtProp, 0, 0);
		m_CStates.Active = vtProp.boolVal;
		//wcout << " Active : " <<   vtProp.boolVal << endl; 
		VariantClear(&vtProp);

		hr = pclsKernelIdleStatesObj->Get(L"Count", 0, &vtProp, 0, 0);
		m_CStates.Count = vtProp.intVal;
		//wcout << " Count : " <<   vtProp.intVal << endl; 
		VariantClear(&vtProp);

		hr = pclsKernelIdleStatesObj->Get(L"Type", 0, &vtProp, 0, 0);
		m_CStates.Type = vtProp.intVal;
		//wcout << " Type : " <<   vtProp.intVal << endl; 
		VariantClear(&vtProp);

		hr = pclsKernelIdleStatesObj->Get(L"OldState", 0, &vtProp, 0, 0);
		m_CStates.OldState = vtProp.intVal;
		//wcout << " OldState : " <<   vtProp.intVal << endl; 
		VariantClear(&vtProp);

		hr = pclsKernelIdleStatesObj->Get(L"TargetState", 0, &vtProp, 0, 0);
		m_CStates.TargetState = vtProp.intVal;
		//wcout << " TargetState : " <<   vtProp.intVal << endl; 
		VariantClear(&vtProp); 

		hr = pclsKernelIdleStatesObj->Get(L"TargetProcessors", 0, &vtProp, 0, 0);
		m_CStates.TargetProcessors = vtProp.intVal;
		//wcout << " TargetProcessors : " <<   vtProp.intVal << endl; 
		VariantClear(&vtProp);

		_variant_t vtStateProp;  
		/* Get CPU C-states */
		hr = pclsKernelIdleStatesObj->Get( L"State", 0, &vtStateProp, NULL, NULL );
		m_CStates.State = new WMI_KernelIdleState[m_CStates.Count]; 
		SAFEARRAY* psaCStates = vtStateProp.parray; 
		LONG lower, upper;  
		SafeArrayGetLBound(psaCStates, 1, &lower);
		SafeArrayGetUBound(psaCStates, 1, &upper);
		IWbemClassObject *pclsCStateObj = NULL;

		VARIANT vtCState;
		for (long i = lower; i <= upper; i++) 
		{
			/* Get State object from array */
			hres = SafeArrayGetElement(psaCStates, &i, &pclsCStateObj);

			pclsCStateObj->Get( L"Context", 0, &vtCState, NULL, NULL );
			m_CStates.State[i].Context = vtCState.intVal;
			//wcout << " Context : " << vtCState.intVal << endl;
			VariantClear(&vtCState); 

			pclsCStateObj->Get( L"DemotePercent", 0, &vtCState, NULL, NULL );
			m_CStates.State[i].DemotePercent = vtCState.intVal;
			//wcout << " DemotePercent : " << vtCState.intVal << endl;
			VariantClear(&vtCState); 

			pclsCStateObj->Get( L"IdleHandler", 0, &vtCState, NULL, NULL );
			m_CStates.State[i].IdleHandler = vtCState.intVal;
			//wcout << " IdleHandler : " << vtCState.intVal << endl;
			VariantClear(&vtCState); 


			pclsCStateObj->Get( L"Latency", 0, &vtCState, NULL, NULL );
			m_CStates.State[i].Latency = vtCState.intVal;
			//wcout << " Latency : " << vtCState.intVal << endl;
			VariantClear(&vtCState); 

			pclsCStateObj->Get( L"Power", 0, &vtCState, NULL, NULL );
			m_CStates.State[i].Power = vtCState.intVal;
			//wcout << " Power : " << vtCState.intVal << endl;
			VariantClear(&vtCState); 


			pclsCStateObj->Get( L"PromotePercent", 0, &vtCState, NULL, NULL );
			m_CStates.State[i].PromotePercent = vtCState.intVal;
			//wcout << " PromotePercent : " << vtCState.intVal << endl;
			VariantClear(&vtCState); 

			pclsCStateObj->Get( L"Reserved", 0, &vtCState, NULL, NULL );
			m_CStates.State[i].Reserved = vtCState.intVal;
			//wcout << " Reserved : " << vtCState.intVal << endl;
			VariantClear(&vtCState); 

			pclsCStateObj->Get( L"Reserved1", 0, &vtCState, NULL, NULL );
			m_CStates.State[i].Reserved1 = vtCState.intVal;
			//wcout << " Reserved1 : " << vtCState.intVal << endl;
			VariantClear(&vtCState); 


			pclsCStateObj->Get( L"StateFlags", 0, &vtCState, NULL, NULL );
			m_CStates.State[i].StateFlags = vtCState.intVal;
			//wcout << " StateFlags : " << vtCState.intVal << endl;
			VariantClear(&vtCState); 

			pclsCStateObj->Get( L"StateType", 0, &vtCState, NULL, NULL );
			m_CStates.State[i].StateType = vtCState.intVal;
			//wcout << " StateType : " << vtCState.intVal << endl;
			VariantClear(&vtCState); 

			pclsCStateObj->Get( L"TimeCheck", 0, &vtCState, NULL, NULL );
			m_CStates.State[i].TimeCheck = vtCState.intVal;
			//wcout << " TimeCheck : " << vtCState.intVal << endl;
			VariantClear(&vtCState); 
		} // End for 

		SafeArrayDestroy(psaCStates); 
		pclsCStateObj->Release(); 
		pclsKernelIdleStatesObj->Release();

		/*  Other core is the same */
		break;
	} // end while C-State

	pKernelIdleStatesEnumerator->Release(); 
	if(!pclsKernelIdleStatesObj)  pclsKernelIdleStatesObj->Release();

	// Cleanup
	// ========

	pService->Release();
	pLocator->Release(); 

	if(instanceCount==0) return ERR_WMI_NOT_FOUND_ANY_INSTANCE;

	return SUCCESS;
}

/// <summary>
/// Gets the cpu p-states.
/// </summary>
/// <returns></returns>
RC WMI::GetCpuPStates()
{
	// http://msdn.microsoft.com/en-us/library/aa390423(v=vs.85).aspx

	if(!isInitWmi) return rc;  
	if(!isReady) return rc;  

	hres = S_OK;

	// Obtain the initial locator to WMI -------------------------

	IWbemLocator *pLocator = NULL;

	hres = CoCreateInstance(
		CLSID_WbemLocator,             
		0, 
		CLSCTX_INPROC_SERVER, 
		IID_IWbemLocator, (LPVOID *) &pLocator);

	if (FAILED(hres))
	{ 
		DebugPrint(_T("WMI::GetCpuPStates(): Failed to create IWbemLocator object")); 
		return ERR_WMI_FAILED_TO_CREATE_IWBEMLOCATOR_OBJECT;                 // Program has failed.
	}

	// Connect to WMI through the IWbemLocator::ConnectServer method 
	IWbemServices *pService = NULL;

	// Connect to the root\cimv2 namespace with
	// the current user and obtain pointer pSvc
	// to make IWbemServices calls.
	hres = pLocator->ConnectServer(
		_bstr_t(L"ROOT\\WMI"), // Object path of WMI namespace
		NULL,                    // User name. NULL = current user
		NULL,                    // User password. NULL = current
		0,                       // Locale. NULL indicates current
		NULL,                    // Security flags.
		0,                       // Authority (for example, Kerberos)
		0,                       // Context object 
		&pService                    // pointer to IWbemServices proxy
		);

	if (FAILED(hres))
	{ 
		DebugPrint(_T("WMI::GetCpuPStates(): Could not connect WMI namespace ROOT|WMI"));
		pLocator->Release();     
		return ERR_WMI_COULD_NOT_CONNECT_WMI_NAMESPACE;  // Program has failed.
	}

	// Set security levels on the proxy ------------------------- 
	hres = CoSetProxyBlanket(
		pService,                    // Indicates the proxy to set
		RPC_C_AUTHN_WINNT,           // RPC_C_AUTHN_xxx
		RPC_C_AUTHZ_NONE,            // RPC_C_AUTHZ_xxx
		NULL,                        // Server principal name 
		RPC_C_AUTHN_LEVEL_CALL,      // RPC_C_AUTHN_LEVEL_xxx 
		RPC_C_IMP_LEVEL_IMPERSONATE, // RPC_C_IMP_LEVEL_xxx
		NULL,                        // client identity
		EOAC_NONE                    // proxy capabilities 
		);

	if (FAILED(hres))
	{ 
		DebugPrint(_T("WMI::GetCpuPStates(): Could not set proxy blanket"));
		pService->Release();
		pLocator->Release();   
		return ERR_WMI_COULD_NOT_SET_PROXY_BLANKET;    // Program has failed.
	}

	// Use the IWbemServices pointer to make requests of WMI ---- 
	/************************************************************************/
	/*  Get P-States                                                        */
	/************************************************************************/
	IEnumWbemClassObject* pProcessorStatusEnumerator = NULL;
	hres = pService->ExecQuery(
		bstr_t("WQL"), 
		bstr_t("SELECT * FROM ProcessorStatus"), // ProcessorStatus 
		WBEM_FLAG_FORWARD_ONLY | WBEM_FLAG_RETURN_IMMEDIATELY, 
		NULL,
		&pProcessorStatusEnumerator);

	if (FAILED(hres))
	{ 
		DebugPrint(_T("WMI::GetCpuPStates(): Query for ProcessorStatus failed"));
		pService->Release();
		pLocator->Release(); 
		return ERR_WMI_QUERY_FAILED;   // Program has failed.
	}

	/************************************************************************/  
	/* Step 7: -------------------------------------------------------------*/
	/* Get the data from the query in step 6 -------------------------------*/
	/* http://msdn.microsoft.com/en-us/library/aa394373(v=vs.85).aspx       */
	/************************************************************************/
	IWbemClassObject *pclsProcessorStatusObj = NULL;
	ULONG uReturn = 0; 

	DWORD instanceCount = 0;

	while (pProcessorStatusEnumerator)
	{
		HRESULT hr = pProcessorStatusEnumerator->Next(WBEM_INFINITE, 1, &pclsProcessorStatusObj, &uReturn);

		/* Checking */
		if(0 == uReturn)  break; 
		instanceCount++;

		VARIANT vtProp;

		hr = pclsProcessorStatusObj->Get(L"Active", 0, &vtProp, 0, 0);
		m_PStates.Active = vtProp.boolVal; 
		VariantClear(&vtProp);

		hr = pclsProcessorStatusObj->Get(L"InstanceName", 0, &vtProp, 0, 0);
		m_PStates.InstanceName = (LPWSTR)vtProp.bstrVal; 
		VariantClear(&vtProp);

		hr = pclsProcessorStatusObj->Get(L"CurrentPerfState", 0, &vtProp, 0, 0);
		m_PStates.CurrentPerfState = vtProp.intVal; 
		VariantClear(&vtProp);

		hr = pclsProcessorStatusObj->Get(L"LastRequestedThrottle", 0, &vtProp, 0, 0);
		m_PStates.LastRequestedThrottle = vtProp.intVal;  
		VariantClear(&vtProp);

		hr = pclsProcessorStatusObj->Get(L"LastTransitionResult", 0, &vtProp, 0, 0);
		m_PStates.LastTransitionResult = vtProp.intVal; 
		VariantClear(&vtProp);

		hr = pclsProcessorStatusObj->Get(L"LowestPerfState", 0, &vtProp, 0, 0);
		m_PStates.LowestPerfState = vtProp.intVal; 
		VariantClear(&vtProp);

		hr = pclsProcessorStatusObj->Get(L"ThrottleValue", 0, &vtProp, 0, 0);
		m_PStates.ThrottleValue = vtProp.intVal; 
		VariantClear(&vtProp);

		hr = pclsProcessorStatusObj->Get(L"UsingLegacyInterface", 0, &vtProp, 0, 0);
		m_PStates.UsingLegacyInterface = vtProp.intVal; 
		VariantClear(&vtProp); 

		_variant_t vtPfProp; 
		hr = pclsProcessorStatusObj->Get(L"PerfStates", 0, &vtPfProp, 0, 0); 
		IUnknown* pclsPerfStatesObj = vtPfProp; 
		hr = pclsPerfStatesObj->QueryInterface( IID_IWbemClassObject, reinterpret_cast< void** >( &pclsProcessorStatusObj ) );

		/* Get PerfStates properties */ 
		hr = pclsProcessorStatusObj->Get( L"Count", 0, &vtProp, NULL, NULL ); 
		m_PStates.PerfStates.Count = vtProp.intVal;  
		VariantClear(&vtProp);

		hr = pclsProcessorStatusObj->Get( L"Current", 0, &vtProp, NULL, NULL );
		m_PStates.PerfStates.Current = vtProp.intVal; 
		VariantClear(&vtProp);

		/* Get CPU P-states */
		hr = pclsProcessorStatusObj->Get( L"State", 0, &vtProp, NULL, NULL );
		m_PStates.PerfStates.State = new WMI_PerformanceState[m_PStates.PerfStates.Count]; 
		SAFEARRAY* psaPStates = vtProp.parray; 
		LONG lower, upper;  
		SafeArrayGetLBound(psaPStates, 1, &lower);
		SafeArrayGetUBound(psaPStates, 1, &upper);
		IWbemClassObject *pclsStateObj = NULL;
		VARIANT vtState;
		for (long i = lower; i <= upper; i++) 
		{
			/* Get State object from array */
			hres = SafeArrayGetElement(psaPStates, &i, &pclsStateObj);

			pclsStateObj->Get( L"Flags", 0, &vtState, NULL, NULL );
			m_PStates.PerfStates.State[i].Flags = vtState.intVal; 
			VariantClear(&vtState); 

			pclsStateObj->Get( L"Frequency", 0, &vtState, NULL, NULL );
			m_PStates.PerfStates.State[i].Frequency = vtState.intVal; 
			VariantClear(&vtState); 

			pclsStateObj->Get( L"PercentFrequency", 0, &vtState, NULL, NULL );
			m_PStates.PerfStates.State[i].PercentFrequency = vtState.intVal; 
			VariantClear(&vtState); 
		}

		/* Cleaning */
		VariantClear(&vtProp); 
		VariantClear(&vtPfProp);
		SafeArrayDestroy(psaPStates); 

		pclsStateObj->Release();
		pclsProcessorStatusObj->Release();

		/* Get information of 1 core Only */
		break;
	}

	// Cleanup
	// ========
	pProcessorStatusEnumerator->Release(); 
	if(!pclsProcessorStatusObj)  pclsProcessorStatusObj->Release(); 

	pService->Release();
	pLocator->Release(); 

	if(instanceCount==0) return ERR_WMI_NOT_FOUND_ANY_INSTANCE;

	return SUCCESS;
}

/// <summary>
/// Gets the cpu infos.
/// </summary>
/// <returns></returns>
RC WMI::GetCpuInfos()
{
	// http://msdn.microsoft.com/en-us/library/aa390423(v=vs.85).aspx

	if(!isInitWmi)  return rc;   
	if(!isReady)  return rc;   

	// Obtain the initial locator to WMI ------------------------- 
	IWbemLocator *pLoc = NULL;

	hres = CoCreateInstance(
		CLSID_WbemLocator,             
		0, 
		CLSCTX_INPROC_SERVER, 
		IID_IWbemLocator, (LPVOID *) &pLoc);

	if (FAILED(hres))
	{
		//cout << "Failed to create IWbemLocator object." << " Err code = 0x" << hex << hres << endl; 
		return ERR_WMI_FAILED_TO_CREATE_IWBEMLOCATOR_OBJECT;                 // Program has failed.
	}

	// Connect to WMI through the IWbemLocator::ConnectServer method 
	IWbemServices *pSvc = NULL;

	// Connect to the root\cimv2 namespace with
	// the current user and obtain pointer pSvc
	// to make IWbemServices calls.
	hres = pLoc->ConnectServer(
		_bstr_t(L"ROOT\\CIMV2"), // Object path of WMI namespace
		NULL,                    // User name. NULL = current user
		NULL,                    // User password. NULL = current
		0,                       // Locale. NULL indicates current
		NULL,                    // Security flags.
		0,                       // Authority (for example, Kerberos)
		0,                       // Context object 
		&pSvc                    // pointer to IWbemServices proxy
		);

	if (FAILED(hres))
	{
		//cout << "Could not connect. Error code = 0x"  << hex << hres << endl;
		pLoc->Release();  
		return ERR_WMI_COULD_NOT_CONNECT_WMI_NAMESPACE;                // Program has failed.
	}

	// Set security levels on the proxy -------------------------

	hres = CoSetProxyBlanket(
		pSvc,                        // Indicates the proxy to set
		RPC_C_AUTHN_WINNT,           // RPC_C_AUTHN_xxx
		RPC_C_AUTHZ_NONE,            // RPC_C_AUTHZ_xxx
		NULL,                        // Server principal name 
		RPC_C_AUTHN_LEVEL_CALL,      // RPC_C_AUTHN_LEVEL_xxx 
		RPC_C_IMP_LEVEL_IMPERSONATE, // RPC_C_IMP_LEVEL_xxx
		NULL,                        // client identity
		EOAC_NONE                    // proxy capabilities 
		);

	if (FAILED(hres))
	{
		//cout << "Could not set proxy blanket. Error code = 0x"  << hex << hres << endl;
		pSvc->Release();
		pLoc->Release();     
		return ERR_WMI_COULD_NOT_SET_PROXY_BLANKET;               // Program has failed.
	}


	// Use the IWbemServices pointer to make requests of WMI ----  
	IEnumWbemClassObject* pEnumerator = NULL;
	hres = pSvc->ExecQuery(
		bstr_t("WQL"), 
		bstr_t("SELECT * FROM Win32_Processor"), // Win32_OperatingSystem
		WBEM_FLAG_FORWARD_ONLY | WBEM_FLAG_RETURN_IMMEDIATELY, 
		NULL,
		&pEnumerator);

	if (FAILED(hres))
	{
		//cout << "Query for operating system name failed." << " Error code = 0x"  << hex << hres << endl;
		pSvc->Release();
		pLoc->Release(); 
		return ERR_WMI_QUERY_FAILED;               // Program has failed.
	}

	// http://msdn.microsoft.com/en-us/library/aa394373(v=vs.85).aspx
	IWbemClassObject *pclsObj = NULL;
	ULONG uReturn = 0;
	DWORD instanceCount = 0;

	while (pEnumerator)
	{
		HRESULT hr = pEnumerator->Next(WBEM_INFINITE, 1, &pclsObj, &uReturn);

		if(0 == uReturn)  break; 
		instanceCount++;

		VARIANT vtProp;

		hr = pclsObj->Get(L"ProcessorId", 0, &vtProp, 0, 0); 
		VariantClear(&vtProp);

		hr = pclsObj->Get(L"Stepping", 0, &vtProp, 0, 0); 
		VariantClear(&vtProp);

		hr = pclsObj->Get(L"Name", 0, &vtProp, 0, 0); 
		VariantClear(&vtProp);

		hr = pclsObj->Get(L"Description", 0, &vtProp, 0, 0); 
		VariantClear(&vtProp);

		hr = pclsObj->Get(L"NumberOfCores", 0, &vtProp, 0, 0); 
		VariantClear(&vtProp);

		hr = pclsObj->Get(L"NumberOfLogicalProcessors", 0, &vtProp, 0, 0); 
		VariantClear(&vtProp);

		hr = pclsObj->Get(L"L2CacheSize", 0, &vtProp, 0, 0); 
		VariantClear(&vtProp);

		hr = pclsObj->Get(L"L2CacheSpeed", 0, &vtProp, 0, 0); 
		VariantClear(&vtProp);

		hr = pclsObj->Get(L"L3CacheSize", 0, &vtProp, 0, 0); 
		VariantClear(&vtProp);

		hr = pclsObj->Get(L"VoltageCaps", 0, &vtProp, 0, 0);   
		VariantClear(&vtProp);

		hr = pclsObj->Get(L"CurrentVoltage", 0, &vtProp, 0, 0); //Voltage of the processor. If the eighth bit is set, bits 0-6 contain the voltage multiplied by 10. If the eighth bit is not set, then the bit setting in VoltageCaps represents the voltage value. CurrentVoltage is only set when SMBIOS designates a voltage value.
		VariantClear(&vtProp);    

		hr = pclsObj->Get(L"ExtClock", 0, &vtProp, 0, 0); 
		VariantClear(&vtProp);

		hr = pclsObj->Get(L"CurrentClockSpeed", 0, &vtProp, 0, 0); 
		VariantClear(&vtProp);

		hr = pclsObj->Get(L"MaxClockSpeed", 0, &vtProp, 0, 0); 
		VariantClear(&vtProp); 

		pclsObj->Release();
	}

	// Cleanup
	// ========

	pSvc->Release();
	pLoc->Release();
	pEnumerator->Release();

	if(!pclsObj)  pclsObj->Release(); 

	if(instanceCount==0) return ERR_WMI_NOT_FOUND_ANY_INSTANCE;

	return SUCCESS;
} 

/// <summary>
/// Gets the disk information.
/// </summary>
/// <returns></returns>
RC WMI::GetDiskInfo()
{
	// http://msdn.microsoft.com/en-us/library/aa394132(v=vs.85).aspx

	if(!isInitWmi)  return rc; 
	if(!isReady)  return rc;

	// Obtain the initial locator to WMI ------------------------- 
	IWbemLocator *pLoc = NULL;

	hres = CoCreateInstance(
		CLSID_WbemLocator,             
		0, 
		CLSCTX_INPROC_SERVER, 
		IID_IWbemLocator, (LPVOID *) &pLoc);

	if (FAILED(hres))
	{
		//cout << "Failed to create IWbemLocator object." << " Err code = 0x" << hex << hres << endl; 
		return ERR_WMI_FAILED_TO_CREATE_IWBEMLOCATOR_OBJECT;                 // Program has failed.
	}

	// Connect to WMI through the IWbemLocator::ConnectServer method

	IWbemServices *pSvc = NULL;

	// Connect to the root\cimv2 namespace with
	// the current user and obtain pointer pSvc
	// to make IWbemServices calls.
	hres = pLoc->ConnectServer(
		_bstr_t(L"ROOT\\CIMV2"), // Object path of WMI namespace
		NULL,                    // User name. NULL = current user
		NULL,                    // User password. NULL = current
		0,                       // Locale. NULL indicates current
		NULL,                    // Security flags.
		0,                       // Authority (for example, Kerberos)
		0,                       // Context object 
		&pSvc                    // pointer to IWbemServices proxy
		);

	if (FAILED(hres))
	{
		//cout << "Could not connect. Error code = 0x"  << hex << hres << endl;
		pLoc->Release();      
		return ERR_WMI_COULD_NOT_CONNECT_WMI_NAMESPACE;                // Program has failed.
	}  

	// Set security levels on the proxy -------------------------

	hres = CoSetProxyBlanket(
		pSvc,                        // Indicates the proxy to set
		RPC_C_AUTHN_WINNT,           // RPC_C_AUTHN_xxx
		RPC_C_AUTHZ_NONE,            // RPC_C_AUTHZ_xxx
		NULL,                        // Server principal name 
		RPC_C_AUTHN_LEVEL_CALL,      // RPC_C_AUTHN_LEVEL_xxx 
		RPC_C_IMP_LEVEL_IMPERSONATE, // RPC_C_IMP_LEVEL_xxx
		NULL,                        // client identity
		EOAC_NONE                    // proxy capabilities 
		);

	if (FAILED(hres))
	{
		//cout << "Could not set proxy blanket. Error code = 0x"  << hex << hres << endl;
		pSvc->Release();
		pLoc->Release();      
		return ERR_WMI_COULD_NOT_SET_PROXY_BLANKET;               // Program has failed.
	}


	// Use the IWbemServices pointer to make requests of WMI ---- 

	IEnumWbemClassObject* pEnumerator = NULL;
	hres = pSvc->ExecQuery(
		bstr_t("WQL"), 
		bstr_t("SELECT * FROM Win32_DiskDrive"), // Win32_OperatingSystem
		WBEM_FLAG_FORWARD_ONLY | WBEM_FLAG_RETURN_IMMEDIATELY, 
		NULL,
		&pEnumerator);

	if (FAILED(hres))
	{
		//cout << "Query for operating system name failed." << " Error code = 0x"  << hex << hres << endl;
		pSvc->Release();
		pLoc->Release(); 
		return ERR_WMI_QUERY_FAILED;               // Program has failed.
	}

	// Get the data from the query in step 6 -------------------
	// http://msdn.microsoft.com/en-us/library/aa394373(v=vs.85).aspx
	IWbemClassObject *pclsObj = NULL;
	ULONG uReturn = 0;

	while (pEnumerator)
	{
		HRESULT hr = pEnumerator->Next(WBEM_INFINITE, 1, &pclsObj, &uReturn);

		if(0 == uReturn) { break; }

		_variant_t vtProp;


		hr = pclsObj->Get(L"Caption", 0, &vtProp, 0, 0); 
		VariantClear(&vtProp);

		hr = pclsObj->Get(L"Index", 0, &vtProp, NULL, NULL);
		DWORD  Index = vtProp; // uint32 
		VariantClear(&vtProp);

		hr = pclsObj->Get(L"TotalHeads", 0, &vtProp, 0, 0);
		DWORD  TotalHeads = vtProp; // uint32 
		VariantClear(&vtProp);

		hr = pclsObj->Get(L"Size", 0, &vtProp, 0, 0);   
		DWORD64 Size = vtProp;  // uint64 
		VariantClear(&vtProp);

		//_variant_t vtTotalCylinders; // uint64
		//hr = pclsObj->Get(L"TotalCylinders", 0, &vtTotalCylinders, NULL, NULL);  

		//wcout << " TotalCylinders : " << (DWORD64)vtTotalCylinders  << endl; 
		//VariantClear(&vtTotalCylinders);

		//_variant_t vtMinBlockSize;
		//hr = pclsObj->Get(L"MinBlockSize", 0, &vtMinBlockSize, NULL, NULL);
		//wcout << " MinBlockSize : " << (DWORD64) vtMinBlockSize  << endl; 
		//VariantClear(&vtMinBlockSize);

		//_variant_t vtMaxBlockSize;
		//hr = pclsObj->Get(L"MaxBlockSize", 0, &vtMaxBlockSize, NULL, NULL);
		//wcout << " MaxBlockSize : " << (DWORD64)vtMaxBlockSize  << endl; 
		//VariantClear(&vtMaxBlockSize);
		pclsObj->Release();
	}

	// Cleanup
	// ======== 
	pSvc->Release();
	pLoc->Release();
	pEnumerator->Release();
	if(!pclsObj)  pclsObj->Release(); 

	return SUCCESS;
}

/// <summary>
/// Gets the performance counter.
/// </summary>
/// <returns></returns>
RC WMI::GetPerformanceCounter()
{
	// Win32_PerfFormattedData_PerfOS_Processor class
	// http://msdn.microsoft.com/en-us/library/aa394271(v=vs.85).aspx
	// Accessing WMI Preinstalled Performance Classes
	// http://msdn.microsoft.com/en-us/library/aa384740(v=vs.85).aspx

	if(!isInitWmi)  return rc; 
	if(!isReady)  return rc; 

	// To add error checking,
	// check returned HRESULT below where collected.
	HRESULT                 hr = S_OK;
	IWbemRefresher          *pRefresher = NULL;
	IWbemConfigureRefresher *pConfig = NULL;
	IWbemHiPerfEnum         *pEnum = NULL;
	IWbemServices           *pNameSpace = NULL;
	IWbemLocator            *pWbemLocator = NULL;
	IWbemObjectAccess       **apEnumAccess = NULL;
	BSTR                    bstrNameSpace = NULL;
	long                    lID = 0;
	long                    lVirtualBytesHandle = 0;
	long                    lIDProcessHandle = 0;
	DWORD                   dwVirtualBytes = 0;
	DWORD                   dwProcessId = 0;
	DWORD                   dwNumObjects = 0;
	DWORD                   dwNumReturned = 0;
	DWORD                   dwIDProcess = 0;
	DWORD                   i=0;
	int                     x=0;

	if (FAILED (hr = CoInitializeEx(NULL,COINIT_MULTITHREADED)))
	{
		goto CLEANUP;
	}

	if (FAILED (hr = CoInitializeSecurity(
		NULL,
		-1,
		NULL,
		NULL,
		RPC_C_AUTHN_LEVEL_NONE,
		RPC_C_IMP_LEVEL_IMPERSONATE,
		NULL, EOAC_NONE, 0)))
	{
		goto CLEANUP;
	}

	if (FAILED (hr = CoCreateInstance(
		CLSID_WbemLocator, 
		NULL,
		CLSCTX_INPROC_SERVER,
		IID_IWbemLocator,
		(void**) &pWbemLocator)))
	{
		goto CLEANUP;
	}

	// Connect to the desired namespace.
	bstrNameSpace = SysAllocString(L"\\\\.\\root\\cimv2");
	if (NULL == bstrNameSpace)
	{
		hr = E_OUTOFMEMORY;
		goto CLEANUP;
	}
	if (FAILED (hr = pWbemLocator->ConnectServer(
		bstrNameSpace,
		NULL, // User name
		NULL, // Password
		NULL, // Locale
		0L,   // Security flags
		NULL, // Authority
		NULL, // Wbem context
		&pNameSpace)))
	{
		goto CLEANUP;
	}
	pWbemLocator->Release();
	pWbemLocator=NULL;
	SysFreeString(bstrNameSpace);
	bstrNameSpace = NULL;

	if (FAILED (hr = CoCreateInstance(
		CLSID_WbemRefresher,
		NULL,
		CLSCTX_INPROC_SERVER,
		IID_IWbemRefresher, 
		(void**) &pRefresher)))
	{
		goto CLEANUP;
	}

	if (FAILED (hr = pRefresher->QueryInterface(
		IID_IWbemConfigureRefresher,
		(void **)&pConfig)))
	{
		goto CLEANUP;
	}

	// Add an enumerator to the refresher.
	if (FAILED (hr = pConfig->AddEnum(
		pNameSpace, 
		L"Win32_PerfRawData_PerfProc_Process", 
		0, 
		NULL, 
		&pEnum, 
		&lID)))
	{
		goto CLEANUP;
	}
	pConfig->Release();
	pConfig = NULL;

	// Get a property handle for the VirtualBytes property.

	// Refresh the object ten times and retrieve the value.
	for(x = 0; x < 10; x++)
	{
		dwNumReturned = 0;
		dwIDProcess = 0;
		dwNumObjects = 0;

		if (FAILED (hr =pRefresher->Refresh(0L)))
		{
			goto CLEANUP;
		}

		hr = pEnum->GetObjects(0L, 
			dwNumObjects, 
			apEnumAccess, 
			&dwNumReturned);
		// If the buffer was not big enough,
		// allocate a bigger buffer and retry.
		if (hr == WBEM_E_BUFFER_TOO_SMALL 
			&& dwNumReturned > dwNumObjects)
		{
			apEnumAccess = new IWbemObjectAccess*[dwNumReturned];
			if (NULL == apEnumAccess)
			{
				hr = E_OUTOFMEMORY;
				goto CLEANUP;
			}
			SecureZeroMemory(apEnumAccess,
				dwNumReturned*sizeof(IWbemObjectAccess*));
			dwNumObjects = dwNumReturned;

			if (FAILED (hr = pEnum->GetObjects(0L, 
				dwNumObjects, 
				apEnumAccess, 
				&dwNumReturned)))
			{
				goto CLEANUP;
			}
		}
		else
		{
			if (hr == WBEM_S_NO_ERROR)
			{
				hr = WBEM_E_NOT_FOUND;
				goto CLEANUP;
			}
		}

		// First time through, get the handles.
		if (0 == x)
		{
			CIMTYPE VirtualBytesType;
			CIMTYPE ProcessHandleType;
			if (FAILED (hr = apEnumAccess[0]->GetPropertyHandle(
				L"VirtualBytes",
				&VirtualBytesType,
				&lVirtualBytesHandle)))
			{
				goto CLEANUP;
			}
			if (FAILED (hr = apEnumAccess[0]->GetPropertyHandle(
				L"IDProcess",
				&ProcessHandleType,
				&lIDProcessHandle)))
			{
				goto CLEANUP;
			}
		}

		for (i = 0; i < dwNumReturned; i++)
		{
			if (FAILED (hr = apEnumAccess[i]->ReadDWORD(
				lVirtualBytesHandle,
				&dwVirtualBytes)))
			{
				goto CLEANUP;
			}
			if (FAILED (hr = apEnumAccess[i]->ReadDWORD(
				lIDProcessHandle,
				&dwIDProcess)))
			{
				goto CLEANUP;
			}

			wprintf(L"Process ID %lu is using %lu bytes\n",
				dwIDProcess, dwVirtualBytes);

			// Done with the object
			apEnumAccess[i]->Release();
			apEnumAccess[i] = NULL;
		}

		if (NULL != apEnumAccess)
		{
			delete [] apEnumAccess;
			apEnumAccess = NULL;
		}

		// Sleep for a second.
		Sleep(1000);
	}
	// exit loop here
CLEANUP:

	if (NULL != bstrNameSpace)
	{
		SysFreeString(bstrNameSpace);
	}

	if (NULL != apEnumAccess)
	{
		for (i = 0; i < dwNumReturned; i++)
		{
			if (apEnumAccess[i] != NULL)
			{
				apEnumAccess[i]->Release();
				apEnumAccess[i] = NULL;
			}
		}
		delete [] apEnumAccess;
	}
	if (NULL != pWbemLocator)
	{
		pWbemLocator->Release();
	}
	if (NULL != pNameSpace)
	{
		pNameSpace->Release();
	}
	if (NULL != pEnum)
	{
		pEnum->Release();
	}
	if (NULL != pConfig)
	{
		pConfig->Release();
	}
	if (NULL != pRefresher)
	{
		pRefresher->Release();
	}

	CoUninitialize();

	if (FAILED (hr))
	{
		wprintf (L"Error status=%08x\n",hr);
	}

	return SUCCESS; 

}