// 
// Name: Exceptions.cpp : implementation file 
// Author: hieunt
// Description: Handle exceptions in this application
//

#include "stdafx.h"
#include "Exceptions.h"
 
/// <summary>
/// Prints the error by return code rc.
/// </summary>
/// <param name="rc">The return code.</param>
/// <returns></returns>
LPWSTR PrintErrorRC(RC rc) 
{ 
	switch (rc) {
	case SUCCESS:	
		return L"Operation Success"; 
		/* Errors */
	case ERR_CANNOT_GET_FILE_SIZE:		
		return L"Unable to get the size of file";  
	case ERR_SORT_MERGE_FILE_NOT_ENOUGH:		
		return L"File number for merge is less than 2";  
	case ERR_NOT_ENOUGH_MEMORY:		
		return L"Not enough memory";  
	case ERR_CANNOT_INITIAL_MEMORY:		
		return L"Cannot initialize memory";  
	case ERR_NOT_INIT_MEMORY:
		return L"Memory is not initialized";  
	case ERR_CANNOT_INITIAL_BUFFER_POOL:		
		return L"Cannot initialize Buffer Pool";  
	case ERR_CANNOT_CREATE_HANDLE:		
		return L"Cannot create handle";  
	case ERR_END_OF_FILE:		
		return L"End-Of-File";  
	case ERR_INVALID_COMMAND_LINE_PARAMETERS:		
		return L"Invalid command line parameters"; 
	case ERR_SORT_EMPTY_BUFFER:
		return L"Buffer is empty";  
	case ERR_SORT_CANNOT_ASYNC_READ:
		return L"Cannot read async";   
	case ERR_UNKNOWN_EXCEPTION:	/* Unknown return constant */
		return L"Unknow error";  
	case ERR_CANNOT_GET_TUPLE_KEY:
		return L"Tuple length is zero, cannot get tuple key."; 
	case  ERR_WMI_FAILED_TO_INITIALIZE_COM_LIBRARY:
		return L"WMI: Failed to initialize COM library."; 
	case ERR_WMI_FAILED_TO_INITIALIZE_SECURITY:
		return L"WMI: Failed to initialize security."; 
	case  ERR_WMI_FAILED_TO_CREATE_IWBEMLOCATOR_OBJECT: 
		return L"WMI: Failed to create IWbemLocator object."; 
	case ERR_WMI_COULD_NOT_CONNECT_WMI_NAMESPACE:
		return L"WMI: Could not connect WMI namespace.";  
	case ERR_WMI_COULD_NOT_SET_PROXY_BLANKET:
		return L"WMI: Could not set proxy blanket.";  
	case ERR_WMI_QUERY_FAILED:
		return L"WMI: Query failed."; 
	case ERR_WMI_NOT_FOUND_ANY_INSTANCE:
		return L"WMI: Query failed. Not found any instance"; 
	case ERR_SMJ_INIT_PARAM_FAILED:
		return L"SMJ: Initialize() fail.";
	default:		
		return L"Unknown error";   
	}  
}