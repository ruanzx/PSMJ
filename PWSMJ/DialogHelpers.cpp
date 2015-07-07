// 
// Name: DialogHelpers.cpp : implementation file 
// Author: hieunt
// Description: Helpers function use for make dialog esier
//

#include "stdafx.h"
#include "DialogHelpers.h"

/// <summary>
/// Get the combobox index by text.
/// </summary>
/// <param name="cbbCtl">The CBB control.</param>
/// <param name="text">The text.</param>
VOID ComboboxSetSelectedIndexByText(CComboBox &cbbCtl, LPTSTR text)
{
	UINT cbbCount = cbbCtl.GetCount(); 
	for(UINT i = 0; i < cbbCount; i++)
	{
		if(wcscmp(ComboboxGetTextByIndex(cbbCtl, i), text) == 0)
		{
			cbbCtl.SetCurSel(i); 
			break; 
		} 
	}
} 

/// <summary>
/// Get the comboboxes text by index.
/// </summary>
/// <param name="cbbCtl">The CBB control.</param>
/// <param name="index">The index.</param>
/// <returns></returns>
LPTSTR ComboboxGetTextByIndex(CComboBox &cbbCtl, UINT index)
{
	LPTSTR text = new TCHAR[MAX_PATH]; 
	cbbCtl.GetLBText(index, text); 
	return text;
}

/// <summary>
/// Shows the message box.
/// </summary>
/// <param name="pszFormat">The PSZ format.</param> 
VOID ShowMB(LPWSTR pszFormat, ...) 
{   
	LPWSTR pszMessage = new TCHAR[1024];

	va_list argList;
	va_start(argList, pszFormat);  
	_vstprintf_s(pszMessage, 1024, pszFormat, argList); 
	va_end(argList);

	AfxMessageBox(pszMessage, MB_OK);
	delete pszMessage; 
}
