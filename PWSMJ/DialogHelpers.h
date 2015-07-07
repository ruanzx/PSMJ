// 
// Name: DialogHelpers.h : implementation file 
// Author: hieunt
// Description: Helpers function use for make dialog esier
//

#pragma once

// Helpers function for GUI

VOID ComboboxSetSelectedIndexByText(CComboBox &cbbCtl, LPTSTR text);
LPTSTR ComboboxGetTextByIndex(CComboBox &cbbCtl, UINT index);
VOID ShowMB(LPWSTR pszFormat, ...);

