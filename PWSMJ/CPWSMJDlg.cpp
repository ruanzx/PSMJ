 
// 
// Name: PWSMJDlg.cpp : implementation file 
// Author: hieunt
// Description: main dialog file for the PWSMJ application
//

#include "stdafx.h"
#include "afxdialogex.h"

#include "Main.h"
#include "CPWSMJDlg.h" 


#ifdef _DEBUG
#define new DEBUG_NEW
#endif


// CPWSMJDlg dialog

CPWSMJDlg::CPWSMJDlg(CWnd* pParent /*=NULL*/)
	: CDialog(CPWSMJDlg::IDD, pParent)
{
	m_hIcon = AfxGetApp()->LoadIcon(IDR_MAINFRAME);
}

void CPWSMJDlg::DoDataExchange(CDataExchange* pDX)
{
	CDialog::DoDataExchange(pDX);
}

BEGIN_MESSAGE_MAP(CPWSMJDlg, CDialog)
	ON_WM_SYSCOMMAND()
	ON_WM_PAINT()
	ON_WM_QUERYDRAGICON()
	ON_BN_CLICKED(IDC_BT_POWER_CONTROL, &CPWSMJDlg::OnBnPowerControl_Clicked)
	ON_BN_CLICKED(IDC_BNLJ, &CPWSMJDlg::OnBnBNLJ_Clicked)
	ON_BN_CLICKED(IDC_BT_CPU_INFO, &CPWSMJDlg::OnBnCpuInfo_Clicked)
	ON_BN_CLICKED(IDC_SMJ, &CPWSMJDlg::OnBnSmj_Clicked)
	ON_BN_CLICKED(IDC_PEMS, &CPWSMJDlg::OnBnPems_Clicked)
	ON_BN_CLICKED(IDC_NSM, &CPWSMJDlg::OnBnNsm_Clicked)
	ON_BN_CLICKED(IDC_READ_PAGE, &CPWSMJDlg::OnBnReadPage_Clicked)
	ON_BN_CLICKED(IDC_HHJ, &CPWSMJDlg::OnBnHhj_Clicked)
	ON_BN_CLICKED(IDC_GHJ, &CPWSMJDlg::OnBnGhj_Clicked)
	ON_BN_CLICKED(IDC_RP, &CPWSMJDlg::OnBnRp_Clicked)
	ON_BN_CLICKED(IDC_PSMJ, &CPWSMJDlg::OnBnPsmj_Clicked)
	ON_BN_CLICKED(IDC_SMJ2, &CPWSMJDlg::OnBnSmj2_Clicked)
	ON_BN_CLICKED(IDC_CEMS, &CPWSMJDlg::OnBnCems_Clicked)
END_MESSAGE_MAP()


// CPWSMJDlg message handlers

BOOL CPWSMJDlg::OnInitDialog()
{
	CDialog::OnInitDialog();

	// Add "About..." menu item to system menu.

	// IDM_ABOUTBOX must be in the system command range.

	ASSERT((IDM_ABOUTBOX & 0xFFF0) == IDM_ABOUTBOX);
	ASSERT(IDM_ABOUTBOX < 0xF000);
	//
	CMenu* pSysMenu = GetSystemMenu(FALSE);
	if (pSysMenu != NULL)
	{
		BOOL bNameValid;
		CString strAboutMenu;
		bNameValid = strAboutMenu.LoadString(IDS_ABOUTBOX);
		ASSERT(bNameValid);
		if (!strAboutMenu.IsEmpty())
		{
			pSysMenu->AppendMenu(MF_SEPARATOR);
			pSysMenu->AppendMenu(MF_STRING, IDM_ABOUTBOX, strAboutMenu);
		}
	}

	// Set the icon for this dialog.  The framework does this automatically
	//  when the application's main window is not a dialog
	SetIcon(m_hIcon, TRUE);			// Set big icon
	SetIcon(m_hIcon, FALSE);		// Set small icon

	// TODO: Add extra initialization here

	// Creat modaless dialog
	powerControlDlg = new CPowerControlDlg;
	powerControlDlg->Create(IDD_POWER_CONTROL_DIALOG, this);

	processorInfoDlg = new CProcessorInfoDlg;
	processorInfoDlg->Create(IDD_CPU_INFO_DIALOG, this);


	return TRUE;  // return TRUE  unless you set the focus to a control
}

void CPWSMJDlg::OnSysCommand(UINT nID, LPARAM lParam)
{
	if ((nID & 0xFFF0) == IDM_ABOUTBOX)
	{
		CAboutDlg dlgAbout;
		dlgAbout.DoModal();
	}
	else
	{
		CDialog::OnSysCommand(nID, lParam);
	}
}

// If you add a minimize button to your dialog, you will need the code below
//  to draw the icon.  For MFC applications using the document/view model,
//  this is automatically done for you by the framework.

void CPWSMJDlg::OnPaint()
{
	if (IsIconic())
	{
		CPaintDC dc(this); // device context for painting

		SendMessage(WM_ICONERASEBKGND, reinterpret_cast<WPARAM>(dc.GetSafeHdc()), 0);

		// Center icon in client rectangle
		int cxIcon = GetSystemMetrics(SM_CXICON);
		int cyIcon = GetSystemMetrics(SM_CYICON);
		CRect rect;
		GetClientRect(&rect);
		int x = (rect.Width() - cxIcon + 1) / 2;
		int y = (rect.Height() - cyIcon + 1) / 2;

		// Draw the icon
		dc.DrawIcon(x, y, m_hIcon);
	}
	else
	{
		CDialog::OnPaint();
	}
}

// The system calls this function to obtain the cursor to display while the user drags
//  the minimized window.
HCURSOR CPWSMJDlg::OnQueryDragIcon()
{
	return static_cast<HCURSOR>(m_hIcon);
}

void CPWSMJDlg::OnBnPowerControl_Clicked()
{   
	powerControlDlg->ShowWindow(SW_SHOW); 
} 

// Block nested loop join dialog
void CPWSMJDlg::OnBnBNLJ_Clicked()
{  
	CBnljDlg *bnljDlg = new CBnljDlg;
	bnljDlg->Create(IDD_BNLJ_DIALOG, this);
	bnljDlg->ShowWindow(SW_SHOW); 
}

// Processor power dialog
void CPWSMJDlg::OnBnCpuInfo_Clicked()
{
	processorInfoDlg->ShowWindow(SW_SHOW);
} 

// Classic Sort Merge join dialog
void CPWSMJDlg::OnBnSmj_Clicked()
{
	CSmjDlg *smjDlg = new CSmjDlg;
	smjDlg->Create(IDD_SMJ_DIALOG, this);
	smjDlg->ShowWindow(SW_SHOW); 
}

// Parallel external merge sort dialog
void CPWSMJDlg::OnBnPems_Clicked()
{ 
	CPemsDlg *pexsDlg = new CPemsDlg;
	pexsDlg->Create(IDD_PEMS_DIALOG, this);
	pexsDlg->ShowWindow(SW_SHOW); 
}

// Create NSM page dialog
void CPWSMJDlg::OnBnNsm_Clicked()
{
	CNsmDlg *nsmDlg = new CNsmDlg;
	nsmDlg->Create(IDD_NSM_DIALOG, this);
	nsmDlg->ShowWindow(SW_SHOW); 
}

// Debug
void CPWSMJDlg::OnBnReadPage_Clicked()
{
	CLargeFileDlg *debugDlg = new CLargeFileDlg;
	debugDlg->Create(IDD_DEBUG_DIALOG, this);
	debugDlg->ShowWindow(SW_SHOW); 
}

// Hybrid Hash Join dialog
void CPWSMJDlg::OnBnHhj_Clicked()
{ 
	CHhjDlg *hhjDlg = new CHhjDlg;
	hhjDlg->Create(IDD_HHJ_DIALOG, this);
	hhjDlg->ShowWindow(SW_SHOW);  
}

// Grace Hash Join dialog
void CPWSMJDlg::OnBnGhj_Clicked()
{
	CGhjDlg *ghjDlg = new CGhjDlg;
	ghjDlg->Create(IDD_GHJ_DIALOG, this);
	ghjDlg->ShowWindow(SW_SHOW);   
}

// Replacement Selection dialog
void CPWSMJDlg::OnBnRp_Clicked()
{
	CRpDlg *rpDlg = new CRpDlg;
	rpDlg->Create(IDD_RP_DIALOG, this);
	rpDlg->ShowWindow(SW_SHOW);   
}

// Parallel Sort Merge Join dialog
void CPWSMJDlg::OnBnPsmj_Clicked()
{ 
	CPsmjDlg *psmjDlg = new CPsmjDlg;
	psmjDlg->Create(IDD_PSMJ_DIALOG, this);
	psmjDlg->ShowWindow(SW_SHOW); 

}

// Parallel Sort Merge Join dialog
void CPWSMJDlg::OnBnSmj2_Clicked()
{
	CSmj2Dlg *smjDlg = new CSmj2Dlg;
	smjDlg->Create(IDD_SMJ2_DIALOG, this);
	smjDlg->ShowWindow(SW_SHOW); 
}

// Hybrid external merge sort dialog
void CPWSMJDlg::OnBnCems_Clicked()
{
	CCemsDlg *cemsDlg = new CCemsDlg;
	cemsDlg->Create(IDD_CEMS_DIALOG, this);
	cemsDlg->ShowWindow(SW_SHOW); 
}
