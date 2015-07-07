// 
// Name: CProcessorInfoDlg.cpp
// Author: hieunt
// Description: Processor information, power capping, frequency,.. dialog
//

#include "stdafx.h"
#include "CProcessorInfoDlg.h" 

// Global variables
extern volatile DWORD  g_Sample_Interval; // Refresh sample interval
extern volatile DWORD  g_Gui_Interval; // Refresh gui interval
extern volatile DOUBLE g_ProcessUsage; // Process cpu utilizion
extern volatile DOUBLE g_ProcessPower; // Current process power
extern volatile DOUBLE g_PackagePower; // Processor package power
extern volatile DOUBLE g_CapPower; // Process cap power
extern volatile DWORD g_TimeBase; // Cap interval time 
extern Loggers g_Logger; // log to CPU_report file
extern CpuUsage usage; // CPU utilization

/// <summary>
/// Initializes a new instance of the <see cref="CProcessorInfoDlg"/> class.
/// </summary>
CProcessorInfoDlg::CProcessorInfoDlg()
{ 
	m_Quit = FALSE; // Quit monitor
}

/// <summary>
/// Finalizes an instance of the <see cref="CProcessorInfoDlg"/> class.
/// </summary>
CProcessorInfoDlg::~CProcessorInfoDlg()
{

}

void CProcessorInfoDlg::DoDataExchange(CDataExchange* pDX)
{
	CDialogEx::DoDataExchange(pDX);
	DDX_Control(pDX, IDC_TB_CPU_MODEL, m_CpuModel);
	DDX_Control(pDX, IDC_TB_CPU_PHYSICAL_CORE_NUM, m_PhysicalCoreNum);
	DDX_Control(pDX, IDC_TB_CPU_LOGICAL_CORE_NUM, m_LogicalCoreNum);
	DDX_Control(pDX, IDC_TB_CPU_MULTIPLIER, m_Multiplier);
	DDX_Control(pDX, IDC_TB_PKG_POWER_MIN, m_PackageMinPower);
	DDX_Control(pDX, IDC_TB_PKG_POWER_MAX, m_PackageMaxPower);
	DDX_Control(pDX, IDC_TB_PACKAGE_POWER, m_PackagePower);
	DDX_Control(pDX, IDC_TB_CPU_TDP, m_TDPPower);
	DDX_Control(pDX, IDC_TB_CORE0_POWER, m_Core_0_Power);
	DDX_Control(pDX, IDC_TB_CORE1_POWER, m_Core_1_Power);
	DDX_Control(pDX, IDC_TB_CPU_VOLTAGE, m_Voltage);
	DDX_Control(pDX, IDC_TB_CPU_FREQ_MSR, m_FreqMsr);
	DDX_Control(pDX, IDC_TB_CPU_EXT_CLOCK, m_BaseFreq);
	DDX_Control(pDX, IDC_TB_CURRENT_MULTIPLIER, m_CurrentMultiplier);
	DDX_Control(pDX, IDC_TB_CPU_FREQ_PERCENT_CYCLES, m_FreqCycles);
	DDX_Control(pDX, IDC_TB_CPU_FREQ_PERCENT_CYCLES_BUSY, m_FreqCyclesBusyPercent);
	DDX_Control(pDX, IDC_TB_CPU_FREQ_TSC, m_FreqRdTSC);
	DDX_Control(pDX, IDC_TB_CPU_TEMP_PACKAGE, m_PackageTemp);
	DDX_Control(pDX, IDC_TB_CPU_TJMAX, m_TjMax);
	DDX_Control(pDX, IDC_TB_CPU_TEMP_CORE_0, m_Core0Temp);
	DDX_Control(pDX, IDC_TB_CPU_TEMP_CORE_1, m_Core1Temp);
	DDX_Control(pDX, IDC_TB_INTERVAL, m_RefreshInterval);
	DDX_Control(pDX, IDC_TB_POWER_CONSTRAIN, m_PowerCap);
	DDX_Control(pDX, IDC_BT_STRESS, m_StressWorkLoad);
	DDX_Control(pDX, IDC_TB_DISK_READ_RATE, m_DiskReadRate);
	DDX_Control(pDX, IDC_TB_DISK_WRITE_RATE, m_DiskWriteRate);
	DDX_Control(pDX, IDC_TB_PROCESS_POWER, m_tbProcessPower);
	DDX_Control(pDX, IDC_TB_PROCESS_USAGE, m_tbProcessUsage);
	DDX_Control(pDX, IDC_BT_SET_POWER_CONSTRAIN, m_btSetPowerCap);
	DDX_Control(pDX, IDC_TB_GUI_INTERVAL, m_tbGuiInterval);
}

BEGIN_MESSAGE_MAP(CProcessorInfoDlg, CDialogEx)
	ON_BN_CLICKED(IDC_BT_SET_INTERVAL, &CProcessorInfoDlg::OnBnSetInterval_Clicked)
	ON_WM_DESTROY()
	ON_BN_CLICKED(IDOK, &CProcessorInfoDlg::OnBnOk_Clicked)
	ON_BN_CLICKED(IDC_BT_STRESS, &CProcessorInfoDlg::OnBnStress_Clicked)
	ON_BN_CLICKED(IDC_BT_SET_POWER_CONSTRAIN, &CProcessorInfoDlg::OnBtSetPowerConstrain_Clicked)
	ON_BN_CLICKED(IDC_BT_SET_GUI_INTERVAL, &CProcessorInfoDlg::OnBnClickedBtSetGuiInterval)
	ON_BN_CLICKED(IDC_BT_SET_POWER_TIMEBASE, &CProcessorInfoDlg::OnBnClickedBtSetPowerTimebase)
END_MESSAGE_MAP()

/// <summary>
/// Called when [initialize dialog].
/// </summary>
/// <returns></returns>
BOOL CProcessorInfoDlg::OnInitDialog()
{
	CDialog::OnInitDialog(); 

	// Start init MSR driver
	if(!m_CPU.IsDriverReady())
	{
		AfxMessageBox(L"Cannot find MSR driver, Check your driver path!", MB_OK);
		return FALSE;
	}

	// TODO: Add extra initialization here

	usage.Calculate();

	// Set default value for GUI
	SetDlgItemInt(IDC_TB_POWER_CONSTRAIN, CPU_POWER_CONSTRAIN_WATT, FALSE);  
	SetDlgItemInt(IDC_TB_GUI_INTERVAL, REFRESH_GUI_INTERVAL_MILISECOND, FALSE); // GUI check interval
	SetDlgItemInt(IDC_TB_INTERVAL, REFRESH_SAMPLE_INTERVAL_MILISECOND, FALSE); // GUI check interval
	SetDlgItemInt(IDC_TB_POWER_TIMEBASE, 100, FALSE); // Time base capping interval 

	// Update global interval
	g_Gui_Interval = REFRESH_GUI_INTERVAL_MILISECOND;
	g_Sample_Interval = REFRESH_SAMPLE_INTERVAL_MILISECOND;
	g_CapPower = CPU_POWER_CONSTRAIN_WATT;
	g_TimeBase = 100;

	m_CPU.GetBasicInfos();  
	m_CPU.GetCurrentEnergyStatus(); // first run
	m_CPU.GetPlatformInfo(); 
	m_CPU.GetTurboLimit(); 
	m_CPU.GetPackageThermalStatus();
	m_CPU.GetCoreThermalStatus(); 
	m_CPU.GetPowerInfos();
	m_CPU.GetCurrentEnergyStatus(); // second run
	m_CPU.FrequencyStatus.NominalFrequency = m_CPU.BasicInfo.NominalFrequency / 1000000;
	m_CPU.GetCurrentCyclesFrequency(); 
	//m_CPU.EnergyStatus.Interval = g_Interval;  

	// CPU Basic information
	//////////////////////////////////////////////////////////////////////////
	m_CpuModel.SetWindowTextW(m_CPU.GetCpuCodeName(m_CPU.BasicInfo.OriginalCpuModel));

	m_PhysicalCoreNum.SetWindowTextW(LongToString(m_CPU.BasicInfo.NumberOfPhysicalCore, L"%d"));
	m_LogicalCoreNum.SetWindowTextW(LongToString(m_CPU.BasicInfo.NumberOfLogicalCore, L"%d"));
	m_BaseFreq.SetWindowTextW(LongToString(m_CPU.BasicInfo.BusFrequency / 1000000L, L"%d")); 
	m_Multiplier.SetWindowTextW(m_CPU.GetMultiplierStr());
	m_TjMax.SetWindowTextW(LongToString(m_CPU.ThermalStatus.TjMax, L"%d C"));
	m_TDPPower.SetWindowTextW(LongToString(m_CPU.PkgPowerInfo.ThermalSpecPower, L"%d W"));
	m_PackageMinPower.SetWindowTextW(LongToString(m_CPU.PkgPowerInfo.MinimumPower, L"%d W"));
	m_PackageMaxPower.SetWindowTextW(LongToString(m_CPU.PkgPowerInfo.MaximumPower, L"%d W")); 

	DWORD multiplier = m_CPU.GetCurrentMultiplier( m_CPU.BasicInfo.Model, 0);
	UINT64 msr_freq = m_CPU.GetCurrentMsrFrequency(multiplier);
	m_FreqMsr.SetWindowTextW(LongToString(msr_freq / 1000000L, L"%d Mhz")); 

	m_FreqRdTSC.SetWindowTextW(DoubleToString(m_CPU.FrequencyStatus.TscFrequency, L"%.f Mhz")); 

	// Start monitor thread
	ThreadParams *myParams = new ThreadParams;
	myParams->_this = this;

	m_hCpuStatusThread = CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)UpdateCpuStatusEx, myParams, CREATE_SUSPENDED, NULL); 
	SetThreadPriority( m_hCpuStatusThread, THREAD_PRIORITY_HIGHEST);
	//	SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_HIGHEST);
	ResumeThread(m_hCpuStatusThread); 

	return TRUE;  // return TRUE  unless you set the focus to a control
}

DWORD WINAPI CProcessorInfoDlg::UpdateCpuStatusEx(LPVOID lpParam)
{
	ThreadParams* p = (ThreadParams*)(lpParam);
	p->_this->UpdateCpuStatus((LPVOID)(p));
	return 0;
}

DWORD WINAPI CProcessorInfoDlg::UpdateCpuStatus(LPVOID lpParam)
{  
	PCHAR systemTime = "time";//
	UINT64 rdtsc = 0;
	DOUBLE totalCpuTime = 0;
	DOUBLE cpuTime[2];
	cpuTime[0] = 0; cpuTime[1] = 0; // log cpu time each measure interval

	DOUBLE voltage = 0; 
	DWORD  multiplier=0;
	UINT64 msrFrequency=0;  

	// Log variables
	DWORD frequency = 0;
	DOUBLE packagePower = 0;
	DOUBLE processPower = 0;
	DOUBLE processUsage = 0;
	DOUBLE packageEnergyInJoules = 0;
	DOUBLE packageEnergyInmWh = 0;
	DWORD packageTemperature = 0;
	DWORD packagePowerLimit = 0; 
	UINT64 bytesRead = 0, bytesWritten = 0, totalReadRead = 0;
	DOUBLE readRate = 0, writeRate = 0;
	INT64  diskTimeTransfer = 0; 

	m_CPU.stw.Start();

	BOOL firstMeasure = TRUE;
	PCHAR logData = new CHAR[1024];

	sprintf_s(logData, 1024, "%s, %.2f\n", "Package Static Power", CPU_IDLE_POWER_WATT); g_Logger.Write(logData);  
	const PCHAR logTitle = "System Time,RDTSC,Cpu Time (Total), Cpu Time, Frequency (MHz), Multiplier, Voltage, Processor Power (Watt),Package Energy (Joules), Package Energy (mWh), Process Power, Process Usage, Power Cap Value, Package Temperature (C),Package Power Limit (Watt), Read Bytes Count,Write Bytes Count, Read Rate (MB/s), Write Rate (MB/s), Disk Time (ms), Total Byte Read\n";
	const PCHAR logPattern = "%s, %lld, %.2f, %.2f, %d, %d, %.6f, %.2f, %.3f, %.3f, %.2f, %.2f, %.2f, %d, %d, %lld, %lld, %.f, %.f, %lld, %lld\n";


	g_Logger.Write(logTitle); 

	DWORD guidRfr = 0;

	while(!m_Quit)
	{     
		// Calculate CPU usage
		usage.Calculate();

		// Set Cpu usage to global variable
		g_ProcessUsage = (DOUBLE) usage.GetCpuUsage();

		voltage = m_CPU.GetCurrentVoltage(); 
		frequency = m_CPU.FrequencyStatus.ActiveAverageFrequency;
		multiplier = m_CPU.GetCurrentMultiplier(m_CPU.BasicInfo.Model, 0);
		msrFrequency = m_CPU.GetCurrentMsrFrequency(multiplier); 

		m_CPU.GetCurrentEnergyStatus();  
		m_CPU.GetPackageThermalStatus();
		m_CPU.GetCoreThermalStatus(); 
		m_CPU.GetCurrentCyclesFrequency();
		//m_Disk.GetDrivePerformance();
		m_Disk.GetProcessIoCounter(bytesRead, bytesWritten, readRate, writeRate, diskTimeTransfer, totalReadRead); 

		//bytesRead = m_Disk.diskStates.AvgBytes.BytesRead;
		//bytesWritten = m_Disk.diskStates.AvgBytes.BytesWritten;

		systemTime = GetTime();
		rdtsc = m_CPU.GetTimeStampCounter(0);
		totalCpuTime = GetCpuTime();
		cpuTime[1] = totalCpuTime;

		packagePower = m_CPU.EnergyStatus.Package.Power;
		packageEnergyInJoules = m_CPU.EnergyStatus.Package.Delta.Energy;
		packageEnergyInmWh = JouleToMiliwattHour(packageEnergyInJoules);
		packageTemperature = m_CPU.ThermalStatus.Package.TemperatureInDegreesCelsius;
		packagePowerLimit = m_CPU.PkgPowerInfo.ThermalSpecPower;

		// Upadate global variable
		g_ProcessPower = (g_ProcessUsage / 100) * (packagePower - CPU_IDLE_POWER_WATT); // processUsage * ( P(cpu) - P(static) )
		g_PackagePower = packagePower;

		if(!chINRANGE(0,g_ProcessPower,100)) { g_ProcessPower = 0; }
		processPower = g_ProcessPower; 

		if(!chINRANGE(0, g_ProcessUsage, 100)) { g_ProcessUsage = 0; }
		processUsage = g_ProcessUsage;

		// Update GUI
		if(guidRfr >= g_Gui_Interval)
		{
			m_Voltage.SetWindowTextW(DoubleToString(voltage, L"%.7f V"));
			m_FreqMsr.SetWindowTextW(LongToString(msrFrequency / 1000000L, L"%d Mhz"));
			m_CurrentMultiplier.SetWindowTextW(LongToString(multiplier, L"x %d")); 

			m_FreqCycles.SetWindowTextW(DoubleToString(frequency, L"%.f Mhz")); 
			m_FreqCyclesBusyPercent.SetWindowTextW(DoubleToString(m_CPU.FrequencyStatus.PercentUnhalted, L"%.f%%")); 
			m_FreqRdTSC.SetWindowTextW(DoubleToString(m_CPU.FrequencyStatus.TscFrequency, L"%.f Mhz"));  

			m_PackagePower.SetWindowTextW(DoubleToString(m_CPU.EnergyStatus.Package.Power, L"%.2f W")); 
			m_Core_0_Power.SetWindowTextW(DoubleToString(m_CPU.EnergyStatus.Core[0].Power, L"%.2f W")); 
			m_Core_1_Power.SetWindowTextW(DoubleToString(m_CPU.EnergyStatus.Core[1].Power, L"%.2f W"));  

			m_PackageTemp.SetWindowTextW(LongToString(m_CPU.ThermalStatus.Package.TemperatureInDegreesCelsius, L"%d C")); 
			m_Core0Temp.SetWindowTextW(LongToString(m_CPU.ThermalStatus.Core[0].TemperatureInDegreesCelsius, L"%d C")); 
			m_Core1Temp.SetWindowTextW(LongToString(m_CPU.ThermalStatus.Core[1].TemperatureInDegreesCelsius, L"%d C"));  

			m_DiskReadRate.SetWindowTextW(LongToString(readRate, L"%d MBps")); 
			m_DiskWriteRate.SetWindowTextW(LongToString(writeRate, L"%d MBps"));   

			m_tbProcessUsage.SetWindowTextW(DoubleToString(g_ProcessUsage, L"%.f")); 
			m_tbProcessPower.SetWindowTextW(DoubleToString(g_ProcessPower, L"%.2f W")); 

			// the first time measure we don't log data
			if (firstMeasure==FALSE)
			{
				sprintf_s(logData, 1024, logPattern, 
					systemTime,
					rdtsc,  
					totalCpuTime, 
					cpuTime[1] - cpuTime[0],
					frequency, 
					multiplier,
					voltage,
					packagePower, 
					packageEnergyInJoules, 
					packageEnergyInmWh, 
					processPower,
					processUsage,
					g_CapPower,
					packageTemperature, 
					packagePowerLimit,
					bytesRead,
					bytesWritten,
					readRate,
					writeRate,
					diskTimeTransfer,
					totalReadRead / (1024 * 1024)); 

				g_Logger.Write(logData);  
			}

			// reset interval
			guidRfr = 0;
		}


		if (firstMeasure) { firstMeasure=FALSE; }

		// Reset values
		bytesRead = bytesWritten = 0;
		totalReadRead = 0;
		cpuTime[0] = cpuTime[1];

		guidRfr = guidRfr + g_Sample_Interval;
		// Sleep until next inteval
		SleepMilisecond(g_Sample_Interval);
	} 

	delete logData;
	return 0;
}

/// <summary>
/// Called when [bn set interval_ clicked]
/// Update get sample interval.
/// </summary>
void CProcessorInfoDlg::OnBnSetInterval_Clicked()
{
	// TODO: Add your control notification handler code here
	g_Sample_Interval = GetDlgItemInt(IDC_TB_INTERVAL, FALSE, FALSE); // Time check interval 
	//m_CPU.EnergyStatus.Interval = g_Interval; 
}


/// <summary>
/// Called when [destroy dialog].
/// </summary>
void CProcessorInfoDlg::OnDestroy()
{
	CDialogEx::OnDestroy(); 

	m_Quit=TRUE;
	WaitForSingleObject(m_hCpuStatusThread, INFINITE);
	CloseHandle(m_hCpuStatusThread);   
}


void CProcessorInfoDlg::OnBnOk_Clicked()
{
	// TODO: Add your control notification handler code here
	CDialogEx::OnOK();

	this->ShowWindow(SW_HIDE);
}


/// <summary>
/// Called when [bn stress_ clicked].
/// Burn CPU with many works
/// </summary>
void CProcessorInfoDlg::OnBnStress_Clicked()
{
	TCHAR szText[MAX_PATH];
	m_StressWorkLoad.GetWindowTextW(szText, MAX_PATH);

	if(wcscmp(szText, L"Stress")==0)
	{
		m_StressWorkLoad.SetWindowTextW(L"Stop"); 
		DWORD jobNum = 10000000;
		m_WorkLoad.SetJobLimit(jobNum); 
		m_WorkLoad.Start(); 
	}
	else
	{ 
		m_StressWorkLoad.SetWindowTextW(L"Stress"); 
		m_WorkLoad.Stop();  
	} 
}

/// <summary>
/// Called when [bt set power constrain_ clicked].
/// Set the power constrain value
/// </summary>
void CProcessorInfoDlg::OnBtSetPowerConstrain_Clicked()
{
	// TODO: Add your control notification handler code here
	g_CapPower = GetDlgItemInt(IDC_TB_POWER_CONSTRAIN, FALSE, FALSE);

	LPWSTR sVal = new TCHAR[50]; 
	CHAR *cVal = new CHAR[MAX_PATH]; 
	m_PowerCap.GetWindowTextW(sVal, 50); 
	size_t  count = wcstombs(cVal, sVal, MAX_PATH); // C4996  
	g_CapPower = atof( cVal );
 
	delete sVal;
	delete cVal;
}


void CProcessorInfoDlg::OnBnClickedBtSetGuiInterval()
{
	// TODO: Add your control notification handler code here
	g_Gui_Interval = GetDlgItemInt(IDC_TB_POWER_CONSTRAIN, FALSE, FALSE);
}


void CProcessorInfoDlg::OnBnClickedBtSetPowerTimebase()
{
	// TODO: Add your control notification handler code here
	if(chINRANGE(25, GetDlgItemInt(IDC_TB_POWER_TIMEBASE, FALSE, FALSE), 100))
	{
		g_TimeBase = GetDlgItemInt(IDC_TB_POWER_TIMEBASE, FALSE, FALSE);
	}
	else
	{
		ShowMB(L"Time mus in range from 25 to 100");
	}
}
