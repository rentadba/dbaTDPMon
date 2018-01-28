SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2017.12 to 2018.1 (2018.01.28)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: monitoring																							   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20180128-patch-upgrade-from-v2017_12-to-v2018_1-mon.sql', 10, 1) WITH NOWAIT

IF NOT EXISTS(SELECT * FROM [monitoring].[alertThresholds] WHERE [category]='performance' AND [alert_name] = 'Active Request/Session Elapsed Time (sec)')
	INSERT	INTO [monitoring].[alertThresholds] ([category], [alert_name], [operator], [warning_limit], [critical_limit])
			SELECT 'performance', 'Active Request/Session Elapsed Time (sec)', '>', 600, 900
GO
