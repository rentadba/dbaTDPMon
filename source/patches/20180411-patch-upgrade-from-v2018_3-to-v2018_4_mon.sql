SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2018.3 to 2018.4 (2018.04.11)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: monitoring																							   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20180411-patch-upgrade-from-v2018_3-to-v2018_4-mon.sql', 10, 1) WITH NOWAIT

UPDATE [dbo].[jobExecutionQueue]
	SET [descriptor] = 'dbo.usp_monAlarmCustomReplicationLatency'
WHERE [descriptor] = 'usp_monAlarmCustomReplicationLatency'
GO
UPDATE [dbo].[jobExecutionQueue]
	SET [descriptor] = 'dbo.usp_monAlarmCustomSQLAgentFailedJobs'
WHERE [descriptor] = 'usp_monAlarmCustomSQLAgentFailedJobs'
GO
UPDATE [dbo].[jobExecutionQueue]
	SET [descriptor] = 'dbo.usp_monAlarmCustomTransactionsStatus'
WHERE [descriptor] = 'usp_monAlarmCustomTransactionsStatus'
GO
