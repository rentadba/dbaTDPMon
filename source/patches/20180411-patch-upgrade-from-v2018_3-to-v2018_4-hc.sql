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
/* patch module: health-check																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20180411-patch-upgrade-from-v2018_3-to-v2018_4-hc.sql', 10, 1) WITH NOWAIT

INSERT	INTO [dbo].[appInternalTasks] ([id], [descriptor], [task_name], [flg_actions])
		SELECT S.[id], S.[descriptor], S.[task_name], S.[flg_actions]
		FROM (
				SELECT   16384 AS [id], 'dbo.usp_hcCollectDatabaseDetails' AS [descriptor], 'Collect Database Details' AS [task_name], NULL AS [flg_actions] UNION ALL
				SELECT   32768, 'dbo.usp_hcCollectDiskSpaceUsage', 'Collect Disk Space Usage', NULL UNION ALL
				SELECT   65536, 'dbo.usp_hcCollectErrorlogMessages', 'Collect SQL Server errorlog Messages', NULL UNION ALL
				SELECT  131072, 'dbo.usp_hcCollectOSEventLogs', 'Collect OS Event Logs', NULL UNION ALL
				SELECT  262144, 'dbo.usp_hcCollectSQLServerAgentJobsStatus', 'Collect SQL Server Agent Jobs Status', NULL UNION ALL
				SELECT  524288, 'dbo.usp_hcCollectEventMessages', 'Collect Internal Event Messages', NULL 
			)S
		LEFT JOIN [dbo].[appInternalTasks] ait ON S.[id] = ait.[id]
		WHERE ait.[id] IS NULL
GO
