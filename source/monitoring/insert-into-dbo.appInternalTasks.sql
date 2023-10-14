-- ============================================================================
-- Copyright (c) 2004-2018 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 11.04.2018
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-----------------------------------------------------------------------------------------------------
--
-----------------------------------------------------------------------------------------------------
SET NOCOUNT ON
GO
INSERT	INTO [dbo].[appInternalTasks] ([id], [descriptor], [task_name], [flg_actions], [priority])
		SELECT S.[id], S.[descriptor], S.[task_name], S.[flg_actions], S.[priority]
		FROM (
				SELECT 1048576 AS [id], 'dbo.usp_monAlarmCustomReplicationLatency' AS [descriptor], 'Monitor Replication Latency' AS [task_name], NULL AS [flg_actions], 3 AS [priority] UNION ALL
				SELECT 2097152, 'dbo.usp_monAlarmCustomSQLAgentFailedJobs', 'Monitor Failed SQL Server Agent Jobs', NULL, 2 AS [priority] UNION ALL
				SELECT 4194304, 'dbo.usp_monAlarmCustomTransactionsStatus', 'Monitor Transaction and Session Status', NULL, 1 AS [priority] 
			)S
		LEFT JOIN [dbo].[appInternalTasks] ait ON S.[id] = ait.[id]
		WHERE ait.[id] IS NULL
GO
