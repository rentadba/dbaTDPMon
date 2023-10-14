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
				SELECT       1 AS [id], 'dbo.usp_mpDatabaseConsistencyCheck' AS [descriptor], 'Database Consistency Check' AS [task_name], 1 AS [flg_actions], 1 AS [priority] UNION ALL
				SELECT       2, 'dbo.usp_mpDatabaseConsistencyCheck', 'Allocation Consistency Check', 12, 2 AS [priority] UNION ALL
				SELECT       4, 'dbo.usp_mpDatabaseConsistencyCheck', 'Tables Consistency Check', 2, 3 AS [priority] UNION ALL
				SELECT       8, 'dbo.usp_mpDatabaseConsistencyCheck', 'Reference Consistency Check', 16, 4 AS [priority] UNION ALL
				SELECT      16, 'dbo.usp_mpDatabaseConsistencyCheck', 'Perform Correction to Space Usage', 64, 5 AS [priority] UNION ALL
				SELECT      32, 'dbo.usp_mpDatabaseOptimize', 'Rebuild Heap Tables', 16, 6 AS [priority] UNION ALL
				SELECT      64, 'dbo.usp_mpDatabaseOptimize', 'Rebuild or Reorganize Indexes', 3, 7 AS [priority] UNION ALL
				SELECT     128, 'dbo.usp_mpDatabaseOptimize', 'Update Statistics', 8, 8 AS [priority] UNION ALL
				SELECT     256, 'dbo.usp_mpDatabaseShrink', 'Shrink Database (TRUNCATEONLY)', 2 , 9 AS [priority]UNION ALL
				SELECT     512, 'dbo.usp_mpDatabaseShrink', 'Shrink Log File', 1, 10 AS [priority] UNION ALL
				SELECT    1024, 'dbo.usp_mpDatabaseBackup', 'User Databases (diff)', 2, 12 AS [priority] UNION ALL
				SELECT    2048, 'dbo.usp_mpDatabaseBackup', 'User Databases (full)' , 1, 11 AS [priority] UNION ALL
				SELECT    4096, 'dbo.usp_mpDatabaseBackup', 'System Databases (full)', 1, 13 AS [priority] UNION ALL
				SELECT    8192, 'dbo.usp_mpDatabaseBackup', 'User Databases Transaction Log', 4, 14 AS [priority]
			)S
		LEFT JOIN [dbo].[appInternalTasks] ait ON S.[id] = ait.[id]
		WHERE ait.[id] IS NULL
GO
UPDATE [dbo].[appInternalTasks]
	SET [is_resource_intensive] = 1
WHERE [task_name] IN (  'Database Consistency Check'
					  , 'Allocation Consistency Check'
					  , 'Tables Consistency Check'
					  , 'Reference Consistency Check'
					  , 'Perform Correction to Space Usage'
					  , 'Rebuild Heap Tables'
					  , 'User Databases (diff)'
					  , 'User Databases (full)'
				     )

GO

INSERT	INTO[maintenance-plan].[internalScheduler] ([project_id], [task_id], [scheduled_weekday], [active])
		SELECT NULL,    1, 'Saturday', 1 UNION ALL
		SELECT NULL,    2, 'Daily', 1 UNION ALL
		SELECT NULL,    4, 'Sunday', 1 UNION ALL
		SELECT NULL,    8, 'Sunday', 1 UNION ALL
		SELECT NULL,   16, 'N/A', 0 UNION ALL
		SELECT NULL,   32, 'Daily', 1 UNION ALL
		SELECT NULL,   64, 'Daily', 1 UNION ALL
		SELECT NULL,  128, 'Daily', 1 UNION ALL
		SELECT NULL,  256, 'Sunday', 1 UNION ALL
		SELECT NULL,  512, 'Saturday', 1 UNION ALL
		SELECT NULL, 1024, 'Daily', 1 UNION ALL
		SELECT NULL, 2048, 'Saturday', 1 UNION ALL
		SELECT NULL, 4096, 'Saturday', 1 UNION ALL
		SELECT NULL, 8192, 'Daily', 1
GO
