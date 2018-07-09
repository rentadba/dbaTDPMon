SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2018.6 to 2018.7 (2018.07.09)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: common																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20180709-patch-upgrade-from-v2018_6-to-v2018_7-common.sql', 10, 1) WITH NOWAIT

/* delete duplicated jobs. keep the last executed one */
DELETE jeq
FROM [dbo].[jobExecutionQueue] jeq
inner JOIN  
	(
		SELECT    [project_id], [job_name], [task_id], [database_name]
				, COUNT(*) AS [job_count]
				, MAX([execution_date]) AS [execution_date]
				, MAX([id]) AS [last_id]
		FROM [dbo].[jobExecutionQueue]
		GROUP BY [project_id], [job_name], [task_id], [database_name]
	)jeqDup ON jeqDup.[project_id] = jeq.[project_id] AND jeqDup.[job_name] = jeq.[job_name] AND jeqDup.[task_id] = jeq.[task_id] AND ISNULL(jeqDup.[database_name], '') = ISNULL(jeq.[database_name], '')
				AND NOT (   jeqDup.[job_count] = 1 
						OR (jeqDup.[job_count] <> 1 AND jeq.[execution_date] IS NOT NULL AND jeqDup.[execution_date] = jeq.[execution_date])
						OR (jeqDup.[job_count] <> 1 AND jeq.[execution_date] IS NULL AND jeqDup.[last_id] = jeq.[id])
						)
GO

DELETE jeq
FROM (
		SELECT    [project_id], [job_name], [task_id], [database_name], [execution_date], [id]
				, ROW_NUMBER() OVER(PARTITION BY [project_id], [job_name], [task_id], [database_name] ORDER BY [id]) AS [row_no]
		FROM [dbo].[jobExecutionQueue]
	 ) jeq
inner JOIN  
	(
		SELECT    [project_id], [job_name], [task_id], [database_name]
				, COUNT(*) AS [job_count]
				, MAX([execution_date]) AS [execution_date]
				, MAX([id]) AS [last_id]
		FROM [dbo].[jobExecutionQueue]
		GROUP BY [project_id], [job_name], [task_id], [database_name]
	)jeqDup ON jeqDup.[project_id] = jeq.[project_id] AND jeqDup.[job_name] = jeq.[job_name] AND jeqDup.[task_id] = jeq.[task_id] AND ISNULL(jeqDup.[database_name], '') = ISNULL(jeq.[database_name], '')
				AND NOT (   jeqDup.[job_count] = 1 
						OR (jeqDup.[job_count] <> 1 AND jeq.[execution_date] IS NOT NULL AND jeqDup.[execution_date] = jeq.[execution_date])
						OR (jeqDup.[job_count] <> 1 AND jeq.[execution_date] IS NULL AND jeqDup.[last_id] = jeq.[id])
						)
WHERE jeq.[row_no] > 1
GO


IF EXISTS (SELECT * FROM sys.indexes WHERE [object_id] = OBJECT_ID(N'dbo.jobExecutionQueue') AND [name] = N'IX_jobExecutionQueue_JobQueue') 
	DROP INDEX [IX_jobExecutionQueue_JobQueue] ON [dbo].[jobExecutionQueue]
GO
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE [object_id] = OBJECT_ID(N'dbo.jobExecutionQueue') AND [name] = N'IX_jobExecutionQueue_JobQueue') 
	CREATE INDEX [IX_jobExecutionQueue_JobQueue] ON [dbo].[jobExecutionQueue]
			([for_instance_id], [project_id], [task_id], [database_name], [instance_id], [job_name], [module], [descriptor], [job_step_name], [job_database_name]) 
		INCLUDE
			([status], [event_date_utc], [priority])
		ON [FG_Statistics_Index]
GO
