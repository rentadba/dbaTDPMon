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
		SELECT [job_name], [task_id], [database_name], COUNT(*) AS [job_count], MAX([execution_date]) AS [execution_date]
		FROM [dbo].[jobExecutionQueue]
		GROUP BY [job_name], [task_id], [database_name]
	)jeqDup ON jeqDup.[job_name] = jeq.[job_name] AND jeqDup.[task_id] = jeq.[task_id] AND ISNULL(jeqDup.[database_name], '') = ISNULL(jeq.[database_name], '')
				AND NOT (   jeqDup.[job_count] = 1 
						OR (jeqDup.[job_count] <> 1 AND jeqDup.[execution_date] = jeq.[execution_date])
						)
GO
