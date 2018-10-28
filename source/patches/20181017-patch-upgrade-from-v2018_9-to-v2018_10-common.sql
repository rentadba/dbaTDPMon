SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2018.9 to 2018.10 (2018.10.17)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: common																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20181017-patch-upgrade-from-v2018_9-to-v2018_10-common.sql', 10, 1) WITH NOWAIT

IF EXISTS (SELECT * FROM sys.indexes WHERE [object_id] = OBJECT_ID(N'dbo.jobExecutionQueue') AND [name] = N'IX_jobExecutionQueue_JobQueue') 
	DROP INDEX [IX_jobExecutionQueue_JobQueue] ON [dbo].[jobExecutionQueue]
GO
CREATE INDEX [IX_jobExecutionQueue_JobQueue] ON [dbo].[jobExecutionQueue]
		([for_instance_id], [project_id], [instance_id], [job_name], [module], [descriptor], [job_step_name], [job_database_name]) 
	INCLUDE
		([status], [event_date_utc], [priority], [execution_date])
	ON [FG_Statistics_Index]
GO
