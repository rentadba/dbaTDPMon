SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2018.9 to 2018.10 (2018.10.10)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: common																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20181010-patch-upgrade-from-v2018_9-to-v2018_10-common.sql', 10, 1) WITH NOWAIT

IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='jobExecutionQueue' AND COLUMN_NAME='priority' AND DATA_TYPE = 'int')
	begin
		EXEC ('DROP INDEX [IX_jobExecutionQueue_JobQueue] ON [dbo].[jobExecutionQueue]');
		EXEC ('ALTER TABLE [dbo].[jobExecutionQueue] DROP CONSTRAINT [DF_jobExecutionQueue_Priority]');
		EXEC ('ALTER TABLE [dbo].[jobExecutionQueue] ALTER COLUMN [priority] [int] NOT NULL');
		EXEC ('ALTER TABLE [dbo].[jobExecutionQueue] ADD CONSTRAINT [DF_jobExecutionQueue_Priority] DEFAULT (1) FOR [priority]');
		EXEC ('CREATE INDEX [IX_jobExecutionQueue_JobQueue] ON [dbo].[jobExecutionQueue]([for_instance_id], [project_id], [task_id], [database_name], [instance_id], [job_name], [module], [descriptor], [job_step_name], [job_database_name]) INCLUDE([status], [event_date_utc], [priority])ON [FG_Statistics_Index]');
	end
GO
