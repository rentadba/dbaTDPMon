SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2019.11 to 2019.12 (2019.12.03)				  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: common																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20191203-patch-upgrade-from-v2019_11-to-v2019_12-common.sql', 10, 1) WITH NOWAIT

IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='jobExecutionHistory' AND COLUMN_NAME='id' AND DATA_TYPE = 'bigint')
begin
	EXEC ('IF EXISTS (SELECT * FROM sys.indexes WHERE [object_id] = OBJECT_ID(''dbo.jobExecutionHistory'') AND [name] = ''IX_jobExecutionHistory_InstanceID'') DROP INDEX [IX_jobExecutionHistory_InstanceID] ON [dbo].[jobExecutionHistory]');
	EXEC ('IF EXISTS (SELECT * FROM sys.indexes WHERE [object_id] = OBJECT_ID(''dbo.jobExecutionHistory'') AND [name] = ''IX_jobExecutionHistory_ProjectID'') DROP INDEX [IX_jobExecutionHistory_ProjectID] ON [dbo].[jobExecutionHistory]');
	EXEC ('IF EXISTS (SELECT * FROM sys.indexes WHERE [object_id] = OBJECT_ID(''dbo.jobExecutionHistory'') AND [name] = ''IX_jobExecutionHistory_JobName'') DROP INDEX [IX_jobExecutionHistory_JobName] ON [dbo].[jobExecutionHistory]');
	EXEC ('IF EXISTS (SELECT * FROM sys.indexes WHERE [object_id] = OBJECT_ID(''dbo.jobExecutionHistory'') AND [name] = ''IX_jobExecutionHistory_Descriptor'') DROP INDEX [IX_jobExecutionHistory_Descriptor] ON [dbo].[jobExecutionHistory]');
	EXEC ('IF EXISTS (SELECT * FROM sys.indexes WHERE [object_id] = OBJECT_ID(''dbo.jobExecutionHistory'') AND [name] = ''IX_jobExecutionHistory'') DROP INDEX [IX_jobExecutionHistory] ON [dbo].[jobExecutionHistory]');
	EXEC ('ALTER TABLE [dbo].[jobExecutionHistory] DROP CONSTRAINT [PK_jobExecutionHistory]');
	EXEC ('ALTER TABLE [dbo].[jobExecutionHistory] ALTER COLUMN [id] [bigint] NOT NULL');
	EXEC ('ALTER TABLE [dbo].[jobExecutionHistory] ADD CONSTRAINT [PK_jobExecutionHistory] PRIMARY KEY CLUSTERED ([id])');
	EXEC ('CREATE INDEX [IX_jobExecutionHistory_InstanceID] ON [dbo].[jobExecutionHistory]([instance_id], [project_id])');
	EXEC ('CREATE INDEX [IX_jobExecutionHistory_ProjectID] ON [dbo].[jobExecutionHistory] ([project_id], [event_date_utc]) INCLUDE ([instance_id])');
	EXEC ('CREATE INDEX [IX_jobExecutionHistory_JobName] ON [dbo].[jobExecutionHistory]([job_name], [job_step_name])');
	EXEC ('CREATE INDEX [IX_jobExecutionHistory_Descriptor] ON [dbo].[jobExecutionHistory]([project_id], [status], [module], [descriptor]) INCLUDE ([instance_id], [for_instance_id], [job_name])');
end
GO

IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='jobExecutionHistory' AND COLUMN_NAME='remote_id')
begin
	EXEC ('ALTER TABLE [dbo].[jobExecutionHistory] ADD [remote_id] [bigint] NULL');
end
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE [object_id] = OBJECT_ID('dbo.jobExecutionHistory') AND [name] = 'IX_jobExecutionHistory_RemoteID') 
begin
	EXEC ('CREATE INDEX [IX_jobExecutionHistory_RemoteID] ON [dbo].[jobExecutionHistory]([remote_id], [instance_id], [project_id])');
end
GO

IF EXISTS(SELECT * FROM sys.foreign_keys WHERE [name]='FK_jobExecutionHistory_ForInstanceID_catalogInstanceNames' AND [parent_object_id] = OBJECT_ID('dbo.jobExecutionHistory'))
begin
	EXEC ('ALTER TABLE [dbo].[jobExecutionHistory] DROP CONSTRAINT [FK_jobExecutionHistory_ForInstanceID_catalogInstanceNames]');
end
GO


