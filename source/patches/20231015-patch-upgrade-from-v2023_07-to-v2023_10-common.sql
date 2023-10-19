SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2023.07 to 2023.10 (2023.10.15)				  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: common																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20231015-patch-upgrade-from-v2023_07-to-v2023_10-common.sql', 10, 1) WITH NOWAIT

IF EXISTS (SELECT * FROM sys.indexes WHERE [object_id] = OBJECT_ID('dbo.jobExecutionQueue') AND [name] = 'IX_jobExecutionQueue_running_time_sec') 
	DROP INDEX [IX_jobExecutionQueue_running_time_sec] ON [dbo].[jobExecutionQueue];
GO
CREATE INDEX [IX_jobExecutionQueue_running_time_sec] ON [dbo].[jobExecutionQueue] ([running_time_sec], [for_instance_id], [project_id])
GO

IF EXISTS (SELECT * FROM sys.indexes WHERE [object_id] = OBJECT_ID(N'dbo.jobExecutionHistory') AND [name] = N'IX_jobExecutionHistory')
	DROP INDEX [IX_jobExecutionHistory] ON [dbo].[jobExecutionHistory];
GO

IF EXISTS (SELECT * FROM sys.indexes WHERE [object_id] = OBJECT_ID(N'dbo.jobExecutionHistory') AND [name] = N'IX_jobExecutionHistory_JobName') 
	DROP INDEX [IX_jobExecutionHistory_JobName] ON [dbo].[jobExecutionHistory];
GO

IF EXISTS (SELECT * FROM sys.indexes WHERE [object_id] = OBJECT_ID(N'dbo.jobExecutionHistory') AND [name] = N'IX_jobExecutionHistory_RemoteID') 
	DROP INDEX [IX_jobExecutionHistory_RemoteID] ON [dbo].[jobExecutionHistory];
GO
