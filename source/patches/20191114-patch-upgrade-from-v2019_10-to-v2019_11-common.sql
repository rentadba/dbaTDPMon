SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2019.10 to 2019.11 (2019.11.14)				  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: common																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20191114-patch-upgrade-from-v2019_10-to-v2019_11-common.sql', 10, 1) WITH NOWAIT

IF EXISTS(SELECT * FROM sys.indexes WHERE [name]='IX_jobExecutionStatisticsHistory_Module_Descriptor' AND [object_id]=OBJECT_ID('[dbo].[jobExecutionStatisticsHistory]'))
	DROP INDEX [IX_jobExecutionStatisticsHistory_Module_Descriptor] ON [dbo].[jobExecutionStatisticsHistory]
GO
CREATE INDEX [IX_jobExecutionStatisticsHistory_Module_Descriptor] ON [dbo].[jobExecutionStatisticsHistory]([module], [descriptor], [project_id], [instance_id], [start_date], [task_id])
GO

IF EXISTS(SELECT * FROM sys.indexes WHERE [name]='IX_jobExecutionQueue_Descriptor' AND [object_id]=OBJECT_ID('[dbo].[jobExecutionQueue]'))
	DROP INDEX [IX_jobExecutionQueue_Descriptor] ON [dbo].[jobExecutionQueue]
GO
CREATE INDEX [IX_jobExecutionQueue_Descriptor] ON [dbo].[jobExecutionQueue]([project_id], [status], [module], [descriptor], [task_id]) INCLUDE ([instance_id], [for_instance_id], [job_name], [execution_date]) 
GO
