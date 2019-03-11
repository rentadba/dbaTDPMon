SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2019.1 to 2019.3 (2019.03.11)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: common																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20190311-patch-upgrade-from-v2019_1-to-v2019_3-common.sql', 10, 1) WITH NOWAIT

IF EXISTS(SELECT * FROM sys.indexes WHERE [name] = 'IX_jobExecutionHistory_InstanceID' AND [object_id]=OBJECT_ID('[dbo].[jobExecutionHistory]'))
	DROP INDEX [IX_jobExecutionHistory_InstanceID] ON [dbo].[jobExecutionHistory]
GO

IF EXISTS(SELECT * FROM sys.indexes WHERE [name] = 'IX_jobExecutionHistory' AND [object_id]=OBJECT_ID('[dbo].[jobExecutionHistory]'))
	DROP INDEX [IX_jobExecutionHistory] ON [dbo].[jobExecutionHistory]
GO

IF EXISTS(SELECT * FROM sys.indexes WHERE [name] = 'IX_jobExecutionQueue_InstanceID' AND [object_id]=OBJECT_ID('[dbo].[jobExecutionQueue]'))
	DROP INDEX [IX_jobExecutionQueue_InstanceID] ON [dbo].[jobExecutionQueue]
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE [object_id] = OBJECT_ID('dbo.logEventMessages') AND [name] = 'IX_logEventMessages_project_id') 
	CREATE INDEX [IX_logEventMessages_project_id] ON [dbo].[logEventMessages] 
			([project_id], [instance_id], [is_email_sent]) 
		INCLUDE 
			([event_date_utc]) 
		ON [FG_Statistics_Index]
GO
