SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2018.3 to 2018.4 (2018.04.20)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: common																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20180420-patch-upgrade-from-v2018_3-to-v2018_4-common.sql', 10, 1) WITH NOWAIT

IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='jobExecutionStatisticsHistory' AND COLUMN_NAME='instance_id')
	ALTER TABLE [dbo].[jobExecutionStatisticsHistory] ADD [instance_id] [smallint] NULL
GO
IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_SCHEMA='dbo' AND CONSTRAINT_NAME='FK_jobExecutionStatisticsHistory_catalogInstanceNames')
	ALTER TABLE [dbo].[jobExecutionStatisticsHistory]
			ADD	CONSTRAINT [FK_jobExecutionStatisticsHistory_catalogInstanceNames] FOREIGN KEY 
			(
				[instance_id],
				[project_id]
			) 
			REFERENCES [dbo].[catalogInstanceNames] 
			(
				[id],
				[project_id]
			)
GO
IF NOT EXISTS(SELECT * FROM sys.indexes WHERE [name]='IX_jobExecutionStatisticsHistory_Instance_ID_ProjectID' AND [object_id]=OBJECT_ID('[dbo].[jobExecutionStatisticsHistory]'))
	CREATE INDEX [IX_jobExecutionStatisticsHistory_Instance_ID_ProjectID] ON [dbo].[jobExecutionStatisticsHistory](instance_id, [project_id]) ON [FG_Statistics_Index]
GO
