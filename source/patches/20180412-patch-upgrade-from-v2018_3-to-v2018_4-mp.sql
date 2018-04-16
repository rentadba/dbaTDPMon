SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2018.3 to 2018.4 (2018.04.12)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: maintenance-plan																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20180412-patch-upgrade-from-v2018_3-to-v2018_4-mp.sql', 10, 1) WITH NOWAIT

/* changes to [maintenance-plan].[objectSkipList] table */
IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_SCHEMA='maintenance-plan' AND CONSTRAINT_NAME='FK_MaintenancePlan_objectSkipList_MaintenancePlan_internalTasks')
	ALTER TABLE [maintenance-plan].[objectSkipList] DROP CONSTRAINT [FK_MaintenancePlan_objectSkipList_MaintenancePlan_internalTasks]
GO
IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_SCHEMA='maintenance-plan' AND CONSTRAINT_NAME='UK_objectSkipList_Name')
	ALTER TABLE [maintenance-plan].[objectSkipList] DROP CONSTRAINT [UK_objectSkipList_Name]
GO
IF EXISTS(SELECT * FROM sys.indexes WHERE [name]='IX_MaintenancePlan_objectSkipList_TaskID' AND [object_id]=OBJECT_ID('[maintenance-plan].[objectSkipList]'))
	DROP INDEX [IX_MaintenancePlan_objectSkipList_TaskID] ON [maintenance-plan].[objectSkipList]
GO

IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='maintenance-plan' AND TABLE_NAME='objectSkipList' AND COLUMN_NAME='task_id' AND DATA_TYPE='bigint')
	ALTER TABLE [maintenance-plan].[objectSkipList] ALTER COLUMN [task_id] [bigint] NOT NULL
GO

IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_SCHEMA='maintenance-plan' AND CONSTRAINT_NAME='FK_MaintenancePlan_objectSkipList_appInternalTasks')
	ALTER TABLE [maintenance-plan].[objectSkipList] 
			ADD	CONSTRAINT [FK_MaintenancePlan_objectSkipList_appInternalTasks] FOREIGN KEY 
			(
				[task_id]
			) 
			REFERENCES [dbo].[appInternalTasks] 
			(
				[id]
			)
GO

/* changes to [maintenance-plan].[internalScheduler] table */
IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_SCHEMA='maintenance-plan' AND CONSTRAINT_NAME='FK_internalScheduler_MaintenancePlan_internalTasks')
	ALTER TABLE [maintenance-plan].[internalScheduler] DROP CONSTRAINT [FK_internalScheduler_MaintenancePlan_internalTasks]
GO
IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_SCHEMA='maintenance-plan' AND CONSTRAINT_NAME='UK_internalScheduler')
	ALTER TABLE [maintenance-plan].[internalScheduler] DROP CONSTRAINT [UK_internalScheduler]
GO
IF EXISTS(SELECT * FROM sys.indexes WHERE [name]='IX_MaintenancePlan_internalScheduler_TaskID' AND [object_id]=OBJECT_ID('[maintenance-plan].[internalScheduler]'))
	DROP INDEX [IX_MaintenancePlan_internalScheduler_TaskID] ON [maintenance-plan].[internalScheduler]
GO

IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='maintenance-plan' AND TABLE_NAME='internalScheduler' AND COLUMN_NAME='task_id' AND DATA_TYPE='bigint')
	ALTER TABLE [maintenance-plan].[internalScheduler] ALTER COLUMN [task_id] [bigint] NOT NULL
GO

IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_SCHEMA='maintenance-plan' AND CONSTRAINT_NAME='UK_internalScheduler')
	ALTER TABLE [maintenance-plan].[internalScheduler] 
		ADD CONSTRAINT [UK_internalScheduler] UNIQUE
			(
					[project_id]
				, [task_id]
			) ON [PRIMARY]
GO
IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_SCHEMA='maintenance-plan' AND CONSTRAINT_NAME='FK_internalScheduler_appInternalTasks')
	ALTER TABLE [maintenance-plan].[internalScheduler] 
			ADD	CONSTRAINT [FK_internalScheduler_appInternalTasks] FOREIGN KEY 
			(
				[task_id]
			) 
			REFERENCES [dbo].[appInternalTasks] 
			(
				[id]
			)
GO
IF NOT EXISTS(SELECT * FROM sys.indexes WHERE [name]='IX_MaintenancePlan_internalScheduler_TaskID' AND [object_id]=OBJECT_ID('[maintenance-plan].[internalScheduler]'))
	CREATE INDEX [IX_MaintenancePlan_internalScheduler_TaskID] ON [maintenance-plan].[internalScheduler]
			([task_id], [project_id])
		INCLUDE
			([scheduled_weekday], [active])
		ON [FG_Statistics_Index]
GO

IF  EXISTS (SELECT * FROM sysobjects WHERE id = OBJECT_ID(N'[maintenance-plan].[internalTasks]') AND type in (N'U'))
	begin
		RAISERROR('	Drop table: [maintenance-plan].[internalTasks]', 10, 1) WITH NOWAIT;
		DROP TABLE [maintenance-plan].[internalTasks];
	end
GO
