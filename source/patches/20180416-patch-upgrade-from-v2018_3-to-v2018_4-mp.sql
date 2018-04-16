SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2018.3 to 2018.4 (2018.04.16)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: maintenance-plan																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20180416-patch-upgrade-from-v2018_3-to-v2018_4-mp.sql', 10, 1) WITH NOWAIT

IF NOT EXISTS(SELECT * FROM [dbo].[appConfigurations] WHERE [name] = 'Use Default Scheduler for maintenance tasks if project specific not defined' and [module] = 'maintenance-plan')
	INSERT	INTO [dbo].[appConfigurations] ([module], [name], [value])
			SELECT 'maintenance-plan' AS [module], 'Use Default Scheduler for maintenance tasks if project specific not defined'	AS [name], '1' AS [value]
GO

/* changes to [maintenance-plan].[objectSkipList] table */
IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='maintenance-plan' AND TABLE_NAME='objectSkipList' AND COLUMN_NAME='instance_name')
	ALTER TABLE [maintenance-plan].[objectSkipList] ADD [instance_name] [sysname] NOT NULL
GO
IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='maintenance-plan' AND TABLE_NAME='objectSkipList' AND COLUMN_NAME='database_name')
	ALTER TABLE [maintenance-plan].[objectSkipList] ADD [database_name] [sysname] NOT NULL
GO
IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_SCHEMA='maintenance-plan' AND CONSTRAINT_NAME='FK_MaintenancePlan_objectSkipList_catalogProjects')
	ALTER TABLE [maintenance-plan].[objectSkipList] DROP CONSTRAINT [FK_MaintenancePlan_objectSkipList_catalogProjects]
GO
IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_SCHEMA='maintenance-plan' AND CONSTRAINT_NAME='UK_objectSkipList_Name')
	ALTER TABLE [maintenance-plan].[objectSkipList] DROP CONSTRAINT [UK_objectSkipList_Name]
GO
IF EXISTS(SELECT * FROM sys.indexes WHERE [name]='IX_MaintenancePlan_objectSkipList_TaskID' AND [object_id]=OBJECT_ID('[maintenance-plan].[objectSkipList]'))
	DROP INDEX [IX_MaintenancePlan_objectSkipList_TaskID] ON [maintenance-plan].[objectSkipList]
GO
IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='maintenance-plan' AND TABLE_NAME='objectSkipList' AND COLUMN_NAME='project_id')
	ALTER TABLE [maintenance-plan].[objectSkipList] DROP COLUMN [project_id]
GO
IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='maintenance-plan' AND TABLE_NAME='objectSkipList' AND COLUMN_NAME='object_type')
	ALTER TABLE [maintenance-plan].[objectSkipList] DROP COLUMN [object_type]
GO
IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_SCHEMA='maintenance-plan' AND CONSTRAINT_NAME='UK_objectSkipList_Name')
	ALTER TABLE [maintenance-plan].[objectSkipList] 
		ADD CONSTRAINT [UK_objectSkipList_Name] UNIQUE  NONCLUSTERED 
		(
			[instance_name],
			[database_name],
			[task_id],
			[schema_name],
			[object_name]
		) ON [PRIMARY]
GO
IF NOT EXISTS(SELECT * FROM sys.indexes WHERE [name]='IX_MaintenancePlan_objectSkipList_TaskID' AND [object_id]=OBJECT_ID('[maintenance-plan].[objectSkipList]'))
	CREATE INDEX [IX_MaintenancePlan_objectSkipList_TaskID] ON [maintenance-plan].[objectSkipList]
			([task_id])
		ON [FG_Statistics_Index]
GO
IF NOT EXISTS(SELECT * FROM sys.indexes WHERE [name]='IX_MaintenancePlan_objectSkipList_DatabaseName_TaskID_ObjectType' AND [object_id]=OBJECT_ID('[maintenance-plan].[objectSkipList]'))
	CREATE INDEX [IX_MaintenancePlan_objectSkipList_DatabaseName_TaskID_ObjectType] ON [maintenance-plan].[objectSkipList]
			([instance_name], [database_name], [task_id])
		INCLUDE
			([schema_name], [object_name])
		ON [FG_Statistics_Index]
GO
