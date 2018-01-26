SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2017.6 to 2017.12 (2017.12.22)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: maintenance-plan																					   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20180112-patch-upgrade-from-v2017_6-to-v2017_12-mp.sql', 10, 1) WITH NOWAIT

IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE WHERE [CONSTRAINT_NAME] = 'FK_internalScheduler_MaintenancePlan_internalTasks')
	ALTER TABLE [maintenance-plan].[internalScheduler] DROP CONSTRAINT [FK_internalScheduler_MaintenancePlan_internalTasks]
GO
IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE WHERE [CONSTRAINT_NAME] = 'FK_MaintenancePlan_objectSkipList_MaintenancePlan_internalTasks')
	ALTER TABLE [maintenance-plan].[objectSkipList] DROP CONSTRAINT [FK_MaintenancePlan_objectSkipList_MaintenancePlan_internalTasks]
GO

IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE WHERE [CONSTRAINT_NAME] = 'PK_internalTasks')
	ALTER TABLE [maintenance-plan].[internalTasks] DROP CONSTRAINT [PK_internalTasks]
GO

ALTER TABLE [maintenance-plan].[internalTasks] ALTER COLUMN [id] [smallint]	NOT NULL
GO

ALTER TABLE [maintenance-plan].[internalTasks] ADD CONSTRAINT [PK_internalTasks] PRIMARY KEY  CLUSTERED 
	(
		[id]
	) ON [PRIMARY]
GO

IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE [TABLE_SCHEMA]='maintenance-plan' AND [TABLE_NAME]='internalTasks' AND [COLUMN_NAME]='flg_actions')
	begin
		EXEC (' ALTER TABLE [maintenance-plan].[internalTasks] ADD [flg_actions] [smallint]	NULL');

		EXEC (' UPDATE [maintenance-plan].[internalTasks] SET [flg_actions] =  1 WHERE [id] = 1;
				UPDATE [maintenance-plan].[internalTasks] SET [flg_actions] = 12 WHERE [id] = 2;
				UPDATE [maintenance-plan].[internalTasks] SET [flg_actions] =  2 WHERE [id] = 4;
				UPDATE [maintenance-plan].[internalTasks] SET [flg_actions] = 16 WHERE [id] = 8;
				UPDATE [maintenance-plan].[internalTasks] SET [flg_actions] = 64 WHERE [id] = 16;
				UPDATE [maintenance-plan].[internalTasks] SET [flg_actions] = 16 WHERE [id] = 32;
				UPDATE [maintenance-plan].[internalTasks] SET [flg_actions] =  3 WHERE [id] = 64;
				UPDATE [maintenance-plan].[internalTasks] SET [flg_actions] =  8 WHERE [id] = 128;
				UPDATE [maintenance-plan].[internalTasks] SET [flg_actions] =  2 WHERE [id] = 256;
				UPDATE [maintenance-plan].[internalTasks] SET [flg_actions] =  1 WHERE [id] = 512;
				UPDATE [maintenance-plan].[internalTasks] SET [flg_actions] =  2 WHERE [id] = 1024;
				UPDATE [maintenance-plan].[internalTasks] SET [flg_actions] =  1 WHERE [id] = 2048;
				UPDATE [maintenance-plan].[internalTasks] SET [flg_actions] =  1 WHERE [id] = 4096;
				UPDATE [maintenance-plan].[internalTasks] SET [flg_actions] =  4 WHERE [id] = 8192;');

		EXEC (' ALTER TABLE [maintenance-plan].[internalTasks] ALTER COLUMN [flg_actions] [smallint] NOT NULL');
	end
GO

UPDATE [maintenance-plan].[internalTasks] SET [job_descriptor] = 'dbo.usp_mpDatabaseConsistencyCheck' WHERE [id] = 16
GO

IF EXISTS (SELECT * FROM sys.indexes WHERE [name] = 'IX_MaintenancePlan_internalScheduler_TaskID' AND [object_id] = OBJECT_ID('[maintenance-plan].[internalScheduler]'))
	DROP INDEX [IX_MaintenancePlan_internalScheduler_TaskID] ON [maintenance-plan].[internalScheduler]
GO

IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE WHERE [CONSTRAINT_NAME] = 'UK_internalScheduler')
	ALTER TABLE [maintenance-plan].[internalScheduler] DROP CONSTRAINT [UK_internalScheduler]
GO

ALTER TABLE [maintenance-plan].[internalScheduler] ALTER COLUMN [task_id] [smallint] NOT NULL
GO	

ALTER TABLE [maintenance-plan].[internalScheduler] ADD
	CONSTRAINT [FK_internalScheduler_MaintenancePlan_internalTasks] FOREIGN KEY 
	(
		[task_id]
	) 
	REFERENCES [maintenance-plan].[internalTasks]
	(
		[id]
	)	
GO

ALTER TABLE [maintenance-plan].[internalScheduler] ADD CONSTRAINT [UK_internalScheduler] UNIQUE
	(
		  [project_id]
		, [task_id]
	) ON [PRIMARY];
	

CREATE INDEX [IX_MaintenancePlan_internalScheduler_TaskID] ON [maintenance-plan].[internalScheduler]
		([task_id], [project_id])
	INCLUDE
		([scheduled_weekday], [active])
	ON [FG_Statistics_Index]
GO


IF NOT EXISTS (SELECT * FROM sysobjects WHERE id = OBJECT_ID(N'[maintenance-plan].[objectSkipList]') AND type in (N'U'))
	begin 
		RAISERROR('	Create table: [maintenance-plan].[objectSkipList]', 10, 1) WITH NOWAIT

		CREATE TABLE [maintenance-plan].[objectSkipList] 
		(
			[id]					[int]			IDENTITY (1, 1)	NOT NULL,
			[project_id]			[smallint]		NULL,
			[task_id]				[smallint]		NOT NULL,
			[schema_name]			[sysname]		NOT NULL,
			[object_name]			[sysname]		NOT NULL,
			CONSTRAINT [PK_objectSkipList] PRIMARY KEY  CLUSTERED 
			(
				[id]
			) ON [PRIMARY],
			CONSTRAINT [UK_objectSkipList_Name] UNIQUE  NONCLUSTERED 
			(
				[project_id],
				[task_id],
				[schema_name],
				[object_name]
			) ON [PRIMARY],
			CONSTRAINT [FK_MaintenancePlan_objectSkipList_catalogProjects] FOREIGN KEY 
			(
				[project_id]
			) 
			REFERENCES [dbo].[catalogProjects] 
			(
				[id]
			),
			CONSTRAINT [FK_MaintenancePlan_objectSkipList_MaintenancePlan_internalTasks] FOREIGN KEY 
			(
				[task_id]
			) 
			REFERENCES [maintenance-plan].[internalTasks]
			(
				[id]
			)
		)  ON [PRIMARY];

		CREATE INDEX [IX_MaintenancePlan_objectSkipList_TaskID] ON [maintenance-plan].[objectSkipList]
				([task_id], [project_id])
			INCLUDE
				([schema_name], [object_name])
			ON [FG_Statistics_Index]
	end
ELSE
	IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE WHERE [CONSTRAINT_NAME] = 'FK_MaintenancePlan_objectSkipList_MaintenancePlan_internalTasks')
			ALTER TABLE [maintenance-plan].[objectSkipList] ADD 
				CONSTRAINT [FK_MaintenancePlan_objectSkipList_MaintenancePlan_internalTasks] FOREIGN KEY 
				(
					[task_id]
				) 
				REFERENCES [maintenance-plan].[internalTasks]
				(
					[id]
				)
GO	

IF EXISTS (SELECT * FROM sys.indexes WHERE [name] = 'IX_MaintenancePlan_objectSkipList_TaskID' AND [object_id] = OBJECT_ID('[maintenance-plan].[objectSkipList]') AND [data_space_id] = 1)
	begin
		DROP INDEX [IX_MaintenancePlan_objectSkipList_TaskID] ON [maintenance-plan].[objectSkipList];

		CREATE INDEX [IX_MaintenancePlan_objectSkipList_TaskID] ON [maintenance-plan].[objectSkipList]
				([task_id], [project_id])
			INCLUDE
				([schema_name], [object_name])
			ON [FG_Statistics_Index];
	end
GO