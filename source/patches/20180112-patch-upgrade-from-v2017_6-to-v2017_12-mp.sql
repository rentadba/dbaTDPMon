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

IF NOT EXISTS (SELECT * FROM sysobjects WHERE id = OBJECT_ID(N'[maintenance-plan].[objectSkipList]') AND type in (N'U'))
	begin 
		RAISERROR('	Create table: [maintenance-plan].[objectSkipList]', 10, 1) WITH NOWAIT

		CREATE TABLE [maintenance-plan].[objectSkipList] 
		(
			[id]					[int]			IDENTITY (1, 1)	NOT NULL,
			[project_id]			[smallint]		NULL,
			[task_id]				[bigint]		NOT NULL,
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
			)
		)  ON [PRIMARY];

		CREATE INDEX [IX_MaintenancePlan_objectSkipList_TaskID] ON [maintenance-plan].[objectSkipList]
				([task_id], [project_id])
			INCLUDE
				([schema_name], [object_name])
			ON [FG_Statistics_Index]
	end
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