-- ============================================================================
-- Copyright (c) 2004-2017 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 14.06.2017
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-----------------------------------------------------------------------------------------------------
--list of the objects (databases, tables, index name or stats name) to be excluded from maintenance.
-----------------------------------------------------------------------------------------------------
RAISERROR('Create table: [maintenance-plan].[objectSkipList]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sysobjects WHERE id = OBJECT_ID(N'[maintenance-plan].[objectSkipList]') AND type in (N'U'))
DROP TABLE [maintenance-plan].[objectSkipList]
GO

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
	),
	CONSTRAINT [FK_MaintenancePlan_objectSkipList_appInternalTasks] FOREIGN KEY 
	(
		[task_id]
	) 
	REFERENCES [dbo].[appInternalTasks]
	(
		[id]
	)
)  ON [PRIMARY]
GO

CREATE INDEX [IX_MaintenancePlan_objectSkipList_TaskID] ON [maintenance-plan].[objectSkipList]
		([task_id], [project_id])
	INCLUDE
		([schema_name], [object_name])
	ON [FG_Statistics_Index]
GO
