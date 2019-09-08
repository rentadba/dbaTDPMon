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
	[instance_name]			[sysname]		NOT NULL,
	[database_name]			[sysname]		NOT NULL,
	[task_id]				[bigint]		NOT NULL,
	[schema_name]			[sysname]		NOT NULL,
	[object_name]			[sysname]		NOT NULL,
	[active]				[bit]			NOT NULL CONSTRAINT [DF_objectSkipList_Active] DEFAULT (1),
	CONSTRAINT [PK_objectSkipList] PRIMARY KEY  CLUSTERED 
	(
		[id]
	),
	CONSTRAINT [UK_objectSkipList_Name] UNIQUE  NONCLUSTERED 
	(
		[instance_name],
		[database_name],
		[task_id],
		[schema_name],
		[object_name]
	),
	CONSTRAINT [FK_MaintenancePlan_objectSkipList_appInternalTasks] FOREIGN KEY 
	(
		[task_id]
	) 
	REFERENCES [dbo].[appInternalTasks]
	(
		[id]
	)
)
GO

CREATE INDEX [IX_MaintenancePlan_objectSkipList_TaskID] ON [maintenance-plan].[objectSkipList]
		([task_id])
GO
