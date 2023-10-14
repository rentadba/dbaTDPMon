-- ============================================================================
-- Copyright (c) 2004-2016 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 25.10.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================
-- table will maintenance tasks and their default schedule per project
-----------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------
--
-----------------------------------------------------------------------------------------------------
RAISERROR('Create table: [maintenance-plan].[internalScheduler]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sysobjects WHERE id = OBJECT_ID(N'[maintenance-plan].[internalScheduler]') AND type in (N'U'))
DROP TABLE [maintenance-plan].[internalScheduler]
GO

CREATE TABLE [maintenance-plan].[internalScheduler]
(
	[id]					[bigint] IDENTITY (1, 1)NOT NULL,
	[project_id]			[smallint]		NULL,
	[task_id]				[bigint]		NOT NULL,
	[scheduled_weekday]		[varchar](16)	NOT NULL,
	[active]				[bit]			NOT NULL CONSTRAINT [DF_internalScheduler_Active] DEFAULT (1),
	CONSTRAINT [PK_internalScheduler] PRIMARY KEY  CLUSTERED 
	(
		[id]
	),
	CONSTRAINT [UK_internalScheduler] UNIQUE
	(
		  [project_id]
		, [task_id]
	),
	CONSTRAINT [FK_internalScheduler_catalogProjects] FOREIGN KEY 
	(
		[project_id]
	) 
	REFERENCES [dbo].[catalogProjects] 
	(
		[id]
	),
	CONSTRAINT [FK_internalScheduler_appInternalTasks] FOREIGN KEY 
	(
		[task_id]
	) 
	REFERENCES [dbo].[appInternalTasks]
	(
		[id]
	)	
)
GO


CREATE INDEX [IX_MaintenancePlan_internalScheduler_TaskID] ON [maintenance-plan].[internalScheduler]
		([task_id], [project_id])
	INCLUDE
		([scheduled_weekday], [active])
GO
