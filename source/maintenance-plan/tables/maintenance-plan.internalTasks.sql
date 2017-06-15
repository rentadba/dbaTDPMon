-- ============================================================================
-- Copyright (c) 2004-2016 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 25.10.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================
-- table will maintenance tasks and their default internal values
-----------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------
--
-----------------------------------------------------------------------------------------------------
RAISERROR('Create table: [maintenance-plan].[internalTasks]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sysobjects WHERE id = OBJECT_ID(N'[maintenance-plan].[internalTasks]') AND type in (N'U'))
DROP TABLE [maintenance-plan].[internalTasks]
GO

CREATE TABLE [maintenance-plan].[internalTasks]
(
	[id]					[smallint]		NOT NULL,
	[job_descriptor]		[varchar](256)	NOT NULL,
	[task_name]				[varchar](256)	NOT NULL,
	[flg_actions]			[smallint]		NOT NULL,
	CONSTRAINT [PK_internalTasks] PRIMARY KEY  CLUSTERED 
	(
		[id]
	) ON [PRIMARY],
	CONSTRAINT [UK_internalTasks] UNIQUE
	(
		  [job_descriptor]
		, [task_name]
	) ON [PRIMARY]
) ON [PRIMARY]
GO

-----------------------------------------------------------------------------------------------------
RAISERROR('		...insert default data', 10, 1) WITH NOWAIT
GO
SET NOCOUNT ON
GO
INSERT	INTO [maintenance-plan].[internalTasks] ([id], [job_descriptor], [task_name], [flg_actions])
		SELECT    1, 'dbo.usp_mpDatabaseConsistencyCheck', 'Database Consistency Check', 1 UNION ALL
		SELECT    2, 'dbo.usp_mpDatabaseConsistencyCheck', 'Allocation Consistency Check', 12 UNION ALL
		SELECT    4, 'dbo.usp_mpDatabaseConsistencyCheck', 'Tables Consistency Check', 2 UNION ALL
		SELECT    8, 'dbo.usp_mpDatabaseConsistencyCheck', 'Reference Consistency Check', 16 UNION ALL
		SELECT   16, 'dbo.usp_mpDatabaseConsistencyCheck', 'Perform Correction to Space Usage', 64 UNION ALL
		SELECT   32, 'dbo.usp_mpDatabaseOptimize', 'Rebuild Heap Tables', 16 UNION ALL
		SELECT   64, 'dbo.usp_mpDatabaseOptimize', 'Rebuild or Reorganize Indexes', 3 UNION ALL
		SELECT  128, 'dbo.usp_mpDatabaseOptimize', 'Update Statistics', 8 UNION ALL
		SELECT  256, 'dbo.usp_mpDatabaseShrink', 'Shrink Database (TRUNCATEONLY)', 2 UNION ALL
		SELECT  512, 'dbo.usp_mpDatabaseShrink', 'Shrink Log File', 1 UNION ALL
		SELECT 1024, 'dbo.usp_mpDatabaseBackup', 'User Databases (diff)', 2 UNION ALL
		SELECT 2048, 'dbo.usp_mpDatabaseBackup', 'User Databases (full)' , 1UNION ALL
		SELECT 4096, 'dbo.usp_mpDatabaseBackup', 'System Databases (full)', 1 UNION ALL
		SELECT 8192, 'dbo.usp_mpDatabaseBackup', 'User Databases Transaction Log', 4
GO