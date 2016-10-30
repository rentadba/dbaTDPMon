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
	[id]					[bigint]		NOT NULL,
	[job_descriptor]		[varchar](256)	NOT NULL,
	[task_name]				[varchar](256)	NOT NULL
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
INSERT	INTO[maintenance-plan].[internalTasks] ([id], [job_descriptor], [task_name])
		SELECT    1, 'dbo.usp_mpDatabaseConsistencyCheck', 'Database Consistency Check' UNION ALL
		SELECT    2, 'dbo.usp_mpDatabaseConsistencyCheck', 'Allocation Consistency Check' UNION ALL
		SELECT    4, 'dbo.usp_mpDatabaseConsistencyCheck', 'Tables Consistency Check' UNION ALL
		SELECT    8, 'dbo.usp_mpDatabaseConsistencyCheck', 'Reference Consistency Check' UNION ALL
		SELECT   16, 'dbo.usp_mpDatabaseOptimize', 'Perform Correction to Space Usage' UNION ALL
		SELECT   32, 'dbo.usp_mpDatabaseOptimize', 'Rebuild Heap Tables' UNION ALL
		SELECT   64, 'dbo.usp_mpDatabaseOptimize', 'Rebuild or Reorganize Indexes' UNION ALL
		SELECT  128, 'dbo.usp_mpDatabaseOptimize', 'Update Statistics' UNION ALL
		SELECT  256, 'dbo.usp_mpDatabaseShrink', 'Shrink Database (TRUNCATEONLY)' UNION ALL
		SELECT  512, 'dbo.usp_mpDatabaseShrink', 'Shrink Log File' UNION ALL
		SELECT 1024, 'dbo.usp_mpDatabaseBackup', 'User Databases (diff)' UNION ALL
		SELECT 2048, 'dbo.usp_mpDatabaseBackup', 'User Databases (full)' UNION ALL
		SELECT 4096, 'dbo.usp_mpDatabaseBackup', 'System Databases (full)' UNION ALL
		SELECT 8192, 'dbo.usp_mpDatabaseBackup', 'User Databases Transaction Log'
GO