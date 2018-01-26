SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2016.9 to 2016.11 (2016.10.25)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: maintenance-plan																							   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20161025-patch-upgrade-from-v2016_9-to-v2016_11-mp.sql', 10, 1) WITH NOWAIT


IF NOT EXISTS (SELECT * FROM sysobjects WHERE id = OBJECT_ID(N'[maintenance-plan].[internalTasks]') AND type in (N'U'))
	begin
		RAISERROR('	Create table: [maintenance-plan].[internalTasks]', 10, 1) WITH NOWAIT

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
		) ON [PRIMARY];


		RAISERROR('		...insert default data', 10, 1) WITH NOWAIT

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
	end
GO

IF NOT EXISTS (SELECT * FROM sysobjects WHERE id = OBJECT_ID(N'[maintenance-plan].[internalScheduler]') AND type in (N'U'))
	begin
		RAISERROR('	Create table: [maintenance-plan].[internalScheduler]', 10, 1) WITH NOWAIT

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
			) ON [PRIMARY],
			CONSTRAINT [UK_internalScheduler] UNIQUE
			(
				  [project_id]
				, [task_id]
			) ON [PRIMARY],
			CONSTRAINT [FK_internalScheduler_catalogProjects] FOREIGN KEY 
			(
				[project_id]
			) 
			REFERENCES [dbo].[catalogProjects] 
			(
				[id]
			),
			CONSTRAINT [FK_internalScheduler_MaintenancePlan_internalTasks] FOREIGN KEY 
			(
				[task_id]
			) 
			REFERENCES [maintenance-plan].[internalTasks]
			(
				[id]
			)	
		) ON [PRIMARY];

		CREATE INDEX [IX_MaintenancePlan_internalScheduler_TaskID] ON [maintenance-plan].[internalScheduler]
				([task_id], [project_id])
			INCLUDE
				([scheduled_weekday], [active])
			ON [FG_Statistics_Index];

		RAISERROR('		...insert default data', 10, 1) WITH NOWAIT

		INSERT	INTO[maintenance-plan].[internalScheduler] ([project_id], [task_id], [scheduled_weekday], [active])
				SELECT NULL,    1, 'Saturday', 1 UNION ALL
				SELECT NULL,    2, 'Daily', 1 UNION ALL
				SELECT NULL,    4, 'Sunday', 1 UNION ALL
				SELECT NULL,    8, 'Sunday', 1 UNION ALL
				SELECT NULL,   16, 'N/A', 1 UNION ALL
				SELECT NULL,   32, 'Daily', 1 UNION ALL
				SELECT NULL,   64, 'Daily', 1 UNION ALL
				SELECT NULL,  128, 'Daily', 1 UNION ALL
				SELECT NULL,  256, 'Sunday', 1 UNION ALL
				SELECT NULL,  512, 'Saturday', 1 UNION ALL
				SELECT NULL, 1024, 'Daily', 1 UNION ALL
				SELECT NULL, 2048, 'Saturday', 1 UNION ALL
				SELECT NULL, 4096, 'Saturday', 1 UNION ALL
				SELECT NULL, 8192, 'Daily', 1;
	end