SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2018.3 to 2018.4 (2018.04.05)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: common																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20180405-patch-upgrade-from-v2018_3-to-v2018_4-common.sql', 10, 1) WITH NOWAIT

/* merge code from external implementation for table: dbo.logEventMessages */
IF EXISTS(SELECT * FROM sys.indexes WHERE [name]='IX_logEventMessages_EventType' AND [object_id]=OBJECT_ID('[dbo].[logEventMessages]'))
	DROP INDEX [IX_logEventMessages_EventType] ON [dbo].[logEventMessages]
GO
IF EXISTS(SELECT * FROM sys.indexes WHERE [name]='ix_logEventMessages_event_type_event_date_utc_instance_id' AND [object_id]=OBJECT_ID('[dbo].[logEventMessages]'))
	DROP INDEX [ix_logEventMessages_event_type_event_date_utc_instance_id] ON [dbo].[logEventMessages]
GO

IF NOT EXISTS(SELECT * FROM sys.indexes WHERE [name]='IX_logEventMessages_EventType_EventDateUTC_Instance_ID' AND [object_id]=OBJECT_ID('[dbo].[logEventMessages]'))
	CREATE INDEX [IX_logEventMessages_EventType_EventDateUTC_Instance_ID] ON [dbo].[logEventMessages] ([event_type], [event_date_utc], [instance_id]) ON [FG_Statistics_Index]
GO


/* merge code from external implementation for table: dbo.jobExecutionQueue */
IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='jobExecutionQueue' AND COLUMN_NAME='task_id')
	ALTER TABLE [dbo].[jobExecutionQueue] ADD [task_id]	[smallint] NULL
GO
IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='jobExecutionQueue' AND COLUMN_NAME='task_id' AND DATA_TYPE='smallint')
	ALTER TABLE [dbo].[jobExecutionQueue] ALTER COLUMN [task_id] [smallint] NULL
GO
IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='jobExecutionQueue' AND COLUMN_NAME='database_name')
	ALTER TABLE [dbo].[jobExecutionQueue] ADD [database_name] [sysname] NULL
GO
IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='jobExecutionQueue' AND COLUMN_NAME='database_name' AND DATA_TYPE='nvarchar')
	ALTER TABLE [dbo].[jobExecutionQueue] ALTER COLUMN [database_name] [sysname] NULL
GO

RAISERROR('	Alter view : [dbo].[vw_jobExecutionQueue]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[dbo].[vw_jobExecutionQueue]'))
DROP VIEW [dbo].[vw_jobExecutionQueue]
GO
CREATE VIEW [dbo].[vw_jobExecutionQueue]
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 21.09.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SELECT    jeq.[id]
		, jeq.[project_id]
		, cp.[code]		AS [project_code]
		, jeq.[instance_id]
		, cin.[name]	AS [instance_name]
		, jeq.[for_instance_id]
		, cinF.[name]	AS [for_instance_name]
		, jeq.[module]
		, jeq.[descriptor]
		, jeq.[filter]
		, jeq.[task_id]
		, jeq.[database_name]
		, jeq.[job_name]
		, jeq.[job_step_name]
		, jeq.[job_database_name]
		, jeq.[job_command]
		, jeq.[execution_date]
		, jeq.[running_time_sec]
		, jeq.[status]
		, CASE jeq.[status] WHEN '-1' THEN 'Not executed'
							WHEN '0' THEN 'Failed'
							WHEN '1' THEN 'Succeded'				
							WHEN '2' THEN 'Retry'
							WHEN '3' THEN 'Canceled'
							WHEN '4' THEN 'In progress'
							ELSE 'Unknown'
			END AS [status_desc]
		, jeq.[log_message]
		, jeq.[event_date_utc]
FROM [dbo].[jobExecutionQueue]		jeq
INNER JOIN [dbo].[catalogInstanceNames]	 cin	ON cin.[id] = jeq.[instance_id] AND cin.[project_id] = jeq.[project_id]
INNER JOIN [dbo].[catalogInstanceNames]	 cinF	ON cinF.[id] = jeq.[for_instance_id] AND cinF.[project_id] = jeq.[project_id]
INNER JOIN [dbo].[catalogProjects]		 cp		ON cp.[id] = jeq.[project_id]
GO


/* merge code from external implementation for table: dbo.jobExecutionHistory */
IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='jobExecutionHistory' AND COLUMN_NAME='task_id')
	ALTER TABLE [dbo].[jobExecutionHistory] ADD [task_id]	[smallint] NULL
GO
IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='jobExecutionHistory' AND COLUMN_NAME='database_name')
	ALTER TABLE [dbo].[jobExecutionHistory] ADD [database_name] [sysname] NULL
GO

RAISERROR('	Alter view : [dbo].[vw_jobExecutionHistory]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[dbo].[vw_jobExecutionHistory]'))
DROP VIEW [dbo].[vw_jobExecutionHistory]
GO
CREATE VIEW [dbo].[vw_jobExecutionHistory]
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 21.09.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

/* previous / history executions */
SELECT    jeq.[id]
		, jeq.[project_id]
		, cp.[code]		AS [project_code]
		, jeq.[instance_id]
		, cin.[name]	AS [instance_name]
		, jeq.[for_instance_id]
		, cinF.[name]	AS [for_instance_name]
		, jeq.[module]
		, jeq.[descriptor]
		, jeq.[filter]
		, jeq.[task_id]
		, jeq.[database_name]
		, jeq.[job_name]
		, jeq.[job_step_name]
		, jeq.[job_database_name]
		, jeq.[job_command]
		, jeq.[execution_date]
		, jeq.[running_time_sec]
		, jeq.[status]
		, CASE jeq.[status] WHEN '-1' THEN 'Not executed'
							WHEN '0' THEN 'Failed'
							WHEN '1' THEN 'Succeded'				
							WHEN '2' THEN 'Retry'
							WHEN '3' THEN 'Canceled'
							WHEN '4' THEN 'In progress'
							ELSE 'Unknown'
			END AS [status_desc]
		, jeq.[log_message]
		, jeq.[event_date_utc]
FROM [dbo].[jobExecutionHistory]		 jeq
INNER JOIN [dbo].[catalogInstanceNames]	 cin	ON cin.[id] = jeq.[instance_id] AND cin.[project_id] = jeq.[project_id]
INNER JOIN [dbo].[catalogInstanceNames]	 cinF	ON cinF.[id] = jeq.[for_instance_id] AND cinF.[project_id] = jeq.[project_id]
INNER JOIN [dbo].[catalogProjects]		 cp		ON cp.[id] = jeq.[project_id]

UNION ALL

/* currernt / last executions */
SELECT    jeq.[id]
		, jeq.[project_id]
		, cp.[code]		AS [project_code]
		, jeq.[instance_id]
		, cin.[name]	AS [instance_name]
		, jeq.[for_instance_id]
		, cinF.[name]	AS [for_instance_name]
		, jeq.[module]
		, jeq.[descriptor]
		, jeq.[filter]
		, jeq.[task_id]
		, jeq.[database_name]
		, jeq.[job_name]
		, jeq.[job_step_name]
		, jeq.[job_database_name]
		, jeq.[job_command]
		, jeq.[execution_date]
		, jeq.[running_time_sec]
		, jeq.[status]
		, CASE jeq.[status] WHEN '-1' THEN 'Not executed'
							WHEN '0' THEN 'Failed'
							WHEN '1' THEN 'Succeded'				
							WHEN '2' THEN 'Retry'
							WHEN '3' THEN 'Canceled'
							WHEN '4' THEN 'In progress'
							ELSE 'Unknown'
			END AS [status_desc]
		, jeq.[log_message]
		, jeq.[event_date_utc]
FROM [dbo].[jobExecutionQueue]		jeq
INNER JOIN [dbo].[catalogInstanceNames]	 cin	ON cin.[id] = jeq.[instance_id] AND cin.[project_id] = jeq.[project_id]
INNER JOIN [dbo].[catalogInstanceNames]	 cinF	ON cinF.[id] = jeq.[for_instance_id] AND cinF.[project_id] = jeq.[project_id]
INNER JOIN [dbo].[catalogProjects]		 cp		ON cp.[id] = jeq.[project_id]
GO


/* merge code from external implementation for table: dbo.catalogSolutions */
IF NOT EXISTS (SELECT * FROM sysobjects WHERE id = OBJECT_ID(N'[dbo].[catalogSolutions]') AND type in (N'U'))
begin
	RAISERROR('	Create table: [dbo].[catalogSolutions]', 10, 1) WITH NOWAIT;
	CREATE TABLE [dbo].[catalogSolutions]
	(
		[id]			[smallint]				NOT NULL IDENTITY(1, 1),
		[name]			[nvarchar](128)			NOT NULL,
		[contact]		[nvarchar](256)			NULL,
		[details]		[nvarchar](512)			NULL,
		CONSTRAINT [PK_catalogSolutions] PRIMARY KEY  CLUSTERED 
		(
			[id]
		) ON [PRIMARY] ,
		CONSTRAINT [UK_catalogSolutions_Name] UNIQUE  NONCLUSTERED 
		(
			[name]
		) ON [PRIMARY]
	) ON [PRIMARY];
end
GO

-----------------------------------------------------------------------------------------------------
IF (SELECT COUNT(*) FROM [dbo].[catalogSolutions] WHERE [name] = 'Default') = 0
begin
	RAISERROR('		...insert default data', 10, 1) WITH NOWAIT
	INSERT	INTO [dbo].[catalogSolutions]([name])
			SELECT 'Default'
end
GO


/* merge code from external implementation for table: dbo.catalogProjects */
IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='catalogProjects' AND COLUMN_NAME='solution_id')
	ALTER TABLE [dbo].[catalogProjects] ADD [solution_id] [smallint] NULL
GO
IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='catalogProjects' AND COLUMN_NAME='dbFilter')
	ALTER TABLE [dbo].[catalogProjects] ADD [dbFilter] [sysname] NULL
GO

IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='catalogProjects' AND COLUMN_NAME='isProduction')
begin
	EXEC ('ALTER TABLE [dbo].[catalogProjects] ADD [isProduction] [bit] NULL');
	EXEC ('ALTER TABLE [dbo].[catalogProjects] ADD CONSTRAINT [DF_catalogProjects_isProduction] DEFAULT (0) FOR [isProduction]');
	EXEC ('UPDATE [dbo].[catalogProjects] SET [isProduction] = 0 WHERE [isProduction] IS NULL');
	EXEC ('ALTER TABLE [dbo].[catalogProjects] ALTER COLUMN [isProduction] [bit] NOT NULL');
end
GO

IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_SCHEMA='dbo' AND CONSTRAINT_NAME='FK_catalogProjects_catalogSolutions')
	ALTER TABLE [dbo].[catalogProjects] 
			ADD	CONSTRAINT [FK_catalogProjects_catalogSolutions] FOREIGN KEY 
			(
				[solution_id]
			) 
			REFERENCES [dbo].[catalogSolutions] 
			(
				[id]
			)
GO
IF NOT EXISTS(SELECT * FROM sys.indexes WHERE [name]='IX_catalogProjects_ProjectID_SolutionID' AND [object_id]=OBJECT_ID('[dbo].[catalogProjects]'))
	CREATE INDEX [IX_catalogProjects_ProjectID_SolutionID] ON [dbo].[catalogProjects] ([solution_id]) ON [FG_Statistics_Index]
GO

IF NOT EXISTS (SELECT * FROM sysobjects WHERE id = OBJECT_ID(N'[dbo].[vw_catalogProjects]') AND type in (N'V'))
begin
	RAISERROR('	Create view : [dbo].[vw_catalogProjects]', 10, 1) WITH NOWAIT

	EXEC ('CREATE VIEW [dbo].[vw_catalogProjects]
	/* WITH ENCRYPTION */
	AS

	-- ============================================================================
	-- Copyright (c) 2004-2018 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
	-- ============================================================================
	-- Author			 : Dan Andrei STEFAN
	-- Create date		 : 05.04.2018
	-- Module			 : Database Analysis & Performance Monitoring
	-- ============================================================================

	SELECT 	  cp.[id]				AS [project_id]
			, cp.[code]				AS [project_code]
			, cp.[name]				AS [project_name]
			, cp.[description]		AS [project_description]
			, cp.[isProduction]		AS [is_production]
			, cp.[dbFilter]			AS [db_filter]
			, cs.[name]				AS [solution_name]
			, cs.[contact]
			, cs.[details]
			, cp.[active]
	FROM [dbo].[catalogProjects]		cp
	LEFT JOIN [dbo].[catalogSolutions]	cs ON cp.[solution_id] = cs.[id]')
end
GO
