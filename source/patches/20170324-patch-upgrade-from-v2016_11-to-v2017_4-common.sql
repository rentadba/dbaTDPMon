SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2016.11 to 2017.4 (2017.03.24)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: commons																							   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20170324-patch-upgrade-from-v2016_11-to-v2017_4-common.sql', 10, 1) WITH NOWAIT

IF NOT EXISTS(SELECT * FROM [dbo].[appConfigurations] WHERE [name] = 'Default folder for logs' and [module] = 'common')
	INSERT	INTO [dbo].[appConfigurations] ([module], [name], [value])
		  SELECT 'common' AS [module], 'Default folder for logs' AS [name], 
		  (SELECT [value] FROM [dbo].[appConfigurations] WHERE [name] = 'Default backup location' and [module] = 'maintenance-plan') AS [value]
GO
IF NOT EXISTS(SELECT * FROM [dbo].[appConfigurations] WHERE [name] = 'Internal jobs log retention (days)' and [module] = 'common')
	INSERT	INTO [dbo].[appConfigurations] ([module], [name], [value])
		  SELECT 'common' AS [module], 'Internal jobs log retention (days)' AS [name], '30' AS [value]
GO
IF NOT EXISTS(SELECT * FROM [dbo].[appConfigurations] WHERE [name] = 'History data retention (days)' and [module] = 'health-check')
	INSERT	INTO [dbo].[appConfigurations] ([module], [name], [value])
		  SELECT 'health-check' AS [module], 'History data retention (days)' AS [name], '367' AS [value]
GO

IF EXISTS(SELECT * FROM sys.schemas WHERE [name] = 'report') AND EXISTS(SELECT * FROM sys.objects WHERE [object_id] = OBJECT_ID('[report].[hardcodedFilters]'))
	EXEC ('
			IF EXISTS(SELECT * FROM [report].[hardcodedFilters] WHERE [object_name] = ''statsSQLServerErrorlogDetails'' AND [module] = ''health-check'')
				UPDATE [report].[hardcodedFilters]
					SET [object_name] = ''statsErrorlogDetails''
				WHERE [object_name] = ''statsSQLServerErrorlogDetails''
					AND [module] = ''health-check'''
		)
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[jobExecutionQueue]') AND type in (N'U'))
	begin
		RAISERROR('	Create table: [dbo].[jobExecutionQueue]', 10, 1) WITH NOWAIT

		CREATE TABLE [dbo].[jobExecutionQueue]
		(
			[id]					[int]	 IDENTITY (1, 1)	NOT NULL,
			[instance_id]			[smallint]		NOT NULL,
			[project_id]			[smallint]		NOT NULL,
			[module]				[varchar](32)	NOT NULL,
			[descriptor]			[varchar](256)	NOT NULL,
			[filter]				[sysname]		NULL,
			[for_instance_id]		[smallint]		NOT NULL,
			[job_name]				[sysname]		NOT NULL,
			[job_step_name]			[sysname]		NOT NULL,
			[job_database_name]		[sysname]		NOT NULL,
			[job_command]			[nvarchar](max) NOT NULL,
			[execution_date]		[datetime]		NULL,
			[running_time_sec]		[bigint]		NULL,
			[log_message]			[nvarchar](max) NULL,
			[status]				[smallint]		NOT NULL CONSTRAINT [DF_jobExecutionQueue_Status] DEFAULT (-1),
			[event_date_utc]		[datetime]		NOT NULL CONSTRAINT [DF_jobExecutionQueue_EventDateUTC] DEFAULT (GETUTCDATE()),
			CONSTRAINT [PK_jobExecutionQueue] PRIMARY KEY  CLUSTERED 
			(
				[id]
			) ON [FG_Statistics_Data],
			CONSTRAINT [UK_jobExecutionQueue] UNIQUE
			(
				[module],
				[for_instance_id],
				[project_id],
				[instance_id],
				[job_name],
				[job_step_name],
				[filter]
			) ON [FG_Statistics_Data],
			CONSTRAINT [FK_jobExecutionQueue_catalogProjects] FOREIGN KEY 
			(
				[project_id]
			) 
			REFERENCES [dbo].[catalogProjects] 
			(
				[id]
			),
			CONSTRAINT [FK_jobExecutionQueue_InstanceID_catalogInstanceNames] FOREIGN KEY 
			(
				[instance_id],
				[project_id]
			) 
			REFERENCES [dbo].[catalogInstanceNames] 
			(
				[id],
				[project_id]
			),
			CONSTRAINT [FK_jobExecutionQueue_ForInstanceID_catalogInstanceNames] FOREIGN KEY 
			(
				[for_instance_id],
				[project_id]
			) 
			REFERENCES [dbo].[catalogInstanceNames] 
			(
				[id],
				[project_id]
			)
		)ON [FG_Statistics_Data];

		CREATE INDEX [IX_jobExecutionQueue_InstanceID] ON [dbo].[jobExecutionQueue]([instance_id], [project_id]) ON [FG_Statistics_Index];
		CREATE INDEX [IX_jobExecutionQueue_ProjectID] ON [dbo].[jobExecutionQueue] ([project_id], [event_date_utc]) INCLUDE ([instance_id]) ON [FG_Statistics_Index];
		CREATE INDEX [IX_jobExecutionQueue_JobName] ON [dbo].[jobExecutionQueue]([job_name], [job_step_name]) ON [FG_Statistics_Index];
		CREATE INDEX [IX_jobExecutionQueue_Descriptor] ON [dbo].[jobExecutionQueue]([project_id], [status], [module], [descriptor]) INCLUDE ([instance_id], [for_instance_id], [job_name]) ON [FG_Statistics_Index];;
	end
ELSE
	begin
		IF EXISTS (SELECT * FROM sys.indexes WHERE [object_id] = OBJECT_ID(N'dbo.jobExecutionQueue') AND [name] = N'IX_jobExecutionQueue_Descriptor') 
			DROP INDEX [IX_jobExecutionQueue_Descriptor] ON [dbo].[jobExecutionQueue];
		CREATE INDEX [IX_jobExecutionQueue_Descriptor] ON [dbo].[jobExecutionQueue]([project_id], [status], [module], [descriptor]) INCLUDE ([instance_id], [for_instance_id], [job_name]) ON [FG_Statistics_Index];
	end
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[catalogDatabaseNames]') AND type in (N'U'))
	begin
		RAISERROR('	Create table: [dbo].[catalogDatabaseNames]', 10, 1) WITH NOWAIT

		CREATE TABLE [dbo].[catalogDatabaseNames] 
		(
			[id]					[smallint] IDENTITY (1, 1)	NOT NULL,
			[instance_id]			[smallint]		NOT NULL,
			[project_id]			[smallint]		NOT NULL,
			[database_id]			[int]			NOT NULL,
			[name]					[sysname]		NOT NULL,
			[state]					[int]			NOT NULL,
			[state_desc]			[nvarchar](64)	NOT NULL,
			[active]				[bit]			NOT NULL CONSTRAINT [DF_catalogDatabaseNames_Active] DEFAULT (1)
			CONSTRAINT [PK_catalogDatabaseNames] PRIMARY KEY  CLUSTERED 
			(
				[id],
				[instance_id]
			) ON [PRIMARY],
			CONSTRAINT [UK_catalogDatabaseNames_Name] UNIQUE  NONCLUSTERED 
			(
				[name],
				[instance_id]
			) ON [PRIMARY],
			CONSTRAINT [FK_catalogDatabaseNames_catalogProjects] FOREIGN KEY 
			(
				[project_id]
			) 
			REFERENCES [dbo].[catalogProjects] 
			(
				[id]
			),
			CONSTRAINT [FK_catalogDatabaseNames_catalogInstanceNames] FOREIGN KEY 
			(
				[instance_id],
				[project_id]
			) 
			REFERENCES [dbo].[catalogInstanceNames] 
			(
				[id],
				[project_id]
			)
		) ON [PRIMARY];

		CREATE INDEX [IX_catalogDatabaseNames_InstanceID] ON [dbo].[catalogDatabaseNames]([instance_id], [project_id]) ON [PRIMARY];
		CREATE INDEX [IX_catalogDatabaseNames_ProjecteID] ON [dbo].[catalogDatabaseNames]([project_id]) ON [PRIMARY];
	end
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[jobExecutionHistory]') AND type in (N'U'))
	begin
		RAISERROR('	Create table: [dbo].[jobExecutionHistory]', 10, 1) WITH NOWAIT

		CREATE TABLE [dbo].[jobExecutionHistory]
		(
			[id]					[int]	 IDENTITY (1, 1)	NOT NULL,
			[instance_id]			[smallint]		NOT NULL,
			[project_id]			[smallint]		NOT NULL,
			[module]				[varchar](32)	NOT NULL,
			[descriptor]			[varchar](256)	NOT NULL,
			[filter]				[sysname]		NULL,
			[for_instance_id]		[smallint]		NOT NULL,
			[job_name]				[sysname]		NOT NULL,
			[job_step_name]			[sysname]		NOT NULL,
			[job_database_name]		[sysname]		NOT NULL,
			[job_command]			[nvarchar](max) NOT NULL,
			[execution_date]		[datetime]		NULL,
			[running_time_sec]		[bigint]		NULL,
			[log_message]			[nvarchar](max) NULL,
			[status]				[smallint]		NOT NULL CONSTRAINT [DF_jobExecutionHistory_Status] DEFAULT (-1),
			[event_date_utc]		[datetime]		NOT NULL CONSTRAINT [DF_jobExecutionHistory_EventDateUTC] DEFAULT (GETUTCDATE()),
			CONSTRAINT [PK_jobExecutionHistory] PRIMARY KEY  CLUSTERED 
			(
				[id]
			) ON [FG_Statistics_Data],
			CONSTRAINT [FK_jobExecutionHistory_catalogProjects] FOREIGN KEY 
			(
				[project_id]
			) 
			REFERENCES [dbo].[catalogProjects] 
			(
				[id]
			),
			CONSTRAINT [FK_jobExecutionHistory_InstanceID_catalogInstanceNames] FOREIGN KEY 
			(
				[instance_id],
				[project_id]
			) 
			REFERENCES [dbo].[catalogInstanceNames] 
			(
				[id],
				[project_id]
			),
			CONSTRAINT [FK_jobExecutionHistory_ForInstanceID_catalogInstanceNames] FOREIGN KEY 
			(
				[for_instance_id],
				[project_id]
			) 
			REFERENCES [dbo].[catalogInstanceNames] 
			(
				[id],
				[project_id]
			)
		)ON [FG_Statistics_Data];

		CREATE INDEX [IX_jobExecutionHistory_InstanceID] ON [dbo].[jobExecutionHistory]([instance_id], [project_id]) ON [FG_Statistics_Index];
		CREATE INDEX [IX_jobExecutionHistory_ProjectID] ON [dbo].[jobExecutionHistory] ([project_id], [event_date_utc]) INCLUDE ([instance_id]) ON [FG_Statistics_Index];
		CREATE INDEX [IX_jobExecutionHistory_JobName] ON [dbo].[jobExecutionHistory]([job_name], [job_step_name]) ON [FG_Statistics_Index];
		CREATE INDEX [IX_jobExecutionHistory_Descriptor] ON [dbo].[jobExecutionHistory]([project_id], [status], [module], [descriptor]) INCLUDE ([instance_id], [for_instance_id], [job_name]) ON [FG_Statistics_Index];;
		CREATE INDEX [IX_jobExecutionHistory] ON [dbo].[jobExecutionHistory] ([module], [for_instance_id], [project_id], [instance_id], [job_name], [job_step_name], [filter]) ON [FG_Statistics_Index];
	end
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[logAnalysisMessages]') AND type in (N'U'))
	begin
		RAISERROR('	Create table: [dbo].[logAnalysisMessages]', 10, 1) WITH NOWAIT

		CREATE TABLE [dbo].[logAnalysisMessages]
		(
			[id]					[int]	 IDENTITY (1, 1)	NOT NULL,
			[instance_id]			[smallint]		NOT NULL,
			[project_id]			[smallint]		NOT NULL,
			[event_date_utc]		[datetime]		NOT NULL,
			[descriptor]			[varchar](256)	NULL,
			[message]				[varchar](max)	NULL,
			CONSTRAINT [PK_logAnalysisMessages] PRIMARY KEY  CLUSTERED 
			(
				[id],
				[instance_id]
			) ON [FG_Statistics_Data],
			CONSTRAINT [FK_logAnalysisMessages_catalogProjects] FOREIGN KEY 
			(
				[project_id]
			) 
			REFERENCES [dbo].[catalogProjects] 
			(
				[id]
			),
			CONSTRAINT [FK_logAnalysisMessages_catalogInstanceNames] FOREIGN KEY 
			(
				[instance_id],
				[project_id]
			) 
			REFERENCES [dbo].[catalogInstanceNames] 
			(
				[id],
				[project_id]
			)
		) ON [FG_Statistics_Data];

		CREATE INDEX [IX_logAnalysisMessages_InstanceID] ON [dbo].[logAnalysisMessages]([instance_id], [project_id]) ON [FG_Statistics_Index];
		CREATE INDEX [IX_logAnalysisMessages_ProjecteID] ON [dbo].[logAnalysisMessages]([project_id]) ON [FG_Statistics_Index];
	end
GO
