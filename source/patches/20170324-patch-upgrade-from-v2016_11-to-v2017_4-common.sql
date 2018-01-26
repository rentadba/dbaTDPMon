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

IF EXISTS (SELECT * FROM sys.indexes WHERE [object_id] = OBJECT_ID(N'dbo.jobExecutionQueue') AND [name] = N'IX_jobExecutionQueue_Descriptor') 
	DROP INDEX [IX_jobExecutionQueue_Descriptor] ON [dbo].[jobExecutionQueue]
GO
	CREATE INDEX [IX_jobExecutionQueue_Descriptor] ON [dbo].[jobExecutionQueue]([project_id], [status], [module], [descriptor]) INCLUDE ([instance_id], [for_instance_id], [job_name]) ON [FG_Statistics_Index];
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
