USE [dbaTDPMon]
GO
EXEC [dbo].[usp_mpDatabaseBackup]	@sqlServerName		= @@SERVERNAME,
									@dbName				= 'dbaTDPMon',
									@backupLocation		= DEFAULT,
									@flgActions			= 1,	
									@flgOptions			= DEFAULT,	
									@retentionDays		= DEFAULT,
									@executionLevel		= DEFAULT,
									@debugMode			= DEFAULT
GO

IF EXISTS (SELECT * FROM sys.tables WHERE [name]='reportHTMLSkipRules')
	begin
		IF EXISTS(SELECT * FROM [dbo].[reportHTMLSkipRules] WHERE [module] = 'monitoring' AND [rule_name] = 'alarm-custom: low disk space')
			DELETE FROM [dbo].[reportHTMLSkipRules] WHERE [module] = 'monitoring' AND [rule_name] = 'alarm-custom: low disk space'
	end
GO

IF NOT EXISTS(SELECT * FROM sys.schemas WHERE [name] = 'health-check' AND [principal_id] IN (SELECT [principal_id] FROM sys.database_principals WHERE [name] = 'dbo'))
	begin
		RAISERROR('Create schema: [health-check]', 10, 1) WITH NOWAIT
		EXEC ('CREATE SCHEMA [health-check] AUTHORIZATION [dbo]')
	end
GO

IF NOT EXISTS(SELECT * FROM sys.schemas WHERE [name] = 'maintenance-plan' AND [principal_id] IN (SELECT [principal_id] FROM sys.database_principals WHERE [name] = 'dbo'))
	begin
		RAISERROR('Create schema: [maintenance-plan]', 10, 1) WITH NOWAIT
		EXEC ('CREATE SCHEMA [maintenance-plan] AUTHORIZATION [dbo]')
	end
GO

IF NOT EXISTS(SELECT * FROM sys.schemas WHERE [name] = 'monitoring' AND [principal_id] IN (SELECT [principal_id] FROM sys.database_principals WHERE [name] = 'dbo'))
	begin
		RAISERROR('Create schema: [monitoring]', 10, 1) WITH NOWAIT
		EXEC ('CREATE SCHEMA [monitoring] AUTHORIZATION [dbo]')
	end
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[statsHealthCheckDatabaseDetails]') AND type in (N'U'))
	begin
		RAISERROR('Drop table: [dbo].[statsHealthCheckDatabaseDetails]', 10, 1) WITH NOWAIT
		DROP TABLE [dbo].[statsHealthCheckDatabaseDetails]
	end
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[statsDiskSpaceInfo]') AND type in (N'U'))
	begin
		DROP TABLE [dbo].[statsDiskSpaceInfo]
	end
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[statsOSEventLogs]') AND type in (N'U'))
	begin
		DROP TABLE [dbo].[statsOSEventLogs]
	end
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[statsSQLServerAgentJobsHistory]') AND type in (N'U'))
	begin
		DROP TABLE [dbo].[statsSQLServerAgentJobsHistory]
	end
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[statsSQLServerErrorlogDetails]') AND type in (N'U'))
	begin
		DROP TABLE [dbo].[statsSQLServerErrorlogDetails]
	end
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[statsDatabaseDetails]') AND type in (N'U'))
	begin
		DROP TABLE [dbo].[statsDatabaseDetails]
	end
GO



-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 35.12.2014
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-----------------------------------------------------------------------------------------------------
--Health Check: database statistics & details
-----------------------------------------------------------------------------------------------------
RAISERROR('Create table: [health-check].[statsDatabaseDetails]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[health-check].[statsDatabaseDetails]') AND type in (N'U'))
DROP TABLE [health-check].[statsDatabaseDetails]
GO
CREATE TABLE [health-check].[statsDatabaseDetails]
(
	[id]						[int]	 IDENTITY (1, 1)	NOT NULL,
	[catalog_database_id]		[smallint]		NOT NULL,
	[instance_id]				[smallint]		NOT NULL,
	[recovery_model]			[tinyint]		NULL,
	[page_verify_option]		[tinyint]		NULL,
	[compatibility_level]		[tinyint]		NULL,
	[data_size_mb]				[numeric](20,3)	NULL,
	[data_space_used_percent]	[numeric](6,2)	NULL,
	[log_size_mb]				[numeric](20,3)	NULL,
	[log_space_used_percent]	[numeric](6,2)	NULL,
	[is_auto_close]				[bit]			NULL,
	[is_auto_shrink]			[bit]			NULL,
	[physical_drives]			[sysname]		NULL,
	[last_backup_time]			[datetime]		NULL,
	[last_dbcc checkdb_time]	[datetime]		NULL,
	[is_growth_limited]			[bit]			NULL,
	[event_date_utc]			[datetime]		NOT NULL,
	CONSTRAINT [PK_statsDatabaseDetails] PRIMARY KEY  CLUSTERED 
	(
		[id],
		[catalog_database_id]
	) ON [FG_Statistics_Data],
	CONSTRAINT [FK_statsDatabaseDetails_catalogDatabaseNames] FOREIGN KEY 
	(
		  [catalog_database_id]
		, [instance_id]
	) 
	REFERENCES [dbo].[catalogDatabaseNames] 
	(
		  [id]
		, [instance_id]
	)
)ON [FG_Statistics_Data]
GO

CREATE INDEX [IX_statsDatabaseDetails_CatalogDatabaseID] ON [health-check].[statsDatabaseDetails]( [catalog_database_id], [instance_id]) ON [FG_Statistics_Index]
GO
CREATE INDEX [IX_statsDatabaseDetails_InstanceID] ON [health-check].[statsDatabaseDetails] ([instance_id]) ON [FG_Statistics_Index]
GO


-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 29.05.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-----------------------------------------------------------------------------------------------------
--log for SQL Server Agent job statuses
-----------------------------------------------------------------------------------------------------
RAISERROR('Create table: [health-check].[statsSQLServerErrorlogDetails]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[health-check].[statsSQLServerErrorlogDetails]') AND type in (N'U'))
DROP TABLE [health-check].[statsSQLServerErrorlogDetails]
GO
CREATE TABLE [health-check].[statsSQLServerErrorlogDetails]
(
	[id]					[int]	 IDENTITY (1, 1)	NOT NULL,
	[instance_id]			[smallint]		NOT NULL,
	[project_id]			[smallint]		NOT NULL,
	[event_date_utc]		[datetime]		NOT NULL,
	[log_date]				[datetime]		NULL,
	[process_info]			[sysname]		NULL,
	[text]					[varchar](max)	NULL,
	CONSTRAINT [PK_statsSQLServerErrorlogDetails] PRIMARY KEY  CLUSTERED 
	(
		[id],
		[instance_id]
	) ON [FG_Statistics_Data],
	CONSTRAINT [FK_statsSQLServerErrorlogDetails_catalogProjects] FOREIGN KEY 
	(
		[project_id]
	) 
	REFERENCES [dbo].[catalogProjects] 
	(
		[id]
	),
	CONSTRAINT [FK_statsSQLServerErrorlogDetails_catalogInstanceNames] FOREIGN KEY 
	(
		[instance_id],
		[project_id]
	) 
	REFERENCES [dbo].[catalogInstanceNames] 
	(
		[id],
		[project_id]
	)
)ON [FG_Statistics_Data]
GO

CREATE INDEX [IX_statsSQLServerErrorlogDetails_ProjectID] ON [health-check].[statsSQLServerErrorlogDetails] ([project_id]) ON [FG_Statistics_Index]
GO
CREATE INDEX [IX_statsSQLServerErrorlogDetails_InstanceID] ON [health-check].[statsSQLServerErrorlogDetails] ([instance_id], [project_id], [log_date]) ON [FG_Statistics_Index]
GO
CREATE INDEX [IX_statsSQLServerErrorlogDetails_LogDate] ON [health-check].[statsSQLServerErrorlogDetails]([log_date], [instance_id], [project_id]) ON [FG_Statistics_Index]
GO


-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 05.12.2014
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-----------------------------------------------------------------------------------------------------
--log for SQL Server Agent job statuses
-----------------------------------------------------------------------------------------------------
RAISERROR('Create table: [health-check].[statsSQLServerAgentJobsHistory]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[health-check].[statsSQLServerAgentJobsHistory]') AND type in (N'U'))
DROP TABLE [health-check].[statsSQLServerAgentJobsHistory]
GO
CREATE TABLE [health-check].[statsSQLServerAgentJobsHistory]
(
	[id]					[int]	 IDENTITY (1, 1)	NOT NULL,
	[instance_id]			[smallint]		NOT NULL,
	[project_id]			[smallint]		NOT NULL,
	[event_date_utc]		[datetime]		NOT NULL,
	[job_name]				[sysname]		NOT NULL,
	[last_execution_status] [int]			NOT NULL,
	[last_execution_date]	[varchar](10)	NULL, 
	[last_execution_time]	[varchar](8)	NULL,
	[running_time_sec]		[bigint]		NULL,
	[message]				[varchar](max)	NULL, 
	CONSTRAINT [PK_statsSQLServerAgentJobsHistory] PRIMARY KEY  CLUSTERED 
	(
		[id],
		[instance_id]
	) ON [FG_Statistics_Data],
	CONSTRAINT [FK_statsSQLServerAgentJobsHistory_catalogProjects] FOREIGN KEY 
	(
		[project_id]
	) 
	REFERENCES [dbo].[catalogProjects] 
	(
		[id]
	),
	CONSTRAINT [FK_statsSQLServerAgentJobsHistory_catalogInstanceNames] FOREIGN KEY 
	(
		[instance_id],
		[project_id]
	) 
	REFERENCES [dbo].[catalogInstanceNames] 
	(
		[id],
		[project_id]
	)
)ON [FG_Statistics_Data]
GO

CREATE INDEX [IX_statsSQLServerAgentJobsHistory_InstanceID] ON [health-check].[statsSQLServerAgentJobsHistory]([instance_id], [project_id]) ON [FG_Statistics_Index]
GO
CREATE INDEX [IX_statsSQLServerAgentJobsHistory_ProjecteID] ON [health-check].[statsSQLServerAgentJobsHistory]([project_id]) ON [FG_Statistics_Index]
GO


-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 04.09.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-----------------------------------------------------------------------------------------------------
--OS Events
-----------------------------------------------------------------------------------------------------
RAISERROR('Create table: [health-check].[statsOSEventLogs]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[health-check].[statsOSEventLogs]') AND type in (N'U'))
DROP TABLE [health-check].[statsOSEventLogs]
GO
CREATE TABLE [health-check].[statsOSEventLogs]
(
	[id]					[int]	 IDENTITY (1, 1)	NOT NULL,
	[instance_id]			[smallint]		NOT NULL,
	[project_id]			[smallint]		NOT NULL,
	[machine_id]			[smallint]		NOT NULL,
	[event_date_utc]		[datetime]		NOT NULL,
	[log_type_id]			[tinyint]		NOT NULL,
	[event_id]				[int]				NULL,
	[level_id] 				[tinyint]			NULL,
	[record_id]				[bigint]			NULL,
	[category_id]			[int]				NULL,
	[category_name]			[nvarchar](256)		NULL,
	[source] 				[nvarchar](512)		NULL,
	[process_id]			[int]				NULL,
	[thread_id]				[int]				NULL,
	[machine_name]			[sysname]			NULL,
	[user_id]				[nvarchar](256)		NULL,
	[time_created]			[varchar](32)		NULL,
	[message] 				[nvarchar](max)		NULL
	CONSTRAINT [PK_statsOSEventLogs] PRIMARY KEY  CLUSTERED 
	(
		[id],
		[instance_id]
	) ON [FG_Statistics_Data],
	CONSTRAINT [FK_statsOSEventLogs_catalogProjects] FOREIGN KEY 
	(
		[project_id]
	) 
	REFERENCES [dbo].[catalogProjects] 
	(
		[id]
	),
	CONSTRAINT [FK_statsOSEventLogs_catalogInstanceNames] FOREIGN KEY 
	(
		[instance_id],
		[project_id]
	) 
	REFERENCES [dbo].[catalogInstanceNames] 
	(
		[id],
		[project_id]
	),
	CONSTRAINT [FK_statsOSEventLogs_catalogMachineNames] FOREIGN KEY 
	(
		[machine_id],
		[project_id]
	) 
	REFERENCES [dbo].[catalogMachineNames] 
	(
		[id],
		[project_id]
	)

)ON [FG_Statistics_Data]
GO

CREATE INDEX [IX_statsOSEventLogs_InstanceID] ON [health-check].[statsOSEventLogs]([instance_id], [project_id]) ON [FG_Statistics_Index]
GO
CREATE INDEX [IX_statsOSEventLogs_ProjecteID] ON [health-check].[statsOSEventLogs]([project_id]) ON [FG_Statistics_Index]
GO
CREATE INDEX [IX_statsOSEventLogs_MachineID] ON [health-check].[statsOSEventLogs]([machine_id], [project_id]) ON [FG_Statistics_Index]
GO


-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 15.12.2014
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-----------------------------------------------------------------------------------------------------
--Health Check: disk space information
-----------------------------------------------------------------------------------------------------
RAISERROR('Create table: [health-check].[statsDiskSpaceInfo]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[health-check].[statsDiskSpaceInfo]') AND type in (N'U'))
DROP TABLE [health-check].[statsDiskSpaceInfo]
GO
CREATE TABLE [health-check].[statsDiskSpaceInfo]
(
	[id]					[int]	 IDENTITY (1, 1)	NOT NULL,
	[instance_id]			[smallint]		NOT NULL,
	[project_id]			[smallint]		NOT NULL,
	[event_date_utc]		[datetime]		NOT NULL,
	[logical_drive]			[char](1)			NULL,
	[volume_mount_point]	[nvarchar](512)		NULL,
	[total_size_mb]			[numeric](18,3)		NULL,
	[available_space_mb]	[numeric](18,3)		NULL,
	[block_size]			[int]				NULL,
	[percent_available]		[numeric](6,2)		NULL
	CONSTRAINT [PK_statsDiskSpaceInfo] PRIMARY KEY  CLUSTERED 
	(
		[id],
		[instance_id]
	) ON [FG_Statistics_Data],
	CONSTRAINT [FK_statsDiskSpaceInfo_catalogProjects] FOREIGN KEY 
	(
		[project_id]
	) 
	REFERENCES [dbo].[catalogProjects] 
	(
		[id]
	),
	CONSTRAINT [FK_statsDiskSpaceInfo_catalogInstanceNames] FOREIGN KEY 
	(
		[instance_id],
		[project_id]
	) 
	REFERENCES [dbo].[catalogInstanceNames] 
	(
		[id],
		[project_id]
	)
)ON [FG_Statistics_Data]
GO

CREATE INDEX [IX_statsDiskSpaceInfo_InstanceID] ON [health-check].[statsDiskSpaceInfo]([instance_id], [project_id]) ON [FG_Statistics_Index]
GO
CREATE INDEX [IX_statsDiskSpaceInfo_ProjecteID] ON [health-check].[statsDiskSpaceInfo]([project_id]) ON [FG_Statistics_Index]
GO


IF  EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[dbo].[vw_statsHealthCheckDatabaseDetails]') AND type in (N'V'))
	begin
		RAISERROR('Drop view : [dbo].[vw_statsHealthCheckDatabaseDetails]', 10, 1) WITH NOWAIT
		DROP VIEW [dbo].[vw_statsHealthCheckDatabaseDetails]
	end
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[vw_statsDiskSpaceInfo]') AND type in (N'V'))
	begin
		RAISERROR('Drop view : [health-check].[vw_statsDiskSpaceInfo]', 10, 1) WITH NOWAIT
		DROP VIEW [dbo].[vw_statsDiskSpaceInfo]
	end
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[vw_statsOSEventLogs]') AND type in (N'V'))
	begin
		RAISERROR('Drop view : [health-check].[vw_statsOSEventLogs]', 10, 1) WITH NOWAIT
		DROP VIEW [dbo].[vw_statsOSEventLogs]
	end
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[vw_statsSQLServerAgentJobsHistory]') AND type in (N'V'))
	begin
		RAISERROR('Drop view : [health-check].[vw_statsSQLServerAgentJobsHistory]', 10, 1) WITH NOWAIT
		DROP VIEW [dbo].[vw_statsSQLServerAgentJobsHistory]
	end
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[vw_statsSQLServerErrorlogDetails]') AND type in (N'V'))
	begin
		RAISERROR('Drop view : [health-check].[vw_statsSQLServerErrorlogDetails]', 10, 1) WITH NOWAIT
		DROP VIEW [dbo].[vw_statsSQLServerErrorlogDetails]
	end
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[vw_statsDatabaseDetails]') AND type in (N'V'))
	begin
		RAISERROR('Drop view : [health-check].[vw_statsDatabaseDetails]', 10, 1) WITH NOWAIT
		DROP VIEW [dbo].[vw_statsDatabaseDetails]
	end
GO

RAISERROR('Create view : [health-check].[vw_statsDiskSpaceInfo]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[health-check].[vw_statsDiskSpaceInfo]'))
DROP VIEW [health-check].[vw_statsDiskSpaceInfo]
GO

CREATE VIEW [health-check].[vw_statsDiskSpaceInfo]
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 05.12.2014
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SELECT 	  cin.[project_id]		AS [project_id]
		, cin.[id]				AS [instance_id]
		, cin.[name]			AS [instance_name]
		, dsi.[event_date_utc]
		, dsi.[logical_drive]
		, dsi.[volume_mount_point]
		, dsi.[total_size_mb]
		, dsi.[available_space_mb]
		, dsi.[percent_available]
		, dsi.[block_size]
FROM [health-check].[statsDiskSpaceInfo]	dsi
INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = dsi.[instance_id] AND cin.[project_id] = dsi.[project_id]
GO

RAISERROR('Create view : [health-check].[vw_statsSQLServerErrorlogDetails]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[health-check].[vw_statsSQLServerErrorlogDetails]'))
DROP VIEW [health-check].[vw_statsSQLServerErrorlogDetails]
GO

CREATE VIEW [health-check].[vw_statsSQLServerErrorlogDetails]
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 05.12.2014
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SELECT 	  sseld.[id]
		, cin.[project_id]		AS [project_id]
		, cin.[id]				AS [instance_id]
		, cin.[name]			AS [instance_name]
		, sseld.[log_date]
		, sseld.[process_info]
		, sseld.[text]
		, sseld.[event_date_utc]
FROM [dbo].[catalogInstanceNames]	cin	
INNER JOIN [health-check].[statsSQLServerErrorlogDetails] sseld ON cin.[id] = sseld.[instance_id] AND cin.[project_id] = sseld.[project_id]
GO

RAISERROR('Create view : [health-check].[vw_statsSQLServerAgentJobsHistory]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[health-check].[vw_statsSQLServerAgentJobsHistory]'))
DROP VIEW [health-check].[vw_statsSQLServerAgentJobsHistory]
GO

CREATE VIEW [health-check].[vw_statsSQLServerAgentJobsHistory]
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 05.12.2014
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SELECT 	  cin.[project_id]		AS [project_id]
		, cin.[id]				AS [instance_id]
		, cin.[name]			AS [instance_name]
		, ssajh.[event_date_utc]
		, ssajh.[job_name]
		, ssajh.[last_execution_status]
		, ssajh.[last_execution_date]
		, ssajh.[last_execution_time]
		, ssajh.[running_time_sec]
		, ssajh.[message]
FROM [health-check].[statsSQLServerAgentJobsHistory]	ssajh
INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = ssajh.[instance_id] AND cin.[project_id] = ssajh.[project_id]
GO

RAISERROR('Create view : [health-check].[vw_statsOSEventLogs]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[health-check].[vw_statsOSEventLogs]'))
DROP VIEW [health-check].[vw_statsOSEventLogs]
GO

CREATE VIEW [health-check].[vw_statsOSEventLogs]
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 04.09.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SELECT 	  cin.[project_id]		AS [project_id]
		, cin.[id]				AS [instance_id]
		, cin.[name]			AS [instance_name]
		, cmn.[id]				AS [machine_id]
		, soel.[event_date_utc]
		, soel.[log_type_id]
		, CASE soel.[log_type_id] WHEN 1 THEN 'Application'
								  WHEN 2 THEN 'System'
								  WHEN 3 THEN 'Setup'
		  END AS [log_type_desc]
		, soel.[event_id]
		, soel.[level_id]
		, CASE soel.[level_id]	WHEN 1 THEN 'Critical'
								WHEN 2 THEN 'Error'
								WHEN 3 THEN 'Warning'
								WHEN 4 THEN 'Information'
		  END AS [level_desc]
		, soel.[record_id]
		, soel.[category_id]
		, soel.[category_name]
		, soel.[source]
		, soel.[process_id]
		, soel.[thread_id]
		, soel.[machine_name]
		, soel.[user_id]
		, soel.[time_created]
		, soel.[message]
FROM [health-check].[statsOSEventLogs]	soel
INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = soel.[instance_id] AND cin.[project_id] = soel.[project_id]
INNER JOIN [dbo].[catalogMachineNames]  cmn ON cmn.[id] = soel.[machine_id] AND cmn.[project_id] = soel.[project_id]
GO

RAISERROR('Create view : [health-check].[vw_statsDatabaseDetails]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[health-check].[vw_statsDatabaseDetails]'))
DROP VIEW [health-check].[vw_statsDatabaseDetails]
GO

CREATE VIEW [health-check].[vw_statsDatabaseDetails]
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 05.12.2014
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SELECT 	  cin.[project_id]		AS [project_id]
		, cin.[id]				AS [instance_id]
		, cin.[name]			AS [instance_name]
		, shcdd.[catalog_database_id]
		, cdn.[database_id]
		, cdn.[name]			AS [database_name]
		, cdn.[active]
		, cdn.[state]
		, cdn.[state_desc] 
		, shcdd.[data_size_mb] + shcdd.[log_size_mb] AS [size_mb]
		, shcdd.[data_size_mb]
		, shcdd.[data_space_used_percent]
		, shcdd.[log_size_mb]
		, shcdd.[log_space_used_percent]
		, shcdd.[is_auto_close]
		, shcdd.[is_auto_shrink]
		, shcdd.[physical_drives]
		, shcdd.[last_backup_time]
		, shcdd.[last_dbcc checkdb_time]
		, CASE shcdd.[recovery_model] WHEN 1 THEN 'FULL' WHEN 2 THEN 'BULK_LOGGED' WHEN 3 THEN 'SIMPLE' ELSE NULL END AS [recovery_model_desc]
		, CASE shcdd.[page_verify_option] WHEN 0 THEN 'NONE' WHEN 1 THEN 'TORN_PAGE_DETECTION' WHEN 2 THEN 'CHECKSUM' ELSE NULL END AS [page_verify_option_desc]
		, shcdd.[compatibility_level]
		, shcdd.[is_growth_limited]
		, shcdd.[event_date_utc]
FROM [dbo].[catalogInstanceNames]	cin	
INNER JOIN [dbo].[catalogDatabaseNames] cdn ON cin.[id] = cdn.[instance_id] AND cin.[project_id] = cdn.[project_id]
INNER JOIN [health-check].[statsDatabaseDetails] shcdd ON shcdd.[catalog_database_id] = cdn.[id] AND shcdd.[instance_id] = cdn.[instance_id]
GO


IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[statsMaintenancePlanInternals]') AND type in (N'U'))
	begin
		DROP TABLE [dbo].[statsMaintenancePlanInternals]
	end
GO

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 06.02.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================
-- table will contain actions made against schema objects, in order to track/troubleshoot
-----------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------
--
-----------------------------------------------------------------------------------------------------
RAISERROR('Create table: [maintenance-plan].[statsMaintenancePlanInternals]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[maintenance-plan].[statsMaintenancePlanInternals]') AND type in (N'U'))
DROP TABLE [maintenance-plan].[statsMaintenancePlanInternals]
GO
CREATE TABLE [maintenance-plan].[statsMaintenancePlanInternals]
(
	[id]				[bigint] IDENTITY (1, 1)NOT NULL,
	[event_date_utc]	[datetime]				NOT NULL CONSTRAINT [DF_statsMaintenancePlanInternals_EventDateUTC] DEFAULT (GETUTCDATE()),
	[session_id]		[smallint]				NOT NULL CONSTRAINT [DF_statsMaintenancePlanInternals_SessionID] DEFAULT (@@SPID),
	[name]				[sysname]				NOT NULL,
	[server_name]		[sysname]				NOT NULL,
	[database_name]		[sysname]				NULL,
	[schema_name]		[sysname]				NULL,
	[object_name]		[sysname]				NULL,
	[child_object_name]	[sysname]				NULL,
	CONSTRAINT [PK_statsMaintenancePlanInternals] PRIMARY KEY  CLUSTERED 
	(
		[id]
	) ON [FG_Statistics_Data]
) ON [FG_Statistics_Data]
GO

CREATE INDEX [IX_statsMaintenancePlanInternals_SessionID_Name] ON [maintenance-plan].[statsMaintenancePlanInternals]
		([session_id], [name]) 
	INCLUDE 
		([server_name], [database_name]) 
	ON [FG_Statistics_Index]
GO

CREATE INDEX [IX_statsMaintenancePlanInternals_Name] ON [maintenance-plan].[statsMaintenancePlanInternals]
		([name], [server_name], [database_name])
	ON [FG_Statistics_Index]
GO


IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[monitoringAlertThresholds]') AND type in (N'U'))
	begin
		RAISERROR('Drop table: [dbo].[monitoringAlertThresholds]', 10, 1) WITH NOWAIT
		DROP TABLE [dbo].[monitoringAlertThresholds]
	end
GO

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 13.10.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-----------------------------------------------------------------------------------------------------
--
-----------------------------------------------------------------------------------------------------
RAISERROR('Create table: [monitoring].[alertThresholds]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[monitoring].[alertThresholds]') AND type in (N'U'))
DROP TABLE [monitoring].[alertThresholds]
GO
CREATE TABLE [monitoring].[alertThresholds]
(
	[id]										[int] IDENTITY (1, 1)NOT NULL,
	[category]									[varchar](32)		NOT NULL,
	[alert_name]								[sysname]			NULL,
	[operator]									[varchar](8)		NULL,
	[warning_limit]								[numeric](18,3)		NOT NULL,
	[critical_limit]								[numeric](18,3)	NOT NULL,
	[is_warning_limit_enabled]					[bit]				NOT NULL CONSTRAINT [DF_alertThresholds_IsWarningLimitEnabled] DEFAULT (1),
	[is_critical_limit_enabled]					[bit]				NOT NULL CONSTRAINT [DF_alertThresholds_IsCriticalLimitEnabled] DEFAULT (1),
	CONSTRAINT [PK_alertThresholds] PRIMARY KEY  CLUSTERED 
	(
		[id]
	)  ON [PRIMARY],
	CONSTRAINT [UK_alertThresholds_AlertName] UNIQUE
		(
			[category]
		  , [alert_name]
		) 
) ON [PRIMARY]
GO

SET NOCOUNT ON
-----------------------------------------------------------------------------------------------------
RAISERROR('		...insert default data', 10, 1) WITH NOWAIT
GO
INSERT	INTO [monitoring].[alertThresholds] ([category], [alert_name], [operator], [warning_limit], [critical_limit])
		SELECT 'disk-space', 'Logical Disk: Free Disk Space (%)', '<',     8.0,    5.0 UNION ALL
		SELECT 'disk-space', 'Logical Disk: Free Disk Space (MB)', '<', 3000.0, 2048.0
GO

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 30.09.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-----------------------------------------------------------------------------------------------------
--HTML reports rules / checks and instances/machines to be skipped
-----------------------------------------------------------------------------------------------------
RAISERROR('Create table: [monitoring].[alertSkipRules]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sysobjects WHERE id = OBJECT_ID(N'[monitoring].[alertSkipRules]') AND type in (N'U'))
DROP TABLE [monitoring].[alertSkipRules]
GO

CREATE TABLE [monitoring].[alertSkipRules] 
(
	[id]					[smallint] IDENTITY (1, 1)	NOT NULL,
	[category]				[varchar](32)	NOT NULL,
	[alert_name]			[sysname]		NULL,
	[skip_value]			[sysname]		NULL,
	[skip_value2]			[sysname]		NULL,
	[active]				[bit]			NOT NULL CONSTRAINT [DF_alertSkipRules_Active] DEFAULT (1),
	CONSTRAINT [PK_reportHTMLSkipRules] PRIMARY KEY  CLUSTERED 
	(
		[id]
	) ON [PRIMARY],
	CONSTRAINT [UK_reportHTMLSkipRules_Name] UNIQUE  NONCLUSTERED 
	(
		[category],
		[alert_name],
		[skip_value],
		[skip_value2]
	) ON [PRIMARY]
)  ON [PRIMARY]
GO

-----------------------------------------------------------------------------------------------------
RAISERROR('		...insert default data', 10, 1) WITH NOWAIT
GO
SET NOCOUNT ON
GO
INSERT	INTO [monitoring].[alertSkipRules] ([category], [alert_name], [skip_value], [active])
		SELECT 'disk-space', 'Logical Disk: Free Disk Space (%)', NULL, 0 UNION ALL
		SELECT 'disk-space', 'Logical Disk: Free Disk Space (MB)', NULL, 0
GO

RAISERROR('Create procedure: [dbo].[usp_removeFromCatalog]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_removeFromCatalog]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_removeFromCatalog]
GO

CREATE PROCEDURE [dbo].[usp_removeFromCatalog]
		@projectCode		[varchar](32),
		@sqlServerName		[sysname],
		@debugMode			[bit] = 0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 20.02.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SET NOCOUNT ON

DECLARE   @returnValue			[smallint]
		, @errMessage			[nvarchar](4000)

DECLARE   @projectID			[smallint]
		, @instanceID			[smallint]
		, @machineID			[smallint]
		
-- { sql_statement | statement_block }
BEGIN TRY
	SET @returnValue=1

	-----------------------------------------------------------------------------------------------------
	SELECT @projectID = [id]
	FROM [dbo].[catalogProjects]
	WHERE [code] = @projectCode 

	IF @projectID IS NULL
		begin
			SET @errMessage=N'The value specifief for Project Code is not valid.'
			RAISERROR(@errMessage, 16, 1) WITH NOWAIT
		end

	SELECT   @instanceID = [id]
		   , @machineID = [machine_id]
	FROM [dbo].[catalogInstanceNames]
	WHERE [project_id] = @projectID
		AND [name] = @sqlServerName

	IF @instanceID IS NULL
		begin
			SET @errMessage=N'The value specifief for SQL Server Instance Name is not valid.'
			RAISERROR(@errMessage, 16, 1) WITH NOWAIT
		end

	BEGIN TRANSACTION
		-----------------------------------------------------------------------------------------------------
		DELETE jeq
		FROM dbo.jobExecutionQueue jeq
		INNER JOIN dbo.catalogInstanceNames cin ON cin.[project_id] = jeq.[project_id] AND cin.[id] = jeq.[instance_id]
		WHERE cin.[project_id] = @projectID
				AND cin.[id] = @instanceID

		-----------------------------------------------------------------------------------------------------
		DELETE jeq
		FROM dbo.jobExecutionQueue jeq
		INNER JOIN dbo.catalogInstanceNames cin ON cin.[project_id] = jeq.[project_id] AND cin.[id] = jeq.[for_instance_id]
		WHERE cin.[project_id] = @projectID
				AND cin.[id] = @instanceID
	
		-----------------------------------------------------------------------------------------------------
		DELETE lem
		FROM dbo.logEventMessages lem
		INNER JOIN dbo.catalogInstanceNames cin ON cin.[project_id] = lem.[project_id] AND cin.[id] = lem.[instance_id]
		WHERE cin.[project_id] = @projectID
				AND cin.[id] = @instanceID

		-----------------------------------------------------------------------------------------------------
		DELETE lsam
		FROM dbo.logServerAnalysisMessages lsam
		INNER JOIN dbo.catalogInstanceNames cin ON cin.[project_id] = lsam.[project_id] AND cin.[id] = lsam.[instance_id]
		WHERE cin.[project_id] = @projectID
				AND cin.[id] = @instanceID

		-----------------------------------------------------------------------------------------------------
		DELETE sosel
		FROM dbo.statsOSEventLogs sosel
		INNER JOIN dbo.catalogInstanceNames cin ON cin.[project_id] = sosel.[project_id] AND cin.[id] = sosel.[instance_id]
		WHERE cin.[project_id] = @projectID
				AND cin.[id] = @instanceID

		-----------------------------------------------------------------------------------------------------
		DELETE sseld
		FROM dbo.statsSQLServerErrorlogDetails sseld
		INNER JOIN dbo.catalogInstanceNames cin ON cin.[project_id] = sseld.[project_id] AND cin.[id] = sseld.[instance_id]
		WHERE cin.[project_id] = @projectID
				AND cin.[id] = @instanceID

		-----------------------------------------------------------------------------------------------------
		DELETE ssajh
		FROM dbo.statsSQLServerAgentJobsHistory ssajh
		INNER JOIN dbo.catalogInstanceNames cin ON cin.[project_id] = ssajh.[project_id] AND cin.[id] = ssajh.[instance_id]
		WHERE cin.[project_id] = @projectID
				AND cin.[id] = @instanceID

		-----------------------------------------------------------------------------------------------------
		DELETE shcdsi
		FROM dbo.statsDiskSpaceInfo shcdsi
		INNER JOIN dbo.catalogInstanceNames cin ON cin.[project_id] = shcdsi.[project_id] AND cin.[id] = shcdsi.[instance_id]
		WHERE cin.[project_id] = @projectID
				AND cin.[id] = @instanceID

		-----------------------------------------------------------------------------------------------------
		DELETE shcdd
		FROM dbo.statsDatabaseDetails shcdd
		INNER JOIN dbo.catalogDatabaseNames cdb ON cdb.[instance_id] = shcdd.[instance_id] AND cdb.[id] = shcdd.[catalog_database_id]
		INNER JOIN dbo.catalogInstanceNames cin ON cin.[id] = cdb.[instance_id] AND cin.[project_id] = cdb.[project_id]
		WHERE cin.[project_id] = @projectID
				AND cin.[id] = @instanceID

				
		-----------------------------------------------------------------------------------------------------
		DELETE FROM  dbo.catalogDatabaseNames 
		WHERE [project_id] = @projectID
				AND [instance_id] = @instanceID

		-----------------------------------------------------------------------------------------------------
		DELETE FROM  dbo.catalogInstanceNames
		WHERE [project_id] = @projectID
				AND [id] = @instanceID

		-----------------------------------------------------------------------------------------------------
		IF NOT EXISTS(	SELECT * FROM dbo.catalogInstanceNames
						WHERE [project_id] = @projectID
							AND [machine_id] = @machineID
					)
			DELETE FROM  dbo.catalogMachineNames
			WHERE [project_id] = @projectID
					AND [id] = @machineID

	COMMIT
END TRY

BEGIN CATCH
DECLARE 
        @ErrorMessage    NVARCHAR(4000),
        @ErrorNumber     INT,
        @ErrorSeverity   INT,
        @ErrorState      INT,
        @ErrorLine       INT,
        @ErrorProcedure  NVARCHAR(200);
    -- Assign variables to error-handling functions that 
    -- capture information for RAISERROR.
    SELECT 
        @ErrorNumber = ERROR_NUMBER(),
        @ErrorSeverity = ERROR_SEVERITY(),
        @ErrorState = CASE WHEN ERROR_STATE() BETWEEN 1 AND 127 THEN ERROR_STATE() ELSE 1 END ,
        @ErrorLine = ERROR_LINE(),
        @ErrorProcedure = ISNULL(ERROR_PROCEDURE(), '-');
	-- Building the message string that will contain original
    -- error information.
    SELECT @ErrorMessage = 
        N'Error %d, Level %d, State %d, Procedure %s, Line %d, ' + 
            'Message: '+ ERROR_MESSAGE();
    -- Raise an error: msg_str parameter of RAISERROR will contain
    -- the original error information.
    
    
    RAISERROR 
        (
        @ErrorMessage, 
        @ErrorSeverity, 
        @ErrorState,               
        @ErrorNumber,    -- parameter: original error number.
        @ErrorSeverity,  -- parameter: original error severity.
        @ErrorState,     -- parameter: original error state.
        @ErrorProcedure, -- parameter: original error procedure name.
        @ErrorLine       -- parameter: original error line number.
        );

        -- Test XACT_STATE:
        -- If 1, the transaction is committable.
        -- If -1, the transaction is uncommittable and should 
        --     be rolled back.
        -- XACT_STATE = 0 means that there is no transaction and
        --     a COMMIT or ROLLBACK would generate an error.

    -- Test if the transaction is uncommittable.
       IF @@TRANCOUNT >0 ROLLBACK TRANSACTION 
END CATCH

RETURN @returnValue
GO


RAISERROR('Create procedure: [dbo].[usp_refreshMachineCatalogs]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_refreshMachineCatalogs]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_refreshMachineCatalogs]
GO

CREATE PROCEDURE [dbo].[usp_refreshMachineCatalogs]
		@projectCode		[varchar](32),
		@sqlServerName		[sysname],
		@debugMode			[bit] = 0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 08.12.2014
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================
-- Change Date: 2015.04.03 / Andrei STEFAN
-- Description: add domain name to machine information
-----------------------------------------------------------------------------------------


SET NOCOUNT ON

DECLARE   @returnValue			[smallint]
		, @errMessage			[nvarchar](4000)
		, @errDescriptor		[nvarchar](256)
		, @errNumber			[int]

DECLARE   @queryToRun			[nvarchar](max)	-- used for dynamic statements
		, @projectID			[smallint]
		, @isClustered			[bit]
		, @isActive				[bit]
		, @instanceID			[smallint]
		, @domainName			[sysname]

DECLARE @optionXPIsAvailable		[bit],
		@optionXPValue				[int],
		@optionXPHasChanged			[bit],
		@optionAdvancedIsAvailable	[bit],
		@optionAdvancedValue		[int],
		@optionAdvancedHasChanged	[bit]


-- { sql_statement | statement_block }
BEGIN TRY
	SET @returnValue=1

	-----------------------------------------------------------------------------------------------------
	SET @errMessage=N'--Getting Instance information: [' + @sqlServerName + '] / project: [' + @projectCode + ']'
	RAISERROR(@errMessage, 10, 1) WITH NOWAIT
	SET @errMessage=N''
	-----------------------------------------------------------------------------------------------------

	-----------------------------------------------------------------------------------------------------
	--check that SQLServerName is defined as local or as a linked server to current sql server instance
	-----------------------------------------------------------------------------------------------------
	IF (SELECT count(*) FROM sys.sysservers WHERE srvname=@sqlServerName)=0
		begin
			PRINT N'Specified instance name is not defined as local or linked server: ' + @sqlServerName
			PRINT N'Create a new linked server.'

			/* create a linked server for the instance found */
			EXEC [dbo].[usp_addLinkedSQLServer] @ServerName = @sqlServerName
		end


	-----------------------------------------------------------------------------------------------------
	IF object_id('#serverPropertyConfig') IS NOT NULL DROP TABLE #serverPropertyConfig
	CREATE TABLE #serverPropertyConfig
			(
				[value]			[sysname]	NULL
			)

	-----------------------------------------------------------------------------------------------------
	IF object_id('tempdb..#xpCMDShellOutput') IS NOT NULL 
	DROP TABLE #xpCMDShellOutput

	CREATE TABLE #xpCMDShellOutput
	(
		[output]	[nvarchar](max)			NULL
	)
			
	-----------------------------------------------------------------------------------------------------
	IF object_id('#catalogMachineNames') IS NOT NULL 
	DROP TABLE #catalogMachineNames

	CREATE TABLE #catalogMachineNames
	(
		[name]					[sysname]		NULL,
		[domain]				[sysname]		NULL
	)

	-----------------------------------------------------------------------------------------------------
	IF object_id('#catalogInstanceNames') IS NOT NULL 
	DROP TABLE #catalogInstanceNames

	CREATE TABLE #catalogInstanceNames
	(
		[name]					[sysname]		NULL,
		[version]				[sysname]		NULL,
		[edition]				[varchar](256)	NULL,
		[machine_name]			[sysname]		NULL
	)

	-----------------------------------------------------------------------------------------------------
	IF object_id('#catalogDatabaseNames') IS NOT NULL 
	DROP TABLE #catalogDatabaseNames

	CREATE TABLE #catalogDatabaseNames
	(
		[database_id]			[int]			NULL,
		[name]					[sysname]		NULL,
		[state]					[int]			NULL,
		[state_desc]			[nvarchar](64)	NULL
	)

	-----------------------------------------------------------------------------------------------------
	SELECT @projectID = [id]
	FROM [dbo].[catalogProjects]
	WHERE [code] = @projectCode 

	IF @projectID IS NULL
		begin
			SET @errMessage=N'The value specifief for Project Code is not valid.'
			RAISERROR(@errMessage, 16, 1) WITH NOWAIT
		end

	-----------------------------------------------------------------------------------------------------
	--check if the connection to machine can be made & discover instance name
	-----------------------------------------------------------------------------------------------------
	SET @queryToRun = N'SELECT    @@SERVERNAME
								, [product_version]
								, [edition]
								, [machine_name]
						FROM (
								SELECT CAST(SERVERPROPERTY(''ProductVersion'') AS [sysname]) AS [product_version]
									 , SUBSTRING(@@VERSION, 1, CHARINDEX(CAST(SERVERPROPERTY(''ProductVersion'') AS [sysname]), @@VERSION)-1) + CAST(SERVERPROPERTY(''Edition'') AS [sysname]) AS [edition]
									 , CAST(SERVERPROPERTY(''ComputerNamePhysicalNetBIOS'') AS [sysname]) AS [machine_name]
							 )X'
	SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
	IF @debugMode = 1 PRINT @queryToRun

	BEGIN TRY
		INSERT	INTO #catalogInstanceNames([name], [version], [edition], [machine_name])
				EXEC (@queryToRun)
		SET @isActive=1
	END TRY
	BEGIN CATCH
		SET @errMessage=ERROR_MESSAGE()
		SET @errDescriptor = 'dbo.usp_refreshMachineCatalogs - Offline'
		RAISERROR(@errMessage, 10, 1) WITH NOWAIT

		SET @isActive=0
	END CATCH
	

	IF @isActive=0
		begin
			INSERT	INTO #catalogMachineNames([name])
					SELECT cmn.[name]
					FROM [dbo].[catalogMachineNames] cmn
					INNER JOIN [dbo].[catalogInstanceNames] cin ON cmn.[id] = cin.[machine_id] AND cmn.[project_id] = cin.[project_id]
					WHERE cin.[project_id] = @projectID
							AND cin.[name] = @sqlServerName
			
			IF @@ROWCOUNT=0				
				INSERT	INTO #catalogMachineNames([name])					
						SELECT SUBSTRING(@sqlServerName, 1, CASE WHEN CHARINDEX('\', @sqlServerName) > 0 THEN CHARINDEX('\', @sqlServerName)-1 ELSE LEN(@sqlServerName) END)
			
			INSERT	INTO #catalogInstanceNames([name], [version])
					SELECT @sqlServerName, NULL
			
			SET @isClustered = 0
		end
	ELSE
		begin
			DECLARE @SQLMajorVersion [int]

			BEGIN TRY
				SELECT @SQLMajorVersion = REPLACE(LEFT(ISNULL([version], ''), 2), '.', '') 
				FROM #catalogInstanceNames
			END TRY
			BEGIN CATCH
				SET @SQLMajorVersion = 8
			END CATCH

			-----------------------------------------------------------------------------------------------------
			--discover machine names (if clustered instance is present, get all cluster nodes)
			-----------------------------------------------------------------------------------------------------
			SET @isClustered=0

			IF @SQLMajorVersion<=8
				SET @queryToRun = N'SELECT [NodeName] FROM ::fn_virtualservernodes()'
			ELSE
				SET @queryToRun = N'SELECT [NodeName] FROM sys.dm_os_cluster_nodes'
			SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
			IF @debugMode = 1 PRINT @queryToRun
			
			BEGIN TRY
				INSERT	INTO #catalogMachineNames([name])
						EXEC (@queryToRun)		
			END TRY
			BEGIN CATCH
				IF @debugMode=1 PRINT 'An error occured. It will be ignored: ' + ERROR_MESSAGE()
			END CATCH
	
			IF (SELECT COUNT(*) FROM #catalogMachineNames)=0
				begin
					SET @queryToRun = N'SELECT CASE WHEN [computer_name] IS NOT NULL 
													THEN [computer_name]
													ELSE [machine_name]
											  END
										FROM (
												SELECT CAST(SERVERPROPERTY(''ComputerNamePhysicalNetBIOS'') AS [sysname]) AS [computer_name]
											)X,
											(
												SELECT CAST(SERVERPROPERTY(''MachineName'') AS [sysname]) AS [machine_name]
											)Y'
					SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
					IF @debugMode = 1 PRINT @queryToRun

					BEGIN TRY
						INSERT	INTO #catalogMachineNames([name])
								EXEC (@queryToRun)
					END TRY
					BEGIN CATCH
						SET @errMessage=ERROR_MESSAGE()
						SET @errDescriptor = 'dbo.usp_refreshMachineCatalogs'
						RAISERROR(@errMessage, 16, 1) WITH NOWAIT
					END CATCH
				end
			ELSE
				begin
					SET @isClustered = 1
				end
				
			
			-----------------------------------------------------------------------------------------------------
			--discover database names
			-----------------------------------------------------------------------------------------------------
			IF @SQLMajorVersion<=8
				SET @queryToRun = N'SELECT sdb.[dbid], sdb.[name], sdb.[status] AS [state]
											, CASE  WHEN sdb.[status] & 4194584 = 4194584 THEN ''SUSPECT''
													WHEN sdb.[status] & 2097152 = 2097152 THEN ''STANDBY''
													WHEN sdb.[status] & 32768 = 32768 THEN ''EMERGENCY MODE''
													WHEN sdb.[status] & 4096 = 4096 THEN ''SINGLE USER''
													WHEN sdb.[status] & 2048 = 2048 THEN ''DBO USE ONLY''
													WHEN sdb.[status] & 1024 = 1024 THEN ''READ ONLY''
													WHEN sdb.[status] & 512 = 512 THEN ''OFFLINE''
													WHEN sdb.[status] & 256 = 256 THEN ''NOT RECOVERED''
													WHEN sdb.[status] & 128 = 128 THEN ''RECOVERING''
													WHEN sdb.[status] & 64 = 64 THEN ''PRE RECOVERY''
													WHEN sdb.[status] & 32 = 32 THEN ''LOADING''
													WHEN sdb.[status] = 0 THEN ''UNKNOWN''
													ELSE ''ONLINE''
												END AS [state_desc]
									FROM master.dbo.sysdatabases sdb'
			ELSE
				SET @queryToRun = N'SELECT sdb.[database_id], sdb.[name], sdb.[state], sdb.[state_desc]
									FROM sys.databases sdb
									WHERE [is_read_only] = 0 AND [is_in_standby] = 0
									UNION ALL
									SELECT sdb.[database_id], sdb.[name], sdb.[state], ''READ ONLY''
									FROM sys.databases sdb
									WHERE [is_read_only] = 1 AND [is_in_standby] = 0
									UNION ALL
									SELECT sdb.[database_id], sdb.[name], sdb.[state], ''STANDBY''
									FROM sys.databases sdb
									WHERE [is_in_standby] = 1'
			SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
			IF @debugMode = 1 PRINT @queryToRun

			BEGIN TRY
				INSERT	INTO #catalogDatabaseNames([database_id], [name], [state], [state_desc])
						EXEC (@queryToRun)		
			END TRY
			BEGIN CATCH
				SET @errMessage=ERROR_MESSAGE()
				SET @errDescriptor = 'dbo.usp_refreshMachineCatalogs'
				RAISERROR(@errMessage, 16, 1) WITH NOWAIT
			END CATCH

			/*-------------------------------------------------------------------------------------------------------------------------------*/
			/* check if xp_cmdshell is enabled or should be enabled																			 */
			BEGIN TRY
				IF @SQLMajorVersion>8
					begin
						SELECT  @optionXPIsAvailable		= 0,
								@optionXPValue				= 0,
								@optionXPHasChanged			= 0,
								@optionAdvancedIsAvailable	= 0,
								@optionAdvancedValue		= 0,
								@optionAdvancedHasChanged	= 0

						/* enable xp_cmdshell configuration option */
						EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @@SERVERNAME,
																			@configOptionName	= 'xp_cmdshell',
																			@configOptionValue	= 1,
																			@optionIsAvailable	= @optionXPIsAvailable OUT,
																			@optionCurrentValue	= @optionXPValue OUT,
																			@optionHasChanged	= @optionXPHasChanged OUT,
																			@executionLevel		= 0,
																			@debugMode			= @debugMode

						IF @optionXPIsAvailable = 0
							begin
								/* enable show advanced options configuration option */
								EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @@SERVERNAME,
																					@configOptionName	= 'show advanced options',
																					@configOptionValue	= 1,
																					@optionIsAvailable	= @optionAdvancedIsAvailable OUT,
																					@optionCurrentValue	= @optionAdvancedValue OUT,
																					@optionHasChanged	= @optionAdvancedHasChanged OUT,
																					@executionLevel		= 0,
																					@debugMode			= @debugMode

								IF @optionAdvancedIsAvailable = 1 AND (@optionAdvancedValue=1 OR @optionAdvancedHasChanged=1)
									EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @@SERVERNAME,
																						@configOptionName	= 'xp_cmdshell',
																						@configOptionValue	= 1,
																						@optionIsAvailable	= @optionXPIsAvailable OUT,
																						@optionCurrentValue	= @optionXPValue OUT,
																						@optionHasChanged	= @optionXPHasChanged OUT,
																						@executionLevel		= 0,
																						@debugMode			= @debugMode
							end
					end

				IF @optionXPValue=1 OR @SQLMajorVersion=8
					begin
						--run wmi to get the domain name
						SET @queryToRun = N''
						SET @queryToRun = @queryToRun + N'DECLARE @cmdQuery [varchar](102); SET @cmdQuery=''wmic computersystem get Domain''; EXEC xp_cmdshell @cmdQuery;'
			
						IF @sqlServerName<>@@SERVERNAME
							SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + '], ''SET FMTONLY OFF; EXEC(''''' + REPLACE(@queryToRun, '''', '''''''''') + ''''')'')'
						IF @debugMode = 1 PRINT @queryToRun

						INSERT	INTO #xpCMDShellOutput([output])
								EXEC (@queryToRun)
									
						UPDATE #xpCMDShellOutput SET [output]=REPLACE(REPLACE(REPLACE(LTRIM(RTRIM([output])), ' ', ''), CHAR(10), ''), CHAR(13), '')
			
						DELETE FROM #xpCMDShellOutput WHERE LEN([output])<=3 OR [output] IS NULL
						DELETE FROM #xpCMDShellOutput WHERE [output] LIKE '%not recognized as an internal or external command%'
						DELETE FROM #xpCMDShellOutput WHERE [output] LIKE '%operable program or batch file%'
						DELETE TOP (1) FROM #xpCMDShellOutput WHERE SUBSTRING([output], 1, 8)='Domain'
			
						SELECT TOP 1 @domainName = LOWER([output])
						FROM #xpCMDShellOutput

						UPDATE #catalogMachineNames SET [domain] = @domainName
					end

				IF @SQLMajorVersion>8 AND (@optionXPHasChanged=1 OR @optionAdvancedHasChanged=1)
					begin
						/* disable xp_cmdshell configuration option */
						IF @optionXPHasChanged = 1
							EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @@SERVERNAME,
																				@configOptionName	= 'xp_cmdshell',
																				@configOptionValue	= 0,
																				@optionIsAvailable	= @optionXPIsAvailable OUT,
																				@optionCurrentValue	= @optionXPValue OUT,
																				@optionHasChanged	= @optionXPHasChanged OUT,
																				@executionLevel		= 0,
																				@debugMode			= @debugMode

						/* disable show advanced options configuration option */
						IF @optionAdvancedHasChanged = 1
								EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @@SERVERNAME,
																					@configOptionName	= 'show advanced options',
																					@configOptionValue	= 0,
																					@optionIsAvailable	= @optionAdvancedIsAvailable OUT,
																					@optionCurrentValue	= @optionAdvancedValue OUT,
																					@optionHasChanged	= @optionAdvancedHasChanged OUT,
																					@executionLevel		= 0,
																					@debugMode			= @debugMode
					end
			END TRY
			BEGIN CATCH
				PRINT ERROR_MESSAGE()
			END CATCH
		end


	-----------------------------------------------------------------------------------------------------
	--upsert catalog tables
	-----------------------------------------------------------------------------------------------------														
	MERGE INTO [dbo].[catalogMachineNames] AS dest
	USING (	
			SELECT [name], [domain]
			FROM #catalogMachineNames
		  ) AS src([name], [domain])
		ON dest.[name] = src.[name] AND dest.[project_id] = @projectID
	WHEN NOT MATCHED BY TARGET THEN
		INSERT ([project_id], [name], [domain]) 
		VALUES (@projectID, src.[name], src.[domain]) 
	WHEN MATCHED THEN
		UPDATE SET dest.[domain]=src.[domain];


	MERGE INTO [dbo].[catalogInstanceNames] AS dest
	USING (	
			SELECT  cmn.[id]	  AS [machine_id]
				  , cin.[name]	  AS [name]
				  , cin.[version]
				  , cin.[edition]
				  , @isClustered  AS [is_clustered]
				  , @isActive	  AS [active]
				  , cmnA.[id]	  AS [cluster_node_machine_id]
			FROM #catalogInstanceNames cin
			INNER JOIN #catalogMachineNames src ON 1=1
			INNER JOIN [dbo].[catalogMachineNames] cmn ON		cmn.[name] = src.[name] 
															AND cmn.[project_id]=@projectID
			LEFT  JOIN [dbo].[catalogMachineNames] cmnA ON		cmnA.[name] = cin.[machine_name] 
															AND cmnA.[project_id]=@projectID 
															AND @isClustered=1
		  ) AS src([machine_id], [name], [version], [edition], [is_clustered], [active], [cluster_node_machine_id])
		ON dest.[machine_id] = src.[machine_id] AND dest.[name] = src.[name] AND dest.[project_id] = @projectID
	WHEN NOT MATCHED BY TARGET THEN
		INSERT ([machine_id], [project_id], [name], [version], [edition], [is_clustered], [active], [cluster_node_machine_id], [last_refresh_date_utc]) 
		VALUES (src.[machine_id], @projectID, src.[name], src.[version], src.[edition], src.[is_clustered]
				, CASE WHEN src.[is_clustered]=1
						THEN CASE	WHEN src.[active]=1 AND src.[machine_id]=src.[cluster_node_machine_id] 
									THEN 1 
									ELSE 0
							 END
						ELSE src.[active]
				 END
				, src.[cluster_node_machine_id]
				, GETUTCDATE())
	WHEN MATCHED THEN
		UPDATE SET    dest.[is_clustered] = src.[is_clustered]
					, dest.[version] = src.[version]
					, dest.[active] = CASE WHEN src.[is_clustered]=1
											THEN CASE	WHEN src.[active]=1 AND src.[machine_id]=src.[cluster_node_machine_id] 
														THEN 1 
														ELSE 0
												 END
											ELSE src.[active]
										END
					, dest.[edition] = src.[edition]
					, dest.[cluster_node_machine_id] = src.[cluster_node_machine_id]
					, dest.[last_refresh_date_utc] = GETUTCDATE();

	UPDATE cdn
		SET cdn.[active] = 0
	FROM [dbo].[catalogDatabaseNames] cdn
	INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = cdn.[instance_id] AND cin.[project_id] = cdn.[project_id]
	INNER JOIN #catalogInstanceNames	srcIN ON cin.[name] = srcIN.[name]
	WHERE cin.[project_id] = @projectID

	MERGE INTO [dbo].[catalogDatabaseNames] AS dest
	USING (	
			SELECT  cin.[id] AS [instance_id]
				  , src.[name]
				  , src.[database_id]
				  , src.[state]
				  , src.[state_desc]
			FROM  #catalogDatabaseNames src
			INNER JOIN #catalogMachineNames srcMn ON 1=1
			INNER JOIN #catalogInstanceNames srcIN ON 1=1
			INNER JOIN [dbo].[catalogMachineNames] cmn ON cmn.[name] = srcMn.[name] AND cmn.[project_id]=@projectID
			INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[name] = srcIN.[name] AND cin.[machine_id] = cmn.[id]
		  ) AS src([instance_id], [name], [database_id], [state], [state_desc])
		ON dest.[instance_id] = src.[instance_id] AND dest.[name] = src.[name]
	WHEN NOT MATCHED BY TARGET THEN
		INSERT ([instance_id], [project_id], [database_id], [name], [state], [state_desc], [active])
		VALUES (src.[instance_id], @projectID, src.[database_id], src.[name], src.[state], src.[state_desc], 1)
	WHEN MATCHED THEN
		UPDATE SET	dest.[database_id] = src.[database_id]
				  , dest.[state] = src.[state]
				  , dest.[state_desc] = src.[state_desc]
				  , dest.[active] = 1;

	SELECT TOP 1 @instanceID = cin.[id]
	FROM  #catalogMachineNames srcMn
	INNER JOIN #catalogInstanceNames srcIN ON 1=1
	INNER JOIN [dbo].[catalogMachineNames] cmn ON cmn.[name] = srcMn.[name] AND cmn.[project_id]=@projectID
	INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[name] = srcIN.[name] AND cin.[machine_id] = cmn.[id]

	IF @errMessage IS NOT NULL AND @errMessage<>''
		INSERT	INTO [dbo].[logServerAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
				SELECT  @instanceID
					  , @projectID
					  , GETUTCDATE()
					  , @errDescriptor
					  , @errMessage

	RETURN @instanceID
END TRY

BEGIN CATCH
DECLARE 
        @ErrorMessage    NVARCHAR(4000),
        @ErrorNumber     INT,
        @ErrorSeverity   INT,
        @ErrorState      INT,
        @ErrorLine       INT,
        @ErrorProcedure  NVARCHAR(200);
    -- Assign variables to error-handling functions that 
    -- capture information for RAISERROR.
    SELECT 
        @ErrorNumber = ERROR_NUMBER(),
        @ErrorSeverity = ERROR_SEVERITY(),
        @ErrorState = CASE WHEN ERROR_STATE() BETWEEN 1 AND 127 THEN ERROR_STATE() ELSE 1 END ,
        @ErrorLine = ERROR_LINE(),
        @ErrorProcedure = ISNULL(ERROR_PROCEDURE(), '-');
	-- Building the message string that will contain original
    -- error information.
    SELECT @ErrorMessage = 
        N'Error %d, Level %d, State %d, Procedure %s, Line %d, ' + 
            'Message: '+ ERROR_MESSAGE();
    -- Raise an error: msg_str parameter of RAISERROR will contain
    -- the original error information.
    RAISERROR 
        (
        @ErrorMessage, 
        @ErrorSeverity, 
        @ErrorState,               
        @ErrorNumber,    -- parameter: original error number.
        @ErrorSeverity,  -- parameter: original error severity.
        @ErrorState,     -- parameter: original error state.
        @ErrorProcedure, -- parameter: original error procedure name.
        @ErrorLine       -- parameter: original error line number.
        );

        -- Test XACT_STATE:
        -- If 1, the transaction is committable.
        -- If -1, the transaction is uncommittable and should 
        --     be rolled back.
        -- XACT_STATE = 0 means that there is no transaction and
        --     a COMMIT or ROLLBACK would generate an error.

    -- Test if the transaction is uncommittable.
    IF (XACT_STATE()) = -1
    BEGIN
        PRINT
            N'The transaction is in an uncommittable state.' +
            'Rolling back transaction.'
        ROLLBACK TRANSACTION 
   END;

END CATCH

RETURN @returnValue
GO

RAISERROR('Create procedure: [dbo].[usp_hcCollectSQLServerAgentJobsStatus]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_hcCollectSQLServerAgentJobsStatus]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_hcCollectSQLServerAgentJobsStatus]
GO

CREATE PROCEDURE [dbo].[usp_hcCollectSQLServerAgentJobsStatus]
		@projectCode			[varchar](32)=NULL,
		@sqlServerNameFilter	[sysname]='%',
		@jobNameFilter			[sysname]='%',
		@debugMode				[bit]=0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 19.10.2010
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SET NOCOUNT ON

------------------------------------------------------------------------------------------------------------------------------------------
DECLARE @sqlServerName			[sysname],
		@jobName				[sysname],
		@queryToRun				[nvarchar](4000),
		@currentRunning			[int],
		@lastExecutionStatus	[int],
		@lastExecutionDate		[varchar](10),
		@lastExecutionTime		[varchar](10),
		@runningTimeSec			[bigint],
		@projectID				[smallint],
		@instanceID				[smallint],
		@collectStepDetails		[bit],
		@strMessage				[nvarchar](max)


-----------------------------------------------------------------------------------------------------
--appConfigurations - check if step details should be collected
-----------------------------------------------------------------------------------------------------
SELECT	@collectStepDetails = CASE WHEN LOWER([value])='true' THEN 1 ELSE 0 END
FROM	[dbo].[appConfigurations]
WHERE	[name]='Collect SQL Agent jobs step details'
		AND [module] = 'health-check'

SET @collectStepDetails = ISNULL(@collectStepDetails, 0)


/*-------------------------------------------------------------------------------------------------------------------------------*/
IF object_id('#msdbSysJobs') IS NOT NULL DROP TABLE #msdbSysJobs

CREATE TABLE #msdbSysJobs
(
	[name]		[sysname]			NULL
)

------------------------------------------------------------------------------------------------------------------------------------------
--get default project code
IF @projectCode IS NULL
	SELECT	@projectCode = [value]
	FROM	[dbo].[appConfigurations]
	WHERE	[name] = 'Default project code'
			AND [module] = 'common'

SELECT @projectID = [id]
FROM [dbo].[catalogProjects]
WHERE [code] = @projectCode 

IF @projectID IS NULL
	begin
		SET @strMessage=N'The value specifief for Project Code is not valid.'
		RAISERROR(@strMessage, 16, 1) WITH NOWAIT
	end


------------------------------------------------------------------------------------------------------------------------------------------
--A. get servers jobs status informations
-------------------------------------------------------------------------------------------------------------------------
RAISERROR('--Step 1: Delete existing information....', 10, 1) WITH NOWAIT

DELETE ssajh
FROM [health-check].[statsSQLServerAgentJobsHistory]		ssajh
INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = ssajh.[instance_id] AND cin.[project_id] = ssajh.[project_id]
WHERE cin.[project_id] = @projectID
		AND cin.[name] LIKE @sqlServerNameFilter

DELETE lsam
FROM [dbo].[logServerAnalysisMessages]	lsam
INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = lsam.[instance_id] AND cin.[project_id] = lsam.[project_id]
WHERE cin.[project_id] = @projectID
		AND cin.[name] LIKE @sqlServerNameFilter
		AND lsam.[descriptor]='dbo.usp_hcCollectSQLServerAgentJobsStatus'


-------------------------------------------------------------------------------------------------------------------------
RAISERROR('--Step 2: Get Jobs Status Information....', 10, 1) WITH NOWAIT

		
DECLARE crsActiveInstances CURSOR LOCAL FOR 	SELECT	cin.[instance_id], cin.[instance_name]
												FROM	[dbo].[vw_catalogInstanceNames] cin
												WHERE 	cin.[project_id] = @projectID
														AND cin.[instance_active]=1
														AND cin.[instance_name] LIKE @sqlServerNameFilter
OPEN crsActiveInstances
FETCH NEXT FROM crsActiveInstances INTO @instanceID, @sqlServerName
WHILE @@FETCH_STATUS=0
	begin
		SET @strMessage='--	Analyzing server: ' + @sqlServerName
		RAISERROR(@strMessage, 10, 1) WITH NOWAIT
		
		TRUNCATE TABLE #msdbSysJobs
		BEGIN TRY
			SET @queryToRun='SELECT [name] FROM msdb.dbo.sysjobs WHERE [name] LIKE ''' + @jobNameFilter + ''' ORDER BY [name]'
			SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
			IF @debugMode = 1 PRINT @queryToRun		

			INSERT INTO #msdbSysJobs EXEC (@queryToRun)
		END TRY
		BEGIN CATCH
			INSERT	INTO [dbo].[logServerAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
					SELECT  @instanceID
							, @projectID
							, GETUTCDATE()
							, 'dbo.usp_hcCollectSQLServerAgentJobsStatus'
							, ERROR_MESSAGE()		
		END CATCH				


		DECLARE crsJobs CURSOR FOR	SELECT REPLACE([name] , '''', '''''')
									FROM #msdbSysJobs
		OPEN crsJobs
		FETCH NEXT FROM crsJobs INTO @jobName
		WHILE @@FETCH_STATUS=0
			begin
				SET @strMessage				= NULL
				SET @currentRunning			= NULL
				SET @lastExecutionStatus	= NULL
				SET @lastExecutionDate		= NULL
				SET @lastExecutionTime 		= NULL

				BEGIN TRY
					EXEC dbo.usp_sqlAgentJobCheckStatus		@sqlServerName			= @sqlServerName,
															@jobName				= @jobName,
															@strMessage				= @strMessage OUT,
															@currentRunning			= @currentRunning OUT,
															@lastExecutionStatus	= @lastExecutionStatus OUT,
															@lastExecutionDate		= @lastExecutionDate OUT,
															@lastExecutionTime 		= @lastExecutionTime OUT,
															@runningTimeSec			= @runningTimeSec OUT,
															@selectResult			= 0,
															@extentedStepDetails	= @collectStepDetails,		
															@debugMode				= @debugMode

					INSERT	INTO [health-check].[statsSQLServerAgentJobsHistory]([instance_id], [project_id], [event_date_utc], [job_name], [message], [last_execution_status], [last_execution_date], [last_execution_time], [running_time_sec])
							SELECT	  @instanceID, @projectID, GETUTCDATE(), @jobName, @strMessage
									, @lastExecutionStatus, @lastExecutionDate, @lastExecutionTime
									, @runningTimeSec
				END TRY
				BEGIN CATCH
					INSERT	INTO [dbo].[logServerAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
							SELECT  @instanceID
								  , @projectID
								  , GETUTCDATE()
								  , 'dbo.usp_hcCollectSQLServerAgentJobsStatus'
								  , ERROR_MESSAGE()
				END CATCH
				FETCH NEXT FROM crsJobs INTO @jobName
			end
		CLOSE crsJobs
		DEALLOCATE crsJobs
		FETCH NEXT FROM crsActiveInstances INTO @instanceID, @sqlServerName
	end
CLOSE crsActiveInstances
DEALLOCATE crsActiveInstances
GO


RAISERROR('Create procedure: [dbo].[usp_hcCollectOSEventLogs]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_hcCollectOSEventLogs]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_hcCollectOSEventLogs]
GO

CREATE PROCEDURE [dbo].[usp_hcCollectOSEventLogs]
		@projectCode			[varchar](32)=NULL,
		@sqlServerNameFilter	[sysname]='%',
		@logNameFilter			[sysname]='%',
		@enableXPCMDSHELL		[bit]=1,
		@debugMode				[bit]=0

/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 20.11.2014
-- Module			 : Database Analysis & Performance Monitoring
-- Description		 : read OS event logs: Application, System, Setup
-- ============================================================================
SET NOCOUNT ON

DECLARE   @eventDescriptor				[varchar](256)
		, @logEntryType					[varchar](64)
		, @psLogTypeName				[sysname]
		, @psLogTypeID					[tinyint]
		, @queryToRun					[nvarchar](max)
		, @eventLog						[varchar](max)
		, @eventLogXML					[XML]
		, @projectID					[smallint]
		, @instanceID					[smallint]
		, @strMessage					[nvarchar](4000)
		, @machineID					[smallint]
		, @machineName					[nvarchar](512)
		, @instanceName					[sysname]
		, @psFileLocation				[nvarchar](260)
		, @psFileName					[nvarchar](260)
		, @configEventsInLastHours		[smallint]
		, @configEventsTimeOutSeconds	[int]
		, @startTime					[datetime]
		, @endTime						[datetime]
		, @getInformationEvent			[bit]=0
		, @getWarningsEvent				[bit]=0
		

DECLARE @optionXPIsAvailable		[bit],
		@optionXPValue				[int],
		@optionXPHasChanged			[bit],
		@optionAdvancedIsAvailable	[bit],
		@optionAdvancedValue		[int],
		@optionAdvancedHasChanged	[bit]

/*-------------------------------------------------------------------------------------------------------------------------------*/
IF object_id('tempdb..#psOutput') IS NOT NULL DROP TABLE #psOutput
CREATE TABLE #psOutput
	(
		  [id]	[int] identity(1,1) primary key
		, [xml] [varchar](max)
	)

------------------------------------------------------------------------------------------------------------------------------------------
SELECT @psFileLocation = REVERSE(SUBSTRING(REVERSE([value]), CHARINDEX('\', REVERSE([value])), LEN(REVERSE([value]))))
FROM (
		SELECT CAST(SERVERPROPERTY('ErrorLogFileName') AS [nvarchar](1024)) AS [value]
	)er
	
IF @psFileLocation IS NULL SET @psFileLocation =N'C:\'

------------------------------------------------------------------------------------------------------------------------------------------
--get default project code
IF @projectCode IS NULL
	SELECT	@projectCode = [value]
	FROM	[dbo].[appConfigurations]
	WHERE	[name] = 'Default project code'
			AND [module] = 'common'

SELECT @projectID = [id]
FROM [dbo].[catalogProjects]
WHERE [code] = @projectCode 

IF @projectID IS NULL
	begin
		SET @strMessage=N'ERROR: The value specifief for Project Code is not valid.'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=1
	end

------------------------------------------------------------------------------------------------------------------------------------------
--get event messages time delta
BEGIN TRY
	SELECT	@configEventsInLastHours = [value]
	FROM	[dbo].[appConfigurations]
	WHERE	[name] = N'Collect OS Events from last hours'
			AND [module] = 'health-check'
END TRY
BEGIN CATCH
	SET @configEventsInLastHours = 24
END CATCH

SET @configEventsInLastHours = ISNULL(@configEventsInLastHours, 24)

------------------------------------------------------------------------------------------------------------------------------------------
--option to fetch also information OS events
BEGIN TRY
	SELECT	@getInformationEvent = CASE WHEN LOWER([value])='true' THEN 1 ELSE 0 END
	FROM	[dbo].[appConfigurations]
	WHERE	[name] = 'Collect Information OS Events'
			AND [module] = 'health-check'
END TRY
BEGIN CATCH
	SET @getInformationEvent = 0
END CATCH

SET @getInformationEvent = ISNULL(@getInformationEvent, 0)

------------------------------------------------------------------------------------------------------------------------------------------
--option to fetch also warnings OS events
BEGIN TRY
	SELECT	@getWarningsEvent = CASE WHEN LOWER([value])='true' THEN 1 ELSE 0 END
	FROM	[dbo].[appConfigurations]
	WHERE	[name] = 'Collect Warning OS Events'
			AND [module] = 'health-check'
END TRY
BEGIN CATCH
	SET @getWarningsEvent = 0
END CATCH

SET @getWarningsEvent = ISNULL(@getWarningsEvent, 0)


------------------------------------------------------------------------------------------------------------------------------------------
--option for timeout when fetching OS events
BEGIN TRY
	SELECT	@configEventsTimeOutSeconds = [value]
	FROM	[dbo].[appConfigurations]
	WHERE	[name] = 'Collect OS Events timeout (seconds)'
			AND [module] = 'health-check'
END TRY
BEGIN CATCH
	SET @configEventsTimeOutSeconds = 600
END CATCH

SET @configEventsTimeOutSeconds = ISNULL(@configEventsTimeOutSeconds, 600)



-------------------------------------------------------------------------------------------------------------------------
IF @enableXPCMDSHELL=1
	begin
		SELECT  @optionXPIsAvailable		= 0,
				@optionXPValue				= 0,
				@optionXPHasChanged			= 0,
				@optionAdvancedIsAvailable	= 0,
				@optionAdvancedValue		= 0,
				@optionAdvancedHasChanged	= 0

		/* enable xp_cmdshell configuration option */
		EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @@SERVERNAME,
															@configOptionName	= 'xp_cmdshell',
															@configOptionValue	= 1,
															@optionIsAvailable	= @optionXPIsAvailable OUT,
															@optionCurrentValue	= @optionXPValue OUT,
															@optionHasChanged	= @optionXPHasChanged OUT,
															@executionLevel		= 3,
															@debugMode			= @debugMode

		IF @optionXPIsAvailable = 0
			begin
				/* enable show advanced options configuration option */
				EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @@SERVERNAME,
																	@configOptionName	= 'show advanced options',
																	@configOptionValue	= 1,
																	@optionIsAvailable	= @optionAdvancedIsAvailable OUT,
																	@optionCurrentValue	= @optionAdvancedValue OUT,
																	@optionHasChanged	= @optionAdvancedHasChanged OUT,
																	@executionLevel		= 3,
																	@debugMode			= @debugMode

				IF @optionAdvancedIsAvailable = 1 AND (@optionAdvancedValue=1 OR @optionAdvancedHasChanged=1)
					EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @@SERVERNAME,
																		@configOptionName	= 'xp_cmdshell',
																		@configOptionValue	= 1,
																		@optionIsAvailable	= @optionXPIsAvailable OUT,
																		@optionCurrentValue	= @optionXPValue OUT,
																		@optionHasChanged	= @optionXPHasChanged OUT,
																		@executionLevel		= 3,
																		@debugMode			= @debugMode

			end

		IF @optionXPIsAvailable=0 OR @optionXPValue=0
			begin
				set @strMessage='xp_cmdshell component is turned off. Cannot continue'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
				RETURN 1
			end		
	end

------------------------------------------------------------------------------------------------------------------------------------------
--A. get servers OS events details
-------------------------------------------------------------------------------------------------------------------------
SET @strMessage=N'Step 1: Delete existing information....'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

DELETE soel
FROM [health-check].[statsOSEventLogs]			soel
INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = soel.[instance_id] AND cin.[project_id] = soel.[project_id]
WHERE cin.[project_id] = @projectID
		AND cin.[name] LIKE @sqlServerNameFilter

DELETE lsam
FROM [dbo].[logServerAnalysisMessages]	lsam
INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = lsam.[instance_id] AND cin.[project_id] = lsam.[project_id]
WHERE cin.[project_id] = @projectID
		AND cin.[name] LIKE @sqlServerNameFilter
		AND lsam.[descriptor]='dbo.usp_hcCollectOSEventLogs'


-------------------------------------------------------------------------------------------------------------------------
SET @strMessage=N'Step 2: Generate PowerShell script ...'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0


/*-------------------------------------------------------------------------------------------------------------------------------*/
SET @logEntryType='1,2' /*Critical, Error*/
IF @getWarningsEvent=1
	SET @logEntryType=@logEntryType + ',3'
IF @getInformationEvent=1
	SET @logEntryType=@logEntryType + ',4'

SET @eventDescriptor = 'dbo.usp_hcCollectOSEventLogs-Powershell'

DECLARE crsMachineList CURSOR READ_ONLY FAST_FORWARD FOR SELECT cin.[id] AS [instance_id], cin.[name] AS [instance_name], cmn.[id] AS [machine_id], cmn.[name] AS [machine_name]
														FROM	[dbo].[catalogInstanceNames] cin
														INNER JOIN [dbo].[catalogMachineNames] cmn ON cmn.[project_id]=cin.[project_id] AND cmn.[id]=cin.[machine_id]
														WHERE 	cin.[project_id] = @projectID
																AND cin.[name] LIKE @sqlServerNameFilter
																AND (   cin.[active] = 1
																		OR 
																		(
																			cin.[active] = 0
																			AND cin.[is_clustered] = 1
																			AND EXISTS (
																						SELECT 1
																						FROM	[dbo].[catalogInstanceNames] cin2
																						INNER JOIN [dbo].[catalogMachineNames] cmn2 ON cmn2.[project_id]=cin2.[project_id] AND cmn2.[id]=cin2.[machine_id]
																						WHERE cin2.[project_id] = @projectID
																								AND cin2.[active] = 1	
																								AND cin2.[name] = cin.[name]
																								AND cmn2.[id] <> cmn.[id]
																					)
																		)
																	)
														ORDER BY cin.[name], cmn.[name]
OPEN crsMachineList
FETCH NEXT FROM crsMachineList INTO @instanceID, @instanceName, @machineID, @machineName
WHILE @@FETCH_STATUS=0
	begin
		SET @strMessage='Analyzing server: ' + @machineName
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 2, @stopExecution=0


		-------------------------------------------------------------------------------------------------------------------------
		DECLARE crsLogName CURSOR READ_ONLY FOR SELECT [log_type_name], [log_type_id]
												FROM (
														SELECT 'Application' AS [log_type_name], 1 AS [log_type_id] UNION ALL
														SELECT 'System'		 AS [log_type_name], 2 AS [log_type_id] UNION ALL
														SELECT 'Setup'		 AS [log_type_name], 3 AS [log_type_id] 
													)l
												WHERE [log_type_name] LIKE @logNameFilter

		OPEN crsLogName
		FETCH NEXT FROM crsLogName INTO @psLogTypeName, @psLogTypeID
		WHILE @@FETCH_STATUS=0
			begin
				SET @strMessage=N'Analyze type: ' + @psLogTypeName
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 3, @stopExecution=0

				SET @strMessage=N'generate powershell script'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 4, @stopExecution=0

				DELETE lsam
				FROM [dbo].[logServerAnalysisMessages]	lsam
				INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = lsam.[instance_id] AND cin.[project_id] = lsam.[project_id]
				WHERE cin.[project_id] = @projectID
						AND cin.[id]= @instanceID
						AND lsam.[descriptor]=@eventDescriptor
						
				SET @queryToRun='SELECT CONVERT([varchar](20), GETDATE(), 120) AS [current_date]'
				SET @queryToRun = dbo.ufn_formatSQLQueryForLinkedServer(@instanceName, @queryToRun)
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 4, @stopExecution=0

				TRUNCATE TABLE #psOutput
				BEGIN TRY
					INSERT	INTO #psOutput([xml])
							EXEC (@queryToRun)

					SELECT TOP 1 @endTime = CONVERT([datetime], [xml], 120)
					FROM #psOutput
				END TRY
				BEGIN CATCH
					SET @endTime = GETDATE()
				END CATCH

				SET @endTime = ISNULL(@endTime, GETDATE())
				SET @startTime = DATEADD(hh, -@configEventsInLastHours, @endTime)

				-------------------------------------------------------------------------------------------------------------------------
				SET @queryToRun = N'
						#-- ============================================================================
						#-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
						#-- ============================================================================
						#-- Author			 : Dan Andrei STEFAN
						#-- Create date		 : 20.11.2014
						#-- Module			 : Database Analysis & Performance Monitoring
						#-- Description		 : read OS event logs: Application, System, Setup
						#-- ============================================================================

						$timeoutSeconds = ' + CAST(@configEventsTimeOutSeconds AS [nvarchar]) + N'
						$code = {
									$ErrorActionPreference = "SilentlyContinue"

									#setup OS event filters
									$machineName = ''' + @machineName + N'''
									$eventName = ''' + @psLogTypeName + '''
									$startTime = ''' + CONVERT([varchar](20), @startTime, 120) + N'''
									$endTime = ''' + CONVERT([varchar](20), @endTime, 120) + N'''
									$level = ' + @logEntryType + N'

									#get OS events
									$Error.Clear()
									Get-WinEvent -Computername $machineName -FilterHashTable @{logname=$eventName; Level=$level; StartTime=$startTime; EndTime=$endTime}|Select-Object Id, Level, RecordId, Task, TaskDisplayName, ProviderName, LogName, ProcessId, ThreadId, MachineName, UserId, TimeCreated, LevelDisplayName, Message|ConvertTo-XML -As string|Out-String -Width 32768

									if ($Error) 
									{
										$Error[0].ToString()
									}
								}
						$j = Start-Job -ScriptBlock $code
						if (Wait-Job $j -Timeout $timeoutSeconds) { Receive-Job $j }
						Remove-Job -force $j'
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 4, @stopExecution=0

				INSERT	INTO [dbo].[logServerAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
						SELECT  @instanceID
								, @projectID
								, GETUTCDATE()
								, @eventDescriptor
								, @queryToRun


			
				-------------------------------------------------------------------------------------------------------------------------
				IF NOT (@optionXPIsAvailable=0 OR @optionXPValue=0)
					begin
						-- save powershell script
						SET @psFileName = 'GetOSSystemEvents_' + REPLACE(@machineName, '\', '_') + '_' + @psLogTypeName + '.ps1'
						SET @queryToRun=N'master.dbo.xp_cmdshell ''bcp "SELECT [message] FROM [' + DB_NAME() + '].[dbo].[logServerAnalysisMessages] WHERE [descriptor]=''''' + @eventDescriptor + ''''' AND [instance_id]=' + CAST(@instanceID AS [varchar]) + ' AND [project_id]=' + CAST(@projectID AS [varchar]) + '" queryout "' + @psFileLocation + @psFileName + '" -c ' + CASE WHEN SERVERPROPERTY('InstanceName') IS NOT NULL THEN N'-S ' + @@SERVERNAME ELSE N'' END + N' -T'', no_output'
						IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 4, @stopExecution=0

						EXEC (@queryToRun) 
					end

				-------------------------------------------------------------------------------------------------------------------------
				--executing script to get the OS events
				IF NOT (@optionXPIsAvailable=0 OR @optionXPValue=0)
					begin
						SET @strMessage=N'running powershell script - get OS events...'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 4, @stopExecution=0

						SET @queryToRun='master.dbo.xp_cmdshell N''@PowerShell -File "' + @psFileLocation + @psFileName + '"'''
						IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 4, @stopExecution=0

						TRUNCATE TABLE #psOutput
						BEGIN TRY
							INSERT	INTO #psOutput([xml])
									EXEC (@queryToRun)
						END TRY
						BEGIN CATCH
							SET @strMessage = ERROR_MESSAGE()
							PRINT @strMessage
			
							INSERT	INTO [dbo].[logServerAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
									SELECT  @instanceID
											, @projectID
											, GETUTCDATE()
											, 'dbo.usp_hcCollectOSEventLogs'
											, @strMessage
						END CATCH

						BEGIN TRY
							SET @queryToRun=N'master.dbo.xp_cmdshell ''del "' + @psFileLocation + @psFileName + '"'', no_output'
							IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 4, @stopExecution=0
							EXEC (@queryToRun) 
						END TRY
						BEGIN CATCH
							SET @strMessage = ERROR_MESSAGE()
							PRINT @strMessage
			
							INSERT	INTO [dbo].[logServerAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
									SELECT  @instanceID
											, @projectID
											, GETUTCDATE()
											, 'dbo.usp_hcCollectOSEventLogs'
											, @strMessage
						END CATCH
					end

				-------------------------------------------------------------------------------------------------------------------------
				--executing script to get the OS events
				SET @strMessage=N'analyzing data...'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 4, @stopExecution=0

				IF @debugMode=1 
					SELECT * FROM #psOutput 

				IF	EXISTS (SELECT * FROM #psOutput WHERE [xml] LIKE '%Objects%')
					AND NOT EXISTS(SELECT * FROM #psOutput WHERE [xml] LIKE '%No events were found that match the specified selection criteria%')
					AND NOT EXISTS(SELECT * FROM #psOutput WHERE [xml] LIKE '%There are no more endpoints available from the endpoint mapper%')
					AND NOT EXISTS(SELECT * FROM #psOutput WHERE [xml] LIKE '%The RPC server is unavailable%')
					begin
						SET @eventLog=''
						SELECT @eventLog = ((
												SELECT [xml]
												FROM #psOutput
												ORDER BY [id]
												FOR XML PATH(''), TYPE
											).value('.', 'nvarchar(max)'))
						/*
						SELECT @eventLog=@eventLog + [xml] 
						FROM #psOutput 
						WHERE [xml] IS NOT NULL 
						ORDER BY [id] 
				  		*/
						SET @eventLogXML = @eventLog

						IF @debugMode=1 
							SELECT    @instanceID, @projectID, @machineID, GETUTCDATE(), @psLogTypeID
									, [Id] AS [EventID], [Level], [RecordId], [Task] AS [Category], [TaskDisplayName] AS [CategoryName]
									, [ProviderName] AS [Source]
									, [ProcessId], [ThreadId], [MachineName], [UserId], [TimeCreated], [Message]
							FROM (
									SELECT [value], [attribute], [unique_object] AS [idX]
									FROM (
											SELECT	[property].value('(./text())[1]', 'Varchar(1024)') AS [value],
													[property].value('@Name', 'Varchar(1024)') AS [attribute],
													DENSE_RANK() OVER (ORDER BY [object]) AS unique_object
											FROM @eventLogXML.nodes('Objects/Object') AS b ([object])
											CROSS APPLY b.object.nodes('./Property') AS c (property)
										)X
									WHERE [attribute] IN ('Id', 'Level', 'RecordId', 'Task', 'TaskDisplayName', 'ProviderName', 'LogName', 'ProcessId', 'ThreadId', 'MachineName', 'UserId', 'TimeCreated', 'LevelDisplayName', 'Message')
								)P
							PIVOT
								(
									MAX([value])
									FOR [attribute] IN ([Id], [Level], [RecordId], [Task], [TaskDisplayName], [ProviderName], [LogName], [ProcessId], [ThreadId], [MachineName], [UserId], [TimeCreated], [LevelDisplayName], [Message])
								)pvt

						/* save results to stats table */
						INSERT	INTO [health-check].[statsOSEventLogs](   [instance_id], [project_id], [machine_id], [event_date_utc], [log_type_id]
																		, [event_id], [level_id], [record_id], [category_id], [category_name]
																		, [source], [process_id], [thread_id], [machine_name], [user_id], [time_created], [message])
								SELECT    @instanceID, @projectID, @machineID, GETUTCDATE(), @psLogTypeID
										, [Id] AS [EventID], [Level], [RecordId], [Task] AS [Category], [TaskDisplayName] AS [CategoryName]
										, [ProviderName] AS [Source]
										, [ProcessId], [ThreadId], [MachineName], [UserId], [TimeCreated], [Message]
								FROM (
										SELECT [value], [attribute], [unique_object] AS [idX]
										FROM (
												SELECT	[property].value('(./text())[1]', 'Varchar(1024)') AS [value],
														[property].value('@Name', 'Varchar(1024)') AS [attribute],
														DENSE_RANK() OVER (ORDER BY [object]) AS unique_object
												FROM @eventLogXML.nodes('Objects/Object') AS b ([object])
												CROSS APPLY b.object.nodes('./Property') AS c (property)
											)X
										WHERE [attribute] IN ('Id', 'Level', 'RecordId', 'Task', 'TaskDisplayName', 'ProviderName', 'LogName', 'ProcessId', 'ThreadId', 'MachineName', 'UserId', 'TimeCreated', 'LevelDisplayName', 'Message')
									)P
								PIVOT
									(
										MAX([value])
										FOR [attribute] IN ([Id], [Level], [RecordId], [Task], [TaskDisplayName], [ProviderName], [LogName], [ProcessId], [ThreadId], [MachineName], [UserId], [TimeCreated], [LevelDisplayName], [Message])
									)pvt

					end
				ELSE
					begin
						IF (SELECT COUNT(*) FROM #psOutput WHERE [xml] IS NOT NULL)=0
							INSERT	INTO [dbo].[logServerAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
									SELECT  @instanceID
											, @projectID
											, GETUTCDATE()
											, 'dbo.usp_hcCollectOSEventLogs'
											, 'Timeout occured while running powershell script. (LogName = ' + @psLogTypeName + ')'

						IF EXISTS(SELECT * FROM #psOutput WHERE [xml] LIKE '%There are no more endpoints available from the endpoint mapper%')
							INSERT	INTO [dbo].[logServerAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
									SELECT  @instanceID
											, @projectID
											, GETUTCDATE()
											, 'dbo.usp_hcCollectOSEventLogs'
											, 'There are no more endpoints available from the endpoint mapper.'

						IF EXISTS(SELECT * FROM #psOutput WHERE [xml] LIKE '%The RPC server is unavailable%')
							INSERT	INTO [dbo].[logServerAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
									SELECT  @instanceID
											, @projectID
											, GETUTCDATE()
											, 'dbo.usp_hcCollectOSEventLogs'
											, 'The RPC server is unavailable.'
					end
					
				FETCH NEXT FROM crsLogName INTO @psLogTypeName, @psLogTypeID
			end
		CLOSE crsLogName
		DEALLOCATE crsLogName

		FETCH NEXT FROM crsMachineList INTO @instanceID, @instanceName, @machineID, @machineName
	end
CLOSE crsMachineList
DEALLOCATE crsMachineList

DELETE lsam
FROM [dbo].[logServerAnalysisMessages]	lsam
INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = lsam.[instance_id] AND cin.[project_id] = lsam.[project_id]
WHERE cin.[project_id] = @projectID
		AND cin.[name] LIKE @sqlServerNameFilter
		AND lsam.[descriptor]=@eventDescriptor

/*-------------------------------------------------------------------------------------------------------------------------------*/
/* disable xp_cmdshell configuration option */
IF @optionXPHasChanged = 1
	EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @@SERVERNAME,
														@configOptionName	= 'xp_cmdshell',
														@configOptionValue	= 0,
														@optionIsAvailable	= @optionXPIsAvailable OUT,
														@optionCurrentValue	= @optionXPValue OUT,
														@optionHasChanged	= @optionXPHasChanged OUT,
														@executionLevel		= 3,
														@debugMode			= @debugMode

/* disable show advanced options configuration option */
IF @optionAdvancedHasChanged = 1
		EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @@SERVERNAME,
															@configOptionName	= 'show advanced options',
															@configOptionValue	= 0,
															@optionIsAvailable	= @optionAdvancedIsAvailable OUT,
															@optionCurrentValue	= @optionAdvancedValue OUT,
															@optionHasChanged	= @optionAdvancedHasChanged OUT,
															@executionLevel		= 3,
															@debugMode			= @debugMode

/*-------------------------------------------------------------------------------------------------------------------------------*/
GO


SET QUOTED_IDENTIFIER ON 
GO
SET ANSI_NULLS ON 
GO

RAISERROR('Create procedure: [dbo].[usp_hcCollectErrorlogMessages]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_hcCollectErrorlogMessages]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_hcCollectErrorlogMessages]
GO

CREATE PROCEDURE [dbo].[usp_hcCollectErrorlogMessages]
		@projectCode			[varchar](32)=NULL,
		@sqlServerNameFilter	[sysname]='%',
		@debugMode				[bit]=0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 29.04.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SET NOCOUNT ON

------------------------------------------------------------------------------------------------------------------------------------------
DECLARE @projectID				[smallint],
		@sqlServerName			[sysname],
		@instanceID				[smallint],
		@queryToRun				[nvarchar](4000),
		@strMessage				[nvarchar](max),
		@errorCode				[int],
		@lineID					[int]

DECLARE @SQLMajorVersion		[int],
		@sqlServerVersion		[sysname],
		@configErrorlogFileNo	[int]


/*-------------------------------------------------------------------------------------------------------------------------------*/
IF object_id('tempdb..#xpReadErrorLog') IS NOT NULL 
DROP TABLE #xpReadErrorLog

CREATE TABLE #xpReadErrorLog
(
	[id]					[int] IDENTITY (1, 1)NOT NULL PRIMARY KEY CLUSTERED ,
	[log_date]				[datetime]		NULL,
	[process_info]			[sysname]		NULL,
	[text]					[varchar](max)	NULL,
	[continuation_row]		[bit]			NULL,
)

------------------------------------------------------------------------------------------------------------------------------------------
--get default project code
IF @projectCode IS NULL
	SELECT	@projectCode = [value]
	FROM	[dbo].[appConfigurations]
	WHERE	[name] = 'Default project code'
			AND [module] = 'common'

SELECT @projectID = [id]
FROM [dbo].[catalogProjects]
WHERE [code] = @projectCode 

IF @projectID IS NULL
	begin
		SET @strMessage=N'The value specified for Project Code is not valid.'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=1
	end


------------------------------------------------------------------------------------------------------------------------------------------
--check the option for number of errorlog files to be analyzed
BEGIN TRY
	SELECT	@configErrorlogFileNo = [value]
	FROM	[dbo].[appConfigurations]
	WHERE	[name] = N'Collect SQL Errorlog last files'
			AND [module] = 'health-check'
END TRY
BEGIN CATCH
	SET @configErrorlogFileNo = 1
END CATCH

SET @configErrorlogFileNo = ISNULL(@configErrorlogFileNo, 1)

------------------------------------------------------------------------------------------------------------------------------------------
--
-------------------------------------------------------------------------------------------------------------------------
SET @strMessage= 'Step 1: Delete existing information...'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=0

DELETE eld
FROM [health-check].[statsSQLServerErrorlogDetails]	eld
INNER JOIN [dbo].[catalogInstanceNames]		cin ON cin.[id] = eld.[instance_id] AND cin.[project_id] = eld.[project_id]
WHERE cin.[project_id] = @projectID
		AND cin.[name] LIKE @sqlServerNameFilter

DELETE lsam
FROM [dbo].[logServerAnalysisMessages]	lsam
INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = lsam.[instance_id] AND cin.[project_id] = lsam.[project_id]
WHERE cin.[project_id] = @projectID
		AND cin.[name] LIKE @sqlServerNameFilter
		AND lsam.[descriptor]='dbo.usp_hcCollectErrorlogMessages'

-------------------------------------------------------------------------------------------------------------------------
SET @strMessage= 'Step 2: Get Errorlog messages...'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=0
		
DECLARE crsActiveInstances CURSOR LOCAL FOR 	SELECT	cin.[instance_id], cin.[instance_name], cin.[version]
												FROM	[dbo].[vw_catalogInstanceNames] cin
												WHERE 	cin.[project_id] = @projectID
														AND cin.[instance_active]=1
														AND cin.[instance_name] LIKE @sqlServerNameFilter
												ORDER BY cin.[instance_name]
OPEN crsActiveInstances
FETCH NEXT FROM crsActiveInstances INTO @instanceID, @sqlServerName, @sqlServerVersion
WHILE @@FETCH_STATUS=0
	begin
		SET @strMessage= 'Analyzing server: ' + @sqlServerName
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

		BEGIN TRY
			SELECT @SQLMajorVersion = REPLACE(LEFT(ISNULL(@sqlServerVersion, ''), 2), '.', '') 
		END TRY
		BEGIN CATCH
			SET @SQLMajorVersion = 8
		END CATCH

		TRUNCATE TABLE #xpReadErrorLog

		/* get errorlog messages */
		WHILE @configErrorlogFileNo > 0
			begin
				IF @sqlServerName <> @@SERVERNAME
					begin
						IF @SQLMajorVersion < 11
							SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + N'], ''SET FMTONLY OFF; EXEC xp_readerrorlog ' + CAST((@configErrorlogFileNo-1) AS [nvarchar]) + ''')x'
						ELSE
							SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + N'], ''SET FMTONLY OFF; EXEC xp_readerrorlog ' + CAST((@configErrorlogFileNo-1) AS [nvarchar]) + ' WITH RESULT SETS(([log_date] [datetime] NULL, [process_info] [sysname] NULL, [text] [varchar](max) NULL))'')x'
					end
				ELSE
					SET @queryToRun = N'xp_readerrorlog ' + CAST((@configErrorlogFileNo-1) AS [nvarchar])
				IF @debugMode=1	PRINT @queryToRun

				BEGIN TRY
					IF @SQLMajorVersion > 8 
						INSERT	INTO #xpReadErrorLog([log_date], [process_info], [text])
								EXEC (@queryToRun)
					ELSE
						INSERT	INTO #xpReadErrorLog([text], [continuation_row])
								EXEC (@queryToRun)
				END TRY
				BEGIN CATCH
					SET @strMessage = ERROR_MESSAGE()
					PRINT @strMessage

					INSERT	INTO [dbo].[logServerAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
							SELECT  @instanceID
									, @projectID
									, GETUTCDATE()
									, 'dbo.usp_hcCollectErrorlogMessages'
									, @strMessage
				END CATCH

				SET @configErrorlogFileNo = @configErrorlogFileNo - 1
			end

		/* re-parse messages for 2k version */
		IF @SQLMajorVersion = 8 
			begin
				SET @strMessage= 'rebuild messages for ContinuationRows'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 2, @stopExecution=0

				DECLARE crsErrorlogContinuation CURSOR FAST_FORWARD FOR SELECT [id], [text]
																		FROM #xpReadErrorLog
																		WHERE [continuation_row]=1
				OPEN crsErrorlogContinuation
				FETCH NEXT FROM crsErrorlogContinuation INTO @lineID, @strMessage
				WHILE @@FETCH_STATUS=0
					begin
						UPDATE #xpReadErrorLog
							SET [text] = [text] + @strMessage
						WHERE [id] = @lineID-1

						FETCH NEXT FROM crsErrorlogContinuation INTO @lineID, @strMessage
					end
				CLOSE crsErrorlogContinuation
				DEALLOCATE crsErrorlogContinuation
				
				DELETE 
				FROM #xpReadErrorLog
				WHERE [continuation_row]=1

				SET @strMessage= 'split messages / SQL Server 2000'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 2, @stopExecution=0

				UPDATE eld
					SET   eld.[log_date] = X.[log_date]
						, eld.[process_info] = X.[process_info]
						, eld.[text] = X.[text]
				FROM #xpReadErrorLog eld
				INNER JOIN 
					(
						SELECT    [id]
								, SUBSTRING([text], 1, 22) AS [log_date]
								, LTRIM(RTRIM(SUBSTRING([text], 24, CHARINDEX(' ', [text], 24) -23))) AS [process_info]
								, LTRIM(RTRIM(SUBSTRING([text], CHARINDEX(' ', [text], 24), LEN([text])))) AS [text]
						FROM #xpReadErrorLog
						WHERE LEFT([text], 4) = CAST(YEAR(GETDATE()) AS [varchar])
							OR LEFT([text], 4) =CAST(YEAR(GETDATE())-1 AS [varchar])
					)X ON X.[id] = eld.[id]
			end

		/* save results to stats table */
		INSERT	INTO [health-check].[statsSQLServerErrorlogDetails]([instance_id], [project_id], [event_date_utc], [log_date], [process_info], [text])
				SELECT @instanceID, @projectID, GETUTCDATE(), [log_date], [process_info], [text]
				FROM #xpReadErrorLog
				WHERE [log_date] IS NOT NULL
				ORDER BY [log_date], [id]

		FETCH NEXT FROM crsActiveInstances INTO @instanceID, @sqlServerName, @sqlServerVersion
	end
CLOSE crsActiveInstances
DEALLOCATE crsActiveInstances
GO


RAISERROR('Create procedure: [dbo].[usp_hcCollectDiskSpaceUsage]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_hcCollectDiskSpaceUsage]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_hcCollectDiskSpaceUsage]
GO

CREATE PROCEDURE [dbo].[usp_hcCollectDiskSpaceUsage]
		@projectCode			[varchar](32)=NULL,
		@sqlServerNameFilter	[sysname]='%',
		@enableXPCMDSHELL		[bit]=0,
		@debugMode				[bit]=0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 28.01.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SET NOCOUNT ON

------------------------------------------------------------------------------------------------------------------------------------------
DECLARE @projectID				[smallint],
		@sqlServerName			[sysname],
		@instanceID				[smallint],
		@queryToRun				[nvarchar](4000),
		@strMessage				[nvarchar](4000),
		@SQLMajorVersion		[int],
		@sqlServerVersion		[sysname],
		@runxpFixedDrives		[bit],
		@runwmicLogicalDisk		[bit],
		@errorCode				[int]

DECLARE @optionXPIsAvailable		[bit],
		@optionXPValue				[int],
		@optionXPHasChanged			[bit],
		@optionAdvancedIsAvailable	[bit],
		@optionAdvancedValue		[int],
		@optionAdvancedHasChanged	[bit]


/*-------------------------------------------------------------------------------------------------------------------------------*/
IF object_id('tempdb..#xpCMDShellOutput') IS NOT NULL 
DROP TABLE #xpCMDShellOutput

CREATE TABLE #xpCMDShellOutput
(
	[id]		[int] IDENTITY(1,1),
	[output]	[nvarchar](max)			NULL
)

/*-------------------------------------------------------------------------------------------------------------------------------*/
IF object_id('#xp_cmdshell') IS NOT NULL DROP TABLE #xp_cmdshell

CREATE TABLE #xp_cmdshell
(
	[output]		[nvarchar](max)		NULL,
	[instance_name]	[sysname]			NULL,
	[machine_name]	[sysname]			NULL
)

/*-------------------------------------------------------------------------------------------------------------------------------*/
IF object_id('#diskSpaceInfo') IS NOT NULL DROP TABLE #diskSpaceInfo
CREATE TABLE #diskSpaceInfo
(
	[logical_drive]			[char](1)			NULL,
	[volume_mount_point]	[nvarchar](512)		NULL,
	[total_size_mb]			[numeric](18,3)		NULL,
	[available_space_mb]	[numeric](18,3)		NULL,
	[block_size]			[int]				NULL,
	[percent_available]		[numeric](6,2)		NULL
)

------------------------------------------------------------------------------------------------------------------------------------------
--get default project code
IF @projectCode IS NULL
	SELECT	@projectCode = [value]
	FROM	[dbo].[appConfigurations]
	WHERE	[name] = 'Default project code'
			AND [module] = 'common'

SELECT @projectID = [id]
FROM [dbo].[catalogProjects]
WHERE [code] = @projectCode 

IF @projectID IS NULL
	begin
		SET @strMessage=N'The value specifief for Project Code is not valid.'
		RAISERROR(@strMessage, 16, 1) WITH NOWAIT
	end


------------------------------------------------------------------------------------------------------------------------------------------
IF @enableXPCMDSHELL=1
	begin
		SELECT  @optionXPIsAvailable		= 0,
				@optionXPValue				= 0,
				@optionXPHasChanged			= 0,
				@optionAdvancedIsAvailable	= 0,
				@optionAdvancedValue		= 0,
				@optionAdvancedHasChanged	= 0

		/* enable xp_cmdshell configuration option */
		EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @@SERVERNAME,
															@configOptionName	= 'xp_cmdshell',
															@configOptionValue	= 1,
															@optionIsAvailable	= @optionXPIsAvailable OUT,
															@optionCurrentValue	= @optionXPValue OUT,
															@optionHasChanged	= @optionXPHasChanged OUT,
															@executionLevel		= 0,
															@debugMode			= @debugMode

		IF @optionXPIsAvailable = 0
			begin
				/* enable show advanced options configuration option */
				EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @@SERVERNAME,
																	@configOptionName	= 'show advanced options',
																	@configOptionValue	= 1,
																	@optionIsAvailable	= @optionAdvancedIsAvailable OUT,
																	@optionCurrentValue	= @optionAdvancedValue OUT,
																	@optionHasChanged	= @optionAdvancedHasChanged OUT,
																	@executionLevel		= 0,
																	@debugMode			= @debugMode

				IF @optionAdvancedIsAvailable = 1 AND (@optionAdvancedValue=1 OR @optionAdvancedHasChanged=1)
					EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @@SERVERNAME,
																		@configOptionName	= 'xp_cmdshell',
																		@configOptionValue	= 1,
																		@optionIsAvailable	= @optionXPIsAvailable OUT,
																		@optionCurrentValue	= @optionXPValue OUT,
																		@optionHasChanged	= @optionXPHasChanged OUT,
																		@executionLevel		= 0,
																		@debugMode			= @debugMode

			end

		IF @optionXPIsAvailable=0 OR @optionXPValue=0
			begin
				set @queryToRun='xp_cmdshell component is turned off. Cannot continue'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
				RETURN 1
			end		
	end


------------------------------------------------------------------------------------------------------------------------------------------
--
-------------------------------------------------------------------------------------------------------------------------
RAISERROR('--Step 1: Delete existing information....', 10, 1) WITH NOWAIT

DELETE dsi
FROM [health-check].[statsDiskSpaceInfo]		dsi
INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = dsi.[instance_id] AND cin.[project_id] = dsi.[project_id]
WHERE cin.[project_id] = @projectID
		AND cin.[name] LIKE @sqlServerNameFilter

DELETE lsam
FROM [dbo].[logServerAnalysisMessages]	lsam
INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = lsam.[instance_id] AND cin.[project_id] = lsam.[project_id]
WHERE cin.[project_id] = @projectID
		AND cin.[name] LIKE @sqlServerNameFilter
		AND lsam.[descriptor]='dbo.usp_hcCollectDiskSpaceUsage'

-------------------------------------------------------------------------------------------------------------------------
RAISERROR('--Step 2: Get Instance Details Information....', 10, 1) WITH NOWAIT
		
DECLARE crsActiveInstances CURSOR LOCAL FOR 	SELECT	cin.[instance_id], cin.[instance_name], cin.[version]
												FROM	[dbo].[vw_catalogInstanceNames] cin
												WHERE 	cin.[project_id] = @projectID
														AND cin.[instance_active]=1
														AND cin.[instance_name] LIKE @sqlServerNameFilter
												ORDER BY cin.[instance_name]
OPEN crsActiveInstances
FETCH NEXT FROM crsActiveInstances INTO @instanceID, @sqlServerName, @sqlServerVersion
WHILE @@FETCH_STATUS=0
	begin
		SET @strMessage='--	Analyzing server: ' + @sqlServerName
		RAISERROR(@strMessage, 10, 1) WITH NOWAIT

		TRUNCATE TABLE #diskSpaceInfo
		TRUNCATE TABLE #xp_cmdshell
		TRUNCATE TABLE #xpCMDShellOutput

		BEGIN TRY
			SELECT @SQLMajorVersion = REPLACE(LEFT(ISNULL(@sqlServerVersion, ''), 2), '.', '') 
		END TRY
		BEGIN CATCH
			SET @SQLMajorVersion = 8
		END CATCH

		/* get volume space / free disk space details */
		SET @runwmicLogicalDisk=1
		SET @runxpFixedDrives=1
		IF @SQLMajorVersion >= 10
			begin				
				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + N'SELECT DISTINCT
													  UPPER(SUBSTRING([physical_name], 1, 1)) [logical_drive]
													, CASE WHEN LEN([volume_mount_point])=3 THEN UPPER([volume_mount_point]) ELSE [volume_mount_point] END [volume_mount_point]
													, [total_bytes] / 1024 / 1024 AS [total_size_mb]
													, [available_bytes] / 1024 / 1024 AS [available_space_mb]
													, CAST(ISNULL(ROUND([available_bytes] / CAST(NULLIF([total_bytes], 0) AS [numeric](20,3)) * 100, 2), 0) AS [numeric](10,2)) AS [percent_available]
												FROM sys.master_files AS f
												CROSS APPLY sys.dm_os_volume_stats(f.[database_id], f.[file_id])'
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode=1	PRINT @queryToRun

				TRUNCATE TABLE #diskSpaceInfo
				BEGIN TRY
						INSERT	INTO #diskSpaceInfo([logical_drive], [volume_mount_point], [total_size_mb], [available_space_mb], [percent_available])
							EXEC (@queryToRun)
						SET @runwmicLogicalDisk=0
						SET @runxpFixedDrives=0
				END TRY
				BEGIN CATCH
					IF @debugMode=1 PRINT 'An error occured. It will be ignored: ' + ERROR_MESSAGE()					
				END CATCH
			end

		IF @runwmicLogicalDisk=1
			begin
				/* try to run wmic */
				IF @enableXPCMDSHELL=1 AND @optionXPIsAvailable=1
					begin
						BEGIN TRY
								SET @queryToRun = N''
								--SET @queryToRun = @queryToRun + N'DECLARE @cmdQuery [varchar](102); SET @cmdQuery=''wmic logicaldisk get Caption, FreeSpace, Size''; EXEC xp_cmdshell @cmdQuery;'
								SET @queryToRun = @queryToRun + N'DECLARE @cmdQuery [varchar](102); SET @cmdQuery=''wmic volume get Name, Capacity, FreeSpace, BlockSize, DriveType''; EXEC xp_cmdshell @cmdQuery;'
			
								IF @sqlServerName<>@@SERVERNAME
									SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + '], ''SET FMTONLY OFF; EXEC(''''' + REPLACE(@queryToRun, '''', '''''''''') + ''''')'')'
								IF @debugMode = 1 PRINT @queryToRun

								INSERT	INTO #xpCMDShellOutput([output])
										EXEC (@queryToRun)

								DELETE FROM #xpCMDShellOutput WHERE LEN([output])<=3 OR [output] LIKE '%\\?\Volume%' OR [output] IS NULL

								INSERT	INTO #diskSpaceInfo([logical_drive], [volume_mount_point], [total_size_mb], [available_space_mb], [block_size])
										SELECT	  LEFT([name], 1) AS [drive]
												, LTRIM(RTRIM([name])) AS [name]
												, CAST(REPLACE([capacity], ' ', '') AS [bigint]) / (1024 * 1024.) AS [total_size_mb]
												, CAST(REPLACE([free_space], ' ', '') AS [bigint]) / (1024 * 1024.) AS [available_space_mb]
												, [block_size]
										FROM (
												SELECT SUBSTRING([output], [block_size_start_pos], [capacity_start_pos] - [block_size_start_pos] - 1)	 AS [block_size],
														SUBSTRING([output], [capacity_start_pos], [drive_type_start_pos] - [capacity_start_pos] - 1)	 AS [capacity],
														SUBSTRING([output], [drive_type_start_pos], [free_space_start_pos] - [drive_type_start_pos] - 1) AS [drive_type],
														SUBSTRING([output], [free_space_start_pos], [name_start_pos] - [free_space_start_pos] - 1)		 AS [free_space],
														SUBSTRING([output], [name_start_pos], LEN([output]) - [name_start_pos] - 1)						 AS [name]
												FROM #xpCMDShellOutput X
												INNER JOIN (
															SELECT  CHARINDEX('BlockSize', [output]) AS [block_size_start_pos],
																	CHARINDEX('Capacity', [output])	 AS [capacity_start_pos],
																	CHARINDEX('DriveType', [output]) AS [drive_type_start_pos],
																	CHARINDEX('FreeSpace', [output]) AS [free_space_start_pos],
																	CHARINDEX('Name', [output])		 AS [name_start_pos]
															FROM	#xpCMDShellOutput 
															WHERE [id]=1
															) P ON 1=1
												WHERE X.[id]>1
											)A
										WHERE [drive_type]=3

								DELETE FROM #diskSpaceInfo WHERE [total_size_mb]=0

								UPDATE #diskSpaceInfo
										SET [percent_available] =  CAST(ISNULL(ROUND([available_space_mb] / CAST(NULLIF([total_size_mb], 0) AS [numeric](20,3)) * 100, 2), 0) AS [numeric](10,2)) 

								SET @runxpFixedDrives=0
						END TRY
						BEGIN CATCH
							IF @debugMode=1 PRINT 'An error occured. It will be ignored: ' + ERROR_MESSAGE()					
						END CATCH
					end
			end

		IF @runxpFixedDrives=1
			begin
				IF @sqlServerName <> @@SERVERNAME
					begin
						SET @queryToRun = N''
						IF @SQLMajorVersion < 11
							SET @queryToRun = @queryToRun + N'SELECT * FROM OPENQUERY([' + @sqlServerName + N'], ''SET FMTONLY OFF; EXEC xp_fixeddrives'')x'
						ELSE
							SET @queryToRun = @queryToRun + N'SELECT * FROM OPENQUERY([' + @sqlServerName + N'], ''SET FMTONLY OFF; EXEC xp_fixeddrives WITH RESULT SETS(([drive] [sysname], [MB free] [bigint]))'')x'

						IF @debugMode=1	PRINT @queryToRun

						TRUNCATE TABLE #diskSpaceInfo
						BEGIN TRY
								INSERT	INTO #diskSpaceInfo([logical_drive], [available_space_mb])
									EXEC (@queryToRun)
						END TRY
						BEGIN CATCH
							SET @strMessage = ERROR_MESSAGE()
							PRINT @strMessage
							INSERT	INTO [dbo].[logServerAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
									SELECT  @instanceID
											, @projectID
											, GETUTCDATE()
											, 'dbo.usp_hcCollectDiskSpaceUsage'
											, @strMessage
						END CATCH

					end
				ELSE
					begin							
						BEGIN TRY
							INSERT	INTO #diskSpaceInfo([logical_drive], [available_space_mb])
									EXEC xp_fixeddrives
						END TRY
						BEGIN CATCH
							SET @strMessage = ERROR_MESSAGE()
							PRINT @strMessage

							INSERT	INTO [dbo].[logServerAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
									SELECT  @instanceID
											, @projectID
											, GETUTCDATE()
											, 'dbo.usp_hcCollectDiskSpaceUsage'
											, @strMessage
						END CATCH
					end

			end
				
		/* save results to stats table */
		INSERT	INTO [health-check].[statsDiskSpaceInfo]([instance_id], [project_id], [event_date_utc], [logical_drive], [volume_mount_point], [total_size_mb], [available_space_mb], [percent_available], [block_size])
				SELECT    @instanceID, @projectID, GETUTCDATE()
						, [logical_drive], [volume_mount_point], [total_size_mb], [available_space_mb], [percent_available], [block_size]
				FROM #diskSpaceInfo
							
		FETCH NEXT FROM crsActiveInstances INTO @instanceID, @sqlServerName, @sqlServerVersion
	end
CLOSE crsActiveInstances
DEALLOCATE crsActiveInstances


/*-------------------------------------------------------------------------------------------------------------------------------*/
/* disable xp_cmdshell configuration option */
IF @optionXPHasChanged = 1
	EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @@SERVERNAME,
														@configOptionName	= 'xp_cmdshell',
														@configOptionValue	= 0,
														@optionIsAvailable	= @optionXPIsAvailable OUT,
														@optionCurrentValue	= @optionXPValue OUT,
														@optionHasChanged	= @optionXPHasChanged OUT,
														@executionLevel		= 0,
														@debugMode			= @debugMode

/* disable show advanced options configuration option */
IF @optionAdvancedHasChanged = 1
		EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @@SERVERNAME,
															@configOptionName	= 'show advanced options',
															@configOptionValue	= 0,
															@optionIsAvailable	= @optionAdvancedIsAvailable OUT,
															@optionCurrentValue	= @optionAdvancedValue OUT,
															@optionHasChanged	= @optionAdvancedHasChanged OUT,
															@executionLevel		= 0,
															@debugMode			= @debugMode


GO


SET QUOTED_IDENTIFIER ON 
GO
SET ANSI_NULLS ON 
GO

RAISERROR('Create procedure: [dbo].[usp_hcCollectDatabaseDetails]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_hcCollectDatabaseDetails]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_hcCollectDatabaseDetails]
GO

CREATE PROCEDURE [dbo].[usp_hcCollectDatabaseDetails]
		@projectCode			[varchar](32)=NULL,
		@sqlServerNameFilter	[sysname]='%',
		@databaseNameFilter		[sysname]='%',
		@debugMode				[bit]=0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 30.12.2014
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SET NOCOUNT ON

------------------------------------------------------------------------------------------------------------------------------------------
DECLARE @projectID				[smallint],
		@sqlServerName			[sysname],
		@instanceID				[smallint],
		@catalogDatabaseID		[smallint],
		@databaseID				[int],
		@databaseName			[sysname],
		@queryToRun				[nvarchar](4000),
		@strMessage				[nvarchar](4000)

DECLARE @SQLMajorVersion		[int],
		@sqlServerVersion		[sysname],
		@dbccLastKnownGood		[datetime]

/*-------------------------------------------------------------------------------------------------------------------------------*/
IF object_id('#databaseSpaceInfo') IS NOT NULL DROP TABLE #databaseSpaceInfo
CREATE TABLE #databaseSpaceInfo
(
	[drive]					[varchar](2)		NULL,
	[is_log_file]			[bit]				NULL,
	[size_kb]				[int]				NULL,
	[space_used_kb]			[int]				NULL,
	[is_growth_limited]		[bit]				NULL
)

/*-------------------------------------------------------------------------------------------------------------------------------*/
IF object_id('#dbccDBINFO') IS NOT NULL DROP TABLE #dbccDBINFO
CREATE TABLE #dbccDBINFO
	(
		[id]				[int] IDENTITY(1,1),
		[ParentObject]		[varchar](255),
		[Object]			[varchar](255),
		[Field]				[varchar](255),
		[Value]				[varchar](255)
	)
	
/*-------------------------------------------------------------------------------------------------------------------------------*/
IF object_id('#dbccLastKnownGood') IS NOT NULL DROP TABLE #dbccLastKnownGood
CREATE TABLE #dbccLastKnownGood
(
	[Value]					[sysname]			NULL
)

/*-------------------------------------------------------------------------------------------------------------------------------*/
IF object_id('#statsDatabaseDetails') IS NOT NULL DROP TABLE #statsDatabaseDetails
CREATE TABLE #statsDatabaseDetails
(
	[database_id]				[int]			NOT NULL,
	[query_type]				[tinyint]		NOT NULL,
	[data_size_mb]				[numeric](20,3)	NULL,
	[data_space_used_percent]	[numeric](6,2)	NULL,
	[log_size_mb]				[numeric](20,3)	NULL,
	[log_space_used_percent]	[numeric](6,2)	NULL,
	[is_auto_close]				[bit]			NULL,
	[is_auto_shrink]			[bit]			NULL,
	[physical_drives]			[sysname]		NULL,
	[last_backup_time]			[datetime]		NULL,
	[last_dbcc checkdb_time]	[datetime]		NULL,
	[recovery_model]			[tinyint]		NULL,
	[page_verify_option]		[tinyint]		NULL,
	[compatibility_level]		[tinyint]		NULL,
	[is_growth_limited]			[bit]			NULL
)

------------------------------------------------------------------------------------------------------------------------------------------
--get default project code
IF @projectCode IS NULL
	SELECT	@projectCode = [value]
	FROM	[dbo].[appConfigurations]
	WHERE	[name] = 'Default project code'
			AND [module] = 'common'

SELECT @projectID = [id]
FROM [dbo].[catalogProjects]
WHERE [code] = @projectCode 

IF @projectID IS NULL
	begin
		SET @strMessage=N'The value specifief for Project Code is not valid.'
		RAISERROR(@strMessage, 16, 1) WITH NOWAIT
	end


------------------------------------------------------------------------------------------------------------------------------------------
--A. get databases informations
-------------------------------------------------------------------------------------------------------------------------
RAISERROR('--Step 1: Delete existing information....', 10, 1) WITH NOWAIT

DELETE shcdd
FROM [health-check].[statsDatabaseDetails]		shcdd
INNER JOIN [dbo].[catalogDatabaseNames]			cdb ON cdb.[id] = shcdd.[catalog_database_id] AND cdb.[instance_id] = shcdd.[instance_id]
INNER JOIN [dbo].[catalogInstanceNames]			cin ON cin.[id] = cdb.[instance_id] AND cin.[project_id] = cdb.[project_id]
WHERE cin.[project_id] = @projectID
		AND cin.[name] LIKE @sqlServerNameFilter
		AND cdb.[name] LIKE @databaseNameFilter

DELETE lsam
FROM [dbo].[logServerAnalysisMessages]	lsam
INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = lsam.[instance_id] AND cin.[project_id] = lsam.[project_id]
WHERE cin.[project_id] = @projectID
		AND cin.[name] LIKE @sqlServerNameFilter
		AND lsam.[descriptor]='dbo.usp_hcCollectDatabaseDetails'


-------------------------------------------------------------------------------------------------------------------------
RAISERROR('--Step 2: Get Database Details Information....', 10, 1) WITH NOWAIT
		
DECLARE crsActiveInstances CURSOR LOCAL FOR 	SELECT	cin.[instance_id], cin.[instance_name], cin.[version]
												FROM	[dbo].[vw_catalogInstanceNames] cin
												WHERE 	cin.[project_id] = @projectID
														AND cin.[instance_active]=1
														AND cin.[instance_name] LIKE @sqlServerNameFilter
												ORDER BY cin.[instance_name]
OPEN crsActiveInstances
FETCH NEXT FROM crsActiveInstances INTO @instanceID, @sqlServerName, @sqlServerVersion
WHILE @@FETCH_STATUS=0
	begin
		SET @strMessage='--	Analyzing server: ' + @sqlServerName
		RAISERROR(@strMessage, 10, 1) WITH NOWAIT

		TRUNCATE TABLE #statsDatabaseDetails

		BEGIN TRY
			SELECT @SQLMajorVersion = REPLACE(LEFT(ISNULL(@sqlServerVersion, ''), 2), '.', '') 
		END TRY
		BEGIN CATCH
			SET @SQLMajorVersion = 8
		END CATCH

		DECLARE crsActiveDatabases CURSOR LOCAL FOR 	SELECT	cdn.[catalog_database_id], cdn.[database_id], cdn.[database_name]
														FROM	[dbo].[vw_catalogDatabaseNames] cdn
														WHERE 	cdn.[project_id] = @projectID
																AND cdn.[instance_id] = @instanceID
																AND cdn.[active]=1
																AND cdn.[database_name] LIKE @databaseNameFilter
																AND CHARINDEX(cdn.[state_desc], 'ONLINE, READ ONLY')<>0
														ORDER BY cdn.[database_name]
		OPEN crsActiveDatabases	
		FETCH NEXT FROM crsActiveDatabases INTO @catalogDatabaseID, @databaseID, @databaseName
		WHILE @@FETCH_STATUS=0
			begin
				SET @strMessage='--		database: ' + @databaseName
				RAISERROR(@strMessage, 10, 1) WITH NOWAIT

				/* get space allocated / used details */
				IF @sqlServerName <> @@SERVERNAME
					SET @queryToRun = N'SELECT *
										FROM OPENQUERY([' + @sqlServerName + N'], ''EXEC(''''USE [' + @databaseName + N']; 
												SELECT    [drive]
														, CAST([is_logfile]		AS [bit]) AS [is_logfile]
														, SUM([size_kb])		AS [size_mb]
														, SUM([space_used_kb])	AS [space_used_mb]
														, MAX(CAST([is_growth_limited] AS [tinyint])) AS [is_growth_limited]
												FROM (		
														SELECT    [name], [size] * 8 as [size_kb]
																, CAST(FILEPROPERTY([name], ''''''''SpaceUsed'''''''') AS [int]) * 8	AS [space_used_kb]
																, CAST(FILEPROPERTY([name], ''''''''IsLogFile'''''''') AS [bit])		AS [is_logfile]
																, REPLACE(LEFT([' + CASE WHEN @SQLMajorVersion <=8 THEN N'filename' ELSE N'physical_name' END + N'], 2), '''''''':'''''''', '''''''''''''''') AS [drive]
																, ' + CASE	WHEN @SQLMajorVersion <= 8 
																			THEN N'CASE WHEN ([maxsize]=-1 AND [groupid]<>0) OR ([maxsize]=-1 AND [groupid]=0) OR ([maxsize]=268435456 AND [groupid]=0) THEN 0 ELSE 1 END ' 
																			ELSE N'CASE WHEN ([max_size]=-1 AND [type]=0) OR ([max_size]=-1 AND [type]=1) OR ([max_size]=268435456 AND [type]=1) THEN 0 ELSE 1 END '
																	 END + N' AS [is_growth_limited]
														FROM [' + @databaseName + N'].' + CASE WHEN @SQLMajorVersion <=8 THEN N'dbo.sysfiles' ELSE N'sys.database_files' END + N'
													)sf
												GROUP BY [drive], [is_logfile]
										'''')'')x'
				ELSE
					SET @queryToRun = N'USE [' + @databaseName + N']; 
										SELECT    [drive]
												, CAST([is_logfile]		AS [bit]) AS [is_logfile]
												, SUM([size_kb])		AS [size_mb]
												, SUM([space_used_kb])	AS [space_used_mb]
												, MAX(CAST([is_growth_limited] AS [tinyint])) AS [is_growth_limited]
										FROM (		
												SELECT    [name], [size] * 8 as [size_kb]
														, CAST(FILEPROPERTY([name], ''SpaceUsed'') AS [int]) * 8	AS [space_used_kb]
														, CAST(FILEPROPERTY([name], ''IsLogFile'') AS [bit])		AS [is_logfile]
														, REPLACE(LEFT([' + CASE WHEN @SQLMajorVersion <=8 THEN N'filename' ELSE N'physical_name' END + N'], 2), '':'', '''') AS [drive]	
														, ' + CASE	WHEN @SQLMajorVersion <= 8 
																	THEN N'CASE WHEN ([maxsize]=-1 AND [groupid]<>0) OR ([maxsize]=-1 AND [groupid]=0) OR ([maxsize]=268435456 AND [groupid]=0) THEN 0 ELSE 1 END ' 
																	ELSE N'CASE WHEN ([max_size]=-1 AND [type]=0) OR ([max_size]=-1 AND [type]=1) OR ([max_size]=268435456 AND [type]=1) THEN 0 ELSE 1 END '
																END + N' AS [is_growth_limited]
												FROM [' + @databaseName + N'].' + CASE WHEN @SQLMajorVersion <=8 THEN N'dbo.sysfiles' ELSE N'sys.database_files' END + N'
											)sf
										GROUP BY [drive], [is_logfile]'			
				IF @debugMode = 1 PRINT @queryToRun
				
				TRUNCATE TABLE #databaseSpaceInfo
				BEGIN TRY
						INSERT	INTO #databaseSpaceInfo([drive], [is_log_file], [size_kb], [space_used_kb], [is_growth_limited])
							EXEC (@queryToRun)
				END TRY
				BEGIN CATCH
					SET @strMessage = ERROR_MESSAGE()
					PRINT @strMessage
					INSERT	INTO [dbo].[logServerAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
							SELECT  @instanceID
								  , @projectID
								  , GETUTCDATE()
								  , 'dbo.usp_hcCollectDatabaseDetails'
								  , '[' + @databaseName + ']:' + @strMessage
				END CATCH

				/* get last date for dbcc checkdb, only for 2k5+ */
				IF @SQLMajorVersion > 8 
					begin
						IF @sqlServerName <> @@SERVERNAME
							begin
								IF @SQLMajorVersion < 11
									SET @queryToRun = N'SELECT MAX([Value]) AS [Value]
														FROM OPENQUERY([' + @sqlServerName + N'], ''SET FMTONLY OFF; EXEC(''''DBCC DBINFO ([' + @databaseName + N']) WITH TABLERESULTS'''')'')x
														WHERE [Field]=''dbi_dbccLastKnownGood'''
								ELSE
									SET @queryToRun = N'SELECT MAX([Value]) AS [Value]
														FROM OPENQUERY([' + @sqlServerName + N'], ''SET FMTONLY OFF; EXEC(''''DBCC DBINFO ([' + @databaseName + N']) WITH TABLERESULTS'''') WITH RESULT SETS(([ParentObject] [nvarchar](max), [Object] [nvarchar](max), [Field] [nvarchar](max), [Value] [nvarchar](max))) '')x
														WHERE [Field]=''dbi_dbccLastKnownGood'''
							end
						ELSE
							begin							
								BEGIN TRY
									INSERT INTO #dbccDBINFO
											EXEC ('DBCC DBINFO (''' + @databaseName + N''') WITH TABLERESULTS')
								END TRY
								BEGIN CATCH
									SET @strMessage = ERROR_MESSAGE()
									PRINT @strMessage

									INSERT	INTO [dbo].[logServerAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
											SELECT  @instanceID
												  , @projectID
												  , GETUTCDATE()
												  , 'dbo.usp_hcCollectDatabaseDetails'
												  , '[' + @databaseName + ']:' + @strMessage
								END CATCH

								SET @queryToRun = N'SELECT MAX([Value]) AS [Value] FROM #dbccDBINFO WHERE [Field]=''dbi_dbccLastKnownGood'''											
							end

						IF @debugMode = 1 PRINT @queryToRun
				
						TRUNCATE TABLE #dbccLastKnownGood
						BEGIN TRY
							INSERT	INTO #dbccLastKnownGood([Value])
									EXEC (@queryToRun)
						END TRY
						BEGIN CATCH
							SET @strMessage = ERROR_MESSAGE()
							PRINT @strMessage

							INSERT	INTO [dbo].[logServerAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
									SELECT  @instanceID
										  , @projectID
										  , GETUTCDATE()
										  , 'dbo.usp_hcCollectDatabaseDetails'
										  , '[' + @databaseName + ']:' + @strMessage
						END CATCH

						BEGIN TRY
							SELECT @dbccLastKnownGood = CASE WHEN [Value] = '1900-01-01 00:00:00.000' THEN NULL ELSE [Value] END 
							FROM #dbccLastKnownGood
						END TRY
						BEGIN CATCH
							SET @dbccLastKnownGood=NULL
						END CATCH
					end

				/* compute database statistics */
				INSERT	INTO #statsDatabaseDetails([query_type], [database_id], [data_size_mb], [data_space_used_percent], [log_size_mb], [log_space_used_percent], [physical_drives], [last_dbcc checkdb_time], [is_growth_limited])
						SELECT    1, @databaseID
								, CAST([data_size_kb] / 1024. AS [numeric](20,3)) AS [data_size_mb]
								, CAST(CASE WHEN [data_size_kb] <>0 THEN [data_space_used_kb] * 100. / [data_size_kb] ELSE 0 END AS [numeric](6,2)) AS [data_used_percent]
								, CAST([log_size_kb] / 1024. AS [numeric](20,3)) AS [log_size_mb]
								, CAST(CASE WHEN [log_size_kb] <>0 THEN [log_space_used_kb] * 100. / [log_size_kb] ELSE 0 END AS [numeric](6,2)) AS [log_used_percent]
								, [drives]
								, @dbccLastKnownGood
								, [is_growth_limited]
						FROM (
								SELECT    SUM(CASE WHEN [is_log_file] = 0 THEN dsi.[size_kb] ELSE 0 END)		AS [data_size_kb]
										, SUM(CASE WHEN [is_log_file] = 0 THEN dsi.[space_used_kb] ELSE 0 END) 	AS [data_space_used_kb]
										, SUM(CASE WHEN [is_log_file] = 1 THEN dsi.[size_kb] ELSE 0 END) 		AS [log_size_kb]
										, SUM(CASE WHEN [is_log_file] = 1 THEN dsi.[space_used_kb] ELSE 0 END) 	AS [log_space_used_kb]
										, MAX(x.[drives]) [drives]
										, MAX(CAST([is_growth_limited] AS [tinyint])) [is_growth_limited]
								FROM #databaseSpaceInfo dsi
								CROSS APPLY(
											SELECT STUFF(
															(	SELECT ', ' + [drive]
																FROM (	
																		SELECT DISTINCT UPPER([drive]) [drive]
																		FROM #databaseSpaceInfo
																	) AS x
																ORDER BY [drive]
																FOR XML PATH('')
															),1,1,''
														) AS [drives]
											)x
							)db
				FETCH NEXT FROM crsActiveDatabases INTO @catalogDatabaseID, @databaseID, @databaseName
			end
		CLOSE crsActiveDatabases
		DEALLOCATE crsActiveDatabases

		/* get last date for backup and other database flags / options */
		SET @queryToRun = N'SELECT	  2 AS [query_type]
									, bkp.[database_id]
									, CASE WHEN bkp.[last_backup_time] = CONVERT([datetime], ''1900-01-01'', 120) THEN NULL ELSE bkp.[last_backup_time] END AS [last_backup_time]
									, CAST(DATABASEPROPERTY(bkp.[database_name], ''IsAutoClose'')  AS [bit])	AS [is_auto_close]
									, CAST(DATABASEPROPERTY(bkp.[database_name], ''IsAutoShrink'')  AS [bit])	AS [is_auto_shrink]
									, bkp.[recovery_model]
									, bkp.[page_verify_option]
									, bkp.[compatibility_level]
							FROM 	
								(' + 
							CASE	WHEN @SQLMajorVersion <= 8 
									THEN N'	SELECT	  sdb.[dbid]	AS [database_id]
													, sdb.[name]	AS [database_name]
													, CASE CAST(DATABASEPROPERTYEX(sdb.[name], ''Recovery'') AS [sysname]) 
															WHEN ''FULL'' THEN 1 
															WHEN ''BULK_LOGGED'' THEN 2
															WHEN ''SIMPLE'' THEN 3
															ELSE NULL
													  END AS [recovery_model]
													, CASE WHEN sdb.[status] & 16 = 16 THEN 1 ELSE 0 END AS [page_verify_option]
													, sdb.[cmptlevel] AS [compatibility_level]
													, MAX(bs.[backup_finish_date]) AS [last_backup_time]
											FROM dbo.sysdatabases sdb
											LEFT OUTER JOIN msdb.dbo.backupset bs ON bs.[database_name] = sdb.[name] AND bs.type IN (''D'', ''I'')
											WHERE sdb.[name] LIKE ''' + @databaseNameFilter + N'''
											GROUP BY sdb.[name], sdb.[dbid], sdb.[status], sdb.[cmptlevel]'
									ELSE N'SELECT	  sdb.[name]	AS [database_name]
													, sdb.[database_id]
													, sdb.[recovery_model]
													, sdb.[page_verify_option]
													, sdb.[compatibility_level]
													, MAX(bs.[backup_finish_date]) AS [last_backup_time]
											FROM sys.databases sdb
											LEFT OUTER JOIN msdb.dbo.backupset bs ON bs.[database_name] = sdb.[name] AND bs.type IN (''D'', ''I'')
											WHERE sdb.[name] LIKE ''' + @databaseNameFilter + N'''
											GROUP BY sdb.[name], sdb.[database_id], sdb.[recovery_model], sdb.[page_verify_option], sdb.[compatibility_level]'
							END + N'
								)bkp'
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode = 1 PRINT @queryToRun
		
		BEGIN TRY
			INSERT	INTO #statsDatabaseDetails([query_type], [database_id], [last_backup_time], [is_auto_close], [is_auto_shrink], [recovery_model], [page_verify_option], [compatibility_level])
					EXEC (@queryToRun)
		END TRY
		BEGIN CATCH
			SET @strMessage = ERROR_MESSAGE()
			PRINT @strMessage

			INSERT	INTO [dbo].[logServerAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
					SELECT  @instanceID
							, @projectID
							, GETUTCDATE()
							, 'dbo.usp_hcCollectDatabaseDetails'
							, @strMessage
		END CATCH

		/* save results to stats table */
		INSERT	INTO [health-check].[statsDatabaseDetails]([catalog_database_id], [instance_id], 
				 											 [data_size_mb], [data_space_used_percent], [log_size_mb], [log_space_used_percent], 
															 [is_auto_close], [is_auto_shrink], [physical_drives], 
															 [last_backup_time], [last_dbcc checkdb_time],  [recovery_model], [page_verify_option], [compatibility_level], [is_growth_limited], [event_date_utc])
				SELECT cdn.[id], @instanceID, 
				 		qt.[data_size_mb], qt.[data_space_used_percent], qt.[log_size_mb], qt.[log_space_used_percent], 
						qt.[is_auto_close], qt.[is_auto_shrink], qt.[physical_drives], 
						qt.[last_backup_time], qt.[last_dbcc checkdb_time],  qt.[recovery_model], qt.[page_verify_option], qt.[compatibility_level], qt.[is_growth_limited], GETUTCDATE()
				FROM (
						SELECT    ISNULL(qt1.[database_id], qt2.[database_id]) [database_id]
								, qt2.[recovery_model]
								, qt2.[page_verify_option]
								, qt2.[compatibility_level]
								, qt1.[data_size_mb]
								, qt1.[data_space_used_percent]
								, qt1.[log_size_mb]
								, qt1.[log_space_used_percent]
								, qt1.[physical_drives]
								, qt2.[is_auto_close]
								, qt2.[is_auto_shrink]
								, qt2.[last_backup_time]
								, qt1.[last_dbcc checkdb_time]
								, qt1.[is_growth_limited]
						FROM (
								SELECT    [database_id]
										, [data_size_mb]
										, [data_space_used_percent]
										, [log_size_mb]
										, [log_space_used_percent]
										, [physical_drives]
										, [last_dbcc checkdb_time]
										, [is_growth_limited]
								FROM #statsDatabaseDetails
								WHERE [query_type]=1
							) qt1
						FULL OUTER JOIN
							(
								SELECT    [database_id]
										, [is_auto_close]
										, [is_auto_shrink]
										, [last_backup_time]
										, [recovery_model]
										, [page_verify_option]
										, [compatibility_level]
								FROM #statsDatabaseDetails
								WHERE [query_type]=2
							) qt2 ON qt1.[database_id] = qt2.[database_id]
					)qt
				INNER JOIN [dbo].[catalogDatabaseNames] cdn ON	cdn.[database_id] = qt.[database_id] 
															AND cdn.[instance_id] = @instanceID 
															AND cdn.[project_id] = @projectID
	
		FETCH NEXT FROM crsActiveInstances INTO @instanceID, @sqlServerName, @sqlServerVersion
	end
CLOSE crsActiveInstances
DEALLOCATE crsActiveInstances
GO


RAISERROR('Create procedure: [dbo].[usp_reportHTMLBuildHealthCheck]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_reportHTMLBuildHealthCheck]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_reportHTMLBuildHealthCheck]
GO

CREATE PROCEDURE [dbo].[usp_reportHTMLBuildHealthCheck]
		@projectCode			[varchar](32)=NULL,
		@flgActions				[int]			= 63,		/*	1 - Instance Availability 
																2 - Databases status
																4 - SQL Server Agent Job status
																8 - Disk Space information
															   16 - Errorlog messages
															   32 - OS Event messages
															*/
		@flgOptions				[int]			= 266338303,/*	 1 - Instances - Offline
																 2 - Instances - Online
																 4 - Databases Status - Issues Detected
																 8 - Databases Status - Complete Details
																16 - SQL Server Agent Jobs - Job Failures
																32 - SQL Server Agent Jobs - Permissions errors
																64 - SQL Server Agent Jobs - Complete Details
															   128 - Big Size for System Databases - Issues Detected
															   256 - Databases Status - Permissions errors
															   512 - Databases with Auto Close / Shrink - Issues Detected
															  1024 - Big Size for Database Log files - Issues Detected
															  2048 - Low Usage of Data Space - Issues Detected
															  4096 - Log vs. Data - Allocated Size - Issues Detected
															  8192 - Outdated Backup for Databases - Issues Detected
															 16384 - Outdated DBCC CHECKDB Databases - Issues Detected
															 32768 - High Usage of Log Space - Issues Detected
															 65536 - Disk Space Information - Complete Detais
														    131072 - Disk Space Information - Permission errors
														    262144 - Low Free Disk Space - Issues Detected
															524288 - Errorlog messages - Permission errors
														   1048576 - Errorlog messages - Issues Detected
														   2097152 - Errorlog messages - Complete Details
														   4194304 - Databases with Fixed File(s) Size - Issues Detected													
														   8388608 - Databases with (Page Verify not CHECKSUM) or (Page Verify is NONE)
														  16777216 - Frequently Fragmented Indexes (consider lowering the fill-factor)
														  33554432 - SQL Server Agent Jobs - Long Running SQL Agent Jobs
														  67108864 - OS Event messages - Permission errors
														 134217728 - OS Event messages - Complete Details
															*/
		@reportDescription		[nvarchar](256) = NULL,
		@reportFileName			[nvarchar](max) = NULL,	/* if file name is null, than the name will be generated */
		@localStoragePath		[nvarchar](260) = NULL,
		@dbMailProfileName		[sysname]		= NULL,		
		@recipientsList			[nvarchar](1024)= NULL,
		@sendReportAsAttachment	[bit]			= 0		/* if set to 1, the report file will always be attached */
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 18.12.2014
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SET NOCOUNT ON

DECLARE   @HTMLReport							[nvarchar](max)
		, @HTMLReportArea						[nvarchar](max)
		, @CSSClass								[nvarchar](max)
		, @tmpHTMLReport						[nvarchar](max)
		, @file_attachments						[nvarchar](1024)
		
		, @ReturnValue							[int]
		, @ErrMessage							[nvarchar](256)
		, @idx									[int]

DECLARE   @queryToRun							[nvarchar](max)

DECLARE   @reportID								[int]
		, @HTMLReportFileName					[nvarchar](260)
		, @reportFilePath						[nvarchar](260)
		, @relativeStoragePath					[nvarchar](260)
		, @projectID							[int]
		, @projectName							[nvarchar](128)
		, @reportBuildStartTime					[datetime]
	
DECLARE   @databaseName							[sysname]
		, @configAdmittedState					[sysname]
		, @configDBMaxSizeMaster				[int]
		, @configDBMaxSizeMSDB					[int]
		, @configLogMaxSize						[int]
		, @configLogVsDataPercent				[numeric](6,2)
		, @configDataSpaceMinPercent			[numeric](6,2)
		, @configLogSpaceMaxPercent				[numeric](6,2)
		, @configDBMinSizeForAnalysis			[int]
		, @configFailuresInLastHours			[int]
		, @configUserDBCCCHECKDBAgeDays			[int]
		, @configSystemDBCCCHECKDBAgeDays		[int]
		, @configUserDatabaseBACKUPAgeDays		[int]
		, @configSystemDatabaseBACKUPAgeDays	[int]
		, @configFreeDiskMinPercent				[numeric](6,2)
		, @configFreeDiskMinSpace				[int]
		, @configErrorlogMessageLastHours		[int]
		, @configErrorlogMessageLimit			[int]
		, @configMaxJobRunningTimeInHours		[int]
		, @configOSEventMessageLastHours		[int]
		, @configOSEventMessageLimit			[int]
		, @configOSEventGetInformationEvent		[bit]
		, @configOSEventGetWarningsEvent		[bit]
		, @configOSEventsTimeOutSeconds			[int]

		, @logSizeMB							[numeric](20,3)
		, @dataSizeMB							[numeric](18,3)
		, @stateDesc							[nvarchar](64)
		, @dataSpaceUsedPercent					[numeric](6,2)
		, @logSpaceUsedPercent					[numeric](6,2)
		, @reclaimableSpaceMB					[numeric](18,3)
		, @logVSDataPercent						[numeric](20,2)
		, @lastBackupDate						[datetime]
		, @lastCheckDBDate						[datetime]
		, @lastDatabaseEventAgeDays				[int]
		, @logicalDrive							[char](1)
		, @volumeMountPoint						[nvarchar](512)
		, @diskTotalSizeMB						[numeric](18,3)
		, @diskAvailableSpaceMB					[numeric](18,3)
		, @diskPercentAvailable					[numeric](6,2)
		, @dateTimeLowerLimit					[datetime]

		, @messageCount							[int]
		, @issuesDetectedCount					[int]

DECLARE @eventMessageData						[varchar](8000)

/*-------------------------------------------------------------------------------------------------------------------------------*/
-- { sql_statement | statement_block }
BEGIN TRY
	SET @reportBuildStartTime = GETUTCDATE()
	SET @ReturnValue=1
	
	-----------------------------------------------------------------------------------------------------
	--get default project code
	IF @projectCode IS NULL
		SELECT	@projectCode = [value]
		FROM	[dbo].[appConfigurations]
		WHERE	[name] = 'Default project code'
				AND [module] = 'common'

	SELECT    @projectID = [id]
			, @projectName = [name]
	FROM [dbo].[catalogProjects]
	WHERE [code] = @projectCode 

	IF @projectID IS NULL
		begin
			SET @ErrMessage=N'The value specifief for Project Code is not valid.'
			RAISERROR(@ErrMessage, 16, 1) WITH NOWAIT
		end
			
	-----------------------------------------------------------------------------------------------------
	SET @ErrMessage='Building Daily Health Check Report for: [' + @projectCode + ']'
	RAISERROR(@ErrMessage, 10, 1) WITH NOWAIT


	-----------------------------------------------------------------------------------------------------
	--generating file name
	-----------------------------------------------------------------------------------------------------
	IF @reportFileName IS NOT NULL AND LEFT(@reportFileName, 1) <> '+'
		SET @HTMLReportFileName = @reportFileName
	ELSE
		SET @HTMLReportFileName = 'Daily_HealthCheck_Report_for_' + REPLACE(@projectName, '\', '_') + '_from_' +
						CONVERT([varchar](8), @reportBuildStartTime, 112)
							+ '_' + LEFT(REPLACE(CONVERT([varchar](8),@reportBuildStartTime, 108), ':', ''), 4)
	
	SET @HTMLReportFileName = REPLACE(@HTMLReportFileName, ' ', '_')

	IF @localStoragePath IS NULL
		EXEC [dbo].[usp_reportHTMLGetStorageFolder]	@projectID					= @projectID,
													@instanceID					= NULL,
													@StartDate					= @reportBuildStartTime,
													@StopDate					= @reportBuildStartTime,
													@flgCreateOutputFolder		= DEFAULT,
													@localStoragePath			= @localStoragePath OUTPUT,
													@relativeStoragePath		= @relativeStoragePath OUTPUT,
													@debugMode					= 0

	-----------------------------------------------------------------------------------------------------
	--reading report options
	-----------------------------------------------------------------------------------------------------
	SELECT	@configAdmittedState = [value]
	FROM	[dbo].[reportHTMLOptions]
	WHERE	[name] = N'Database online admitted state'
			AND [module] = 'health-check'

	SET @configAdmittedState = ISNULL(@configAdmittedState, 'ONLINE, READ ONLY')
			
	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@configDBMaxSizeMaster = [value]
		FROM	[dbo].[reportHTMLOptions]
		WHERE	[name] = N'Database max size (mb) - master'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @configDBMaxSizeMaster = 0
	END CATCH
	SET @configDBMaxSizeMaster = ISNULL(@configDBMaxSizeMaster, 0)

	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@configDBMaxSizeMSDB = [value]
		FROM	[dbo].[reportHTMLOptions]
		WHERE	[name] = N'Database max size (mb) - msdb'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @configDBMaxSizeMSDB = 0
	END CATCH
	SET @configDBMaxSizeMSDB = ISNULL(@configDBMaxSizeMSDB, 0)
			
	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@configLogMaxSize = [value]
		FROM	[dbo].[reportHTMLOptions]
		WHERE	[name] = N'Database Max Log Size (mb)'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @configLogMaxSize = 32768
	END CATCH
	SET @configLogMaxSize = ISNULL(@configLogMaxSize, 32768)
			
	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@configDataSpaceMinPercent = [value]
		FROM	[dbo].[reportHTMLOptions]
		WHERE	[name] = N'Database Min Data Usage (percent)'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @configDataSpaceMinPercent = 50
	END CATCH
	SET @configDataSpaceMinPercent = ISNULL(@configDataSpaceMinPercent, 50)

	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@configLogSpaceMaxPercent = [value]
		FROM	[dbo].[reportHTMLOptions]
		WHERE	[name] = N'Database Max Log Usage (percent)'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @configLogSpaceMaxPercent = 90
	END CATCH
	SET @configLogSpaceMaxPercent = ISNULL(@configLogSpaceMaxPercent, 90)

	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@configDBMinSizeForAnalysis = [value]
		FROM	[dbo].[reportHTMLOptions]
		WHERE	[name] = N'Database Min Size for Analysis (mb)'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @configDBMinSizeForAnalysis = 512
	END CATCH
	SET @configDBMinSizeForAnalysis = ISNULL(@configDBMinSizeForAnalysis, 512)

	-----------------------------------------------------------------------------------------------------			
	BEGIN TRY
		SELECT	@configLogVsDataPercent = [value]
		FROM	[dbo].[reportHTMLOptions]
		WHERE	[name] = N'Database Log vs. Data Size (percent)'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @configLogVsDataPercent = 50
	END CATCH
	SET @configLogVsDataPercent = ISNULL(@configLogVsDataPercent, 50)
									
	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@configFailuresInLastHours = [value]
		FROM	[dbo].[reportHTMLOptions]
		WHERE	[name] = N'SQL Agent Job - Failures in last hours'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @configFailuresInLastHours = 24
	END CATCH
	SET @configFailuresInLastHours = ISNULL(@configFailuresInLastHours, 24)

	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@configUserDatabaseBACKUPAgeDays = [value]
		FROM	[dbo].[reportHTMLOptions]
		WHERE	[name] = N'User Database BACKUP Age (days)'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @configUserDatabaseBACKUPAgeDays = 2
	END CATCH
	SET @configUserDatabaseBACKUPAgeDays = ISNULL(@configUserDatabaseBACKUPAgeDays, 2)

	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@configSystemDatabaseBACKUPAgeDays = [value]
		FROM	[dbo].[reportHTMLOptions]
		WHERE	[name] = N'System Database BACKUP Age (days)'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @configSystemDatabaseBACKUPAgeDays = 14
	END CATCH
	SET @configSystemDatabaseBACKUPAgeDays = ISNULL(@configSystemDatabaseBACKUPAgeDays, 14)

	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@configUserDBCCCHECKDBAgeDays = [value]
		FROM	[dbo].[reportHTMLOptions]
		WHERE	[name] = N'User Database DBCC CHECKDB Age (days)'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @configUserDBCCCHECKDBAgeDays = 30
	END CATCH
	SET @configUserDBCCCHECKDBAgeDays = ISNULL(@configUserDBCCCHECKDBAgeDays, 30)

	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@configSystemDBCCCHECKDBAgeDays = [value]
		FROM	[dbo].[reportHTMLOptions]
		WHERE	[name] = N'System Database DBCC CHECKDB Age (days)'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @configSystemDBCCCHECKDBAgeDays = 90
	END CATCH
	SET @configSystemDBCCCHECKDBAgeDays = ISNULL(@configSystemDBCCCHECKDBAgeDays, 90)

	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@configFreeDiskMinPercent = [value]
		FROM	[dbo].[reportHTMLOptions]
		WHERE	[name] = N'Free Disk Space Min Percent (percent)'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @configFreeDiskMinPercent = 10
	END CATCH
	SET @configFreeDiskMinPercent = ISNULL(@configFreeDiskMinPercent, 10)

	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@configFreeDiskMinSpace = [value]
		FROM	[dbo].[reportHTMLOptions]
		WHERE	[name] = N'Free Disk Space Min Space (mb)'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @configFreeDiskMinSpace = 3000
	END CATCH
	SET @configFreeDiskMinSpace = ISNULL(@configFreeDiskMinSpace, 3000)

	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@configErrorlogMessageLastHours = [value]
		FROM	[dbo].[reportHTMLOptions]
		WHERE	[name] = N'Errorlog Messages in last hours'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @configErrorlogMessageLastHours = 24
	END CATCH
	SET @configErrorlogMessageLastHours = ISNULL(@configErrorlogMessageLastHours, 24)

	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@configErrorlogMessageLimit = [value]
		FROM	[dbo].[reportHTMLOptions]
		WHERE	[name] = N'Errorlog Messages Limit to Max'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @configErrorlogMessageLimit = 1000
	END CATCH
	SET @configErrorlogMessageLimit = ISNULL(@configErrorlogMessageLimit, 1000)

	IF @configErrorlogMessageLimit= 0 SET @configErrorlogMessageLimit=2147483647

	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@configMaxJobRunningTimeInHours = [value]
		FROM	[dbo].[reportHTMLOptions]
		WHERE	[name] = N'SQL Agent Job - Maximum Running Time (hours)'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @configMaxJobRunningTimeInHours = 3
	END CATCH
	SET @configMaxJobRunningTimeInHours = ISNULL(@configMaxJobRunningTimeInHours, 3)

	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@configOSEventMessageLastHours = [value]
		FROM	[dbo].[appConfigurations]
		WHERE	[name] = N'Collect OS Events from last hours'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @configOSEventMessageLastHours = 24
	END CATCH
	SET @configOSEventMessageLastHours = ISNULL(@configOSEventMessageLastHours, 24)

	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@configOSEventMessageLimit = [value]
		FROM	[dbo].[reportHTMLOptions]
		WHERE	[name] = N'OS Event Messages Limit to Max'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @configOSEventMessageLimit = 1000
	END CATCH
	SET @configOSEventMessageLimit = ISNULL(@configOSEventMessageLimit, 1000)

	IF @configOSEventMessageLimit= 0 SET @configOSEventMessageLimit=2147483647
		
	------------------------------------------------------------------------------------------------------------------------------------------
	--option for timeout when fetching OS events
	BEGIN TRY
		SELECT	@configOSEventsTimeOutSeconds = [value]
		FROM	[dbo].[appConfigurations]
		WHERE	[name] = 'Collect OS Events timeout (seconds)'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @configOSEventsTimeOutSeconds = 600
	END CATCH

	SET @configOSEventsTimeOutSeconds = ISNULL(@configOSEventsTimeOutSeconds, 600)
	
	------------------------------------------------------------------------------------------------------------------------------------------
	--option to fetch also information OS events
	BEGIN TRY
		SELECT	@configOSEventGetInformationEvent = CASE WHEN LOWER([value])='true' THEN 1 ELSE 0 END
		FROM	[dbo].[appConfigurations]
		WHERE	[name] = 'Collect Information OS Events'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @configOSEventGetInformationEvent = 0
	END CATCH

	SET @configOSEventGetInformationEvent = ISNULL(@configOSEventGetInformationEvent, 0)

	------------------------------------------------------------------------------------------------------------------------------------------
	--option to fetch also warnings OS events
	BEGIN TRY
		SELECT	@configOSEventGetWarningsEvent = CASE WHEN LOWER([value])='true' THEN 1 ELSE 0 END
		FROM	[dbo].[appConfigurations]
		WHERE	[name] = 'Collect Warning OS Events'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @configOSEventGetWarningsEvent = 0
	END CATCH

	SET @configOSEventGetWarningsEvent = ISNULL(@configOSEventGetWarningsEvent, 0)

		
	
	-----------------------------------------------------------------------------------------------------
	--setting styles used in html report
	-----------------------------------------------------------------------------------------------------
	SET @CSSClass=N''
	SET @CSSClass = @CSSClass + N'
<style type="text/css">
	dummmy
		{
		font-family: Arial, Tahoma; 
		}
	body.normal
		{
		font-family: Arial, Tahoma; 
		margin-top: 0px;
		}
	p.title-style
		{
		font-size:24px; 
		font-weight:bold;
		}
	p.title2-style
		{
		font-size:18px; 
		font-weight:bold;
		}
	p.title3-style
		{
		font-size:14px; 
		}
	p.title4-style
		{
		font-size:12px; 
		font-style:italic;
		}
	p.title5-style
		{
		font-size:12px; 
		}
	p.disclaimer
		{
		font-size:9px; 
		}
	a.category-style
		{
		font-size:20px; 
		font-weight:bold;
		text-decoration: none;
		}
	td.summary-style-title
		{
		font-size:12px; 
		font-weight:bold;
		text-decoration: none;
		color: #000000;
		}
	a.summary-style
		{
		font-size:12px; 
		text-decoration: none;
		}
	a.graphs-style
		{
		font-size:16px; 
		font-weight:bold;
		text-decoration: none;
		}
	a.graphs-summary
		{
		font-size:12px; 
		text-decoration: none;
		}	
	td.small-size
		{
		font-size:10px; 
		}
	td.category-style
		{
		font-size:20px; 
		font-weight:bold;
		text-decoration: none;
		}
	td.summary-style
		{
		font-size:12px; 
		text-decoration: none;
		}
	table.no-border
		{
		border-style: solid; 
		border-width: 0 0 0 0; 
		border-color: #ccc;
		}
	table.with-border
		{
		border-style: solid; 
		border-width: 0 0 1px 1px; 
		border-color: #ccc;
		}
	td.color-1
		{
		background-color: #EDF8FE;
		}
	td.color-2
		{
		background-color: #FFFFFF;
		}
	td.color-3
		{
		background-color: #00AEEF;
		}
	td.color-alert-warning
		{
		font-size:12px; 
		border-style: solid; 
		border-width: 1px 1px 0 0; 
		border-color: #ccc;
		background-color: #FDD017;
		}
	td.color-alert-out-of-range
		{
		font-size:12px; 
		border-style: solid; 
		border-width: 1px 1px 0 0; 
		border-color: #ccc;
		background-color: #E42217;
		color: #FFFFFF;
		}
	tr.color-1
		{
		background-color: #EDF8FE;
		}
	tr.color-2
		{
		background-color: #FFFFFF;
		}
	tr.color-3
		{
		background-color: #00AEEF;
		}
	tr.color-alert-out-of-range
		{
		background-color: #E42217;
		color: #FFFFFF;
		}
	td.graphs-style
		{
		font-size:16px; 
		font-weight:bold;
		text-decoration: none;
		}
	td.graphs-style-title
		{
		font-size:12px; 
		font-weight:bold;
		text-decoration: none;
		}
	td.graphs-summary
		{
		font-size:12px; 
		text-decoration: none;
		}
	td.add-border
		{
		border-style: solid; 
		border-width: 1px 1px 0 0; 
		border-color: #ccc;
		}
	td.details
		{
		font-size:12px; 
		border-style: solid; 
		border-width: 1px 1px 0 0; 
		border-color: #ccc;
		}
	td.details-very-small
		{
		font-size:9px; 
		border-style: solid; 
		border-width: 1px 1px 0 0; 
		border-color: #ccc;
		}
	td.details-small-blank-line
		{
		font-size:4px; 
		border-style: solid; 
		border-width: 1px 1px 0 0; 
		border-color: #ccc;
		}
	td.details-very-very-small
		{
		font-size:6px; 
		border-style: solid; 
		border-width: 0 0 0 0; 
		}
	th.details-bold
		{
		font-size:12px; 
		font-weight:bold; 
		border-style: solid; 
		border-width: 1px 1px 0 0; 
		border-color: #ccc;
		color: #000000
		}
	td.wrap
		{
		font-size:12px; 
		border-style: solid; 
		border-width: 1px 1px 0 0; 
		border-color: #ccc;
		white-space: pre-wrap; 
		white-space: -moz-pre-wrap; 
		white-space: -pre-wrap; 
		white-space: -o-pre-wrap; 
		word-wrap: break-word;
		max-width: 150px;
		}
	p.normal
		{
		font-size:12px;
		}
	a.normal
		{
		font-size:12px; 
		text-decoration: none;
		}
	input.summary-checkbox
		{
		font-size: 6px
		width: 10px;
		height: 10px;
		}
	indent-from-margin
		{
		text-indent:10px;
		}		
		
	a.tooltip
		{
		font-size:11px; 
		text-decoration: none;
		}
	a.tooltip span 
		{
		display:none; 
		padding:2px 3px; 
		margin-left:8px; 
		width:250px;
		font-size:12px; 
		text-decoration: none;
		}
	a.tooltip:hover span
		{
		display:inline; 
		position:absolute; 
		border:1px solid #cccccc; 
		background:	#FFF8C6;
		color:#000000;
		font-size:12px; 
		text-decoration: none;
		}	
</style>'
	
	
	-----------------------------------------------------------------------------------------------------
	--report header
	-----------------------------------------------------------------------------------------------------
	RAISERROR('	...Build Report: Header', 10, 1) WITH NOWAIT

	SET @HTMLReport = N''	
	SET @HTMLReport = @HTMLReport + N'<html><head>
											<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
											<title>dbaTDPMon: Daily Health Check Report for ' + @projectName + N'</title>
											<meta name="Author" content="Dan Andrei STEFAN">' + @CSSClass + N'</head><body class="normal">'

	SET @HTMLReport = @HTMLReport + N'
	<A NAME="Home" class="normal">&nbsp;</A>
	<HR WIDTH="1130px" ALIGN=LEFT><br>
	<TABLE BORDER=0 CELLSPACING=0 CELLPADDING="3px" WIDTH="1130px">
	<TR VALIGN=TOP>
		<TD WIDTH="410px" ALIGN=LEFT>
			<TABLE CELLSPACING=0 CELLPADDING="3px" border=0> 
				<TR VALIGN=TOP>
					<TD WIDTH="200px">' + [dbo].[ufn_reportHTMLGetImage]('Logo') + N'</TD>	
					<TD WIDTH="210px" ALIGN=CENTER><P class="title2-style" ALIGN=CENTER>dbaTDPMon<br>Health Check Report</P></TD>
				</TR>
			</TABLE>
			<HR WIDTH="400px" ALIGN=LEFT>
			<TABLE CELLSPACING=0 CELLPADDING="3px" border=0> 
				<TR>
					<TD ALIGN=RIGHT WIDTH="60px"><P class="title3-style">Project:</P></TD>
					<TD ALIGN=LEFT  WIDTH="340px"><P class="title-style">' +  @projectName + N'</P></TD>
				</TR>
				<TR>
					<TD ALIGN=RIGHT WIDTH="60px"><P class="title3-style">@</P></TD>
					<TD ALIGN=LEFT  WIDTH="340px"><P class="title2-style">' + CONVERT([varchar](20), ISNULL(@reportBuildStartTime, CONVERT([datetime], N'1900-01-01', 120)), 120) + N' (UTC)</P></TD>							
				</TR>
			</TABLE>' + 
			CASE WHEN @reportDescription IS NOT NULL
				 THEN N'
						<HR WIDTH="400px" ALIGN=LEFT>
						<DIV ALIGN=CENTER>
						<TABLE CELLSPACING=0 CELLPADDING="3px" border=0> 
							<TR>
								<TD ALIGN=CENTER><P class="title4-style">' + @reportDescription + N'</P></TD>							
							</TR>
						</TABLE>
						</DIV>'
				 ELSE N''
			END + 
			N'
		</TD>
		<TD ALIGN=RIGHT>'


	SET @HTMLReport = @HTMLReport + N'				
			<TABLE CELLSPACING=0 CELLPADDING="3px" border=0 width="360px">
			<TR VALIGN="TOP">
				<TD WIDTH="360px">
					<TABLE CELLSPACING=0 CELLPADDING="1px" border=0 width="360px" class="with-border">
						<TR VALIGN="TOP" class="color-1">
							<TD WIDTH="180px" class="details-very-small" ALIGN="LEFT">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;' + CASE WHEN @flgActions &   1 =   1 THEN [dbo].[ufn_reportHTMLGetImage]('check-checked') ELSE [dbo].[ufn_reportHTMLGetImage]('check-unchecked') END + N'&nbsp;&nbsp;Instance Availability</TD>
						</TR>
						<TR VALIGN="TOP" class="color-2">
							<TD WIDTH="180px" class="details-very-small" ALIGN="LEFT">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;' + CASE WHEN @flgActions &   2 =   2 THEN [dbo].[ufn_reportHTMLGetImage]('check-checked') ELSE [dbo].[ufn_reportHTMLGetImage]('check-unchecked') END + N'&nbsp;&nbsp;Databases status</TD>
						</TR>
						<TR VALIGN="TOP" class="color-1">
							<TD WIDTH="180px" class="details-very-small" ALIGN="LEFT">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;' + CASE WHEN @flgActions &   4 =   4  THEN [dbo].[ufn_reportHTMLGetImage]('check-checked') ELSE [dbo].[ufn_reportHTMLGetImage]('check-unchecked') END  + N'&nbsp;&nbsp;SQL Server Agent Jobs status</TD>
						</TR>
						<TR VALIGN="TOP" class="color-2">
							<TD WIDTH="180px" class="details-very-small" ALIGN="LEFT">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;' + CASE WHEN @flgActions &   8 =   8 THEN [dbo].[ufn_reportHTMLGetImage]('check-checked') ELSE [dbo].[ufn_reportHTMLGetImage]('check-unchecked') END + N'&nbsp;&nbsp;Disk Space information</TD>
						</TR>
						<TR VALIGN="TOP" class="color-1">
							<TD WIDTH="180px" class="details-very-small" ALIGN="LEFT">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;' + CASE WHEN @flgActions &  16 =  16  THEN [dbo].[ufn_reportHTMLGetImage]('check-checked') ELSE [dbo].[ufn_reportHTMLGetImage]('check-unchecked') END  + N'&nbsp;&nbsp;Errorlog messages</TD>
						</TR>
						<TR VALIGN="TOP" class="color-2">
							<TD WIDTH="180px" class="details-very-small" ALIGN="LEFT">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;' + CASE WHEN @flgActions &  32 =  32  THEN [dbo].[ufn_reportHTMLGetImage]('check-checked') ELSE [dbo].[ufn_reportHTMLGetImage]('check-unchecked') END  + N'&nbsp;&nbsp;OS Event messages</TD>
						</TR>
					</TABLE>
				</TD>
			</TR>
			</TABLE>
			'

	SET @HTMLReportArea=N''
	SET @HTMLReportArea = @HTMLReportArea + N'				
			<P class="disclaimer">Browser support: IE 8, Firefox 3.5 and Google Chrome 7 (on lower versions, some features may be missing).</P>
		</TD>
	</TR>
	</TABLE>
	<HR WIDTH="1130px" ALIGN=LEFT><br>'
	
	SET @HTMLReport = @HTMLReport + @HTMLReportArea

	SET @HTMLReport = @HTMLReport + N'
	<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px">
	<TR VALIGN=TOP>	
		<TD COLSPAN="2">
			<A NAME="TableOfContents" class="category-style">Table of Contents</A>
		</TD>
	<TR VALIGN=TOP>	
		<TD class="graphs-style-title" width="452px">
			<table CELLSPACING=0 CELLPADDING="3px" border=0 width="452px" class="with-border">' + 
			CASE WHEN (@flgActions & 1 = 1)
				 THEN N'
				<TR VALIGN="TOP" class="color-3">
					<TD ALIGN=LEFT class="summary-style-title add-border color-3" colspan="3">Modules</TD>
				</TR>
				<TR VALIGN="TOP" class="color-1">
					<TD ALIGN=LEFT class="summary-style add-border color-1">
						Instance Availability
					</TD>
					<TD ALIGN=CENTER class="summary-style add-border color-1">' +
					CASE WHEN (@flgOptions & 1 = 1)
						  THEN N'<A HREF="#InstancesOnline" class="summary-style color-1">Online {InstancesOnlineCount}</A>'
						  ELSE N'Online'
					END + N'
					</TD>
					<TD ALIGN=CENTER class="summary-style add-border color-1">' +
					CASE WHEN (@flgOptions & 2 = 2)
						  THEN N'<A HREF="#InstancesOffline" class="summary-style color-1">Offline {InstancesOfflineCount}</A>'
						  ELSE N'Offline'
					END + N'
					</TD>
				</TR>'
				ELSE N''
			END + 
 			CASE WHEN (@flgActions & 2 = 2) 
				 THEN N'
				<TR VALIGN="TOP" class="color-2">
					<TD ALIGN=LEFT class="summary-style add-border color-2">
						Databases Status
					</TD>
					<TD ALIGN=CENTER class="summary-style add-border color-2">' +
					CASE WHEN (@flgOptions & 8 = 8)
						  THEN N'<A HREF="#DatabasesStatusCompleteDetails" class="summary-style color-2">Complete Details</A>'
						  ELSE N'Complete Details'
					END + N'
					</TD>
					<TD ALIGN=CENTER class="summary-style add-border color-2">' +
					CASE WHEN (@flgOptions & 256 = 256)
						  THEN N'<A HREF="#DatabasesStatusPermissionErrors" class="summary-style color-2">Permission Errors {DatabasesStatusPermissionErrorsCount}</A>'
						  ELSE N'&Permission Errors'
					END + N'
					</TD>
				</TR>'
				ELSE N''
			END + 
 			CASE WHEN (@flgActions & 4 = 4) 
				 THEN N'
				<TR VALIGN="TOP" class="color-1">
					<TD ALIGN=LEFT class="summary-style add-border color-1">
						SQL Server Agent Jobs Status
					</TD>
					<TD ALIGN=CENTER class="summary-style add-border color-1">' +
					CASE WHEN (@flgOptions & 64 = 64)
						  THEN N'<A HREF="#SQLServerAgentJobsStatusCompleteDetails" class="summary-style color-1">Complete Details</A>'
						  ELSE N'Complete Details'
					END + N'
					</TD>
					<TD ALIGN=CENTER class="summary-style add-border color-1">' +
					CASE WHEN (@flgOptions & 32 = 32)
						  THEN N'<A HREF="#SQLServerAgentJobsStatusPermissionErrors" class="summary-style color-1">Permission Errors {SQLServerAgentJobsStatusPermissionErrorsCount}</A>'
						  ELSE N'Permission Errors'
					END + N'
					</TD>
				</TR>'
				ELSE N''
			END + 
 			CASE WHEN (@flgActions & 8 = 8) 
				 THEN N'
				<TR VALIGN="TOP" class="color-1">
					<TD ALIGN=LEFT class="summary-style add-border color-1">
						Disk Space Information
					</TD>
					<TD ALIGN=CENTER class="summary-style add-border color-1">' +
					CASE WHEN (@flgOptions & 65536 = 65536)
						  THEN N'<A HREF="#DiskSpaceInformationCompleteDetails" class="summary-style color-1">Complete Details</A>'
						  ELSE N'Complete Details'
					END + N'
					</TD>
					<TD ALIGN=CENTER class="summary-style add-border color-1">' +
					CASE WHEN (@flgOptions & 131072 = 131072)
						  THEN N'<A HREF="#DiskSpaceInformationPermissionErrors" class="summary-style color-1">Permission Errors {DiskSpaceInformationPermissionErrorsCount}</A>'
						  ELSE N'Permission Errors'
					END + N'
					</TD>
				</TR>'
				ELSE N''
			END + 
 			CASE WHEN (@flgActions & 16 = 16) 
				 THEN N'
				<TR VALIGN="TOP" class="color-2">
					<TD ALIGN=LEFT class="summary-style add-border color-2">
						Errorlog Messages
					</TD>
					<TD ALIGN=CENTER class="summary-style add-border color-2">' +
					CASE WHEN (@flgOptions & 2097152 = 2097152)
						  THEN N'<A HREF="#ErrorlogMessagesCompleteDetails" class="summary-style color-2">Complete Details</A>'
						  ELSE N'Complete Details'
					END + N'
					</TD>
					<TD ALIGN=CENTER class="summary-style add-border color-2">' +
					CASE WHEN (@flgOptions & 524288 = 524288)
						  THEN N'<A HREF="#ErrorlogMessagesPermissionErrors" class="summary-style color-2">Permission Errors {ErrorlogMessagesPermissionErrorsCount}</A>'
						  ELSE N'Permission Errors;'
					END + N'
					</TD>
				</TR>'
				ELSE N''
			END + 
 			CASE WHEN (@flgActions & 32 = 32) 
				 THEN N'
				<TR VALIGN="TOP" class="color-1">
					<TD ALIGN=LEFT class="summary-style add-border color-1">
						OS Event Messages
					</TD>
					<TD ALIGN=CENTER class="summary-style add-border color-1">' +
					CASE WHEN (@flgOptions & 134217728 = 134217728)
						  THEN N'<A HREF="#OSEventMessagesCompleteDetails" class="summary-style color-1">Complete Details</A>'
						  ELSE N'Complete Details'
					END + N'
					</TD>
					<TD ALIGN=CENTER class="summary-style add-border color-1">' +
					CASE WHEN (@flgOptions & 67108864 = 67108864)
						  THEN N'<A HREF="#OSEventMessagesPermissionErrors" class="summary-style color-1">Permission Errors {OSEventMessagesPermissionErrorsCount}</A>'
						  ELSE N'Permission Errors;'
					END + N'
					</TD>
				</TR>'
				ELSE N''
			END + N'
			</table>
		</TD>
		<TD class="graphs-style-title" width="126px">
			&nbsp;
		</TD>
		<TD class="graphs-style-title" width="552px">
			<table CELLSPACING=0 CELLPADDING="3px" border=0 width="552px" class="with-border">
				<TR VALIGN="TOP" class="color-3">
					<TD ALIGN=LEFT class="summary-style-title add-border color-3" colspan="2">Potential Issues</TD>
				</TR>
				<TR VALIGN="TOP" class="color-1">
					<TD ALIGN=LEFT class="summary-style add-border color-1">' +
					CASE WHEN (@flgActions & 2 = 2) AND (@flgOptions & 4 = 4)
						  THEN N'<A HREF="#DatabasesStatusIssuesDetected" class="summary-style color-1">Offline Databases {DatabasesStatusIssuesDetectedCount}</A>'
						  ELSE N'Offline Databases (N/A)'
					END + N'
					</TD>
					<TD ALIGN=LEFT class="summary-style add-border color-1">' +
					CASE WHEN (@flgActions & 2 = 2)  AND (@flgOptions & 128 = 128)
						  THEN N'<A HREF="#SystemDatabasesSizeIssuesDetected" class="summary-style color-1">Big Size for System Databases {SystemDatabasesSizeIssuesDetectedCount}</A>'
						  ELSE N'Big Size for System Databases (N/A)'
					END + N'
					</TD>
				</TR>
				<TR VALIGN="TOP" class="color-2">
					<TD ALIGN=LEFT class="summary-style add-border color-2">' +
					CASE WHEN (@flgActions & 4 = 4) AND (@flgOptions & 16 = 16)
						  THEN N'<A HREF="#SQLServerAgentJobsStatusIssuesDetected" class="summary-style color-2">SQL Server Agent Job Failures {SQLServerAgentJobsStatusIssuesDetectedCount}</A>'
						  ELSE N'SQL Server Agent Job Failures'
					END + N'
					</TD>
					<TD ALIGN=LEFT class="summary-style add-border color-2">' +
					CASE WHEN (@flgActions & 2 = 2) AND (@flgOptions & 1024 = 1024)
						  THEN N'<A HREF="#DatabaseMaxLogSizeIssuesDetected" class="summary-style color-2">Big Size for Database Log files {DatabaseMaxLogSizeIssuesDetectedCount}</A>'
						  ELSE N'Big Size for Database Log files (N/A)'
					END + N'
					</TD>
				</TR>
				<TR VALIGN="TOP" class="color-1">
					<TD ALIGN=LEFT class="summary-style add-border color-1">' +
					CASE WHEN (@flgActions & 4 = 4) AND (@flgOptions & 33554432 = 33554432)
						  THEN N'<A HREF="#LongRunningSQLAgentJobsIssuesDetected" class="summary-style color-1">Long Running SQL Agent Jobs {LongRunningSQLAgentJobsIssuesDetectedCount}</A>'
						  ELSE N'Long Running SQL Agent Jobs'
					END + N'
					</TD>
					<TD ALIGN=LEFT class="summary-style add-border color-1">' +
					CASE WHEN (@flgActions & 2 = 2) AND (@flgOptions & 512 = 512)
						  THEN N'<A HREF="#DatabasesWithAutoCloseShrinkIssuesDetected" class="summary-style color-1">Databases with Auto Close / Shrink {DatabasesWithAutoCloseShrinkIssuesDetectedCount}</A>'
						  ELSE N'Auto Close / Shrink Databases (N/A)'
					END + N'
					</TD>
				</TR>
				<TR VALIGN="TOP" class="color-2">
					<TD ALIGN=LEFT class="summary-style add-border color-2">' +
					CASE WHEN (@flgActions & 8 = 8) AND (@flgOptions & 262144 = 262144)
						  THEN N'<A HREF="#DiskSpaceInformationIssuesDetected" class="summary-style color-2">Low Free Disk Space {DiskSpaceInformationIssuesDetectedCount}</A>'
						  ELSE N'Low Free Disk Space (N/A)'
					END + N'
					</TD>
					<TD ALIGN=LEFT class="summary-style add-border color-2">' +
					CASE WHEN (@flgActions & 2 = 2) AND (@flgOptions & 2048 = 2048)
						  THEN N'<A HREF="#DatabaseMinDataSpaceIssuesDetected" class="summary-style color-2">Low Usage of Data Space {DatabaseMinDataSpaceIssuesDetectedCount}</A>'
						  ELSE N'Low Usage of Data Space (N/A)'
					END + N'
					</TD>
				</TR>
				<TR VALIGN="TOP" class="color-1">
					<TD ALIGN=LEFT class="summary-style add-border color-1">' +
					CASE WHEN (@flgActions & 2 = 2) AND (@flgOptions & 8192 = 8192)
						  THEN N'<A HREF="#DatabaseBACKUPAgeIssuesDetected" class="summary-style color-1">Outdated Backup for Databases {DatabaseBACKUPAgeIssuesDetectedCount}</A>'
						  ELSE N'Outdated Backup for Databases (N/A)'
					END + N'
					</TD>
					<TD ALIGN=LEFT class="summary-style add-border color-1">' +
					CASE WHEN (@flgActions & 2 = 2) AND (@flgOptions & 32768 = 32768)
						  THEN N'<A HREF="#DatabaseMaxLogSpaceIssuesDetected" class="summary-style color-1">High Usage of Log Space {DatabaseMaxLogSpaceIssuesDetectedCount}</A>'
						  ELSE N'High Usage of Log Spacee (N/A)'
					END + N'
					</TD>
				</TR> 
				<TR VALIGN="TOP" class="color-2">
					<TD ALIGN=LEFT class="summary-style add-border color-2">' +
					CASE WHEN (@flgActions & 2 = 2) AND (@flgOptions & 16384 = 16384)
						  THEN N'<A HREF="#DatabaseDBCCCHECKDBAgeIssuesDetected" class="summary-style color-2">Outdated DBCC CHECKDB Databases {DatabaseDBCCCHECKDBAgeIssuesDetectedCount}</A>'
						  ELSE N'Outdated DBCC CHECKDB Databases (N/A)'
					END + N'
					</TD>
					<TD ALIGN=LEFT class="summary-style add-border color-2">' +
					CASE WHEN (@flgActions & 2 = 2) AND (@flgOptions & 4096 = 4096)
						  THEN N'<A HREF="#DatabaseLogVsDataSizeIssuesDetected" class="summary-style color-2">Log vs. Data - Allocated Size {DatabaseLogVsDataSizeIssuesDetectedCount}</A>'
						  ELSE N'Log vs. Data - Allocated Size (N/A)'
					END + N'
					</TD>
				</TR> 
				<TR VALIGN="TOP" class="color-1">
					<TD ALIGN=LEFT class="summary-style add-border color-1">' +
					CASE WHEN (@flgActions & 16 = 16) AND (@flgOptions & 1048576 = 1048576)
						  THEN N'<A HREF="#ErrorlogMessagesIssuesDetected" class="summary-style color-1">Errorlog Messages {ErrorlogMessagesIssuesDetectedCount}</A>'
						  ELSE N'ErrorlogMessagesIssuesDetected (N/A)'
					END + N'
					</TD>
					<TD ALIGN=LEFT class="summary-style add-border color-1">' +
					CASE WHEN (@flgActions & 2 = 2) AND (@flgOptions & 4194304 = 4194304)
						  THEN N'<A HREF="#DatabaseFixedFileSizeIssuesDetected" class="summary-style color-1">Databases with Fixed File(s) Size {DatabaseFixedFileSizeIssuesDetectedCount}</A>'
						  ELSE N'>Databases with Fixed File(s) Size (N/A)'
					END + N'
					</TD>
				</TR> 
				<TR VALIGN="TOP" class="color-2">
					<TD ALIGN=LEFT class="summary-style add-border color-2">' +
					CASE WHEN (@flgActions & 2 = 2) AND (@flgOptions & 16777216 = 16777216)
						  THEN N'<A HREF="#FrequentlyFragmentedIndexesIssuesDetected" class="summary-style color-2">Frequently Fragmented Indexes {FrequentlyFragmentedIndexesIssuesDetectedCount}</A>'
						  ELSE N'>Frequently Fragmented Indexes (N/A)'
					END + N'
					</TD>
					<TD ALIGN=LEFT class="summary-style add-border color-2">' +
					CASE WHEN (@flgActions & 2 = 2) AND (@flgOptions & 8388608 = 8388608)
						  THEN N'<A HREF="#DatabasePageVerifyIssuesDetected" class="summary-style color-2">Databases with Improper Page Verify Option {DatabasePageVerifyIssuesDetectedCount}</A>'
						  ELSE N'>Databases with Improper Page Verify Option (N/A)'
					END + N'
					</TD>
				</TR>
			</table>
		</TD>
	</TR>
	</TABLE>			
	<HR WIDTH="1130px" ALIGN=LEFT><br>'



	-----------------------------------------------------------------------------------------------------
	--Offline Instances
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 1 = 1) AND (@flgOptions & 1 = 1)
		begin
			RAISERROR('	...Build Report: Instance Availability - Offline', 10, 1) WITH NOWAIT
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="InstancesOffline" class="category-style">Instance Availability - Offline</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="120px" class="details-bold" nowrap>Machine Name</TH>
											<TH WIDTH="200px" class="details-bold" nowrap>Instance Name</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Clustered</TH>
											<TH WIDTH="150px" class="details-bold" nowrap>Event Date (UTC)</TH>
											<TH WIDTH="540px" class="details-bold">Message</TH>'

			DECLARE   @machineName		[sysname]
					, @instanceName		[sysname]
					, @isClustered		[bit]
					, @clusterNodeName	[sysname]
					, @eventDate		[datetime]
					, @message			[nvarchar](max)

			SET @idx=1		

			DECLARE crsInstancesOffline CURSOR READ_ONLY LOCAL FOR	SELECT    cin.[machine_name], cin.[instance_name]
																			, cin.[is_clustered], cin.[cluster_node_machine_name]
																			, MAX(lsam.[event_date_utc]) [event_date_utc]
																			, lsam.[message]
																	FROM [dbo].[vw_catalogInstanceNames]  cin
																	INNER JOIN [dbo].[vw_logServerAnalysisMessages] lsam ON lsam.[project_id] = cin.[project_id] AND lsam.[instance_id] = cin.[instance_id]
																	LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
																													AND rsr.[rule_id] = 1
																													AND rsr.[active] = 1
																													AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																	WHERE	cin.[instance_active]=0
																			AND cin.[project_id] = @projectID
																			AND lsam.[descriptor] IN (N'dbo.usp_refreshMachineCatalogs - Offline')
																			AND rsr.[id] IS NULL

																	GROUP BY cin.[machine_name], cin.[instance_name], cin.[is_clustered], cin.[cluster_node_machine_name], lsam.[message]
																	ORDER BY cin.[instance_name], cin.[machine_name], [event_date_utc]
			OPEN crsInstancesOffline
			FETCH NEXT FROM crsInstancesOffline INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @eventDate, @message
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap>' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName) END + N'</TD>' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap>' + @instanceName + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="CENTER" nowrap>' + CASE WHEN @isClustered=0 THEN N'No' ELSE N'Yes' + ISNULL(N'<BR>[' + @clusterNodeName + ']', N'&nbsp;') END + N'</TD>' + 
										N'<TD WIDTH="150px" class="details" ALIGN="CENTER" nowrap>' + ISNULL(CONVERT([nvarchar](24), @eventDate, 121), N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="540px" class="details" ALIGN="LEFT">' + ISNULL([dbo].[ufn_reportHTMLPrepareText](@message, 0), N'&nbsp;') + N'</TD>' + 
									N'</TR>'
					SET @idx=@idx+1

					FETCH NEXT FROM crsInstancesOffline INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @eventDate, @message
				end
			CLOSE crsInstancesOffline
			DEALLOCATE crsInstancesOffline

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

			SET @HTMLReport = REPLACE(@HTMLReport, '{InstancesOfflineCount}', '(' + CAST((@idx-1) AS [nvarchar]) + ')')
		end


	-----------------------------------------------------------------------------------------------------
	--Online Instances
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 1 = 1) AND (@flgOptions & 2 = 2)
		begin
			RAISERROR('	...Build Report: Instance Availability - Online', 10, 1) WITH NOWAIT
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="InstancesOnline" class="category-style">Instance Availability - Online</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="100px" class="details-bold" nowrap>Details</TH>
											<TH WIDTH="150px" class="details-bold">Machine Name</TH>
											<TH WIDTH="200px" class="details-bold">Instance Name</TH>
											<TH WIDTH="100px" class="details-bold">Clustered</TH>
											<TH WIDTH= "90px" class="details-bold" nowrap >Version</TH>
											<TH WIDTH="260px" class="details-bold">Edition</TH>
											<TH WIDTH= "80px" class="details-bold" nowrap>DB Size (MB)</TH>
											<TH WIDTH="150px" class="details-bold" nowrap>Refresh Date (UTC)</TH>'

			DECLARE   @version				[sysname]
					, @edition				[varchar](256)
					, @hasDatabaseDetails	[int]
					, @hasSQLagentJob		[int]
					, @hasDiskSpaceInfo		[int]
					, @hasErrorlogMessages	[int]
					, @hasOSEventMessages	[int]
					, @lastRefreshDate		[datetime]
					, @dbSize				[numeric](20,3)

			SET @idx=1		

			DECLARE crsInstancesOffline CURSOR READ_ONLY LOCAL FOR	SELECT    cin.[machine_name], cin.[instance_name]
																			, cin.[is_clustered], cin.[cluster_node_machine_name]
																			, cin.[version], cin.[edition], cin.[last_refresh_date_utc]	
																			, shcdd.[size_mb]
																	FROM [dbo].[vw_catalogInstanceNames]  cin
																	LEFT JOIN 
																		(
																			SELECT    [project_id], [instance_id]
																					, SUM(ISNULL([size_mb], 0)) [size_mb]
																			FROM [health-check].[vw_statsDatabaseDetails]
																			WHERE [project_id] = @projectID
																			GROUP BY [project_id], [instance_id]
																		) shcdd ON shcdd.[instance_id] = cin.[instance_id] AND shcdd.[project_id] = cin.[project_id]
																	LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
																													AND rsr.[rule_id] = 2
																													AND rsr.[active] = 1
																													AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																	WHERE cin.[instance_active]=1
																			AND cin.[project_id] = @projectID
																			AND rsr.[id] IS NULL
																	ORDER BY cin.[instance_name], cin.[machine_name]
			OPEN crsInstancesOffline
			FETCH NEXT FROM crsInstancesOffline INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @version, @edition, @lastRefreshDate, @dbSize
			WHILE @@FETCH_STATUS=0
				begin
					SELECT	@hasDatabaseDetails = COUNT(*)
					FROM	[dbo].[vw_catalogDatabaseNames]
					WHERE	[project_id]=@projectID
							AND [instance_name] = @instanceName

					SELECT	@hasSQLagentJob = COUNT(*)
					FROM	[health-check].[vw_statsSQLServerAgentJobsHistory]
					WHERE	[project_id]=@projectID
							AND [instance_name] = @instanceName

					SELECT	@hasDiskSpaceInfo = COUNT(*)
					FROM	[health-check].[vw_statsDiskSpaceInfo]
					WHERE	[project_id]=@projectID
							AND [instance_name] = @instanceName
					
					SELECT	@hasErrorlogMessages = COUNT(*)
					FROM	[health-check].[vw_statsSQLServerErrorlogDetails]
					WHERE	[project_id]=@projectID
							AND [instance_name] = @instanceName

					SELECT	@hasOSEventMessages = COUNT(*)
					FROM	[health-check].[vw_statsOSEventLogs] 
					WHERE	[project_id]=@projectID
							AND [instance_name] = @machineName
																				  

					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="CENTER" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="100px" class="details" ALIGN="CENTER">' + 
										CASE	WHEN @hasDatabaseDetails<>0 AND @flgOptions & 8 = 8
												THEN N'<BR><A HREF="#DatabasesStatusCompleteDetails' + @instanceName + N'">Databases</A>'
												ELSE N''
										END +
										CASE WHEN @hasSQLagentJob<>0 AND @flgOptions & 64 = 64
												THEN N'<BR><A HREF="#SQLServerAgentJobsStatusCompleteDetails' + @instanceName + N'">SQL Agent Jobs</A>'
												ELSE N''
										END +
										CASE WHEN @hasDiskSpaceInfo<>0 AND @flgOptions & 65536 = 65536
												THEN N'<BR><A HREF="#DiskSpaceInformationCompleteDetails' + CASE WHEN @isClustered=0 THEN @machineName ELSE @clusterNodeName END + N'">Disk Space</A>'
												ELSE N''
										END +  
										CASE WHEN @hasErrorlogMessages<>0 AND @flgOptions & 2097152 = 2097152
												THEN N'<BR><A HREF="#ErrorlogMessagesCompleteDetails' + @instanceName + N'">Errorlog</A>'
												ELSE N''
										END +  
											N'<BR><BR>
										</TD>' + 
										N'<TD WIDTH="150px" class="details" ALIGN="LEFT">' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName) END + N'</TD>' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT">' + @instanceName + N'</TD>' + 
										N'<TD WIDTH="100px" class="details" ALIGN="CENTER">' + CASE WHEN @isClustered=0 THEN N'No' ELSE N'Yes' + ISNULL(N'<BR>[' + @clusterNodeName + ']', N'&nbsp;') END + N'</TD>' + 
										N'<TD WIDTH= "90px" class="details" ALIGN="CENTER" nowrap>' + ISNULL(@version, N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="260px" class="details" ALIGN="LEFT">' + ISNULL([dbo].[ufn_reportHTMLPrepareText](@edition, 0), N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH= "80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@dbSize AS [nvarchar](64)), N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="150px" class="details" ALIGN="LEFT" nowrap>' + ISNULL(CONVERT([nvarchar](24), @lastRefreshDate, 121), N'&nbsp;') + N'</TD>' + 
									N'</TR>'
					SET @idx=@idx+1

					FETCH NEXT FROM crsInstancesOffline INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @version, @edition, @lastRefreshDate, @dbSize
				end
			CLOSE crsInstancesOffline
			DEALLOCATE crsInstancesOffline

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

			SET @HTMLReport = REPLACE(@HTMLReport, '{InstancesOnlineCount}', '(' + CAST((@idx-1) AS [nvarchar]) + ')')
		end
		

	-----------------------------------------------------------------------------------------------------
	--Databases Status - Permission Errors
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 2 = 2) AND (@flgOptions & 256 = 256)
		begin
			RAISERROR('	...Build Report: Databases Status - Permission Errors', 10, 1) WITH NOWAIT
			
			SET @messageCount=0

			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="DatabasesStatusPermissionErrors" class="category-style">Databases Status - Permission Errors</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="120px" class="details-bold" nowrap>Machine Name</TH>
											<TH WIDTH="200px" class="details-bold" nowrap>Instance Name</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Clustered</TH>
											<TH WIDTH="150px" class="details-bold" nowrap>Event Date (UTC)</TH>
											<TH WIDTH="540px" class="details-bold">Message</TH>'

			SET @idx=1		
			
			DECLARE crsDatabasesStatusPermissionErrors CURSOR READ_ONLY LOCAL FOR	SELECT    cin.[machine_name], cin.[instance_name]
																							, cin.[is_clustered], cin.[cluster_node_machine_name]
																							, COUNT(DISTINCT lsam.[message]) AS [message_count]
																					FROM [dbo].[vw_catalogInstanceNames]  cin
																					INNER JOIN [dbo].[vw_logServerAnalysisMessages] lsam ON lsam.[project_id] = cin.[project_id] AND lsam.[instance_id] = cin.[instance_id]
																					LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
																																	AND rsr.[rule_id] = 256
																																	AND rsr.[active] = 1
																																	AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																					WHERE	cin.[instance_active]=1
																							AND cin.[project_id] = @projectID
																							AND lsam.descriptor IN (N'dbo.usp_hcCollectDatabaseDetails')
																							AND rsr.[id] IS NULL
																					GROUP BY cin.[machine_name], cin.[instance_name], cin.[is_clustered], cin.[cluster_node_machine_name]
																					ORDER BY cin.[instance_name], cin.[machine_name]
			OPEN crsDatabasesStatusPermissionErrors
			FETCH NEXT FROM crsDatabasesStatusPermissionErrors INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @messageCount
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap ROWSPAN="' + CAST(@messageCount AS [nvarchar](64)) + N'"><A NAME="DatabasesStatusPermissionErrors' + @instanceName + N'">' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName) END + N'</A></TD>' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap ROWSPAN="' + CAST(@messageCount AS [nvarchar](64)) + N'">' + @instanceName + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="CENTER" nowrap ROWSPAN="' + CAST(@messageCount AS [nvarchar](64)) + N'">' + CASE WHEN @isClustered=0 THEN N'No' ELSE N'Yes' + ISNULL(N'<BR>[' + @clusterNodeName + ']', N'&nbsp;') END + N'</TD>' 

					SET @tmpHTMLReport=N''
					SELECT @tmpHTMLReport=((
											SELECT	N'<TD WIDTH="150px" class="details" ALIGN="CENTER" nowrap>' + ISNULL(CONVERT([nvarchar](24), [event_date_utc], 121), N'&nbsp;') + N'</TD>' + 
													N'<TD WIDTH="540px" class="details" ALIGN="LEFT">' + ISNULL([dbo].[ufn_reportHTMLPrepareText]([message], 0), N'&nbsp;') + N'</TD>' + 
													N'</TR>' + 
													CASE WHEN [row_count] > [row_no]
														 THEN N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">'
														 ELSE N''
													END
											FROM (
													SELECT [message], [event_date_utc]
															, ROW_NUMBER() OVER(ORDER BY [event_date_utc]) [row_no]
															, SUM(1) OVER() AS [row_count]
													FROM (
															SELECT    lsam.[message]
																	, MAX(lsam.[event_date_utc]) [event_date_utc]
															FROM [dbo].[vw_catalogInstanceNames]  cin
															INNER JOIN [dbo].[vw_logServerAnalysisMessages] lsam ON lsam.[project_id] = cin.[project_id] AND lsam.[instance_id] = cin.[instance_id]
															WHERE	cin.[instance_active]=1
																	AND cin.[project_id] = @projectID	
																	AND cin.[instance_name] = @instanceName
																	AND cin.[machine_name] = @machineName
																	AND lsam.descriptor IN (N'dbo.usp_hcCollectDatabaseDetails')
															GROUP BY lsam.[message]
														)Z
												)X
											ORDER BY [event_date_utc]
											FOR XML PATH(''), TYPE
											).value('.', 'nvarchar(max)'))

					SET @idx=@idx+1
					SET @HTMLReportArea = @HTMLReportArea + COALESCE(@tmpHTMLReport, '')

					FETCH NEXT FROM crsDatabasesStatusPermissionErrors INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @messageCount

					IF @@FETCH_STATUS=0
						SET @HTMLReportArea = @HTMLReportArea + N'<TR VALIGN="TOP" class="color-2" HEIGHT="5px">
																	<TD class="details" COLSPAN=5>&nbsp;</TD>
															</TR>'
				end
			CLOSE crsDatabasesStatusPermissionErrors
			DEALLOCATE crsDatabasesStatusPermissionErrors

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

			SELECT    @idx = COUNT(*)
			FROM [dbo].[vw_catalogInstanceNames]  cin
			INNER JOIN [dbo].[vw_logServerAnalysisMessages] lsam ON lsam.[project_id] = cin.[project_id] AND lsam.[instance_id] = cin.[instance_id]
			WHERE	cin.[instance_active]=1
					AND cin.[project_id] = @projectID
					AND lsam.descriptor IN (N'dbo.usp_hcCollectDatabaseDetails')

			SET @HTMLReport = REPLACE(@HTMLReport, '{DatabasesStatusPermissionErrorsCount}', '(' + CAST((@idx) AS [nvarchar]) + ')')
		end


	-----------------------------------------------------------------------------------------------------
	--Databases Status - Issues Detected
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 2 = 2) AND (@flgOptions & 4 = 4)
		begin
			RAISERROR('	...Build Report: Databases Status - Issues Detected', 10, 1) WITH NOWAIT
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="DatabasesStatusIssuesDetected" class="category-style">Databases Status - Issues Detected</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">	
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="5">database status not in (' + @configAdmittedState + N')</TD>
							</TR>
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="120px" class="details-bold" nowrap>Machine Name</TH>
											<TH WIDTH="200px" class="details-bold" nowrap>Instance Name</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Clustered</TH>
											<TH WIDTH="490px" class="details-bold">Database Name</TH>
											<TH WIDTH="200px" class="details-bold" nowrap>State</TH>'


			SET @idx=1		

			DECLARE crsDatabasesStatusIssuesDetected CURSOR READ_ONLY LOCAL FOR	SELECT    cin.[machine_name], cin.[instance_name]
																						, cin.[is_clustered], cin.[cluster_node_machine_name]
																						, cdn.[database_name]
																						, cdn.[state_desc]
																				FROM [dbo].[vw_catalogInstanceNames]  cin
																				INNER JOIN [dbo].[vw_catalogDatabaseNames] cdn ON cdn.[project_id] = cin.[project_id] AND cdn.[instance_id] = cin.[instance_id]
																				LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
																																AND rsr.[rule_id] = 4
																																AND rsr.[active] = 1
																																AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																				WHERE cin.[instance_active]=1
																						AND cdn.[active]=1
																						AND cin.[project_id] = @projectID	
																						AND CHARINDEX(cdn.[state_desc], @configAdmittedState)=0
																						AND rsr.[id] IS NULL
																				ORDER BY cin.[instance_name], cin.[machine_name], cdn.[database_name]
			OPEN crsDatabasesStatusIssuesDetected
			FETCH NEXT FROM crsDatabasesStatusIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @stateDesc
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap>' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName) END + N'</TD>' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap>' + @instanceName + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="CENTER" nowrap>' + CASE WHEN @isClustered=0 THEN N'No' ELSE N'Yes' + ISNULL(N'<BR>[' + @clusterNodeName + ']', N'&nbsp;') END + N'</TD>' + 
										N'<TD WIDTH="490px" class="details" ALIGN="LEFT">' + ISNULL(@databaseName, N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap>' + ISNULL(@stateDesc, N'&nbsp;') + N'</TD>' + 
									N'</TR>'
					SET @idx=@idx+1

					FETCH NEXT FROM crsDatabasesStatusIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @stateDesc
				end
			CLOSE crsDatabasesStatusIssuesDetected
			DEALLOCATE crsDatabasesStatusIssuesDetected

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

			SET @HTMLReport = REPLACE(@HTMLReport, '{DatabasesStatusIssuesDetectedCount}', '(' + CAST((@idx-1) AS [nvarchar]) + ')')
		end


	-----------------------------------------------------------------------------------------------------
	--SQL Server Agent Jobs Status - Permission Errors
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 4 = 4) AND (@flgOptions & 64 = 64)
		begin
			RAISERROR('	...Build Report: SQL Server Agent Jobs Status - Permission Errors', 10, 1) WITH NOWAIT
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="SQLServerAgentJobsStatusPermissionErrors" class="category-style">SQL Server Agent Jobs Status - Permission Errors</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="120px" class="details-bold" nowrap>Machine Name</TH>
											<TH WIDTH="200px" class="details-bold" nowrap>Instance Name</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Clustered</TH>
											<TH WIDTH="150px" class="details-bold" nowrap>Event Date (UTC)</TH>
											<TH WIDTH="540px" class="details-bold">Message</TH>'

			SET @idx=1		

			DECLARE crsSQLServerAgentJobsStatusPermissionErrors CURSOR READ_ONLY LOCAL FOR	SELECT    cin.[machine_name], cin.[instance_name]
																									, cin.[is_clustered], cin.[cluster_node_machine_name]
																									, MAX(lsam.[event_date_utc]) [event_date_utc]
																									, lsam.[message]
																							FROM [dbo].[vw_catalogInstanceNames]  cin
																							INNER JOIN [dbo].[vw_logServerAnalysisMessages] lsam ON lsam.[project_id] = cin.[project_id] AND lsam.[instance_id] = cin.[instance_id]
																							LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
																																			AND rsr.[rule_id] = 64
																																			AND rsr.[active] = 1
																																			AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																							WHERE	cin.[instance_active]=1
																									AND cin.[project_id] = @projectID
																									AND lsam.descriptor IN (N'dbo.usp_hcCollectSQLServerAgentJobsStatus')
																									AND rsr.[id] IS NULL
																							GROUP BY cin.[machine_name], cin.[instance_name], cin.[is_clustered], cin.[cluster_node_machine_name], lsam.[message]
																							ORDER BY cin.[instance_name], cin.[machine_name], [event_date_utc]
			OPEN crsSQLServerAgentJobsStatusPermissionErrors
			FETCH NEXT FROM crsSQLServerAgentJobsStatusPermissionErrors INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @eventDate, @message
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap>' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName) END + N'</TD>' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap>' + @instanceName + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="CENTER" nowrap>' + CASE WHEN @isClustered=0 THEN N'No' ELSE N'Yes<BR>' + ISNULL(N'[' + @clusterNodeName + ']', N'&nbsp;') END + N'</TD>' + 
										N'<TD WIDTH="150px" class="details" ALIGN="CENTER" nowrap>' + ISNULL(CONVERT([nvarchar](24), @eventDate, 121), N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="540px" class="details" ALIGN="LEFT">' + ISNULL([dbo].[ufn_reportHTMLPrepareText](@message, 0), N'&nbsp;') + N'</TD>' + 
									N'</TR>'
					SET @idx=@idx+1

					FETCH NEXT FROM crsSQLServerAgentJobsStatusPermissionErrors INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @eventDate, @message
				end
			CLOSE crsSQLServerAgentJobsStatusPermissionErrors
			DEALLOCATE crsSQLServerAgentJobsStatusPermissionErrors

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

			SET @HTMLReport = REPLACE(@HTMLReport, '{SQLServerAgentJobsStatusPermissionErrorsCount}', '(' + CAST((@idx-1) AS [nvarchar]) + ')')
		end


	-----------------------------------------------------------------------------------------------------
	--SQL Server Agent Jobs Status - Issues Detected
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 4 = 4) AND (@flgOptions & 16 = 16)
		begin
			RAISERROR('	...Build Report: SQL Server Agent Jobs Status - Issues Detected', 10, 1) WITH NOWAIT
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="SQLServerAgentJobsStatusIssuesDetected" class="category-style">SQL Server Agent Jobs Status - Issues Detected (last ' + CAST(@configFailuresInLastHours AS [nvarchar]) + N'h)</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="6">job status not in (Succeded, In progress)</TD>
							</TR>
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="200px" class="details-bold" nowrap>Instance Name</TH>
											<TH WIDTH="200px" class="details-bold" nowrap>Job Name</TH>
											<TH WIDTH="110px" class="details-bold" nowrap>Execution Status</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>Execution Date</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>Execution Time</TH>
											<TH WIDTH="460px" class="details-bold">Message</TH>'


			SET @idx=1		

			DECLARE   @jobName			[sysname]
					, @lastExecStatus	[int]
					, @lastExecDate		[varchar](10)
					, @lastExecTime		[varchar](8)
			
			SET @dateTimeLowerLimit = DATEADD(hh, -@configFailuresInLastHours, GETDATE())
			DECLARE crsSQLServerAgentJobsStatusIssuesDetected CURSOR READ_ONLY LOCAL FOR	SELECT	ssajh.[instance_name], ssajh.[job_name], ssajh.[last_execution_status], ssajh.[last_execution_date], ssajh.[last_execution_time], ssajh.[message]
																							FROM	[health-check].[vw_statsSQLServerAgentJobsHistory] ssajh
																							LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
																																			AND rsr.[rule_id] = 16
																																			AND rsr.[active] = 1
																																			AND (rsr.[skip_value]=ssajh.[instance_name])

																							WHERE	ssajh.[project_id]=@projectID
																									AND ssajh.[last_execution_status] NOT IN (1, 4) /* 1 = Succeded; 4 = In progress */
																									AND CONVERT([datetime], ssajh.[last_execution_date] + ' ' + ssajh.[last_execution_time], 120) >= @dateTimeLowerLimit
																									AND rsr.[id] IS NULL
																							ORDER BY ssajh.[instance_name], ssajh.[job_name], ssajh.[last_execution_date], ssajh.[last_execution_time]
			OPEN crsSQLServerAgentJobsStatusIssuesDetected
			FETCH NEXT FROM crsSQLServerAgentJobsStatusIssuesDetected INTO @instanceName, @jobName, @lastExecStatus, @lastExecDate, @lastExecTime, @message
			WHILE @@FETCH_STATUS=0
				begin
					SET @message = CASE WHEN LEFT(@message, 2) = '--' THEN SUBSTRING(@message, 3, LEN(@message)) ELSE @message END
					SET @message = ISNULL([dbo].[ufn_reportHTMLPrepareText](@message, 0), N'&nbsp;') 
					SET @message = REPLACE(@message, CHAR(13), N'<BR>')
					SET @message = REPLACE(@message, '--', N'<BR>')
					SET @message = REPLACE(@message, N'<BR><BR>', N'<BR>')

					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap>' + @instanceName + N'</TD>' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap>' + @jobName + N'</TD>' + 
										N'<TD WIDTH="110px" class="details" ALIGN="CENTER" nowrap>' + CASE WHEN @lastExecStatus = 0 THEN N'Failed'
																											WHEN @lastExecStatus = 1 THEN N'Succeded'
																											WHEN @lastExecStatus = 2 THEN N'Retry'
																											WHEN @lastExecStatus = 3 THEN N'Canceled'
																											WHEN @lastExecStatus = 4 THEN N'In progress'
																											ELSE N'Unknown'
																										END
										 + N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="CENTER" nowrap>' + @lastExecDate + N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="CENTER" nowrap>' + @lastExecTime + N'</TD>' + 
										N'<TD WIDTH="460px" class="details" ALIGN="LEFT">' + @message + N'</TD>' + 
									N'</TR>'
					SET @idx=@idx+1

					FETCH NEXT FROM crsSQLServerAgentJobsStatusIssuesDetected INTO @instanceName, @jobName, @lastExecStatus, @lastExecDate, @lastExecTime, @message
				end
			CLOSE crsSQLServerAgentJobsStatusIssuesDetected
			DEALLOCATE crsSQLServerAgentJobsStatusIssuesDetected

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

			SET @HTMLReport = REPLACE(@HTMLReport, '{SQLServerAgentJobsStatusIssuesDetectedCount}', '(' + CAST((@idx-1) AS [nvarchar]) + ')')
		end


	-----------------------------------------------------------------------------------------------------
	-- Long Running SQL Agent Jobs
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 4 = 4) AND (@flgOptions & 33554432 = 33554432)
		begin
			RAISERROR('	...Build Report: Long Running SQL Agent Jobs - Issues Detected', 10, 1) WITH NOWAIT
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="LongRunningSQLAgentJobsIssuesDetected" class="category-style">Long Running SQL Agent Jobs - Issues Detected</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="6">jobs currently running for more than ' + CAST(@configMaxJobRunningTimeInHours AS [nvarchar]) + N'hours</TD>
							</TR>
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="200px" class="details-bold" nowrap>Instance Name</TH>
											<TH WIDTH="200px" class="details-bold" nowrap>Job Name</TH>
											<TH WIDTH="110px" class="details-bold" nowrap>Running Time</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>Start Date</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>Start Time</TH>
											<TH WIDTH="460px" class="details-bold">Message</TH>'


			SET @idx=1		

			DECLARE   @runningTime		[varchar](32)
			
			SET @dateTimeLowerLimit = DATEADD(hh, -@configFailuresInLastHours, GETDATE())
			DECLARE crsLongRunningSQLAgentJobsIssuesDetected CURSOR READ_ONLY LOCAL FOR	SELECT	  ssajh.[instance_name], ssajh.[job_name]
																								, ssajh.[last_execution_date] AS [start_date], ssajh.[last_execution_time] AS [start_time]
																								, [dbo].[ufn_reportHTMLFormatTimeValue](CAST(ssajh.[running_time_sec]*1000 AS [bigint])) AS [running_time]
																								, ssajh.[message]
																						FROM [health-check].[vw_statsSQLServerAgentJobsHistory] ssajh
																						LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
																																		AND rsr.[rule_id] = 33554432
																																		AND rsr.[active] = 1
																																		AND (rsr.[skip_value]=ssajh.[instance_name])
																						WHERE ssajh.[last_execution_status] = 4
																								AND ssajh.[last_execution_date] IS NOT NULL
																								AND ssajh.[last_execution_time] IS NOT NULL
																								AND (ssajh.[running_time_sec]/3600) >= @configMaxJobRunningTimeInHours
																								AND rsr.[id] IS NULL
																						ORDER BY [start_date], [start_time]

			OPEN crsLongRunningSQLAgentJobsIssuesDetected
			FETCH NEXT FROM crsLongRunningSQLAgentJobsIssuesDetected INTO @instanceName, @jobName, @lastExecDate, @lastExecTime, @runningTime, @message
			WHILE @@FETCH_STATUS=0
				begin
					SET @message = CASE WHEN LEFT(@message, 2) = '--' THEN SUBSTRING(@message, 3, LEN(@message)) ELSE @message END
					SET @message = ISNULL([dbo].[ufn_reportHTMLPrepareText](@message, 0), N'&nbsp;') 
					SET @message = REPLACE(@message, CHAR(13), N'<BR>')
					SET @message = REPLACE(@message, '--', N'<BR>')
					SET @message = REPLACE(@message, N'<BR><BR>', N'<BR>')

					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap>' + @instanceName + N'</TD>' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap>' + @jobName + N'</TD>' + 
										N'<TD WIDTH="110px" class="details" ALIGN="CENTER" nowrap>' + @runningTime + N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="CENTER" nowrap>' + @lastExecDate + N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="CENTER" nowrap>' + @lastExecTime + N'</TD>' + 
										N'<TD WIDTH="460px" class="details" ALIGN="LEFT">' + @message + N'</TD>' + 
									N'</TR>'
					SET @idx=@idx+1

					FETCH NEXT FROM crsLongRunningSQLAgentJobsIssuesDetected INTO @instanceName, @jobName, @lastExecDate, @lastExecTime, @runningTime, @message
				end
			CLOSE crsLongRunningSQLAgentJobsIssuesDetected
			DEALLOCATE crsLongRunningSQLAgentJobsIssuesDetected

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

			SET @HTMLReport = REPLACE(@HTMLReport, '{LongRunningSQLAgentJobsIssuesDetectedCount}', '(' + CAST((@idx-1) AS [nvarchar]) + ')')
		end
		
		
	-----------------------------------------------------------------------------------------------------
	--Low Free Disk Space - Permission Errors
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 8 = 8) AND (@flgOptions & 131072 = 131072)
		begin
			RAISERROR('	...Build Report: Low Free Disk Space - Permission Errors', 10, 1) WITH NOWAIT
			
			SET @messageCount=0

			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="DiskSpaceInformationPermissionErrors" class="category-style">Low Free Disk Space - Permission Errors</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="120px" class="details-bold" nowrap>Machine Name</TH>
											<TH WIDTH="200px" class="details-bold" nowrap>Instance Name</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Clustered</TH>
											<TH WIDTH="150px" class="details-bold" nowrap>Event Date (UTC)</TH>
											<TH WIDTH="540px" class="details-bold">Message</TH>'

			SET @idx=1		
			
			DECLARE crsDiskSpaceInformationPermissionErrors CURSOR READ_ONLY LOCAL FOR	SELECT    cin.[machine_name], cin.[instance_name]
																								, cin.[is_clustered], cin.[cluster_node_machine_name]
																								, COUNT(DISTINCT lsam.[message]) AS [message_count]
																						FROM [dbo].[vw_catalogInstanceNames]  cin
																						INNER JOIN [dbo].[vw_logServerAnalysisMessages] lsam ON lsam.[project_id] = cin.[project_id] AND lsam.[instance_id] = cin.[instance_id]
																						LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
																																		AND rsr.[rule_id] = 131072
																																		AND rsr.[active] = 1
																																		AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																						WHERE	cin.[instance_active]=1
																								AND cin.[project_id] = @projectID
																								AND lsam.descriptor IN (N'dbo.usp_hcCollectDiskSpaceUsage')
																								AND rsr.[id] IS NULL
																						GROUP BY cin.[machine_name], cin.[instance_name], cin.[is_clustered], cin.[cluster_node_machine_name]
																						ORDER BY cin.[instance_name], cin.[machine_name]
			OPEN crsDiskSpaceInformationPermissionErrors
			FETCH NEXT FROM crsDiskSpaceInformationPermissionErrors INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @messageCount
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap ROWSPAN="' + CAST(@messageCount AS [nvarchar](64)) + N'"><A NAME="DiskSpaceInformationPermissionErrors' + @instanceName + N'">' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName) END + N'</A></TD>' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap ROWSPAN="' + CAST(@messageCount AS [nvarchar](64)) + N'">' + @instanceName + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="CENTER" nowrap ROWSPAN="' + CAST(@messageCount AS [nvarchar](64)) + N'">' + CASE WHEN @isClustered=0 THEN N'No' ELSE N'Yes' + ISNULL(N'<BR>[' + @clusterNodeName + ']', N'&nbsp;') END + N'</TD>' 


					SET @tmpHTMLReport=N''
					SELECT @tmpHTMLReport=((
											SELECT	N'<TD WIDTH="150px" class="details" ALIGN="CENTER" nowrap>' + ISNULL(CONVERT([nvarchar](24), [event_date_utc], 121), N'&nbsp;') + N'</TD>' + 
													N'<TD WIDTH="540px" class="details" ALIGN="LEFT">' + ISNULL([dbo].[ufn_reportHTMLPrepareText]([message], 0), N'&nbsp;') + N'</TD>' + 
													N'</TR>' + 
													CASE WHEN [row_count] > [row_no]
														 THEN N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">'
														 ELSE N''
													END
											FROM (
													SELECT	[message], [event_date_utc]
															, ROW_NUMBER() OVER(ORDER BY [event_date_utc]) [row_no]
															, SUM(1) OVER() AS [row_count]
													FROM (
															SELECT    lsam.[message]
																	, MAX(lsam.[event_date_utc]) [event_date_utc]
															FROM [dbo].[vw_catalogInstanceNames]  cin
															INNER JOIN [dbo].[vw_logServerAnalysisMessages] lsam ON lsam.[project_id] = cin.[project_id] AND lsam.[instance_id] = cin.[instance_id]
																WHERE	cin.[instance_active]=1
																	AND cin.[project_id] = @projectID	
																	AND cin.[instance_name] = @instanceName
																	AND cin.[machine_name] = @machineName
																	AND lsam.descriptor IN (N'dbo.usp_hcCollectDiskSpaceUsage')
															GROUP BY lsam.[message]
														)Z
												)X
											ORDER BY [event_date_utc]
											FOR XML PATH(''), TYPE
											).value('.', 'nvarchar(max)'))

					SET @idx=@idx+1
					SET @HTMLReportArea = @HTMLReportArea + COALESCE(@tmpHTMLReport, '')

					FETCH NEXT FROM crsDiskSpaceInformationPermissionErrors INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @messageCount

					IF @@FETCH_STATUS=0
						SET @HTMLReportArea = @HTMLReportArea + N'<TR VALIGN="TOP" class="color-2" HEIGHT="5px">
																<TD class="details" COLSPAN=5>&nbsp;</TD>
														</TR>'
				end
			CLOSE crsDiskSpaceInformationPermissionErrors
			DEALLOCATE crsDiskSpaceInformationPermissionErrors

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

			SELECT    @idx = COUNT(*) + 1
			FROM [dbo].[vw_catalogInstanceNames]  cin
			INNER JOIN [dbo].[vw_logServerAnalysisMessages] lsam ON lsam.[project_id] = cin.[project_id] AND lsam.[instance_id] = cin.[instance_id]
			WHERE	cin.[instance_active]=1
					AND cin.[project_id] = @projectID
					AND lsam.descriptor IN (N'dbo.usp_hcCollectDiskSpaceUsage')

			SET @HTMLReport = REPLACE(@HTMLReport, '{DiskSpaceInformationPermissionErrorsCount}', '(' + CAST((@idx-1) AS [nvarchar]) + ')')
		end
	

	-----------------------------------------------------------------------------------------------------
	--Low Free Disk Space - Issues Detected
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 8 = 8) AND (@flgOptions & 262144 = 262144)
		begin
			RAISERROR('	...Build Report: Low Free Disk Space - Issues Detected', 10, 1) WITH NOWAIT
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="DiskSpaceInformationIssuesDetected" class="category-style">Low Free Disk Space - Issues Detected</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="8">free disk space (%) &lt; ' + CAST(@configFreeDiskMinPercent AS [nvarchar](32)) + N' OR free disk space (MB) &lt; ' + CAST(@configFreeDiskMinSpace AS [nvarchar](32)) + N'</TD>
							</TR>
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="120px" class="details-bold" nowrap>Machine Name</TH>
											<TH WIDTH="200px" class="details-bold" nowrap>Instance Name</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Clustered</TH>
											<TH WIDTH="100px" class="details-bold" nowrap>Logical Drive</TH>
											<TH WIDTH="230px" class="details-bold" nowrap>Volume Mount Point</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Total Size (MB)</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Available Space (MB)</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Percent Available (%)</TH>'

			SET @idx=1		

			DECLARE crsDiskSpaceInformationIssuesDetected CURSOR READ_ONLY LOCAL FOR	SELECT  DISTINCT
																								  cin.[machine_name], cin.[instance_name]
																								, cin.[is_clustered], cin.[cluster_node_machine_name]
																								, dsi.[logical_drive], dsi.[volume_mount_point]
																								, dsi.[total_size_mb], dsi.[available_space_mb], dsi.[percent_available]
																						FROM [dbo].[vw_catalogInstanceNames]  cin
																						INNER JOIN [health-check].[vw_statsDiskSpaceInfo]		dsi	ON dsi.[project_id] = cin.[project_id] AND dsi.[instance_id] = cin.[instance_id]
																						LEFT  JOIN 
																									(
																										SELECT DISTINCT [project_id], [instance_id], [physical_drives] 
																										FROM [health-check].[vw_statsDatabaseDetails]
																									)   cdd ON cdd.[project_id] = cin.[project_id] AND cdd.[instance_id] = cin.[instance_id]
																						LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
																																		AND rsr.[rule_id] = 262144
																																		AND rsr.[active] = 1
																																		AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																						WHERE cin.[instance_active]=1
																								AND cin.[project_id] = @projectID
																								AND (    (	  dsi.[percent_available] IS NOT NULL 
																											AND dsi.[percent_available] < @configFreeDiskMinPercent
																											)
																										OR 
																										(	   dsi.[percent_available] IS NULL 
																											AND dsi.[available_space_mb] IS NOT NULL 
																											AND dsi.[available_space_mb] < @configFreeDiskMinSpace
																										)
																									)
																								AND (dsi.[logical_drive] IN ('C') OR CHARINDEX(dsi.[logical_drive], cdd.[physical_drives])>0)
																								AND rsr.[id] IS NULL
																						ORDER BY cin.[instance_name], cin.[machine_name]
			OPEN crsDiskSpaceInformationIssuesDetected
			FETCH NEXT FROM crsDiskSpaceInformationIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @logicalDrive, @volumeMountPoint, @diskTotalSizeMB, @diskAvailableSpaceMB, @diskPercentAvailable
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap>' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName) END + N'</TD>' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap>' + @instanceName + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="CENTER" nowrap>' + CASE WHEN @isClustered=0 THEN N'No' ELSE N'Yes' + ISNULL(N'<BR>[' + @clusterNodeName + ']', N'&nbsp;') END + N'</TD>' + 
										N'<TD WIDTH="100px" class="details" ALIGN="CENTER" nowrap>' + ISNULL(@logicalDrive, N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="230px" class="details" ALIGN="LEFT" nowrap>' + ISNULL(@volumeMountPoint, N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@diskTotalSizeMB AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@diskAvailableSpaceMB AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@diskPercentAvailable AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
									N'</TR>'
					SET @idx=@idx+1

					FETCH NEXT FROM crsDiskSpaceInformationIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @logicalDrive, @volumeMountPoint, @diskTotalSizeMB, @diskAvailableSpaceMB, @diskPercentAvailable
				end
			CLOSE crsDiskSpaceInformationIssuesDetected
			DEALLOCATE crsDiskSpaceInformationIssuesDetected

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

			SET @HTMLReport = REPLACE(@HTMLReport, '{DiskSpaceInformationIssuesDetectedCount}', '(' + CAST((@idx-1) AS [nvarchar]) + ')')
		end
	
	
	-----------------------------------------------------------------------------------------------------
	--System Databases Size - Issues Detected
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 2 = 2) AND (@flgOptions & 128 = 128)
		begin
			RAISERROR('	...Build Report: System Databases Size - Issues Detected', 10, 1) WITH NOWAIT
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="SystemDatabasesSizeIssuesDetected" class="category-style">System Databases Size - Issues Detected</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="5">size master (MB) &ge; ' + CAST(@configDBMaxSizeMaster AS [nvarchar](32)) + N' OR size msdb (MB) &ge; ' + CAST(@configDBMaxSizeMSDB AS [nvarchar](32)) + N'</TD>
							</TR>
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="120px" class="details-bold" nowrap>Machine Name</TH>
											<TH WIDTH="200px" class="details-bold" nowrap>Instance Name</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Clustered</TH>
											<TH WIDTH="490px" class="details-bold">Database Name</TH>
											<TH WIDTH="200px" class="details-bold" nowrap>Size (MB)</TH>'

			SET @idx=1		

			DECLARE crsDatabasesStatusIssuesDetected CURSOR READ_ONLY LOCAL FOR	SELECT    cin.[machine_name], cin.[instance_name]
																						, cin.[is_clustered], cin.[cluster_node_machine_name]
																						, cdn.[database_name]
																						, shcdd.[size_mb]
																				FROM [dbo].[vw_catalogInstanceNames]  cin
																				INNER JOIN [dbo].[vw_catalogDatabaseNames]			  cdn	ON cdn.[project_id] = cin.[project_id] AND cdn.[instance_id] = cin.[instance_id]
																				LEFT  JOIN [health-check].[vw_statsDatabaseDetails] shcdd ON shcdd.[catalog_database_id] = cdn.[catalog_database_id] AND shcdd.[instance_id] = cdn.[instance_id]
																				LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
																																AND rsr.[rule_id] = 128
																																AND rsr.[active] = 1
																																AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																				WHERE cin.[instance_active]=1
																						AND cdn.[active]=1
																						AND cin.[project_id] = @projectID	
																						AND (   (cdn.[database_name]='master' AND shcdd.[size_mb] >= @configDBMaxSizeMaster AND @configDBMaxSizeMaster<>0)
																							 OR (cdn.[database_name]='msdb'   AND shcdd.[size_mb] >= @configDBMaxSizeMSDB   AND @configDBMaxSizeMSDB<>0)
																							)
																						AND rsr.[id] IS NULL
																				ORDER BY shcdd.[size_mb] DESC, cin.[instance_name], cin.[machine_name], cdn.[database_name]
			OPEN crsDatabasesStatusIssuesDetected
			FETCH NEXT FROM crsDatabasesStatusIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @dbSize
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap>' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName) END + N'</TD>' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap>' + @instanceName + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="CENTER" nowrap>' + CASE WHEN @isClustered=0 THEN N'No' ELSE N'Yes' + ISNULL(N'<BR>[' + @clusterNodeName + ']', N'&nbsp;') END + N'</TD>' + 
										N'<TD WIDTH="490px" class="details" ALIGN="LEFT">' + ISNULL(@databaseName, N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="200px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@dbSize AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
									N'</TR>'
					SET @idx=@idx+1

					FETCH NEXT FROM crsDatabasesStatusIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @dbSize
				end
			CLOSE crsDatabasesStatusIssuesDetected
			DEALLOCATE crsDatabasesStatusIssuesDetected

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

			SET @HTMLReport = REPLACE(@HTMLReport, '{SystemDatabasesSizeIssuesDetectedCount}', '(' + CAST((@idx-1) AS [nvarchar]) + ')')
		end
		
		
	-----------------------------------------------------------------------------------------------------
	--Databases with Auto Close / Shrink - Issues Detected
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 2 = 2) AND (@flgOptions & 512 = 512)
		begin
			RAISERROR('	...Build Report: Databases with Auto Close / Shrink - Issues Detected', 10, 1) WITH NOWAIT
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="DatabasesWithAutoCloseShrinkIssuesDetected" class="category-style">Databases with Auto Close / Shrink - Issues Detected</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="120px" class="details-bold" nowrap>Machine Name</TH>
											<TH WIDTH="200px" class="details-bold" nowrap>Instance Name</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Clustered</TH>
											<TH WIDTH="490px" class="details-bold">Database Name</TH>
											<TH WIDTH="100px" class="details-bold" nowrap>Auto Close</TH>
											<TH WIDTH="100px" class="details-bold" nowrap>Auto Shrink</TH>'

			SET @idx=1		

			DECLARE   @isAutoClose		[bit]
					, @isAutoShrink		[bit]

			DECLARE crsDatabasesStatusIssuesDetected CURSOR READ_ONLY LOCAL FOR	SELECT    cin.[machine_name], cin.[instance_name]
																						, cin.[is_clustered], cin.[cluster_node_machine_name]
																						, cdn.[database_name]
																						, shcdd.[is_auto_close]
																						, shcdd.[is_auto_shrink]
																				FROM [dbo].[vw_catalogInstanceNames]  cin
																				INNER JOIN [dbo].[vw_catalogDatabaseNames]			  cdn	ON cdn.[project_id] = cin.[project_id] AND cdn.[instance_id] = cin.[instance_id]
																				INNER JOIN [health-check].[vw_statsDatabaseDetails] shcdd ON shcdd.[catalog_database_id] = cdn.[catalog_database_id] AND shcdd.[instance_id] = cdn.[instance_id]
																				LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
																																AND rsr.[rule_id] = 512
																																AND rsr.[active] = 1
																																AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																				WHERE cin.[instance_active]=1
																						AND cdn.[active]=1
																						AND cin.[project_id] = @projectID
																						AND (shcdd.[is_auto_close]=1 OR shcdd.[is_auto_shrink]=1)
																						AND rsr.[id] IS NULL
																				ORDER BY cin.[instance_name], cin.[machine_name], cdn.[database_name]
			OPEN crsDatabasesStatusIssuesDetected
			FETCH NEXT FROM crsDatabasesStatusIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @isAutoClose, @isAutoShrink
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap>' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName) END + N'</TD>' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap>' + @instanceName + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="CENTER" nowrap>' + CASE WHEN @isClustered=0 THEN N'No' ELSE N'Yes' + ISNULL(N'<BR>[' + @clusterNodeName + ']', N'&nbsp;') END + N'</TD>' + 
										N'<TD WIDTH="490px" class="details" ALIGN="LEFT">' + ISNULL(@databaseName, N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="100px" class="details" ALIGN="CENTER" nowrap>' + CASE WHEN @isAutoClose=0 THEN N'No' ELSE N'Yes' END + N'</TD>' + 
										N'<TD WIDTH="100px" class="details" ALIGN="CENTER" nowrap>' + CASE WHEN @isAutoShrink=0 THEN N'No' ELSE N'Yes' END + N'</TD>' + 
									N'</TR>'
					SET @idx=@idx+1

					FETCH NEXT FROM crsDatabasesStatusIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @isAutoClose, @isAutoShrink
				end
			CLOSE crsDatabasesStatusIssuesDetected
			DEALLOCATE crsDatabasesStatusIssuesDetected

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

			SET @HTMLReport = REPLACE(@HTMLReport, '{DatabasesWithAutoCloseShrinkIssuesDetectedCount}', '(' + CAST((@idx-1) AS [nvarchar]) + ')')
		end
		
	
	-----------------------------------------------------------------------------------------------------
	--Big Size for Database Log files - Issues Detected
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 2 = 2) AND (@flgOptions & 1024 = 1024)
		begin
			RAISERROR('	...Build Report: Big Size for Database Log files - Issues Detected', 10, 1) WITH NOWAIT
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="DatabaseMaxLogSizeIssuesDetected" class="category-style">Big Size for Database Log files - Issues Detected</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="6">log size (MB) &ge; ' + CAST(@configLogMaxSize AS [nvarchar](32)) + N'</TD>
							</TR>
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="120px" class="details-bold" nowrap>Machine Name</TH>
											<TH WIDTH="200px" class="details-bold" nowrap>Instance Name</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Clustered</TH>
											<TH WIDTH="450px" class="details-bold">Database Name</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Log Size (MB)</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Log Used (%)</TH>'

			SET @idx=1		

			DECLARE crsDatabaseMaxLogSizeIssuesDetected CURSOR READ_ONLY LOCAL FOR	SELECT    cin.[machine_name], cin.[instance_name]
																							, cin.[is_clustered], cin.[cluster_node_machine_name]
																							, cdn.[database_name]
																							, shcdd.[log_size_mb]
																							, shcdd.[log_space_used_percent]
																					FROM [dbo].[vw_catalogInstanceNames]  cin
																					INNER JOIN [dbo].[vw_catalogDatabaseNames]			  cdn	ON cdn.[project_id] = cin.[project_id] AND cdn.[instance_id] = cin.[instance_id]
																					INNER  JOIN [health-check].[vw_statsDatabaseDetails] shcdd ON shcdd.[catalog_database_id] = cdn.[catalog_database_id] AND shcdd.[instance_id] = cdn.[instance_id]
																					LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
																																	AND rsr.[rule_id] = 1024
																																	AND rsr.[active] = 1
																																	AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																					WHERE cin.[instance_active]=1
																							AND cdn.[active]=1
																							AND cin.[project_id] = @projectID	
																							AND shcdd.[log_size_mb] >= @configLogMaxSize 
																							AND rsr.[id] IS NULL
																					ORDER BY shcdd.[log_size_mb] DESC, cin.[instance_name], cin.[machine_name], cdn.[database_name]
			OPEN crsDatabaseMaxLogSizeIssuesDetected
			FETCH NEXT FROM crsDatabaseMaxLogSizeIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @logSizeMB, @logSpaceUsedPercent
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap>' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName) END + N'</TD>' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap>' + @instanceName + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="CENTER" nowrap>' + CASE WHEN @isClustered=0 THEN N'No' ELSE N'Yes' + ISNULL(N'<BR>[' + @clusterNodeName + ']', N'&nbsp;') END + N'</TD>' + 
										N'<TD WIDTH="450px" class="details" ALIGN="LEFT">' + ISNULL(@databaseName, N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@logSizeMB AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@logSpaceUsedPercent AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
									N'</TR>'
					SET @idx=@idx+1

					FETCH NEXT FROM crsDatabaseMaxLogSizeIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @logSizeMB, @logSpaceUsedPercent
				end
			CLOSE crsDatabaseMaxLogSizeIssuesDetected
			DEALLOCATE crsDatabaseMaxLogSizeIssuesDetected

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

			SET @HTMLReport = REPLACE(@HTMLReport, '{DatabaseMaxLogSizeIssuesDetectedCount}', '(' + CAST((@idx-1) AS [nvarchar]) + ')')
		end


	-----------------------------------------------------------------------------------------------------
	--Low Usage of Data Space - Issues Detected
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 2 = 2) AND (@flgOptions & 2048 = 2048)
		begin
			RAISERROR('	...Build Report: Low Usage of Data Space - Issues Detected', 10, 1) WITH NOWAIT
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="DatabaseMinDataSpaceIssuesDetected" class="category-style">Low Usage of Data Space - Issues Detected</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="8">size (MB) &ge; ' + CAST(@configDBMinSizeForAnalysis AS [nvarchar](32)) + N' AND data size used (%) &le; ' + CAST(@configDataSpaceMinPercent AS [nvarchar](32)) + N'</TD>
							</TR>
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="120px" class="details-bold" nowrap>Machine Name</TH>
											<TH WIDTH="200px" class="details-bold" nowrap>Instance Name</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Clustered</TH>
											<TH WIDTH="370px" class="details-bold">Database Name</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>DB Size (MB)</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>Data Size (MB)</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>Data Space Used (%)</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>Reclaimable Space (MB)</TH>
											'

			SET @idx=1		
					
			DECLARE crsDatabaseMinDataSpaceIssuesDetected CURSOR READ_ONLY LOCAL FOR	SELECT    cin.[machine_name], cin.[instance_name]
																							, cin.[is_clustered], cin.[cluster_node_machine_name]
																							, cdn.[database_name]
																							, shcdd.[size_mb]
																							, shcdd.[data_size_mb]
																							, shcdd.[data_space_used_percent]
																							, ((100.0 - shcdd.[data_space_used_percent]) * shcdd.[data_size_mb]) / 100 AS [reclaimable_space_mb]
																					FROM [dbo].[vw_catalogInstanceNames]  cin
																					INNER JOIN [dbo].[vw_catalogDatabaseNames]			  cdn	ON cdn.[project_id] = cin.[project_id] AND cdn.[instance_id] = cin.[instance_id]
																					INNER  JOIN [health-check].[vw_statsDatabaseDetails] shcdd ON shcdd.[catalog_database_id] = cdn.[catalog_database_id] AND shcdd.[instance_id] = cdn.[instance_id]
																					LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
																																	AND rsr.[rule_id] = 2048
																																	AND rsr.[active] = 1
																																	AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																					WHERE cin.[instance_active]=1
																							AND cdn.[active]=1
																							AND cin.[project_id] = @projectID	
																							AND shcdd.[size_mb]>=@configDBMinSizeForAnalysis
																							AND shcdd.[data_space_used_percent] <= @configDataSpaceMinPercent 
																							AND @configDataSpaceMinPercent<>0
																							AND cdn.[database_name] NOT IN ('master', 'msdb', 'model', 'tempdb')
																							AND rsr.[id] IS NULL
																					ORDER BY --[reclaimable_space_mb] DESC, 
																							 cin.[instance_name], cin.[machine_name], shcdd.[data_space_used_percent] DESC, cdn.[database_name]
			OPEN crsDatabaseMinDataSpaceIssuesDetected
			FETCH NEXT FROM crsDatabaseMinDataSpaceIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @dbSize, @dataSizeMB, @dataSpaceUsedPercent, @reclaimableSpaceMB
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap>' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName) END + N'</TD>' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap>' + @instanceName + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="CENTER" nowrap>' + CASE WHEN @isClustered=0 THEN N'No' ELSE N'Yes' + ISNULL(N'<BR>[' + @clusterNodeName + ']', N'&nbsp;') END + N'</TD>' + 
										N'<TD WIDTH="370px" class="details" ALIGN="LEFT">' + ISNULL(@databaseName, N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@dbSize AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@dataSizeMB AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@dataSpaceUsedPercent AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@reclaimableSpaceMB AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
									N'</TR>'
					SET @idx=@idx+1

					FETCH NEXT FROM crsDatabaseMinDataSpaceIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @dbSize, @dataSizeMB, @dataSpaceUsedPercent, @reclaimableSpaceMB
				end
			CLOSE crsDatabaseMinDataSpaceIssuesDetected
			DEALLOCATE crsDatabaseMinDataSpaceIssuesDetected

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

			SET @HTMLReport = REPLACE(@HTMLReport, '{DatabaseMinDataSpaceIssuesDetectedCount}', '(' + CAST((@idx-1) AS [nvarchar]) + ')')
		end
		
	
	-----------------------------------------------------------------------------------------------------
	--High Usage of Log Space - Issues Detected
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 2 = 2) AND (@flgOptions & 32768 = 32768)
		begin
			RAISERROR('	...Build Report: High Usage of Log Space - Issues Detected', 10, 1) WITH NOWAIT
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="DatabaseMaxLogSpaceIssuesDetected" class="category-style">High Usage of Log Space - Issues Detected</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="8">size (MB) &ge; ' + CAST(@configDBMinSizeForAnalysis AS [nvarchar](32)) + N' AND log size used (%) &ge; ' + CAST(@configLogSpaceMaxPercent AS [nvarchar](32)) + N'</TD>
							</TR>
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="120px" class="details-bold" nowrap>Machine Name</TH>
											<TH WIDTH="200px" class="details-bold" nowrap>Instance Name</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Clustered</TH>
											<TH WIDTH="370px" class="details-bold">Database Name</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>DB Size (MB)</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>Log Size (MB)</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>Log Space Used (%)</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>Available space (MB)</TH>
											'

			SET @idx=1		
					
			DECLARE crsDatabaseMaxLogSpaceIssuesDetected CURSOR READ_ONLY LOCAL FOR	SELECT    cin.[machine_name], cin.[instance_name]
																							, cin.[is_clustered], cin.[cluster_node_machine_name]
																							, cdn.[database_name]
																							, shcdd.[size_mb]
																							, shcdd.[log_size_mb]
																							, shcdd.[log_space_used_percent]
																							, ((100.0 - shcdd.[log_space_used_percent]) * shcdd.[log_size_mb]) / 100 AS [available_space_mb]
																					FROM [dbo].[vw_catalogInstanceNames]  cin
																					INNER JOIN [dbo].[vw_catalogDatabaseNames]			  cdn	ON cdn.[project_id] = cin.[project_id] AND cdn.[instance_id] = cin.[instance_id]
																					INNER  JOIN [health-check].[vw_statsDatabaseDetails] shcdd ON shcdd.[catalog_database_id] = cdn.[catalog_database_id] AND shcdd.[instance_id] = cdn.[instance_id]
																					LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
																																	AND rsr.[rule_id] = 32768
																																	AND rsr.[active] = 1
																																	AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																					WHERE cin.[instance_active]=1
																							AND cdn.[active]=1
																							AND cin.[project_id] = @projectID	
																							AND shcdd.[size_mb]>=@configDBMinSizeForAnalysis
																							AND shcdd.[log_space_used_percent] >= @configLogSpaceMaxPercent 
																							AND @configLogSpaceMaxPercent<>0
																							AND cdn.[database_name] NOT IN ('master', 'msdb', 'model', 'tempdb')
																							AND rsr.[id] IS NULL
																					ORDER BY --[available_space_mb] DESC, 
																							 cin.[instance_name], cin.[machine_name], shcdd.[data_space_used_percent] DESC, cdn.[database_name]
			OPEN crsDatabaseMaxLogSpaceIssuesDetected
			FETCH NEXT FROM crsDatabaseMaxLogSpaceIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @dbSize, @logSizeMB, @logSpaceUsedPercent, @reclaimableSpaceMB
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap>' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName) END + N'</TD>' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap>' + @instanceName + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="CENTER" nowrap>' + CASE WHEN @isClustered=0 THEN N'No' ELSE N'Yes' + ISNULL(N'<BR>[' + @clusterNodeName + ']', N'&nbsp;') END + N'</TD>' + 
										N'<TD WIDTH="370px" class="details" ALIGN="LEFT">' + ISNULL(@databaseName, N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@dbSize AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@logSizeMB AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@logSpaceUsedPercent AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@reclaimableSpaceMB AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
									N'</TR>'
					SET @idx=@idx+1

					FETCH NEXT FROM crsDatabaseMaxLogSpaceIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @dbSize, @logSizeMB, @logSpaceUsedPercent, @reclaimableSpaceMB
				end
			CLOSE crsDatabaseMaxLogSpaceIssuesDetected
			DEALLOCATE crsDatabaseMaxLogSpaceIssuesDetected

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

			SET @HTMLReport = REPLACE(@HTMLReport, '{DatabaseMaxLogSpaceIssuesDetectedCount}', '(' + CAST((@idx-1) AS [nvarchar]) + ')')
		end
		
	
	-----------------------------------------------------------------------------------------------------
	--Log vs. Data - Allocated Size - Issues Detected
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 2 = 2) AND (@flgOptions & 4096 = 4096)
		begin
			RAISERROR('	...Build Report: Log vs. Data - Allocated Size - Issues Detected', 10, 1) WITH NOWAIT
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="DatabaseLogVsDataSizeIssuesDetected" class="category-style">Log vs. Data - Allocated Size - Issues Detected</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="8">size (MB) &ge; ' + CAST(@configDBMinSizeForAnalysis AS [nvarchar](32)) + N' AND log/data size (%) &gt; ' + CAST(@configLogVsDataPercent AS [nvarchar](32)) + N'</TD>
							</TR>
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="120px" class="details-bold" nowrap>Machine Name</TH>
											<TH WIDTH="200px" class="details-bold" nowrap>Instance Name</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Clustered</TH>
											<TH WIDTH="370px" class="details-bold">Database Name</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>DB Size (MB)</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>Data Size (MB)</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>Log Size (MB)</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>Log vs. Data (%)</TH>'

			SET @idx=1		

			DECLARE crsDatabaseLogVsDataSizeIssuesDetected CURSOR READ_ONLY LOCAL FOR	SELECT    [machine_name], [instance_name], [is_clustered], [cluster_node_machine_name], [database_name]
																								, [size_mb], [data_size_mb], [log_size_mb]
																								, [log_vs_data]
																						FROM (
																								SELECT  cin.[machine_name], cin.[instance_name]
																										, cin.[is_clustered], cin.[cluster_node_machine_name]
																										, cdn.[database_name]
																										, shcdd.[size_mb]
																										, shcdd.[data_size_mb]
																										, shcdd.[log_size_mb]
																										, (shcdd.[log_size_mb] / shcdd.[data_size_mb] * 100.) AS [log_vs_data]
																								FROM [dbo].[vw_catalogInstanceNames]  cin
																								INNER JOIN [dbo].[vw_catalogDatabaseNames]			  cdn	ON cdn.[project_id] = cin.[project_id] AND cdn.[instance_id] = cin.[instance_id]
																								INNER JOIN [health-check].[vw_statsDatabaseDetails] shcdd ON shcdd.[catalog_database_id] = cdn.[catalog_database_id] AND shcdd.[instance_id] = cdn.[instance_id]
																								LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
																																				AND rsr.[rule_id] = 4096
																																				AND rsr.[active] = 1
																																				AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																								WHERE cin.[instance_active]=1
																										AND cdn.[active]=1
																										AND cin.[project_id] = @projectID	
																										AND shcdd.[data_size_mb] <> 0
																										AND (shcdd.[log_size_mb] / shcdd.[data_size_mb] * 100.) > @configLogVsDataPercent
																										AND shcdd.[size_mb]>=@configDBMinSizeForAnalysis
																										AND cdn.[database_name] NOT IN ('master', 'msdb', 'model', 'tempdb')
																										AND rsr.[id] IS NULL
																							)X
																						WHERE [log_vs_data] >= @configLogVsDataPercent
																						ORDER BY [instance_name], [machine_name], [log_vs_data] DESC, [database_name]
			OPEN crsDatabaseLogVsDataSizeIssuesDetected
			FETCH NEXT FROM crsDatabaseLogVsDataSizeIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @dbSize, @dataSizeMB, @logSizeMB, @logVSDataPercent
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap>' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName) END + N'</TD>' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap>' + @instanceName + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="CENTER" nowrap>' + CASE WHEN @isClustered=0 THEN N'No' ELSE N'Yes' + ISNULL(N'<BR>[' + @clusterNodeName + ']', N'&nbsp;') END + N'</TD>' + 
										N'<TD WIDTH="370px" class="details" ALIGN="LEFT">' + ISNULL(@databaseName, N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@dbSize AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@dataSizeMB AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@logSizeMB AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@logVSDataPercent AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
									N'</TR>'
					SET @idx=@idx+1

					FETCH NEXT FROM crsDatabaseLogVsDataSizeIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @dbSize, @dataSizeMB, @logSizeMB, @logVSDataPercent
				end
			CLOSE crsDatabaseLogVsDataSizeIssuesDetected
			DEALLOCATE crsDatabaseLogVsDataSizeIssuesDetected

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

			SET @HTMLReport = REPLACE(@HTMLReport, '{DatabaseLogVsDataSizeIssuesDetectedCount}', '(' + CAST((@idx-1) AS [nvarchar]) + ')')
		end
		

	-----------------------------------------------------------------------------------------------------
	--Databases with Fixed File(s) Size - Issues Detected
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 2 = 2) AND (@flgOptions & 4194304 = 4194304)
		begin
			RAISERROR('	...Databases with Fixed File(s) Size - Issues Detected', 10, 1) WITH NOWAIT
		
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="DatabaseFixedFileSizeIssuesDetected" class="category-style">Databases with Fixed File(s) Size</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="200px" class="details-bold">Instance Name</TH>
											<TH WIDTH="220px" class="details-bold">Database Name</TH>
											<TH WIDTH="120px" class="details-bold">Size (MB)</TH>
											<TH WIDTH="120px" class="details-bold">Data Size (MB)</TH>
											<TH WIDTH="100px" class="details-bold">Data Space Used (%)</TH>
											<TH WIDTH="120px" class="details-bold">Log Size (MB)</TH>
											<TH WIDTH="100px" class="details-bold">Log Space Used (%)</TH>
											<TH WIDTH="150px" class="details-bold">State</TH>'

			SET @idx=1		
			
			DECLARE crsDatabaseFixedFileSizeIssuesDetected CURSOR READ_ONLY LOCAL FOR	
																				SELECT    cin.[instance_name]
																						, cdn.[database_name], cdn.[state_desc]
																						, shcdd.[size_mb]
																						, shcdd.[data_size_mb], shcdd.[data_space_used_percent]
																						, shcdd.[log_size_mb], shcdd.[log_space_used_percent] 
																				FROM [dbo].[vw_catalogInstanceNames] cin
																				INNER JOIN [dbo].[vw_catalogDatabaseNames]			  cdn	ON cdn.[project_id] = cin.[project_id] AND cdn.[instance_id] = cin.[instance_id]
																				LEFT  JOIN [health-check].[vw_statsDatabaseDetails] shcdd ON shcdd.[catalog_database_id] = cdn.[catalog_database_id] AND shcdd.[instance_id] = cdn.[instance_id]
																				LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
																																AND rsr.[rule_id] = 4194304
																																AND rsr.[active] = 1
																																AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																				WHERE	cin.[instance_active]=1
																						AND cdn.[active]=1
																						AND cin.[project_id] = @projectID	
																						AND shcdd.[is_growth_limited]=1
																						AND rsr.[id] IS NULL
																				ORDER BY cdn.[database_name]
			OPEN crsDatabaseFixedFileSizeIssuesDetected
			FETCH NEXT FROM crsDatabaseFixedFileSizeIssuesDetected INTO  @instanceName, @databaseName, @stateDesc, @dbSize, @dataSizeMB, @dataSpaceUsedPercent, @logSizeMB, @logSpaceUsedPercent
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="220px" class="details" ALIGN="LEFT" nowrap>' + @instanceName + N'</TD>' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT">' + ISNULL(@databaseName, N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@dbSize AS [nvarchar](64)), N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@dataSizeMB AS [nvarchar](64)), N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="100px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@dataSpaceUsedPercent AS [nvarchar](64)), N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@logSizeMB AS [nvarchar](64)), N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="100px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@logSpaceUsedPercent AS [nvarchar](64)), N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="150px" class="details" ALIGN="LEFT">' + ISNULL(@stateDesc, N'&nbsp;') + N'</TD>' + 
									N'</TR>'
					SET @idx=@idx+1

					FETCH NEXT FROM crsDatabaseFixedFileSizeIssuesDetected INTO @instanceName, @databaseName, @stateDesc, @dbSize, @dataSizeMB, @dataSpaceUsedPercent, @logSizeMB, @logSpaceUsedPercent
				end
			CLOSE crsDatabaseFixedFileSizeIssuesDetected
			DEALLOCATE crsDatabaseFixedFileSizeIssuesDetected

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

			SET @HTMLReport = REPLACE(@HTMLReport, '{DatabaseFixedFileSizeIssuesDetectedCount}', '(' + CAST((@idx-1) AS [nvarchar]) + ')')
		end


	-----------------------------------------------------------------------------------------------------
	--Databases with Improper Page Verify Option
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 2 = 2) AND (@flgOptions & 8388608 = 8388608)
		begin
			RAISERROR('	...Databases with Improper Page Verify Option - Issues Detected', 10, 1) WITH NOWAIT
		
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="DatabasePageVerifyIssuesDetected" class="category-style">Databases with Improper Page Verify Option</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="120px" class="details-bold" nowrap>Machine Name</TH>
											<TH WIDTH="200px" class="details-bold" nowrap>Instance Name</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Clustered</TH>
											<TH WIDTH="340px" class="details-bold">Database Name</TH>
											<TH WIDTH= "90px" class="details-bold" nowrap>SQL Version</TH>
											<TH WIDTH="100px" class="details-bold" nowrap>Compatibility</TH>
											<TH WIDTH="160px" class="details-bold" nowrap>Page Verify</TH>'

			SET @idx=1		

			DECLARE @pageVerify			[sysname],
					@compatibilityLevel	[tinyint]

			DECLARE crsDatabasePageVerifyIssuesDetected CURSOR READ_ONLY LOCAL FOR	SELECT    cin.[machine_name], cin.[instance_name]
																							, cin.[is_clustered], cin.[cluster_node_machine_name]
																							, cdn.[database_name]
																							, cin.[version]
																							, shcdd.[page_verify_option_desc]
																							, shcdd.[compatibility_level]
																					FROM [dbo].[vw_catalogInstanceNames]  cin
																					INNER JOIN [dbo].[vw_catalogDatabaseNames]			  cdn	ON cdn.[project_id] = cin.[project_id] AND cdn.[instance_id] = cin.[instance_id]
																					INNER JOIN [health-check].[vw_statsDatabaseDetails] shcdd ON shcdd.[catalog_database_id] = cdn.[catalog_database_id] AND shcdd.[instance_id] = cdn.[instance_id]
																					LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
																																	AND rsr.[rule_id] = 8388608
																																	AND rsr.[active] = 1
																																	AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																					WHERE cin.[instance_active]=1
																							AND cdn.[active]=1
																							AND cin.[project_id] = @projectID
																							AND cdn.[database_name] NOT IN ('tempdb')
																							AND (   
																									(     shcdd.[page_verify_option_desc] <> 'CHECKSUM'
																									  AND cin.[version] NOT LIKE '8.%'
																									)
																								 OR (     shcdd.[page_verify_option_desc] = 'NONE'
																									  AND cin.[version] LIKE '8.%'
																									)
																								)
																							AND CHARINDEX(cdn.[state_desc], @configAdmittedState)<>0
																							AND rsr.[id] IS NULL
																					ORDER BY cin.[instance_name], cin.[machine_name], cdn.[database_name]
			OPEN crsDatabasePageVerifyIssuesDetected
			FETCH NEXT FROM crsDatabasePageVerifyIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @version, @pageVerify, @compatibilityLevel
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap>' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName) END + N'</TD>' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap>' + @instanceName + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="CENTER" nowrap>' + CASE WHEN @isClustered=0 THEN N'No' ELSE N'Yes' + ISNULL(N'<BR>[' + @clusterNodeName + ']', N'&nbsp;') END + N'</TD>' + 
										N'<TD WIDTH="340px" class="details" ALIGN="LEFT">' + ISNULL(@databaseName, N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH= "90px" class="details" ALIGN="CENTER" nowrap>' + ISNULL(@version, N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="100px" class="details" ALIGN="CENTER" nowrap>' + ISNULL(CAST(@compatibilityLevel AS [sysname]), N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="160px" class="details" ALIGN="CENTER" nowrap>' + ISNULL(@pageVerify, N'&nbsp;') + N'</TD>' + 
									N'</TR>'
					SET @idx=@idx+1

					FETCH NEXT FROM crsDatabasePageVerifyIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @version, @pageVerify, @compatibilityLevel
				end
			CLOSE crsDatabasePageVerifyIssuesDetected
			DEALLOCATE crsDatabasePageVerifyIssuesDetected

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

			SET @HTMLReport = REPLACE(@HTMLReport, '{DatabasePageVerifyIssuesDetectedCount}', '(' + CAST((@idx-1) AS [nvarchar]) + ')')			
		end


	-----------------------------------------------------------------------------------------------------
	--Frequently Fragmented Indexes
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 2 = 2) AND (@flgOptions & 16777216 = 16777216)
		begin
			RAISERROR('	...Frequently Fragmented Indexes - Issues Detected', 10, 1) WITH NOWAIT

			DECLARE @indexAnalyzedCount						[int],
					@indexesPerInstance						[int],
					@minimumIndexMaintenanceFrequencyDays	[tinyint] = 2,
					@analyzeOnlyMessagesFromTheLastHours	[tinyint] = 24 ,
					@analyzeIndexMaintenanceOperation		[nvarchar](128) = 'REBUILD'

		
			-----------------------------------------------------------------------------------------------------
			--reading report options
			SELECT	@minimumIndexMaintenanceFrequencyDays = [value]
			FROM	[dbo].[reportHTMLOptions]
			WHERE	[name] = N'Minimum Index Maintenance Frequency (days)'
					AND [module] = 'health-check'

			SET @minimumIndexMaintenanceFrequencyDays = ISNULL(@minimumIndexMaintenanceFrequencyDays, 2)

			-----------------------------------------------------------------------------------------------------
			SELECT	@analyzeOnlyMessagesFromTheLastHours = [value]
			FROM	[dbo].[reportHTMLOptions]
			WHERE	[name] = N'Analyze Only Messages from the last hours'
					AND [module] = 'health-check'

			SET @analyzeOnlyMessagesFromTheLastHours = ISNULL(@analyzeOnlyMessagesFromTheLastHours, 24)
	
			-----------------------------------------------------------------------------------------------------
			SELECT	@analyzeIndexMaintenanceOperation = [value]
			FROM	[dbo].[reportHTMLOptions]
			WHERE	[name] = N'Analyze Index Maintenance Operation'
					AND [module] = 'health-check'

			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="FrequentlyFragmentedIndexesIssuesDetected" class="category-style">Frequently Fragmented Indexes</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="11">indexes which got fragmented in the last ' + CAST(@minimumIndexMaintenanceFrequencyDays AS [nvarchar](32)) + N' day(s), were analyzed in the last ' + CAST(@analyzeOnlyMessagesFromTheLastHours AS [nvarchar](32)) + N' hours and last action was in (' + @analyzeIndexMaintenanceOperation + N')</TD>
							</TR>
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="11">consider lowering the fill-factor with at least 5 percent</TD>
							</TR>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="120px" class="details-bold">Instance Name</TH>
											<TH WIDTH="120px" class="details-bold">Database Name</TH>
											<TH WIDTH="120px" class="details-bold">Table Name</TH>
											<TH WIDTH="120px" class="details-bold">Index Name</TH>
											<TH WIDTH="100px" class="details-bold">Type</TH>
											<TH WIDTH=" 80px" class="details-bold">Frequency (days)</TH>
											<TH WIDTH=" 80px" class="details-bold">Page Count</TH>
											<TH WIDTH=" 90px" class="details-bold">Fragmentation</TH>
											<TH WIDTH="100px" class="details-bold">Page Density Deviation</TH>
											<TH WIDTH=" 80px" class="details-bold">Fill-Factor</TH>
											<TH WIDTH="120px" class="details-bold">Last Action</TH>
											'
			SET @idx=1		

			-----------------------------------------------------------------------------------------------------
			RAISERROR('		...analyzing fragmentation logs', 10, 1) WITH NOWAIT

			IF OBJECT_ID('tempdb..#filteredStatsIndexesFrequentlyFragmented]') IS NOT NULL
				DROP TABLE #filteredStatsIndexesFrequentlyFragmented

			SELECT *
			INTO #filteredStatsIndexesFrequentlyFragmented
			FROM [dbo].[ufn_hcGetIndexesFrequentlyFragmented](@projectCode, @minimumIndexMaintenanceFrequencyDays, @analyzeOnlyMessagesFromTheLastHours, @analyzeIndexMaintenanceOperation) iff
			LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
															AND rsr.[rule_id] = 16777216
															AND rsr.[active] = 1
															AND (rsr.[skip_value]=iff.[instance_name])

			CREATE INDEX IX_filteredStatsIndexesFrequentlyFragmented_InstanceName ON #filteredStatsIndexesFrequentlyFragmented([instance_name])

			RAISERROR('		...done', 10, 1) WITH NOWAIT
			-----------------------------------------------------------------------------------------------------
			SET @indexAnalyzedCount=0

			DECLARE crsFrequentlyFragmentedIndexesMachineNames CURSOR READ_ONLY LOCAL FOR		SELECT    iff.[instance_name]
																										, COUNT(*) AS [index_count]
																								FROM #filteredStatsIndexesFrequentlyFragmented iff
																								GROUP BY iff.[instance_name]
																								ORDER BY iff.[instance_name]
			OPEN crsFrequentlyFragmentedIndexesMachineNames
			FETCH NEXT FROM crsFrequentlyFragmentedIndexesMachineNames INTO  @instanceName, @indexesPerInstance
			WHILE @@FETCH_STATUS=0
				begin
					SET @indexAnalyzedCount = @indexAnalyzedCount + @indexesPerInstance
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" ROWSPAN="' + CAST(@indexesPerInstance AS [nvarchar](64)) + N'"><A NAME="FrequentlyFragmentedIndexesCompleteDetails' + @instanceName + N'">' + @instanceName + N'</A></TD>' 

					SET @tmpHTMLReport=N''
					SELECT @tmpHTMLReport=((
											SELECT	N'<TD WIDTH="120px" class="details" ALIGN="LEFT">' + ISNULL([database_name], N'&nbsp;') + N'</TD>' + 
													N'<TD WIDTH="120px" class="details" ALIGN="LEFT">' + ISNULL([object_name], N'&nbsp;') + N'</TD>' + 
													N'<TD WIDTH="120px" class="details" ALIGN="LEFT" >' + ISNULL([index_name], N'&nbsp;') + N'</TD>' + 
													N'<TD WIDTH="100px" class="details" ALIGN="LEFT" >' + ISNULL([index_type], N'&nbsp;') + N'</TD>' + 
													N'<TD WIDTH= "80px" class="details" ALIGN="CENTER" >' + ISNULL(CAST([interval_days] AS [nvarchar](64)), N'&nbsp;') + N'</TD>' + 
													N'<TD WIDTH= "80px" class="details" ALIGN="RIGHT" >' + ISNULL(CAST([page_count] AS [nvarchar](64)), N'&nbsp;') + N'</TD>' + 
													N'<TD WIDTH= "90px" class="details" ALIGN="RIGHT" >' + ISNULL(CAST([fragmentation] AS [nvarchar](64)), N'&nbsp;') + N'</TD>' + 
													N'<TD WIDTH="100px" class="details" ALIGN="RIGHT" >' + ISNULL(CAST([page_density_deviation] AS [nvarchar](64)), N'&nbsp;') + N'</TD>' + 
													N'<TD WIDTH= "80px" class="details" ALIGN="RIGHT" >' + ISNULL(CAST([fill_factor] AS [nvarchar](64)), N'&nbsp;') + N'</TD>' + 
													N'<TD WIDTH="120px" class="details" ALIGN="LEFT">' + ISNULL([last_action_made], N'&nbsp;') + N'</TD>' + 
													N'</TR>' + 
													CASE WHEN [row_count] > [row_no]
														 THEN N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">'
														 ELSE N''
													END
											FROM (
													SELECT    [event_date_utc], [database_name], [object_name], [index_name]
															, [interval_days], [index_type], [fragmentation], [page_count], [fill_factor], [page_density_deviation], [last_action_made]
															, ROW_NUMBER() OVER(ORDER BY [database_name], [object_name], [index_name]) [row_no]
															, SUM(1) OVER() AS [row_count]
													FROM	#filteredStatsIndexesFrequentlyFragmented
													WHERE	[instance_name] =  @instanceName
												)X
											ORDER BY [database_name], [object_name], [index_name]
											FOR XML PATH(''), TYPE
											).value('.', 'nvarchar(max)'))

					SET @idx=@idx+1
					SET @HTMLReportArea = @HTMLReportArea + COALESCE(@tmpHTMLReport, '')

					FETCH NEXT FROM crsFrequentlyFragmentedIndexesMachineNames INTO  @instanceName, @indexesPerInstance

					IF @@FETCH_STATUS=0
						SET @HTMLReportArea = @HTMLReportArea + N'<TR VALIGN="TOP" class="color-2" HEIGHT="5px">
												<TD class="details" COLSPAN=11>&nbsp;</TD>
										</TR>'
				end
			CLOSE crsFrequentlyFragmentedIndexesMachineNames
			DEALLOCATE crsFrequentlyFragmentedIndexesMachineNames

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea						

			SET @HTMLReport = REPLACE(@HTMLReport, '{FrequentlyFragmentedIndexesIssuesDetectedCount}', '(' + CAST((@indexAnalyzedCount) AS [nvarchar]) + ')')			
		end
		
	
	-----------------------------------------------------------------------------------------------------
	--Outdated Backup for Databases - Issues Detected
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 2 = 2) AND (@flgOptions & 8192 = 8192)
		begin
			RAISERROR('	...Build Report: Outdated Backup for Databases - Issues Detected', 10, 1) WITH NOWAIT
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="DatabaseBACKUPAgeIssuesDetected" class="category-style">Outdated Backup for Databases - Issues Detected</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="7">backup age (system db) &gt; ' + CAST(@configSystemDatabaseBACKUPAgeDays AS [nvarchar](32)) + N' OR backup age (user db) &gt; ' + CAST(@configUserDatabaseBACKUPAgeDays AS [nvarchar](32)) + N'</TD>
							</TR>
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="120px" class="details-bold" nowrap>Machine Name</TH>
											<TH WIDTH="200px" class="details-bold" nowrap>Instance Name</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Clustered</TH>
											<TH WIDTH="360px" class="details-bold">Database Name</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>DB Size (MB)</TH>
											<TH WIDTH="150px" class="details-bold" nowrap>Last Backup Date</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>Backup Age (Days)</TH>'
			SET @idx=1		

			DECLARE crsDatabaseBACKUPAgeIssuesDetected CURSOR READ_ONLY LOCAL FOR	SELECT    cin.[machine_name], cin.[instance_name]
																							, cin.[is_clustered], cin.[cluster_node_machine_name]
																							, cdn.[database_name]
																							, shcdd.[size_mb]
																							, shcdd.[last_backup_time]
																							, DATEDIFF(dd, shcdd.[last_backup_time], GETDATE()) AS [backup_age_days]
																					FROM [dbo].[vw_catalogInstanceNames]  cin
																					INNER JOIN [dbo].[vw_catalogDatabaseNames]			  cdn	ON cdn.[project_id] = cin.[project_id] AND cdn.[instance_id] = cin.[instance_id]
																					INNER JOIN [health-check].[vw_statsDatabaseDetails] shcdd ON shcdd.[catalog_database_id] = cdn.[catalog_database_id] AND shcdd.[instance_id] = cdn.[instance_id]
																					LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
																																	AND rsr.[rule_id] = 8192
																																	AND rsr.[active] = 1
																																	AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																					WHERE cin.[instance_active]=1
																							AND cdn.[active]=1
																							AND cin.[project_id] = @projectID	
																							AND (
																									(    cdn.[database_name] NOT IN ('master', 'model', 'msdb') 
																										AND DATEDIFF(dd, shcdd.[last_backup_time], GETDATE()) > @configUserDatabaseBACKUPAgeDays
																									)
																								    OR (    cdn.[database_name] IN ('master', 'model', 'msdb') 
																										AND DATEDIFF(dd, shcdd.[last_backup_time], GETDATE()) > @configSystemDatabaseBACKUPAgeDays
																									)
																									OR (
																											cdn.[database_name] NOT IN ('tempdb')
																										AND shcdd.[last_backup_time] IS NULL
																									)
																								)
																							AND CHARINDEX(cdn.[state_desc], @configAdmittedState)<>0
																							AND rsr.[id] IS NULL
																					ORDER BY [instance_name], [machine_name], [backup_age_days] DESC, [database_name]
			OPEN crsDatabaseBACKUPAgeIssuesDetected
			FETCH NEXT FROM crsDatabaseBACKUPAgeIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @dbSize, @lastBackupDate, @lastDatabaseEventAgeDays
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap>' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName) END + N'</TD>' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap>' + @instanceName + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="CENTER" nowrap>' + CASE WHEN @isClustered=0 THEN N'No' ELSE N'Yes' + ISNULL(N'<BR>[' + @clusterNodeName + ']', N'&nbsp;') END + N'</TD>' + 
										N'<TD WIDTH="360px" class="details" ALIGN="LEFT">' + ISNULL(@databaseName, N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@dbSize AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
										N'<TD WIDTH="150px" class="details" ALIGN="CENTER" nowrap>' + ISNULL(CONVERT([nvarchar](24), @lastBackupDate, 121), N'N/A') + N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@lastDatabaseEventAgeDays AS [nvarchar](64)), N'N/A')+ N'</TD>' + 
									N'</TR>'
					SET @idx=@idx+1

					FETCH NEXT FROM crsDatabaseBACKUPAgeIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @dbSize, @lastBackupDate, @lastDatabaseEventAgeDays
				end
			CLOSE crsDatabaseBACKUPAgeIssuesDetected
			DEALLOCATE crsDatabaseBACKUPAgeIssuesDetected

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

			SET @HTMLReport = REPLACE(@HTMLReport, '{DatabaseBACKUPAgeIssuesDetectedCount}', '(' + CAST((@idx-1) AS [nvarchar]) + ')')
		end

		
	-----------------------------------------------------------------------------------------------------
	--Outdated DBCC CHECKDB Databases - Issues Detected
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 2 = 2) AND (@flgOptions & 16384 = 16384)
		begin
			RAISERROR('	...Build Report: Outdated DBCC CHECKDB Databases - Issues Detected', 10, 1) WITH NOWAIT
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="DatabaseDBCCCHECKDBAgeIssuesDetected" class="category-style">Outdated DBCC CHECKDB Databases - Issues Detected</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="7">dbcc checkdb age (system db) &gt; ' + CAST(@configSystemDBCCCHECKDBAgeDays AS [nvarchar](32)) + N' OR dbcc checkdb age (user db) &gt; ' + CAST(@configUserDBCCCHECKDBAgeDays AS [nvarchar](32)) + N'</TD>
							</TR>
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="120px" class="details-bold" nowrap>Machine Name</TH>
											<TH WIDTH="200px" class="details-bold" nowrap>Instance Name</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Clustered</TH>
											<TH WIDTH="360px" class="details-bold">Database Name</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>DB Size (MB)</TH>
											<TH WIDTH="150px" class="details-bold" nowrap>Last CHECKDB Date</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>CHECKDB Age (Days)</TH>'
			SET @idx=1		

			DECLARE crsDatabaseDBCCCHECKDBAgeIssuesDetected CURSOR READ_ONLY LOCAL FOR	SELECT    cin.[machine_name], cin.[instance_name]
																								, cin.[is_clustered], cin.[cluster_node_machine_name]
																								, cdn.[database_name]
																								, shcdd.[size_mb]
																								, shcdd.[last_dbcc checkdb_time]
																								, CASE	 WHEN shcdd.[last_dbcc checkdb_time] IS NOT NULL 
																										THEN DATEDIFF(dd, shcdd.[last_dbcc checkdb_time], GETDATE()) 
																										ELSE NULL
																									END AS [dbcc_checkdb_age_days]
																						FROM [dbo].[vw_catalogInstanceNames]  cin
																						INNER JOIN [dbo].[vw_catalogDatabaseNames]			  cdn	ON cdn.[project_id] = cin.[project_id] AND cdn.[instance_id] = cin.[instance_id]
																						INNER JOIN [health-check].[vw_statsDatabaseDetails] shcdd ON shcdd.[catalog_database_id] = cdn.[catalog_database_id] AND shcdd.[instance_id] = cdn.[instance_id]
																						LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
																																		AND rsr.[rule_id] = 16384
																																		AND rsr.[active] = 1
																																		AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																						WHERE cin.[instance_active]=1
																								AND cdn.[active]=1
																								AND cin.[project_id] = @projectID	
																								AND (
																										(    cdn.[database_name] NOT IN ('master', 'model', 'msdb') 
																											AND DATEDIFF(dd, shcdd.[last_dbcc checkdb_time], GETDATE()) > @configUserDBCCCHECKDBAgeDays
																										)
																										OR (    cdn.[database_name] IN ('master', 'model', 'msdb') 
																											AND DATEDIFF(dd, shcdd.[last_dbcc checkdb_time], GETDATE()) > @configSystemDBCCCHECKDBAgeDays
																										)
																										OR (
																												cdn.[database_name] NOT IN ('tempdb')
																											AND shcdd.[last_dbcc checkdb_time] IS NULL
																										)
																									)
																								AND CHARINDEX(cdn.[state_desc], 'ONLINE')<>0
																								AND cin.[version] NOT LIKE '8.%'
																								AND rsr.[id] IS NULL
																						ORDER BY [instance_name], [machine_name], [dbcc_checkdb_age_days] DESC, [database_name]
			OPEN crsDatabaseDBCCCHECKDBAgeIssuesDetected
			FETCH NEXT FROM crsDatabaseDBCCCHECKDBAgeIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @dbSize, @lastCheckDBDate, @lastDatabaseEventAgeDays
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap>' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName) END + N'</TD>' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap>' + @instanceName + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="CENTER" nowrap>' + CASE WHEN @isClustered=0 THEN N'No' ELSE N'Yes' + ISNULL(N'<BR>[' + @clusterNodeName + ']', N'&nbsp;') END + N'</TD>' + 
										N'<TD WIDTH="360px" class="details" ALIGN="LEFT">' + ISNULL(@databaseName, N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@dbSize AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
										N'<TD WIDTH="150px" class="details" ALIGN="CENTER" nowrap>' + ISNULL(CONVERT([nvarchar](24), @lastCheckDBDate, 121), N'N/A') + N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@lastDatabaseEventAgeDays AS [nvarchar](64)), N'N/A')+ N'</TD>' + 
									N'</TR>'
					SET @idx=@idx+1

					FETCH NEXT FROM crsDatabaseDBCCCHECKDBAgeIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @dbSize, @lastCheckDBDate, @lastDatabaseEventAgeDays
				end
			CLOSE crsDatabaseDBCCCHECKDBAgeIssuesDetected
			DEALLOCATE crsDatabaseDBCCCHECKDBAgeIssuesDetected

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

			SET @HTMLReport = REPLACE(@HTMLReport, '{DatabaseDBCCCHECKDBAgeIssuesDetectedCount}', '(' + CAST((@idx-1) AS [nvarchar]) + ')')
		end
		

	-----------------------------------------------------------------------------------------------------
	--Errorlog Messages - Permission Errors
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 16 = 16) AND (@flgOptions & 524288 = 524288)
		begin
			RAISERROR('	...Build Report: Errorlog Messages - Permission Errors', 10, 1) WITH NOWAIT
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="ErrorlogMessagesPermissionErrors" class="category-style">Errorlog Messages - Permission Errors</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="120px" class="details-bold" nowrap>Machine Name</TH>
											<TH WIDTH="200px" class="details-bold" nowrap>Instance Name</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Clustered</TH>
											<TH WIDTH="150px" class="details-bold" nowrap>Event Date (UTC)</TH>
											<TH WIDTH="540px" class="details-bold">Message</TH>'

			SET @idx=1		

			DECLARE crsErrorlogMessagesPermissionErrors CURSOR READ_ONLY LOCAL FOR	SELECT    cin.[machine_name], cin.[instance_name]
																							, cin.[is_clustered], cin.[cluster_node_machine_name]
																							, MAX(lsam.[event_date_utc]) [event_date_utc]
																							, lsam.[message]
																					FROM [dbo].[vw_catalogInstanceNames]  cin
																					INNER JOIN [dbo].[vw_logServerAnalysisMessages] lsam ON lsam.[project_id] = cin.[project_id] AND lsam.[instance_id] = cin.[instance_id]
																					LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
																																	AND rsr.[rule_id] = 524288
																																	AND rsr.[active] = 1
																																	AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																					WHERE	cin.[instance_active]=1
																							AND cin.[project_id] = @projectID
																							AND lsam.descriptor IN (N'dbo.usp_hcCollectErrorlogMessages')
																							AND rsr.[id] IS NULL
																					GROUP BY cin.[machine_name], cin.[instance_name], cin.[is_clustered], cin.[cluster_node_machine_name], lsam.[message]
																					ORDER BY cin.[instance_name], cin.[machine_name], [event_date_utc]
			OPEN crsErrorlogMessagesPermissionErrors
			FETCH NEXT FROM crsErrorlogMessagesPermissionErrors INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @eventDate, @message
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap>' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName) END + N'</TD>' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap>' + @instanceName + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="CENTER" nowrap>' + CASE WHEN @isClustered=0 THEN N'No' ELSE N'Yes<BR>' + ISNULL(N'[' + @clusterNodeName + ']', N'&nbsp;') END + N'</TD>' + 
										N'<TD WIDTH="150px" class="details" ALIGN="CENTER" nowrap>' + ISNULL(CONVERT([nvarchar](24), @eventDate, 121), N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="540px" class="details" ALIGN="LEFT">' + ISNULL([dbo].[ufn_reportHTMLPrepareText](@message, 0), N'&nbsp;') + N'</TD>' + 
									N'</TR>'
					SET @idx=@idx+1

					FETCH NEXT FROM crsErrorlogMessagesPermissionErrors INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @eventDate, @message
				end
			CLOSE crsErrorlogMessagesPermissionErrors
			DEALLOCATE crsErrorlogMessagesPermissionErrors

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

			SET @HTMLReport = REPLACE(@HTMLReport, '{ErrorlogMessagesPermissionErrorsCount}', '(' + CAST((@idx-1) AS [nvarchar]) + ')')
		end


	-----------------------------------------------------------------------------------------------------
	--Errorlog Messages - Issues Detected
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 16 = 16) AND (@flgOptions & 1048576 = 1048576)
		begin
			RAISERROR('	...Build Report: Errorlog Messages - Issues Detected', 10, 1) WITH NOWAIT
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="ErrorlogMessagesIssuesDetected" class="category-style">Errorlog Messages - Issues Detected (last ' + CAST(@configErrorlogMessageLastHours AS [nvarchar]) + N'h)</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="7">limit messages per instance to maximum ' + CAST(@configErrorlogMessageLimit AS [nvarchar](32)) + N' </TD>
							</TR>
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="200px" class="details-bold" nowrap>Instance Name</TH>
											<TH WIDTH="160px" class="details-bold" nowrap>Log Date</TH>
											<TH WIDTH= "60px" class="details-bold" nowrap>Process Info</TH>
											<TH WIDTH="710px" class="details-bold">Message</TH>'

			SET @idx=1		

			-----------------------------------------------------------------------------------------------------
			RAISERROR('		...analyzing errorlog messages', 10, 1) WITH NOWAIT

			IF OBJECT_ID('tempdb..#filteredStatsSQLServerErrorlogDetail') IS NOT NULL
				DROP TABLE #filteredStatsSQLServerErrorlogDetail

			SET @dateTimeLowerLimit = DATEADD(hh, -@configErrorlogMessageLastHours, GETDATE())

			SELECT DISTINCT 
					cin.[instance_name], 
					eld.[log_date], eld.[id], 
					eld.[process_info], eld.[text]
			INTO #filteredStatsSQLServerErrorlogDetail
			FROM [dbo].[vw_catalogInstanceNames]  cin
			INNER JOIN [health-check].[vw_statsSQLServerErrorlogDetails]	eld	ON eld.[project_id] = cin.[project_id] AND eld.[instance_id] = cin.[instance_id]
			LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
															AND rsr.[rule_id] = 1048576
															AND rsr.[active] = 1
															AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
			WHERE cin.[instance_active]=1
					AND cin.[project_id] = @projectID																							
					AND eld.[log_date] >= @dateTimeLowerLimit
					AND NOT EXISTS	( 
										SELECT 1
										FROM	[dbo].[catalogHardcodedFilters] chf 
										WHERE	chf.[module] = 'health-check'
												AND chf.[object_name] = 'dbo.statsSQLServerErrorlogDetails'
												AND chf.[active] = 1
												AND PATINDEX(chf.[filter_pattern], eld.[text]) > 0
									)
					AND rsr.[id] IS NULL
			
			CREATE INDEX IX_filteredStatsSQLServerErrorlogDetail_InstanceName ON #filteredStatsSQLServerErrorlogDetail([instance_name])

			RAISERROR('		...done', 10, 1) WITH NOWAIT

			-----------------------------------------------------------------------------------------------------
			SET @issuesDetectedCount = 0 
			DECLARE crsErrorlogMessagesInstanceName CURSOR READ_ONLY LOCAL FOR	SELECT DISTINCT
																						  [instance_name]
																						, COUNT(*) AS [messages_count]
																				FROM #filteredStatsSQLServerErrorlogDetail
																				GROUP BY [instance_name]
																				ORDER BY [instance_name]
			OPEN crsErrorlogMessagesInstanceName
			FETCH NEXT FROM crsErrorlogMessagesInstanceName INTO @instanceName, @messageCount
			WHILE @@FETCH_STATUS=0
				begin
					IF @messageCount > @configErrorlogMessageLimit SET @messageCount = @configErrorlogMessageLimit
					SET @issuesDetectedCount = @issuesDetectedCount + @messageCount

					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap ROWSPAN="' + CAST(@messageCount AS [nvarchar](64)) + '"><A NAME="ErrorlogMessagesCompleteDetails' + @instanceName + N'">' + @instanceName + N'</A></TD>' 

					SET @tmpHTMLReport=N''
					SELECT @tmpHTMLReport=((
											SELECT N'<TD WIDTH="160px" class="details" ALIGN="CENTER" nowrap>' + ISNULL(CONVERT([nvarchar](24), [log_date], 121), N'&nbsp;') + N'</TD>' + 
														N'<TD WIDTH="60px" class="details" ALIGN="LEFT">' + ISNULL([process_info], N'&nbsp;') + N'</TD>' + 
														N'<TD WIDTH="710px" class="details" ALIGN="LEFT">' + ISNULL([dbo].[ufn_reportHTMLPrepareText]([text], 0), N'&nbsp;')  + N'</TD>' + 
													N'</TR>' + 
													CASE WHEN [row_count] > [row_no]
														 THEN N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">'
														 ELSE N''
													END

											FROM (
													SELECT	TOP (@messageCount)
															[log_date], [id], 
															[process_info], [text],
															ROW_NUMBER() OVER(ORDER BY [log_date], [id]) [row_no],
															SUM(1) OVER() AS [row_count]
													FROM	#filteredStatsSQLServerErrorlogDetail													
													WHERE	[instance_name] = @instanceName
												)X
											ORDER BY [log_date], [id]
											FOR XML PATH(''), TYPE
											).value('.', 'nvarchar(max)'))

					SET @HTMLReportArea = @HTMLReportArea + COALESCE(@tmpHTMLReport, '')
					SET @idx=@idx + 1

					FETCH NEXT FROM crsErrorlogMessagesInstanceName INTO @instanceName, @messageCount
				end
			CLOSE crsErrorlogMessagesInstanceName
			DEALLOCATE crsErrorlogMessagesInstanceName

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

			SET @HTMLReport = REPLACE(@HTMLReport, '{ErrorlogMessagesIssuesDetectedCount}', '(' + CAST((@issuesDetectedCount) AS [nvarchar]) + ')')
		end

	
	-----------------------------------------------------------------------------------------------------
	--Databases Status - Complete Details
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 2 = 2) AND (@flgOptions & 8 = 8)
		begin
			RAISERROR('	...Build Report: Databases Status - Complete Details', 10, 1) WITH NOWAIT

			DECLARE   @dbCount		[int]
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="DatabasesStatusCompleteDetails" class="category-style">Databases Status - Complete Details</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="200px" class="details-bold">Instance Name</TH>
											<TH WIDTH="200px" class="details-bold">Database Name</TH>
											<TH WIDTH=" 80px" class="details-bold">Size (MB)</TH>
											<TH WIDTH=" 80px" class="details-bold">Data Size (MB)</TH>
											<TH WIDTH=" 60px" class="details-bold">Data Space Used (%)</TH>
											<TH WIDTH=" 80px" class="details-bold">Log Size (MB)</TH>
											<TH WIDTH=" 60px" class="details-bold">Log Space Used (%)</TH>
											<TH WIDTH="150px" class="details-bold">BACKUP Date</TH>
											<TH WIDTH="150px" class="details-bold">CHECKDB Date</TH>
											<TH WIDTH="150px" class="details-bold">State</TH>
											'

			SET @idx=1		
			
			DECLARE crsDatabasesStatusMachineNames CURSOR READ_ONLY LOCAL FOR		SELECT    cin.[machine_name], cin.[instance_name]
																							, COUNT(*) AS [database_count]
																					FROM [dbo].[vw_catalogInstanceNames]  cin
																					INNER JOIN [dbo].[vw_catalogDatabaseNames] cdn ON cdn.[project_id] = cin.[project_id] AND cdn.[instance_id] = cin.[instance_id]
																					LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
																																	AND rsr.[rule_id] = 8
																																	AND rsr.[active] = 1
																																	AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])

																					WHERE cin.[instance_active]=1
																							AND cdn.[active]=1
																							AND cin.[project_id] = @projectID	
																							AND rsr.[id] IS NULL
																					GROUP BY cin.[machine_name], cin.[instance_name]
																							, cin.[is_clustered], cin.[cluster_node_machine_name]
																					ORDER BY cin.[instance_name], cin.[machine_name]
			OPEN crsDatabasesStatusMachineNames
			FETCH NEXT FROM crsDatabasesStatusMachineNames INTO  @machineName, @instanceName, @dbCount
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" ROWSPAN="' + CAST(@dbCount AS [nvarchar](64)) + N'"><A NAME="DatabasesStatusCompleteDetails' + @instanceName + N'">' + @instanceName + N'</A></TD>' 

					SET @tmpHTMLReport=N''
					SELECT @tmpHTMLReport=((
											SELECT N'<TD WIDTH="200px" class="details" ALIGN="LEFT">' + ISNULL([database_name], N'&nbsp;') + N'</TD>' + 
													N'<TD WIDTH= "80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST([size_mb] AS [nvarchar](64)), N'&nbsp;') + N'</TD>' + 
													N'<TD WIDTH= "80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST([data_size_mb] AS [nvarchar](64)), N'&nbsp;') + N'</TD>' + 
													N'<TD WIDTH= "60px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST([data_space_used_percent] AS [nvarchar](64)), N'&nbsp;') + N'</TD>' + 
													N'<TD WIDTH= "80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST([log_size_mb] AS [nvarchar](64)), N'&nbsp;') + N'</TD>' + 
													N'<TD WIDTH= "60px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST([data_space_used_percent] AS [nvarchar](64)), N'&nbsp;') + N'</TD>' + 
													N'<TD WIDTH="150px" class="details" ALIGN="CENTER" nowrap>' + ISNULL(CONVERT([nvarchar](24), [last_backup_time], 121), N'N/A') + N'</TD>' + 
													N'<TD WIDTH="150px" class="details" ALIGN="CENTER" nowrap>' + ISNULL(CONVERT([nvarchar](24), [last_dbcc checkdb_time], 121), N'N/A') + N'</TD>' + 
													N'<TD WIDTH="150px" class="details" ALIGN="LEFT">' + ISNULL([state_desc], N'&nbsp;') + N'</TD>' + 
													N'</TR>' + 
													CASE WHEN [row_count] > [row_no]
														 THEN N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">'
														 ELSE N''
													END
											FROM (
													SELECT    cdn.[database_name], cdn.[state_desc]
															, shcdd.[size_mb]
															, shcdd.[data_size_mb], shcdd.[data_space_used_percent]
															, shcdd.[log_size_mb], shcdd.[log_space_used_percent] 
															, shcdd.[last_backup_time], shcdd.[last_dbcc checkdb_time]
															, ROW_NUMBER() OVER(ORDER BY cdn.[database_name]) [row_no]
															, SUM(1) OVER() AS [row_count]
													FROM [dbo].[vw_catalogInstanceNames] cin
													INNER JOIN [dbo].[vw_catalogDatabaseNames]			  cdn	ON cdn.[project_id] = cin.[project_id] AND cdn.[instance_id] = cin.[instance_id]
													LEFT  JOIN [health-check].[vw_statsDatabaseDetails] shcdd ON shcdd.[catalog_database_id] = cdn.[catalog_database_id] AND shcdd.[instance_id] = cdn.[instance_id]
													WHERE	cin.[instance_active]=1
															AND cdn.[active]=1
															AND cin.[project_id] = @projectID	
															AND cin.[instance_name] =  @instanceName
															AND cin.[machine_name] = @machineName
												)X
											ORDER BY [database_name]
											FOR XML PATH(''), TYPE
											).value('.', 'nvarchar(max)'))

					SET @HTMLReportArea = @HTMLReportArea + COALESCE(@tmpHTMLReport, '')
					SET @idx=@idx+1

					FETCH NEXT FROM crsDatabasesStatusMachineNames INTO @machineName, @instanceName, @dbCount

					IF @@FETCH_STATUS=0
						SET @HTMLReportArea = @HTMLReportArea + N'<TR VALIGN="TOP" class="color-2" HEIGHT="5px">
												<TD class="details" COLSPAN=10>&nbsp;</TD>
										</TR>'
				end
			CLOSE crsDatabasesStatusMachineNames
			DEALLOCATE crsDatabasesStatusMachineNames

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					
		end


	-----------------------------------------------------------------------------------------------------
	--SQL Server Agent Jobs Status - Complete Details
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 4 = 4) AND (@flgOptions & 32 = 32)
		begin
			RAISERROR('	...Build Report: SQL Server Agent Jobs Status - Complete Details', 10, 1) WITH NOWAIT
			
			DECLARE @jobCount [int]

			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="SQLServerAgentJobsStatusCompleteDetails" class="category-style">SQL Server Agent Jobs Status - Complete Details</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="200px" class="details-bold" nowrap>Instance Name</TH>
											<TH WIDTH="200px" class="details-bold">Job Name</TH>
											<TH WIDTH= "80px" class="details-bold" nowrap>Execution Status</TH>
											<TH WIDTH= "80px" class="details-bold" nowrap>Execution Date</TH>
											<TH WIDTH= "80px" class="details-bold" nowrap>Execution Time</TH>
											<TH WIDTH="490px" class="details-bold">Message</TH>'

			SET @idx=1		
			
			DECLARE crsSQLServerAgentJobsInstanceName CURSOR READ_ONLY LOCAL FOR	SELECT	ssajh.[instance_name], COUNT(*) AS [job_count]
																					FROM	[health-check].[vw_statsSQLServerAgentJobsHistory] ssajh
																					LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
																																	AND rsr.[rule_id] = 32
																																	AND rsr.[active] = 1
																																	AND (rsr.[skip_value]=ssajh.[instance_name])
																					WHERE	ssajh.[project_id]=@projectID
																							AND rsr.[id] IS NULL
																					GROUP BY ssajh.[instance_name]
																					ORDER BY ssajh.[instance_name]
			OPEN crsSQLServerAgentJobsInstanceName
			FETCH NEXT FROM crsSQLServerAgentJobsInstanceName INTO @instanceName, @jobCount
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap ROWSPAN="' + CAST(@jobCount AS [nvarchar](64)) + '"><A NAME="SQLServerAgentJobsStatusCompleteDetails' + @instanceName + N'">' + @instanceName + N'</A></TD>' 

					SET @tmpHTMLReport=N''
					SELECT @tmpHTMLReport=((
											SELECT	N'<TD WIDTH="200px" class="details" ALIGN="LEFT">' + [job_name] + N'</TD>' + 
													N'<TD WIDTH="80px" class="details" ALIGN="CENTER" nowrap>' + CASE WHEN [last_execution_status] = 0 THEN N'Failed'
																														WHEN [last_execution_status] = 1 THEN N'Succeded'
																														WHEN [last_execution_status] = 2 THEN N'Retry'
																														WHEN [last_execution_status] = 3 THEN N'Canceled'
																														WHEN [last_execution_status] = 4 THEN N'In progress'
																														ELSE N'&nbsp;'
																													END
																+ N'</TD>' + 
													N'<TD WIDTH= "80px" class="details" ALIGN="CENTER" nowrap>' + isnull([last_execution_date], N'&nbsp;') + N'</TD>' + 
													N'<TD WIDTH= "80px" class="details" ALIGN="CENTER" nowrap>' + isnull([last_execution_time], N'&nbsp;') + N'</TD>' + 
													N'<TD WIDTH="490px" class="details" ALIGN="LEFT">' + REPLACE(REPLACE(REPLACE(ISNULL([dbo].[ufn_reportHTMLPrepareText](CASE WHEN LEFT([message], 2) = '--' THEN SUBSTRING([message], 3, LEN([message])) ELSE [message] END, 0), N'&nbsp;') , CHAR(13), N'<BR>'), '--', N'<BR>'), N'<BR><BR>', N'<BR>') + N'</TD>' + 
													N'</TR>' + 
													CASE WHEN [row_count] > [row_no]
														 THEN N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">'
														 ELSE N''
													END

											FROM (
													SELECT	[job_name], [last_execution_status], [last_execution_date], [last_execution_time], [message]
															, ROW_NUMBER() OVER(ORDER BY [job_name]) [row_no]
															, SUM(1) OVER() AS [row_count]
													FROM	[health-check].[vw_statsSQLServerAgentJobsHistory]
													WHERE	[project_id]=@projectID
															AND [instance_name] = @instanceName
												)X
											ORDER BY [job_name]
											FOR XML PATH(''), TYPE
											).value('.', 'nvarchar(max)'))

					SET @idx=@idx+1
					SET @HTMLReportArea = @HTMLReportArea + COALESCE(@tmpHTMLReport, '')

					FETCH NEXT FROM crsSQLServerAgentJobsInstanceName INTO @instanceName, @jobCount

					IF @@FETCH_STATUS=0
						SET @HTMLReportArea = @HTMLReportArea + N'<TR VALIGN="TOP" class="color-2" HEIGHT="5px">
																	<TD class="details" COLSPAN=6>&nbsp;</TD>
															</TR>'
				end
			CLOSE crsSQLServerAgentJobsInstanceName
			DEALLOCATE crsSQLServerAgentJobsInstanceName

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					
		end


	-----------------------------------------------------------------------------------------------------
	--Disk Space Information - Complete Details
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 8 = 8) AND (@flgOptions & 65536 = 65536)
		begin
			RAISERROR('	...Build Report: Disk Space Information - Complete Details', 10, 1) WITH NOWAIT

			DECLARE   @volumeCount		[int]
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="DiskSpaceInformationCompleteDetails" class="category-style">Disk Space Information - Complete Details</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="300px" class="details-bold" nowrap>Machine Name</TH>
											<TH WIDTH="100px" class="details-bold">Logical Drive</TH>
											<TH WIDTH="370px" class="details-bold">Volume Mount Point</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Total Size (MB)</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Available Space (MB)</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Percent Available (%)</TH>'

			SET @idx=1		

			DECLARE crsDiskSpaceInformationMachineNames CURSOR READ_ONLY LOCAL FOR		SELECT DISTINCT
																								  cin.[machine_name]/*, cin.[instance_name]*/
																								, cin.[is_clustered], cin.[cluster_node_machine_name]
																								, COUNT(*) AS [volume_count]
																						FROM [dbo].[vw_catalogInstanceNames]  cin
																						INNER JOIN [health-check].[vw_statsDiskSpaceInfo]		dsi	ON dsi.[project_id] = cin.[project_id] AND dsi.[instance_id] = cin.[instance_id]
																						LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
																																		AND rsr.[rule_id] = 65536
																																		AND rsr.[active] = 1
																																		AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																						WHERE cin.[instance_active]=1
																								AND cin.[project_id] = @projectID
																								AND rsr.[id] IS NULL	
																						GROUP BY cin.[machine_name], cin.[instance_name], cin.[is_clustered], cin.[cluster_node_machine_name]
																						ORDER BY cin.[machine_name]/*, cin.[instance_name]*/
			OPEN crsDiskSpaceInformationMachineNames
			FETCH NEXT FROM crsDiskSpaceInformationMachineNames INTO  @machineName, /*@instanceName, */@isClustered, @clusterNodeName, @volumeCount
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" ROWSPAN="' + CAST(@volumeCount AS [nvarchar](64)) + N'"><A NAME="DiskSpaceInformationCompleteDetails' + CASE WHEN @isClustered=0 THEN @machineName ELSE @clusterNodeName END + N'">' + CASE WHEN @isClustered=0 THEN @machineName ELSE @clusterNodeName END + N'</A></TD>'

					SET @tmpHTMLReport=N''
					SELECT @tmpHTMLReport=((
											SELECT	N'<TD WIDTH="100px" class="details" ALIGN="CENTER">' + ISNULL([logical_drive], N'&nbsp;') + N'</TD>' + 
													N'<TD WIDTH="270px" class="details" ALIGN="LEFT">' + ISNULL([volume_mount_point], N'&nbsp;') + N'</TD>' + 
													N'<TD WIDTH="120px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST([total_size_mb] AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
													N'<TD WIDTH="120px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST([available_space_mb] AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
													N'<TD WIDTH="120px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST([percent_available] AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
													N'</TR>' + 
													CASE WHEN [row_count] > [row_no]
														 THEN N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">'
														 ELSE N''
													END

											FROM (
													SELECT  DISTINCT
																  dsi.[logical_drive]
																, dsi.[volume_mount_point]
																, MAX(dsi.[total_size_mb])		AS [total_size_mb]
																, MIN(dsi.[available_space_mb]) AS [available_space_mb]
																, MIN(dsi.[percent_available])	AS [percent_available]
																, ROW_NUMBER() OVER(ORDER BY dsi.[logical_drive], dsi.[volume_mount_point]) [row_no]
																, SUM(1) OVER() AS [row_count]
													FROM [dbo].[vw_catalogInstanceNames] cin
													INNER JOIN [health-check].[vw_statsDiskSpaceInfo]		dsi	ON dsi.[project_id] = cin.[project_id] AND dsi.[instance_id] = cin.[instance_id]
													WHERE	cin.[instance_active]=1
															AND cin.[project_id] = @projectID	
															/*AND cin.[instance_name] =  @instanceName*/
															AND cin.[machine_name] = @machineName
													GROUP BY dsi.[logical_drive], dsi.[volume_mount_point]
												)X
											ORDER BY [logical_drive], [volume_mount_point]
											FOR XML PATH(''), TYPE
											).value('.', 'nvarchar(max)'))

					SET @HTMLReportArea = @HTMLReportArea + COALESCE(@tmpHTMLReport, '')
					SET @idx=@idx + 1

					FETCH NEXT FROM crsDiskSpaceInformationMachineNames INTO @machineName, /*@instanceName, */@isClustered, @clusterNodeName, @volumeCount

					IF @@FETCH_STATUS=0
						SET @HTMLReportArea = @HTMLReportArea + N'<TR VALIGN="TOP" class="color-2" HEIGHT="5px">
																	<TD class="details" COLSPAN=6>&nbsp;</TD>
															</TR>'
				end
			CLOSE crsDiskSpaceInformationMachineNames
			DEALLOCATE crsDiskSpaceInformationMachineNames

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					
		end


	-----------------------------------------------------------------------------------------------------
	--Errorlog Messages - Complete Details
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 16 = 16) AND (@flgOptions & 2097152 = 2097152)
		begin
			RAISERROR('	...Build Report: Errorlog Messages - Complete Details', 10, 1) WITH NOWAIT

			SET @idx=1		
			
			SET @HTMLReportArea = N''
			SET @HTMLReportArea = @HTMLReportArea + 
					N'<A NAME="ErrorlogMessagesCompleteDetails" class="category-style">Errorlog Messages - Complete Details (last ' + CAST(@configErrorlogMessageLastHours AS [nvarchar]) + N'h)</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="200px" class="details-bold" nowrap>Instance Name</TH>
											<TH WIDTH="160px" class="details-bold" nowrap>Log Date</TH>
											<TH WIDTH= "60px" class="details-bold" nowrap>Process Info</TH>
											<TH WIDTH="710px" class="details-bold">Message</TH>'

			SET @dateTimeLowerLimit = DATEADD(hh, -@configErrorlogMessageLastHours, GETDATE())
			
			DECLARE crsErrorlogMessagesInstanceName CURSOR READ_ONLY LOCAL FOR	SELECT DISTINCT
																						  cin.[instance_name]
																						, COUNT(*) AS [messages_count]
																				FROM [dbo].[vw_catalogInstanceNames]  cin
																				INNER JOIN [health-check].[vw_statsSQLServerErrorlogDetails]	eld	ON eld.[project_id] = cin.[project_id] AND eld.[instance_id] = cin.[instance_id]
																				LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
																																AND rsr.[rule_id] = 2097152
																																AND rsr.[active] = 1
																																AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																				WHERE cin.[instance_active]=1
																						AND cin.[project_id] = @projectID	
																						AND eld.[log_date] >= @dateTimeLowerLimit
																						AND rsr.[id] IS NULL
																				GROUP BY cin.[instance_name]
																				ORDER BY cin.[instance_name]
			OPEN crsErrorlogMessagesInstanceName
			FETCH NEXT FROM crsErrorlogMessagesInstanceName INTO @instanceName, @messageCount
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap ROWSPAN="' + CAST(@messageCount AS [nvarchar](64)) + '"><A NAME="ErrorlogMessagesCompleteDetails' + @instanceName + N'">' + @instanceName + N'</A></TD>' 

					SET @tmpHTMLReport=N''
					SELECT @tmpHTMLReport=((
											SELECT	N'<TD WIDTH="160px" class="details" ALIGN="CENTER" nowrap>' + ISNULL(CONVERT([nvarchar](24), [log_date], 121), N'&nbsp;') + N'</TD>' + 
														N'<TD WIDTH="60px" class="details" ALIGN="LEFT">' + ISNULL([process_info], N'&nbsp;') + N'</TD>' + 
														N'<TD WIDTH="710px" class="details" ALIGN="LEFT">' + ISNULL([dbo].[ufn_reportHTMLPrepareText]([text], 0), N'&nbsp;') + N'</TD>' + 
													N'</TR>' + 
													CASE WHEN [row_count] > [row_no]
														 THEN N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">'
														 ELSE N''
													END

											FROM (
													SELECT	eld.[log_date], eld.[id], eld.[process_info], eld.[text]
															, ROW_NUMBER() OVER(ORDER BY eld.[log_date], eld.[id]) [row_no]
															, SUM(1) OVER() AS [row_count]
													FROM	[health-check].[vw_statsSQLServerErrorlogDetails] eld
													WHERE	eld.[project_id]=@projectID
															AND eld.[instance_name] = @instanceName
															AND eld.[log_date] >= @dateTimeLowerLimit
												)X
											ORDER BY [log_date], [id]
											FOR XML PATH(''), TYPE
											).value('.', 'nvarchar(max)'))

					SET @HTMLReportArea = @HTMLReportArea + COALESCE(@tmpHTMLReport, '')
					SET @idx=@idx + 1

					FETCH NEXT FROM crsErrorlogMessagesInstanceName INTO @instanceName, @messageCount

					IF @@FETCH_STATUS=0
						SET @HTMLReportArea = @HTMLReportArea + N'<TR VALIGN="TOP" class="color-2" HEIGHT="5px">
											<TD class="details" COLSPAN=4>&nbsp;</TD>
									</TR>'
				end
			CLOSE crsErrorlogMessagesInstanceName
			DEALLOCATE crsErrorlogMessagesInstanceName

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea						
		end


	-----------------------------------------------------------------------------------------------------
	--OS Event Messages - Permission Errors
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 32 = 32) AND (@flgOptions & 67108864 = 67108864)
		begin
			RAISERROR('	...Build Report: OS Event Messages - Permission Errors', 10, 1) WITH NOWAIT
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="OSEventMessagesPermissionErrors" class="category-style">OS Event Messages - Permission Errors</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="7">powershell script timeout value = ' + CAST(@configOSEventsTimeOutSeconds AS [nvarchar](32)) + N' seconds </TD>
							</TR>
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="120px" class="details-bold" nowrap>Machine Name</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Clustered</TH>
											<TH WIDTH="150px" class="details-bold" nowrap>Event Date (UTC)</TH>
											<TH WIDTH="740px" class="details-bold">Message</TH>'

			SET @idx=1		

			DECLARE crsOSEventMessagesPermissionErrors CURSOR READ_ONLY LOCAL FOR	SELECT    cin.[machine_name], cin.[is_clustered], cin.[cluster_node_machine_name]
																							, MAX(lsam.[event_date_utc]) [event_date_utc]
																							, lsam.[message]
																					FROM [dbo].[vw_catalogInstanceNames]  cin
																					INNER JOIN [dbo].[vw_logServerAnalysisMessages] lsam ON lsam.[project_id] = cin.[project_id] AND lsam.[instance_id] = cin.[instance_id]
																					LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
																																	AND rsr.[rule_id] = 67108864
																																	AND rsr.[active] = 1
																																	AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																					WHERE	cin.[instance_active]=1
																							AND cin.[project_id] = @projectID
																							AND lsam.descriptor IN (N'dbo.usp_hcCollectOSEventLogs')
																							AND rsr.[id] IS NULL
																					GROUP BY cin.[machine_name], cin.[is_clustered], cin.[cluster_node_machine_name], lsam.[message]
																					ORDER BY cin.[machine_name], [event_date_utc]
			OPEN crsOSEventMessagesPermissionErrors
			FETCH NEXT FROM crsOSEventMessagesPermissionErrors INTO @machineName, @isClustered, @clusterNodeName, @eventDate, @message
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap>' + @machineName + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="CENTER" nowrap>' + CASE WHEN @isClustered=0 THEN N'No' ELSE N'Yes<BR>' + ISNULL(N'[' + @clusterNodeName + ']', N'&nbsp;') END + N'</TD>' + 
										N'<TD WIDTH="150px" class="details" ALIGN="CENTER" nowrap>' + ISNULL(CONVERT([nvarchar](24), @eventDate, 121), N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="740px" class="details" ALIGN="LEFT">' + ISNULL([dbo].[ufn_reportHTMLPrepareText](@message, 0), N'&nbsp;') + N'</TD>' + 
									N'</TR>'
					SET @idx=@idx+1

					FETCH NEXT FROM crsOSEventMessagesPermissionErrors INTO @machineName, @isClustered, @clusterNodeName, @eventDate, @message
				end
			CLOSE crsOSEventMessagesPermissionErrors
			DEALLOCATE crsOSEventMessagesPermissionErrors

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

			SET @HTMLReport = REPLACE(@HTMLReport, '{OSEventMessagesPermissionErrorsCount}', '(' + CAST((@idx-1) AS [nvarchar]) + ')')
		end


	-----------------------------------------------------------------------------------------------------
	--OS Event messages - Complete Details
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 32 = 32) AND (@flgOptions & 134217728 = 134217728)
		begin
			RAISERROR('	...Build Report: OS Event messages - Complete Details', 10, 1) WITH NOWAIT
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="OSEventMessagesCompleteDetails" class="category-style">OS Event messages - Complete Details (last ' + CAST(@configOSEventMessageLastHours AS [nvarchar]) + N'h)</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="7">limit messages per machine to maximum ' + CAST(@configOSEventMessageLimit AS [nvarchar](32)) + N' </TD>
							</TR>
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="7">Severity: Critical, Error' + CASE WHEN @configOSEventGetWarningsEvent=1 THEN N', Warning' ELSE N'' END + CASE WHEN @configOSEventGetInformationEvent=1 THEN N', Information' ELSE N'' END + N' </TD>
							</TR>
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="200px" class="details-bold" nowrap>Machine Name</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Event Time</TH>
											<TH WIDTH=" 80px" class="details-bold" nowrap>Log Name</TH>
											<TH WIDTH=" 60px" class="details-bold" nowrap>Level</TH>
											<TH WIDTH=" 60px" class="details-bold" nowrap>Event ID</TH>
											<TH WIDTH="120px" class="details-bold">Source</TH>
											<TH WIDTH="480px" class="details-bold">Message</TH>'
			SET @idx=1		

			SET @dateTimeLowerLimit = DATEADD(hh, -@configOSEventMessageLastHours, GETDATE())
			SET @issuesDetectedCount = 0 
			
			DECLARE crsOSEventMessagesInstanceName CURSOR READ_ONLY LOCAL FOR	SELECT DISTINCT
																						  oel.[machine_name]
																						, COUNT(*) AS [messages_count]
																				FROM [dbo].[vw_catalogInstanceNames]	cin
																				INNER JOIN [health-check].[vw_statsOSEventLogs]	oel	ON oel.[project_id] = cin.[project_id] AND oel.[instance_id] = cin.[instance_id]
																				LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
																																AND rsr.[rule_id] = 134217728
																																AND rsr.[active] = 1
																																AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																				WHERE cin.[instance_active]=1
																						AND cin.[project_id] = @projectID
																						AND rsr.[id] IS NULL
																				GROUP BY oel.[machine_name]
																				ORDER BY oel.[machine_name]
			OPEN crsOSEventMessagesInstanceName
			FETCH NEXT FROM crsOSEventMessagesInstanceName INTO @machineName, @messageCount
			WHILE @@FETCH_STATUS=0
				begin
					IF @messageCount > @configOSEventMessageLimit SET @messageCount = @configOSEventMessageLimit
					SET @issuesDetectedCount = @issuesDetectedCount + @messageCount

					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap ROWSPAN="' + CAST(@messageCount AS [nvarchar](64)) + '"><A NAME="OSEventMessagesCompleteDetails' + @machineName + N'">' + @machineName + N'</A></TD>' 

					SET @tmpHTMLReport=N''
					SELECT @tmpHTMLReport=((
											SELECT	N'<TD WIDTH="120px" class="details" ALIGN="CENTER" nowrap>' + ISNULL(CONVERT([nvarchar](24), [time_created], 121), N'&nbsp;') + N'</TD>' + 
														N'<TD WIDTH=" 80px" class="details" ALIGN="LEFT" >' + ISNULL([log_type_desc], N'&nbsp;') + N'</TD>' + 
														N'<TD WIDTH=" 60px" class="details" ALIGN="LEFT">' + ISNULL([level_desc], N'&nbsp;') + N'</TD>' + 
														N'<TD WIDTH=" 60px" class="details" ALIGN="LEFT">' + ISNULL(CAST([event_id] AS [nvarchar]), N'&nbsp;') + N'</TD>' + 
														N'<TD WIDTH="120px" class="details" ALIGN="LEFT">' + ISNULL([source], N'&nbsp;') + N'</TD>' + 
														N'<TD WIDTH="480px" class="details" ALIGN="LEFT">' + ISNULL([dbo].[ufn_reportHTMLPrepareText]([message], 0), N'&nbsp;')  + N'</TD>' + 
													N'</TR>' + 
													CASE WHEN [row_count] > [row_no]
														 THEN N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">'
														 ELSE N''
													END

											FROM (
													SELECT  TOP (@configOSEventMessageLimit)
															oel.[time_created], oel.[log_type_desc], oel.[level_desc], 
															oel.[event_id], oel.[record_id], oel.[source], oel.[message]
															, ROW_NUMBER() OVER(ORDER BY oel.[time_created], oel.[record_id]) [row_no]
															, SUM(1) OVER() AS [row_count]
													FROM [health-check].[vw_statsOSEventLogs]	oel
													WHERE	oel.[project_id]=@projectID
															AND oel.[machine_name] = @machineName
												)X
											ORDER BY [time_created], [record_id]
											FOR XML PATH(''), TYPE
											).value('.', 'nvarchar(max)'))

					SET @HTMLReportArea = @HTMLReportArea + COALESCE(@tmpHTMLReport, '')
					SET @idx=@idx + 1
				
					FETCH NEXT FROM crsOSEventMessagesInstanceName INTO @machineName, @messageCount
				end
			CLOSE crsOSEventMessagesInstanceName
			DEALLOCATE crsOSEventMessagesInstanceName

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea						

			SET @HTMLReport = REPLACE(@HTMLReport, '{OSEventMessagesIssuesDetectedCount}', '(' + CAST((@issuesDetectedCount) AS [nvarchar]) + ')')
		end


	-----------------------------------------------------------------------------------------------------
	SET @HTMLReport = @HTMLReport + N'</body></html>'	
	
	-----------------------------------------------------------------------------------------------------
	--save report entry
	-----------------------------------------------------------------------------------------------------
	INSERT INTO [dbo].[reportHTML](   [project_id], [module], [start_date], [flg_actions], [flg_options]
									, [file_name], [file_path]
									, [build_at], [build_duration], [html_content], [build_in_progress], [report_uid])												

			SELECT    @projectID, 'health-check', @reportBuildStartTime, @flgActions, @flgOptions
					, @HTMLReportFileName, @localStoragePath
					, @reportBuildStartTime, DATEDIFF(ms, @reportBuildStartTime, GETUTCDATE()), @HTMLReport
					, 0, NEWID()

		
	-----------------------------------------------------------------------------------------------------
	--save HTML report to external file
	-----------------------------------------------------------------------------------------------------
	SET @reportID=SCOPE_IDENTITY()

	IF @reportFileName IS NOT NULL AND LEFT(@reportFileName, 1) = '+'
		SET @HTMLReportFileName = REPLACE(REPLACE(@HTMLReportFileName, '.html', ''), '.htm', '') + '_' + CAST(@reportID AS [nvarchar]) + SUBSTRING(@reportFileName, 2, LEN(@reportFileName)-1) + '.html'
	ELSE
		SET @HTMLReportFileName = REPLACE(REPLACE(@HTMLReportFileName, '.html', ''), '.htm', '') + '_' + CAST(@reportID AS [nvarchar]) + '.html'

			
	SET @reportFilePath='"' + @localStoragePath + @HTMLReportFileName + '"'
	

	-----------------------------------------------------------------------------------------------------
	DECLARE @optionXPIsAvailable		[bit],
			@optionXPValue				[int],
			@optionXPHasChanged			[bit],
			@optionAdvancedIsAvailable	[bit],
			@optionAdvancedValue		[int],
			@optionAdvancedHasChanged	[bit]

	SELECT  @optionXPIsAvailable		= 0,
			@optionXPValue				= 0,
			@optionXPHasChanged			= 0,
			@optionAdvancedIsAvailable	= 0,
			@optionAdvancedValue		= 0,
			@optionAdvancedHasChanged	= 0

	/* enable xp_cmdshell configuration option */
	EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @@SERVERNAME,
														@configOptionName	= 'xp_cmdshell',
														@configOptionValue	= 1,
														@optionIsAvailable	= @optionXPIsAvailable OUT,
														@optionCurrentValue	= @optionXPValue OUT,
														@optionHasChanged	= @optionXPHasChanged OUT,
														@executionLevel		= 0,
														@debugMode			= 0

	IF @optionXPIsAvailable = 0
		begin
			/* enable show advanced options configuration option */
			EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @@SERVERNAME,
																@configOptionName	= 'show advanced options',
																@configOptionValue	= 1,
																@optionIsAvailable	= @optionAdvancedIsAvailable OUT,
																@optionCurrentValue	= @optionAdvancedValue OUT,
																@optionHasChanged	= @optionAdvancedHasChanged OUT,
																@executionLevel		= 0,
																@debugMode			= 0

			IF @optionAdvancedIsAvailable = 1 AND (@optionAdvancedValue=1 OR @optionAdvancedHasChanged=1)
				EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @@SERVERNAME,
																	@configOptionName	= 'xp_cmdshell',
																	@configOptionValue	= 1,
																	@optionIsAvailable	= @optionXPIsAvailable OUT,
																	@optionCurrentValue	= @optionXPValue OUT,
																	@optionHasChanged	= @optionXPHasChanged OUT,
																	@executionLevel		= 0,
																	@debugMode			= 0
		end

	/* save report using bcp */	
	SET @queryToRun=N'master.dbo.xp_cmdshell ''bcp "SELECT [html_content] FROM [' + DB_NAME() + '].[dbo].[reportHTML] WHERE [id]=' + CAST(@reportID AS [varchar]) + '" queryout ' + @reportFilePath + ' -c ' + CASE WHEN SERVERPROPERTY('InstanceName') IS NOT NULL THEN N'-S ' + @@SERVERNAME ELSE N'' END + N' -T'''
	EXEC (@queryToRun)
	
	/* disable xp_cmdshell configuration option */
	IF @optionXPHasChanged = 1
		EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @@SERVERNAME,
															@configOptionName	= 'xp_cmdshell',
															@configOptionValue	= 0,
															@optionIsAvailable	= @optionXPIsAvailable OUT,
															@optionCurrentValue	= @optionXPValue OUT,
															@optionHasChanged	= @optionXPHasChanged OUT,
															@executionLevel		= 0,
															@debugMode			= 0

	/* disable show advanced options configuration option */
	IF @optionAdvancedHasChanged = 1
			EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @@SERVERNAME,
																@configOptionName	= 'show advanced options',
																@configOptionValue	= 0,
																@optionIsAvailable	= @optionAdvancedIsAvailable OUT,
																@optionCurrentValue	= @optionAdvancedValue OUT,
																@optionHasChanged	= @optionAdvancedHasChanged OUT,
																@executionLevel		= 0,
																@debugMode			= 0


	IF @@ERROR=0
		UPDATE [dbo].[reportHTML]
			SET   [html_content] = NULL
				, [file_name]	 = @HTMLReportFileName
		WHERE [id] = @reportID
		
	-----------------------------------------------------------------------------------------------------
	--
	-----------------------------------------------------------------------------------------------------
	IF @recipientsList = ''		SET @recipientsList = NULL
	IF @dbMailProfileName = ''	SET @dbMailProfileName = NULL

	DECLARE	@HTTPAddress [nvarchar](128)
	
	--get configuration values
	SELECT	@HTTPAddress=[value] 
	FROM	[dbo].[appConfigurations] 
	WHERE	[name]='HTTP address for report files'
			AND [module] = 'common'

	
	-----------------------------------------------------------------------------------------------------
	--
	-----------------------------------------------------------------------------------------------------
	IF @HTTPAddress IS NOT NULL				
		begin		
			UPDATE [dbo].[reportHTML]
				SET   [http_address] = @HTTPAddress + @relativeStoragePath + @HTMLReportFileName
			WHERE [id] = @reportID
		end

	SELECT @eventMessageData='<report-html><detail>' + 
								'<message>Health Check report is attached.</message>' + 
								'<file_name>' + ISNULL(@HTMLReportFileName,'') + '</file_name>' + 
								CASE WHEN @HTTPAddress IS NOT NULL THEN '<http_address>' + @HTTPAddress + '</http_address>' ELSE '' END + 
								'<relative_path>' + ISNULL(@relativeStoragePath,'') + '</relative_path>' + 
								'</detail></report-html>'

	IF (@sendReportAsAttachment=1) OR (@HTTPAddress IS NULL)
		begin
			SET @file_attachments	= REPLACE(@reportFilePath, '"', '')
			PRINT @reportFilePath
			
			EXEC [dbo].[usp_logEventMessageAndSendEmail]	@projectCode			= @projectCode,
															@sqlServerName			= @@SERVERNAME,
															@module					= 'dbo.usp_reportHTMLBuildHealthCheck',
															@eventName				= 'daily health check',
															@parameters				= @file_attachments,
															@eventMessage			= @eventMessageData,
															@dbMailProfileName		= @dbMailProfileName,
															@recipientsList			= @recipientsList,
															@eventType				= 3 /* Report */
		end
	ELSE
		EXEC [dbo].[usp_logEventMessageAndSendEmail]	@projectCode			= @projectCode,
														@sqlServerName			= @@SERVERNAME,
														@module					= 'dbo.usp_reportHTMLBuildHealthCheck',
														@eventName				= 'daily health check',
														@parameters				= NULL,
														@eventMessage			= @eventMessageData,
														@dbMailProfileName		= @dbMailProfileName,
														@recipientsList			= @recipientsList,
														@eventType				= 3 /* Report */

	-----------------------------------------------------------------------------------------------------

END TRY

BEGIN CATCH
DECLARE 
        @ErrorMessage    NVARCHAR(4000),
        @ErrorNumber     INT,
        @ErrorSeverity   INT,
        @ErrorState      INT,
        @ErrorLine       INT,
        @ErrorProcedure  NVARCHAR(200);
    -- Assign variables to error-handling functions that 
    -- capture information for RAISERROR.
    SELECT 
        @ErrorNumber = ERROR_NUMBER(),
        @ErrorSeverity = ERROR_SEVERITY(),
        @ErrorState = CASE WHEN ERROR_STATE() BETWEEN 1 AND 127 THEN ERROR_STATE() ELSE 1 END ,
        @ErrorLine = ERROR_LINE(),
        @ErrorProcedure = ISNULL(ERROR_PROCEDURE(), '-');
	-- Building the message string that will contain original
    -- error information.
    SELECT @ErrorMessage = 
        N'Error %d, Level %d, State %d, Procedure %s, Line %d, ' + 
            'Message: '+ ERROR_MESSAGE();
    -- Raise an error: msg_str parameter of RAISERROR will contain
    -- the original error information.
    RAISERROR 
        (
        @ErrorMessage, 
        @ErrorSeverity, 
        @ErrorState,               
        @ErrorNumber,    -- parameter: original error number.
        @ErrorSeverity,  -- parameter: original error severity.
        @ErrorState,     -- parameter: original error state.
        @ErrorProcedure, -- parameter: original error procedure name.
        @ErrorLine       -- parameter: original error line number.
        );

        -- Test XACT_STATE:
        -- If 1, the transaction is committable.
        -- If -1, the transaction is uncommittable and should 
        --     be rolled back.
        -- XACT_STATE = 0 means that there is no transaction and
        --     a COMMIT or ROLLBACK would generate an error.

    -- Test if the transaction is uncommittable.
    IF (XACT_STATE()) = -1
    BEGIN
        PRINT
            N'The transaction is in an uncommittable state.' +
            'Rolling back transaction.'
        ROLLBACK TRANSACTION 
   END;

END CATCH

RETURN @ReturnValue
GO


RAISERROR('Create procedure: [dbo].[usp_mpMarkInternalAction]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_mpMarkInternalAction]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpMarkInternalAction]
GO

CREATE PROCEDURE [dbo].[usp_mpMarkInternalAction]
		@actionName				[sysname],
		@flgOperation			[tinyint] = 1, /*	1 - insert action 
													2 - delete action
												*/
		@server_name			[sysname] = NULL,
		@database_name			[sysname] = NULL,
		@schema_name			[sysname] = NULL,
		@object_name			[sysname] = NULL,
		@child_object_name		[sysname] = NULL
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 06.02.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SET NOCOUNT ON

--insert action
IF @flgOperation = 1
	begin
		INSERT	INTO [maintenance-plan].[statsMaintenancePlanInternals]([name], [server_name], [database_name], [schema_name], [object_name], [child_object_name])
				SELECT @actionName, @server_name, @database_name, @schema_name, @object_name, @child_object_name
	end

--delete action
IF @flgOperation = 2
	begin
		IF @database_name <> '%'
			DELETE	FROM [maintenance-plan].[statsMaintenancePlanInternals]
			WHERE	[name] = @actionName
					AND [server_name] = @server_name
					AND ([database_name] = @database_name OR ([database_name] IS NULL AND @database_name IS NULL))
					AND ([schema_name] = @schema_name OR ([schema_name] IS NULL AND @schema_name IS NULL))
					AND ([object_name] = @object_name OR ([object_name] IS NULL AND @object_name IS NULL))
					AND ([child_object_name] = @child_object_name OR ([child_object_name] IS NULL AND @child_object_name IS NULL))
		ELSE
			DELETE	FROM [maintenance-plan].[statsMaintenancePlanInternals]
			WHERE	[name] = @actionName
					AND [server_name] = @server_name

	end
GO


RAISERROR('Create procedure: [dbo].[usp_mpCheckAndRevertInternalActions]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sysobjects 
	     WHERE [id] = OBJECT_ID(N'[dbo].[usp_mpCheckAndRevertInternalActions]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpCheckAndRevertInternalActions]
GO

-----------------------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[usp_mpCheckAndRevertInternalActions]
		@sqlServerName			[sysname],
		@flgOptions				[int]	= 12941,
		@executionLevel			[tinyint]	=     0,
		@debugMode				[bit]		=     0
/* WITH ENCRYPTION */
AS

DECLARE   @crtDatabaseName			[sysname]
		, @crtSchemaName			[sysname]
		, @crtObjectName			[sysname]
		, @crtChildObjectName		[sysname]
		, @queryToRun				[nvarchar](1024)
		, @nestExecutionLevel		[tinyint]
		, @affectedDependentObjects	[nvarchar](max)

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 19.02.2015
-- Module			 : Database Maintenance Plan 
-- Description		 : Optimization and Maintenance Checks
-- ============================================================================
-----------------------------------------------------------------------------------------
-- Input Parameters:
--		@flgOptions		 1  - Compact large objects (LOB) (default)
--						 2  - Rebuild index by create with drop existing on (default)
--						 4  - Rebuild all non-clustered indexes when rebuild clustered indexes (default)
--						 8  - Disable non-clustered index before rebuild (save space) (default)
--						16  - Disable all foreign key constraints that reffered current table before rebuilding clustered indexes
--						32  - Stop execution if an error occurs. Default behaviour is to print error messages and continue execution
--					    64  - When enabling foreign key constraints, do no check values. Default behaviour is to enabled foreign key constraint with check option
--					  4096  - rebuild/reorganize indexes using ONLINE=ON, if applicable (default)
--		@debugMode		 1 - print dynamic SQL statements / 0 - no statements will be displayed
-----------------------------------------------------------------------------------------
-- Return : 
-- 1 : Succes  -1 : Fail 
-----------------------------------------------------------------------------------------
/*
	--usage sample
	EXEC [dbo].[usp_mpCheckAndRevertInternalActions]	@flgOptions				= DEFAULT,
														@debugMode				= DEFAULT
*/

SET NOCOUNT ON

---------------------------------------------------------------------------------------------
--get configuration values
---------------------------------------------------------------------------------------------
DECLARE @queryLockTimeOut [int]
SELECT	@queryLockTimeOut=[value] 
FROM	[dbo].[appConfigurations] 
WHERE	[name]='Default lock timeout (ms)'
		AND [module] = 'common'

--reset configuration value
UPDATE [dbo].[appConfigurations]
	SET [value]='-1'
WHERE	[name]='Default lock timeout (ms)'
		AND [module] = 'common'

-----------------------------------------------------------------------------------------
SET @queryToRun=N'Rebuilding previously disabled indexes...'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

SET @nestExecutionLevel = @executionLevel + 1
DECLARE crsStatsMaintenancePlanInternals CURSOR FOR SELECT	[database_name], [schema_name], [object_name], [child_object_name]
													FROM	[maintenance-plan].[statsMaintenancePlanInternals]
													WHERE	[name] = 'index-made-disable'
															AND [server_name] = @sqlServerName
OPEN crsStatsMaintenancePlanInternals
FETCH NEXT FROM crsStatsMaintenancePlanInternals INTO @crtDatabaseName, @crtSchemaName, @crtObjectName, @crtChildObjectName
WHILE @@FETCH_STATUS=0
	begin
		EXEC [dbo].[usp_mpAlterTableIndexes]		@SQLServerName				= @sqlServerName,
													@DBName						= @crtDatabaseName,
													@TableSchema				= @crtSchemaName,
													@TableName					= @crtObjectName,
													@IndexName					= @crtChildObjectName,
													@IndexID					= NULL,
													@PartitionNumber			= DEFAULT,
													@flgAction					= 1,
													@flgOptions					= @flgOptions,
													@MaxDOP						= 1,
													@executionLevel				= @nestExecutionLevel,
													@affectedDependentObjects	= @affectedDependentObjects OUT,
													@debugMode					= @debugMode

		FETCH NEXT FROM crsStatsMaintenancePlanInternals INTO @crtDatabaseName, @crtSchemaName, @crtObjectName, @crtChildObjectName
	end
CLOSE crsStatsMaintenancePlanInternals
DEALLOCATE crsStatsMaintenancePlanInternals


-----------------------------------------------------------------------------------------
SET @queryToRun=N'Rebuilding previously disabled foreign key constraints...'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

DECLARE crsStatsMaintenancePlanInternals CURSOR FOR SELECT	[database_name], [schema_name], [object_name], [child_object_name]
													FROM	[maintenance-plan].[statsMaintenancePlanInternals]
													WHERE	[name] = 'foreign-key-made-disable'
															AND [server_name] = @sqlServerName
OPEN crsStatsMaintenancePlanInternals
FETCH NEXT FROM crsStatsMaintenancePlanInternals INTO @crtDatabaseName, @crtSchemaName, @crtObjectName, @crtChildObjectName
WHILE @@FETCH_STATUS=0
	begin
		EXEC [dbo].[usp_mpAlterTableForeignKeys]	@SQLServerName		= @sqlServerName,
													@DBName				= @crtDatabaseName,
													@TableSchema		= @crtSchemaName,
													@TableName			= @crtObjectName,
													@ConstraintName		= @crtChildObjectName,
													@flgAction			= 1,
													@flgOptions			= @flgOptions,
													@executionLevel		= @nestExecutionLevel,
													@debugMode			= @debugMode
		FETCH NEXT FROM crsStatsMaintenancePlanInternals INTO @crtDatabaseName, @crtSchemaName, @crtObjectName, @crtChildObjectName
	end
CLOSE crsStatsMaintenancePlanInternals
DEALLOCATE crsStatsMaintenancePlanInternals


-----------------------------------------------------------------------------------------
--restore original configuration value
-----------------------------------------------------------------------------------------
UPDATE [dbo].[appConfigurations]
	SET [value]=@queryLockTimeOut
WHERE	[name]='Default lock timeout (ms)'
		AND [module] = 'common'

GO


RAISERROR('Create procedure: [dbo].[usp_mpAlterTableIndexes]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_mpAlterTableIndexes]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpAlterTableIndexes]
GO

-----------------------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[usp_mpAlterTableIndexes]
		@SQLServerName				[sysname],
		@DBName						[sysname],
		@TableSchema				[sysname] = '%',
		@TableName					[sysname] = '%',
		@IndexName					[sysname] = '%',
		@IndexID					[int],
		@PartitionNumber			[int] = 1,
		@flgAction					[tinyint] = 1,
		@flgOptions					[int] = 6145, --4096 + 2048 + 1	/* 6177 for space optimized index rebuild */
		@MaxDOP						[smallint] = 1,
		@FillFactor					[tinyint] = 0,
		@executionLevel				[tinyint] = 0,
		@affectedDependentObjects	[nvarchar](max) OUTPUT,
		@DebugMode					[bit] = 0
/* WITH ENCRYPTION */
AS


-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 08.01.2010
-- Module			 : Database Maintenance Scripts
-- ============================================================================

-----------------------------------------------------------------------------------------
-- Input Parameters:
--		@SQLServerName	- name of SQL Server instance to analyze
--		@DBName			- database to be analyzed
--		@TableSchema	- schema that current table belongs to
--		@TableName		- specify table name to be analyzed.
--		@IndexName		- name of the index to be analyzed
--		@IndexID		- id of the index to be analyzed. to may specify either index name or id. 
--						  if you specify both, index name will be taken into consideration
--		@PartitionNumber- index partition number. default value = 1 (index with no partitions)
--		@flgAction:		 1	- Rebuild index (default)
--						 2  - Reorganize indexes
--						 4	- Disable index
--		@flgOptions		 1  - Compact large objects (LOB) when reorganize  (default)
--						 2  - 
--						 4  - Rebuild all dependent indexes when rebuild primary indexes
--						 8  - Disable non-clustered index before rebuild (save space) (won't apply when 4096 is applicable)
--						16  - Disable foreign key constraints that reffer current table before rebuilding with disable clustered/unique indexes
--						64  - When enabling foreign key constraints, do no check values. Default behaviour is to enabled foreign key constraint with check option
--					  2048  - send email when a error occurs (default)
--					  4096  - rebuild/reorganize indexes using ONLINE=ON, if applicable (default)
--		@DebugMode:		 1 - print dynamic SQL statements 
--						 0 - no statements will be displayed (default)
-----------------------------------------------------------------------------------------

DECLARE		@queryToRun   				[nvarchar](max),
			@strMessage				[nvarchar](4000),
			@sqlIndexCreate			[nvarchar](max),
			@sqlScriptOnline		[nvarchar](512),
			@objectName				[nvarchar](512),
			@childObjectName		[sysname],
			@crtTableSchema 		[sysname],
			@crtTableName 			[sysname],
			@crtIndexID				[int],
			@crtIndexName			[sysname],			
			@crtIndexType			[tinyint],
			@crtIndexAllowPageLocks	[bit],
			@crtIndexIsDisabled		[bit],
			@crtIndexIsPrimaryXML	[bit],
			@crtIndexHasDependentFK	[bit],
			@crtTableIsReplicated	[bit],
			@flgInheritOptions		[int],
			@tmpIndexName			[sysname],
			@tmpIndexIsPrimaryXML	[bit],
			@nestedExecutionLevel	[tinyint]

DECLARE   @flgRaiseErrorAndStop [bit]
		, @errorCode			[int]

DECLARE @DependentIndexes TABLE	(
									[index_name]		[sysname]	NULL
								  , [is_primary_xml]	[bit]		DEFAULT(0)
								)

SET NOCOUNT ON

DECLARE @tmpTableToAlterIndexes TABLE
			(
				[index_id]			[int]		NULL
			  , [index_name]		[sysname]	NULL
			  , [index_type]		[tinyint]	NULL
			  , [allow_page_locks]	[bit]		NULL
			  , [is_disabled]		[bit]		NULL
			  , [is_primary_xml]	[bit]		NULL
			  , [has_dependent_fk]	[bit]		NULL
			  , [is_replicated]		[bit]		NULL
			)


-- { sql_statement | statement_block }
BEGIN TRY
		SET @errorCode	 = 0

		---------------------------------------------------------------------------------------------
		--get configuration values
		---------------------------------------------------------------------------------------------
		DECLARE @queryLockTimeOut [int]
		SELECT	@queryLockTimeOut=[value] 
		FROM	[dbo].[appConfigurations] 
		WHERE	[name] = 'Default lock timeout (ms)'
				AND [module] = 'common'

		---------------------------------------------------------------------------------------------		
		--get tables list	
		IF object_id('tempdb..#tmpTableList') IS NOT NULL DROP TABLE #tmpTableList
		CREATE TABLE #tmpTableList 
				(
					[table_schema] [sysname],
					[table_name] [sysname]
				)

		SET @queryToRun = N'SELECT TABLE_SCHEMA, TABLE_NAME 
						FROM [' + @DBName + '].INFORMATION_SCHEMA.TABLES 
						WHERE	TABLE_TYPE = ''BASE TABLE'' 
								AND TABLE_NAME LIKE ''' + @TableName + ''' 
								AND TABLE_SCHEMA LIKE ''' + @TableSchema + ''''
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@SQLServerName, @queryToRun)
		IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		INSERT	INTO #tmpTableList ([table_schema], [table_name])
				EXEC (@queryToRun)

		---------------------------------------------------------------------------------------------
		IF EXISTS(SELECT 1 FROM #tmpTableList)
			begin
				DECLARE crsTableList CURSOR LOCAL FAST_FORWARD FOR	SELECT [table_schema], [table_name]
																	FROM #tmpTableList
																	ORDER BY [table_schema], [table_name]
				OPEN crsTableList
				FETCH NEXT FROM crsTableList INTO @crtTableSchema, @crtTableName
				WHILE @@FETCH_STATUS=0
					begin
						SET @strMessage=N'Alter indexes ON [' + @crtTableSchema + '].[' + @crtTableName + '] : ' + 
											CASE @flgAction WHEN 1 THEN 'REBUILD'
															WHEN 2 THEN 'REORGANIZE'
															WHEN 4 THEN 'DISABLE'
															ELSE 'N/A'
											END
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						--if current action is to disable/reorganize indexes, will get only enabled indexes
						--if current action is to rebuild, will get both enabled/disabled indexes
						SET @queryToRun = N''
						SET @queryToRun = @queryToRun + N'SELECT  si.[index_id]
														, si.[name]
														, si.[type]
														, si.[allow_page_locks]
														, si.[is_disabled]
														, CASE WHEN xi.[type]=3 AND xi.[using_xml_index_id] IS NULL THEN 1 ELSE 0 END AS [is_primary_xml]
														, CASE WHEN SUM(CASE WHEN fk.[name] IS NOT NULL THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END AS [has_dependent_fk]
														, ISNULL(st.[is_replicated], 0) | ISNULL(st.[is_merge_published], 0) | ISNULL(st.[is_published], 0) AS [is_replicated]
													FROM [' + @DBName + '].[sys].[indexes]				si
													INNER JOIN [' + @DBName + '].[sys].[objects]		so  ON so.[object_id] = si.[object_id]
													INNER JOIN [' + @DBName + '].[sys].[schemas]		sch ON sch.[schema_id] = so.[schema_id]
													LEFT  JOIN [' + @DBName + '].[sys].[xml_indexes]	xi  ON xi.[object_id] = si.[object_id] AND xi.[index_id] = si.[index_id] AND si.[type]=3
													LEFT  JOIN [' + @DBName + '].[sys].[foreign_keys]	fk  ON fk.[referenced_object_id] = so.[object_id] AND fk.[key_index_id] = si.[index_id]
													LEFT  JOIN [' + @DBName + '].[sys].[tables]			st  ON st.[object_id] = so.[object_id]
													WHERE	so.[name] = ''' + @crtTableName + '''
															AND sch.[name] = ''' + @crtTableSchema + '''
															AND so.[is_ms_shipped] = 0' + 
															CASE	WHEN @IndexName IS NOT NULL 
																	THEN ' AND si.[name] LIKE ''' + @IndexName + ''''
																	ELSE CASE WHEN @IndexID  IS NOT NULL 
																			  THEN ' AND si.[index_id] = ' + CAST(@IndexID AS [nvarchar])
																			  ELSE ''
																		 END
															END + '
															AND si.[is_disabled] IN ( ' + CASE WHEN @flgAction IN (2, 4) THEN '0' ELSE '0,1' END + ')
													GROUP BY si.[index_id]
															, si.[name]
															, si.[type]
															, si.[allow_page_locks]
															, si.[is_disabled]
															, CASE WHEN xi.[type]=3 AND xi.[using_xml_index_id] IS NULL THEN 1 ELSE 0 END
															, ISNULL(st.[is_replicated], 0) | ISNULL(st.[is_merge_published], 0) | ISNULL(st.[is_published], 0)'

						SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@SQLServerName, @queryToRun)
						IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						DELETE FROM @tmpTableToAlterIndexes
						INSERT	INTO @tmpTableToAlterIndexes([index_id], [index_name], [index_type], [allow_page_locks], [is_disabled], [is_primary_xml], [has_dependent_fk], [is_replicated])
								EXEC (@queryToRun)

						FETCH NEXT FROM crsTableList INTO @crtTableSchema, @crtTableName
					end
				CLOSE crsTableList
				DEALLOCATE crsTableList



				DECLARE crsTableToAlterIndexes CURSOR	LOCAL FAST_FORWARD FOR	SELECT DISTINCT [index_id], [index_name], [index_type], [allow_page_locks], [is_disabled], [is_primary_xml], [has_dependent_fk], [is_replicated]
																				FROM @tmpTableToAlterIndexes
																				ORDER BY [index_id], [index_name]						
				OPEN crsTableToAlterIndexes
				FETCH NEXT FROM crsTableToAlterIndexes INTO @crtIndexID, @crtIndexName, @crtIndexType, @crtIndexAllowPageLocks, @crtIndexIsDisabled, @crtIndexIsPrimaryXML, @crtIndexHasDependentFK, @crtTableIsReplicated
				WHILE @@FETCH_STATUS=0
					begin
						SET @strMessage= '[' + @crtIndexName + ']'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

						SET @sqlScriptOnline=N''
						---------------------------------------------------------------------------------------------
						-- 1  - Rebuild indexes
						---------------------------------------------------------------------------------------------
						IF @flgAction = 1
							begin
								-- check for online operation mode	
								IF @flgOptions & 4096 = 4096
									begin
										SET @nestedExecutionLevel = @executionLevel + 3
										EXEC [dbo].[usp_mpCheckIndexOnlineOperation]	@sqlServerName		= @SQLServerName,
																						@dbName				= @DBName,
																						@tableSchema		= @crtTableSchema,
																						@tableName			= @crtTableName,
																						@indexName			= @crtIndexName,
																						@indexID			= @crtIndexID,
																						@partitionNumber	= @PartitionNumber,
																						@sqlScriptOnline	= @sqlScriptOnline OUT,
																						@flgOptions			= @flgOptions,
																						@executionLevel		= @nestedExecutionLevel,
																						@debugMode			= @DebugMode
									end

								---------------------------------------------------------------------------------------------
								--primary / unique index options
								-- 16  - Disable foreign key constraints that reffer current table before rebuilding clustered/unique indexes
								IF @flgOptions & 16 = 16 AND @crtIndexHasDependentFK=1 
									AND @flgOptions & 8 = 8 AND NOT ((@flgOptions & 4096 = 4096) AND (@sqlScriptOnline = N'ONLINE = ON')) 
									AND @crtIndexIsDisabled=0 AND @crtTableIsReplicated=0
									begin
										SET @nestedExecutionLevel = @executionLevel + 2
										EXEC [dbo].[usp_mpAlterTableForeignKeys]	  @SQLServerName	= @SQLServerName
																					, @DBName			= @DBName
																					, @TableSchema		= @crtTableSchema
																					, @TableName		= @crtTableName
																					, @ConstraintName	= '%'
																					, @flgAction		= 0		-- Disable Constraints
																					, @flgOptions		= 1		-- Use tables that have foreign key constraints that reffers current table (default)
																					, @executionLevel	= @nestedExecutionLevel
																					, @DebugMode		= @DebugMode
									end

								---------------------------------------------------------------------------------------------
								--clustered/primary key index options
								IF @crtIndexType = 1
									begin
										--4  - Rebuild all dependent indexes when rebuild primary indexes
										IF @flgOptions & 4 = 4
											begin
												--get all enabled non-clustered/xml/spatial indexes for current table
												SET @queryToRun = N''
												SET @queryToRun = @queryToRun + N'SELECT  si.[name]
																				, CASE WHEN xi.[type]=3 AND xi.[using_xml_index_id] IS NULL THEN 1 ELSE 0 END AS [is_primary_xml]
																			FROM [' + @DBName + '].[sys].[indexes]				si
																			INNER JOIN [' + @DBName + '].[sys].[objects]		so ON  si.[object_id] = so.[object_id]
																			INNER JOIN [' + @DBName + '].[sys].[schemas]		sch ON sch.[schema_id] = so.[schema_id]
																			LEFT  JOIN [' + @DBName + '].[sys].[xml_indexes]	xi  ON xi.[object_id] = si.[object_id] AND xi.[index_id] = si.[index_id] AND si.[type]=3
																			WHERE	so.[name] = ''' + @crtTableName + '''
																					AND sch.[name] = ''' + @crtTableSchema + ''' 
																					AND si.[type] in (2,3,4)
																					AND si.[is_disabled] = 0'
												SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@SQLServerName, @queryToRun)
												IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

												INSERT INTO @DependentIndexes ([index_name], [is_primary_xml])
													EXEC (@queryToRun)
											end

										--8  - Disable non-clustered index before rebuild (save space)
										--won't disable the index when performing online rebuild
										IF @flgOptions & 8 = 8 AND NOT ((@flgOptions & 4096 = 4096) AND (@sqlScriptOnline = N'ONLINE = ON')) AND @crtTableIsReplicated=0
											begin
												DECLARE crsNonClusteredIndexes	CURSOR LOCAL FAST_FORWARD FOR
																				SELECT [index_name]
																				FROM @DependentIndexes
																				ORDER BY [is_primary_xml]
												OPEN crsNonClusteredIndexes
												FETCH NEXT FROM crsNonClusteredIndexes INTO @tmpIndexName
												WHILE @@FETCH_STATUS=0
													begin
														SET @nestedExecutionLevel = @executionLevel + 2
														EXEC [dbo].[usp_mpAlterTableIndexes]	  @SQLServerName	= @SQLServerName
																								, @DBName			= @DBName
																								, @TableSchema		= @crtTableSchema
																								, @TableName		= @crtTableName
																								, @IndexName		= @tmpIndexName
																								, @IndexID			= NULL
																								, @PartitionNumber	= DEFAULT
																								, @flgAction		= 4				--disable
																								, @flgOptions		= @flgOptions
																								, @executionLevel	= @nestedExecutionLevel
																								, @affectedDependentObjects = @affectedDependentObjects OUT
																								, @DebugMode		= @DebugMode										

														FETCH NEXT FROM crsNonClusteredIndexes INTO @tmpIndexName
													end
												CLOSE crsNonClusteredIndexes
												DEALLOCATE crsNonClusteredIndexes
											end
									end
								ELSE
									---------------------------------------------------------------------------------------------
									--xml primary key index options
									IF @crtIndexType = 3 AND @crtIndexIsPrimaryXML=1
										begin
											--4  - Rebuild all dependent indexes when rebuild primary indexes
											IF @flgOptions & 4 = 4
												begin
													--get all enabled secondary xml indexes for current table
													SET @queryToRun = N''
													SET @queryToRun = @queryToRun + N'SELECT  si.[name]
																				FROM [' + @DBName + '].[sys].[indexes]				si
																				INNER JOIN [' + @DBName + '].[sys].[objects]		so ON  si.[object_id] = so.[object_id]
																				INNER JOIN [' + @DBName + '].[sys].[schemas]		sch ON sch.[schema_id] = so.[schema_id]
																				INNER JOIN [' + @DBName + '].[sys].[xml_indexes]	xi  ON xi.[object_id] = si.[object_id] AND xi.[index_id] = si.[index_id]
																				WHERE	so.[name] = ''' + @crtTableName + '''
																						AND sch.[name] = ''' + @crtTableSchema + ''' 
																						AND si.[type] = 3
																						AND xi.[using_xml_index_id] = ''' + CAST(@crtIndexID AS [sysname]) + '''
																						AND si.[is_disabled] = 0'
													SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@SQLServerName, @queryToRun)
													IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

													INSERT INTO @DependentIndexes ([index_name])
														EXEC (@queryToRun)
												end

											--8  - Disable non-clustered index before rebuild (save space)
											--won't disable the index when performing online rebuild
											IF @flgOptions & 8 = 8 AND NOT ((@flgOptions & 4096 = 4096) AND (@sqlScriptOnline = N'ONLINE = ON')) AND @crtTableIsReplicated=0
												begin
													DECLARE crsNonClusteredIndexes	CURSOR LOCAL FAST_FORWARD FOR
																					SELECT [index_name]
																					FROM @DependentIndexes
													OPEN crsNonClusteredIndexes
													FETCH NEXT FROM crsNonClusteredIndexes INTO @tmpIndexName
													WHILE @@FETCH_STATUS=0
														begin
															SET @nestedExecutionLevel = @executionLevel + 2
															EXEC [dbo].[usp_mpAlterTableIndexes]	  @SQLServerName	= @SQLServerName
																									, @DBName			= @DBName
																									, @TableSchema		= @crtTableSchema
																									, @TableName		= @crtTableName
																									, @IndexName		= @tmpIndexName
																									, @IndexID			= NULL
																									, @PartitionNumber	= DEFAULT
																									, @flgAction		= 4				--disable
																									, @flgOptions		= @flgOptions
																									, @executionLevel	= @nestedExecutionLevel
																									, @affectedDependentObjects = @affectedDependentObjects OUT
																									, @DebugMode		= @DebugMode										

															FETCH NEXT FROM crsNonClusteredIndexes INTO @tmpIndexName
														end
													CLOSE crsNonClusteredIndexes
													DEALLOCATE crsNonClusteredIndexes
												end
										end
									ELSE
										--8  - Disable non-clustered index before rebuild (save space)
										--won't disable the index when performing online rebuild										
										IF @flgOptions & 8 = 8 AND NOT ((@flgOptions & 4096 = 4096) AND (@sqlScriptOnline = N'ONLINE = ON')) AND @crtIndexIsDisabled=0 AND @crtTableIsReplicated=0
											begin
												SET @nestedExecutionLevel = @executionLevel + 2
												EXEC [dbo].[usp_mpAlterTableIndexes]	  @SQLServerName	= @SQLServerName
																						, @DBName			= @DBName
																						, @TableSchema		= @crtTableSchema
																						, @TableName		= @crtTableName
																						, @IndexName		= @crtIndexName
																						, @IndexID			= NULL
																						, @PartitionNumber	= @PartitionNumber
																						, @flgAction		= 4				--disable
																						, @flgOptions		= @flgOptions
																						, @executionLevel	= @nestedExecutionLevel
																						, @affectedDependentObjects = @affectedDependentObjects OUT
																						, @DebugMode		= @DebugMode										
										end

								---------------------------------------------------------------------------------------------
								/* FIX: Data corruption occurs in clustered index when you run online index rebuild in SQL Server 2012 or SQL Server 2014 https://support.microsoft.com/en-us/kb/2969896 */
								IF (@sqlScriptOnline = N'ONLINE = ON')
									begin
										--get destination server running version/edition
										DECLARE		@serverEdition					[sysname],
													@serverVersionStr				[sysname],
													@serverVersionNum				[numeric](9,6)

										SET @nestedExecutionLevel = @executionLevel + 1
										EXEC [dbo].[usp_getSQLServerVersion]	@sqlServerName			= @SQLServerName,
																				@serverEdition			= @serverEdition OUT,
																				@serverVersionStr		= @serverVersionStr OUT,
																				@serverVersionNum		= @serverVersionNum OUT,
																				@executionLevel			= @nestedExecutionLevel,
																				@debugMode				= @DebugMode
										
										IF     (@serverVersionNum >= 11.02100 AND @serverVersionNum < 11.03449) /* SQL Server 2012 RTM till SQL Server 2012 SP1 CU 11*/
											OR (@serverVersionNum >= 11.05058 AND @serverVersionNum < 11.05532) /* SQL Server 2012 SP2 till SQL Server 2012 SP2 CU 1*/
											OR (@serverVersionNum >= 12.02000 AND @serverVersionNum < 12.02370) /* SQL Server 2014 RTM CU 2*/
											begin
												SET @MaxDOP=1
											end
									end

								---------------------------------------------------------------------------------------------
								--generate rebuild index script
								SET @queryToRun = N''

								SET @queryToRun = @queryToRun + N'SET QUOTED_IDENTIFIER ON; SET LOCK_TIMEOUT ' + CAST(@queryLockTimeOut AS [nvarchar]) + N'; '
								SET @queryToRun = @queryToRun + N'IF OBJECT_ID(''[' + @crtTableSchema + '].[' + @crtTableName + ']'') IS NOT NULL ALTER INDEX [' + @crtIndexName + '] ON [' + @crtTableSchema + '].[' + @crtTableName + '] REBUILD'
					
								--rebuild options
								SET @queryToRun = @queryToRun + N' WITH (SORT_IN_TEMPDB = ON' + CASE WHEN ISNULL(@MaxDOP, 0) <> 0 THEN N', MAXDOP = ' + CAST(@MaxDOP AS [nvarchar]) ELSE N'' END + 
																						CASE WHEN ISNULL(@sqlScriptOnline, N'')<>N'' THEN N', ' + @sqlScriptOnline ELSE N'' END + 
																						CASE WHEN ISNULL(@FillFactor, 0) <> 0 THEN N', FILLFACTOR = ' + CAST(@FillFactor AS [nvarchar]) ELSE N'' END +
																N')'

								IF @PartitionNumber>1
									SET @queryToRun = @queryToRun + N' PARTITION ' + CAST(@PartitionNumber AS [nvarchar])

								IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

								IF @flgOptions & 8 = 8 AND NOT ((@flgOptions & 4096 = 4096) AND (@sqlScriptOnline = N'ONLINE = ON'))
									begin
										SET @strMessage=N'performing index rebuild'
										SET @nestedExecutionLevel = @executionLevel + 2
										EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = @nestedExecutionLevel, @messageTreelevel = 1, @stopExecution=0
									end

								SET @objectName = '[' + @crtTableSchema + '].[' + @crtTableName + ']'
								SET @childObjectName = QUOTENAME(@crtIndexName)
								SET @nestedExecutionLevel = @executionLevel + 1

								EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @SQLServerName,
																				@dbName			= @DBName,
																				@objectName		= @objectName,
																				@childObjectName= @childObjectName,
																				@module			= 'dbo.usp_mpAlterTableIndexes',
																				@eventName		= 'database maintenance - rebuilding index',
																				@queryToRun  	= @queryToRun,
																				@flgOptions		= @flgOptions,
																				@executionLevel	= @nestedExecutionLevel,
																				@debugMode		= @DebugMode
								
								IF @errorCode=0
									EXEC [dbo].[usp_mpMarkInternalAction]	@actionName			= N'index-made-disable',
																			@flgOperation		= 2,
																			@server_name		= @SQLServerName,
																			@database_name		= @DBName,
																			@schema_name		= @crtTableSchema,
																			@object_name		= @crtTableName,
																			@child_object_name	= @crtIndexName

								---------------------------------------------------------------------------------------------
								--rebuild dependent indexes
								--clustered / xml primary key index options
								IF (@crtIndexType = 1) OR (@crtIndexType = 3 AND @crtIndexIsPrimaryXML=1)
									begin
										--4  - Rebuild all dependent indexes when rebuild primary indexes
										--will rebuild only indexes disabled by this tool
										IF (@flgOptions & 4 = 4)
											begin											
												DECLARE crsNonClusteredIndexes	CURSOR LOCAL FAST_FORWARD FOR
																				SELECT DISTINCT di.[index_name], di.[is_primary_xml]
																				FROM @DependentIndexes di
																				LEFT JOIN [maintenance-plan].[statsMaintenancePlanInternals] smpi ON	smpi.[name]=N'index-made-disable'
																																					AND smpi.[server_name]=@SQLServerName
																																					AND smpi.[database_name]=@DBName
																																					AND smpi.[schema_name]=@crtTableSchema
																																					AND smpi.[object_name]=@crtTableName
																																					AND smpi.[child_object_name]=di.[index_name]
																				WHERE	(
																							/* index was disabled (option selected) and marked as disabled */
																							(@flgOptions & 8 = 8 AND NOT ((@flgOptions & 4096 = 4096) AND (@sqlScriptOnline = N'ONLINE = ON')) AND @crtIndexIsDisabled=0 AND @crtTableIsReplicated=0) 
																							AND smpi.[name]=N'index-made-disable'
																						)
																						OR
																						(
																							/* index was not disabled (option selected) */
																							NOT (@flgOptions & 8 = 8 AND NOT ((@flgOptions & 4096 = 4096) AND (@sqlScriptOnline = N'ONLINE = ON')) AND @crtIndexIsDisabled=0 AND @crtTableIsReplicated=0) 
																							AND smpi.[name] IS NULL
																						)
																				ORDER BY di.[is_primary_xml] DESC
												OPEN crsNonClusteredIndexes
												FETCH NEXT FROM crsNonClusteredIndexes INTO @tmpIndexName, @tmpIndexIsPrimaryXML
												WHILE @@FETCH_STATUS=0
													begin
														SET @nestedExecutionLevel = @executionLevel + 2
														EXEC [dbo].[usp_mpAlterTableIndexes]	  @SQLServerName	= @SQLServerName
																								, @DBName			= @DBName
																								, @TableSchema		= @crtTableSchema
																								, @TableName		= @crtTableName
																								, @IndexName		= @tmpIndexName
																								, @IndexID			= NULL
																								, @PartitionNumber	= DEFAULT
																								, @flgAction		= 1		--rebuild
																								, @flgOptions		= @flgOptions
																								, @executionLevel	= @nestedExecutionLevel
																								, @affectedDependentObjects = @affectedDependentObjects OUT
																								, @DebugMode		= @DebugMode										

														FETCH NEXT FROM crsNonClusteredIndexes INTO @tmpIndexName, @tmpIndexIsPrimaryXML
													end
												CLOSE crsNonClusteredIndexes
												DEALLOCATE crsNonClusteredIndexes
											end
									end		

								---------------------------------------------------------------------------------------------
								-- must enable previous disabled constraints
								-- 16  - Disable foreign key constraints that reffer current table before rebuilding clustered/unique indexes
								IF @flgOptions & 16 = 16 AND @crtIndexHasDependentFK=1 
									AND @flgOptions & 8 = 8 AND NOT ((@flgOptions & 4096 = 4096) 
									AND (@sqlScriptOnline = N'ONLINE = ON')) AND @crtTableIsReplicated=0
									begin
										SET @flgInheritOptions = 1								-- Use tables that have foreign key constraints that reffers current table (default)

										--64  - When enabling foreign key constraints, do no check values. Default behaviour is to enabled foreign key constraint with check option
										IF @flgOptions & 64 = 64
											SET @flgInheritOptions = @flgInheritOptions + 4		-- Enable constraints with NOCHECK. Default is to enable constraints using CHECK option

										SET @nestedExecutionLevel = @executionLevel + 2
										EXEC [dbo].[usp_mpAlterTableForeignKeys]	  @SQLServerName	= @SQLServerName
																					, @DBName			= @DBName
																					, @TableSchema		= @crtTableSchema
																					, @TableName		= @crtTableName
																					, @ConstraintName	= '%'
																					, @flgAction		= 1		-- Enable Constraints
																					, @flgOptions		= @flgInheritOptions
																					, @executionLevel	= @nestedExecutionLevel
																					, @DebugMode		= @DebugMode
									end
							end

						---------------------------------------------------------------------------------------------
						-- 2  - Reorganize indexes
						---------------------------------------------------------------------------------------------
						-- avoid messages like:	The index [...] on table [..] cannot be reorganized because page level locking is disabled.		
						IF @flgAction = 2
							IF @crtIndexAllowPageLocks=1
								begin
									SET @queryToRun = N''
									SET @queryToRun = @queryToRun + N'SET LOCK_TIMEOUT ' + CAST(@queryLockTimeOut AS [nvarchar]) + N'; '
									SET @queryToRun = @queryToRun + N'IF OBJECT_ID(''[' + @crtTableSchema + '].[' + @crtTableName + ']'') IS NOT NULL ALTER INDEX [' + @crtIndexName + '] ON [' + @crtTableSchema + '].[' + @crtTableName + '] REORGANIZE'
				
									--  1  - Compact large objects (LOB) (default)
									IF @flgOptions & 1 = 1
										SET @queryToRun = @queryToRun + N' WITH (LOB_COMPACTION = ON) '
									ELSE
										SET @queryToRun = @queryToRun + N' WITH (LOB_COMPACTION = OFF) '
				
									IF @PartitionNumber>1
										SET @queryToRun = @queryToRun + N' PARTITION ' + CAST(@PartitionNumber AS [nvarchar])
									IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0


									SET @objectName = '[' + @crtTableSchema + '].[' + @crtTableName + ']'
									SET @childObjectName = QUOTENAME(@crtIndexName)
									SET @nestedExecutionLevel = @executionLevel + 1

									EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @SQLServerName,
																					@dbName			= @DBName,
																					@objectName		= @objectName,
																					@childObjectName= @childObjectName,
																					@module			= 'dbo.usp_mpAlterTableIndexes',
																					@eventName		= 'database maintenance - reorganize index',
																					@queryToRun  	= @queryToRun,
																					@flgOptions		= @flgOptions,
																					@executionLevel	= @nestedExecutionLevel,
																					@debugMode		= @DebugMode
								end
							ELSE
								begin
									SET @strMessage=N'--	index cannot be REORGANIZE because ALLOW_PAGE_LOCKS is set to OFF. Skipping...'
									EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0
								end

						---------------------------------------------------------------------------------------------
						-- 4  - Disable indexes 
						---------------------------------------------------------------------------------------------
						IF @flgAction = 4
							begin
								SET @queryToRun = N''
								SET @queryToRun = @queryToRun + N'SET LOCK_TIMEOUT ' + CAST(@queryLockTimeOut AS [nvarchar]) + N'; '
								SET @queryToRun = @queryToRun + N'IF OBJECT_ID(''[' + @crtTableSchema + '].[' + @crtTableName + ']'') IS NOT NULL ALTER INDEX [' + @crtIndexName + '] ON [' + @crtTableSchema + '].[' + @crtTableName + '] DISABLE'
				
								IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

								SET @objectName = '[' + @crtTableSchema + '].[' + @crtTableName + ']'
								SET @childObjectName = QUOTENAME(@crtIndexName)
								SET @nestedExecutionLevel = @executionLevel + 1

								EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @SQLServerName,
																				@dbName			= @DBName,
																				@objectName		= @objectName,
																				@childObjectName= @childObjectName,
																				@module			= 'dbo.usp_mpAlterTableIndexes',
																				@eventName		= 'database maintenance - disable index',
																				@queryToRun  	= @queryToRun,
																				@flgOptions		= @flgOptions,
																				@executionLevel	= @nestedExecutionLevel,
																				@debugMode		= @DebugMode

								/* 4 disable index -> insert action 1 */
								IF @errorCode=0
									EXEC [dbo].[usp_mpMarkInternalAction]	@actionName		= N'index-made-disable',
																			@flgOperation	= 1,
																			@server_name		= @SQLServerName,
																			@database_name		= @DBName,
																			@schema_name		= @crtTableSchema,
																			@object_name		= @crtTableName,
																			@child_object_name	= @crtIndexName
							end

						FETCH NEXT FROM crsTableToAlterIndexes INTO @crtIndexID, @crtIndexName, @crtIndexType, @crtIndexAllowPageLocks, @crtIndexIsDisabled, @crtIndexIsPrimaryXML, @crtIndexHasDependentFK, @crtTableIsReplicated
					end
				CLOSE crsTableToAlterIndexes
				DEALLOCATE crsTableToAlterIndexes
			end

		SET @affectedDependentObjects=N''
		SELECT @affectedDependentObjects = @affectedDependentObjects + N'[' + [index_name] + N'];'
		FROM @DependentIndexes
END TRY

BEGIN CATCH
DECLARE 
        @ErrorMessage    NVARCHAR(4000),
        @ErrorNumber     INT,
        @ErrorSeverity   INT,
        @ErrorState      INT,
        @ErrorLine       INT,
        @ErrorProcedure  NVARCHAR(200);
    -- Assign variables to error-handling functions that 
    -- capture information for RAISERROR.
		SET @errorCode = -1

    SELECT 
        @ErrorNumber = ERROR_NUMBER(),
        @ErrorSeverity = ERROR_SEVERITY(),
        @ErrorState = CASE WHEN ERROR_STATE() BETWEEN 1 AND 127 THEN ERROR_STATE() ELSE 1 END ,
        @ErrorLine = ERROR_LINE(),
        @ErrorProcedure = ISNULL(ERROR_PROCEDURE(), '-');
	-- Building the message string that will contain original
    -- error information.
    SELECT @ErrorMessage = 
        N'Error %d, Level %d, State %d, Procedure %s, Line %d, ' + 
            'Message: '+ ERROR_MESSAGE();
    -- Raise an error: msg_str parameter of RAISERROR will contain
    -- the original error information.
    RAISERROR 
        (
        @ErrorMessage, 
        @ErrorSeverity, 
        @ErrorState,               
        @ErrorNumber,    -- parameter: original error number.
        @ErrorSeverity,  -- parameter: original error severity.
        @ErrorState,     -- parameter: original error state.
        @ErrorProcedure, -- parameter: original error procedure name.
        @ErrorLine       -- parameter: original error line number.
        );

        -- Test XACT_STATE:
        -- If 1, the transaction is committable.
        -- If -1, the transaction is uncommittable and should 
        --     be rolled back.
        -- XACT_STATE = 0 means that there is no transaction and
        --     a COMMIT or ROLLBACK would generate an error.

    -- Test if the transaction is uncommittable.
    IF (XACT_STATE()) = -1
    BEGIN
        PRINT
            N'The transaction is in an uncommittable state.' +
            'Rolling back transaction.'
        ROLLBACK TRANSACTION 
   END;

END CATCH

RETURN @errorCode
GO


RAISERROR('Create procedure: [dbo].[usp_monAlarmCustomFreeDiskSpace]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_monAlarmCustomFreeDiskSpace]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_monAlarmCustomFreeDiskSpace]
GO

CREATE PROCEDURE [dbo].[usp_monAlarmCustomFreeDiskSpace]
		@projectCode		[varchar](32)=NULL,
		@sqlServerName		[sysname]='%'
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 13.10.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================
-- Change Date	: 
-- Description	: 
-----------------------------------------------------------------------------------------

SET NOCOUNT ON 

DECLARE	  @projectID						[int]
		, @warningFreeDiskMinPercent		[numeric](6,2)
		, @warningFreeDiskMinSpaceMB		[numeric](18,3)
		, @criticalFreeDiskMinPercent		[numeric](6,2)
		, @criticalFreeDiskMinSpaceMB		[numeric](18,3)
		, @ErrMessage						[nvarchar](256)
		
-----------------------------------------------------------------------------------------------------
--get default project code
IF @projectCode IS NULL
	SELECT	@projectCode = [value]
	FROM	[dbo].[appConfigurations]
	WHERE	[name] = 'Default project code'
			AND [module] = 'common'

SELECT    @projectID = [id]
FROM [dbo].[catalogProjects]
WHERE [code] = @projectCode 

IF @projectID IS NULL
	begin
		SET @ErrMessage=N'The value specifief for Project Code is not valid.'
		RAISERROR(@ErrMessage, 16, 1) WITH NOWAIT
	end

-----------------------------------------------------------------------------------------------------
SELECT	@warningFreeDiskMinPercent = [warning_limit]
FROM	[monitoring].[alertThresholds]
WHERE	[alert_name] = 'Logical Disk: Free Disk Space (%)'
		AND [category] = 'disk-space'
		AND [is_warning_limit_enabled]=1

SELECT	@criticalFreeDiskMinPercent = [critical_limit]
FROM	[monitoring].[alertThresholds]
WHERE	[alert_name] = 'Logical Disk: Free Disk Space (%)'
		AND [category] = 'disk-space'
		AND [is_critical_limit_enabled]=1

SELECT	@warningFreeDiskMinSpaceMB = [warning_limit]
FROM	[monitoring].[alertThresholds]
WHERE	[alert_name] = 'Logical Disk: Free Disk Space (MB)'
		AND [category] = 'disk-space'
		AND [is_warning_limit_enabled]=1

SELECT	@criticalFreeDiskMinSpaceMB = [critical_limit]
FROM	[monitoring].[alertThresholds]
WHERE	[alert_name] = 'Logical Disk: Free Disk Space (MB)'
		AND [category] = 'disk-space'
		AND [is_critical_limit_enabled]=1

-----------------------------------------------------------------------------------------------------

DECLARE   @instanceName		[sysname]
		, @objectName		[nvarchar](512)
		, @eventName		[sysname]
		, @severity			[sysname]
		, @eventMessage		[nvarchar](max)


DECLARE crsDiskSpaceAlarms CURSOR FOR	SELECT  DISTINCT
												  cin.[instance_name]
												, ISNULL(dsi.[volume_mount_point], dsi.[logical_drive]) AS [object_name]
												, 'warning'			AS [severity]
												, 'low disk space'	AS [event_name]
												, '<alert><detail>' + 
													'<severity>warning</severity>' + 
													'<machine_name>' + cin.[machine_name] + '</machine_name>' + 
													'<counter_name>low disk space</counter_name><target_name>' + ISNULL(dsi.[volume_mount_point], dsi.[logical_drive]) + '</target_name>' + 
													'<measure_unit>MB</measure_unit>' + 
													'<current_value>' + CAST(dsi.[available_space_mb] AS [varchar]) +'</current_value>' + 
													'<current_percentage>' + CAST(dsi.[percent_available] AS [varchar]) + '</current_percentage>' + 
													'<refference_value>' + CAST(dsi.[total_size_mb] AS [varchar]) + '</refference_value>' + 
													'<threshold_value>' + CAST(@warningFreeDiskMinSpaceMB AS [varchar]) + '</threshold_value>' + 
													'<threshold_percentage>' + CAST(@warningFreeDiskMinPercent AS [varchar]) + '</threshold_percentage>' + 
													'<event_date_utc>' + CONVERT([varchar](20), dsi.[event_date_utc], 120) + '</event_date_utc>' + 
													'</detail></alert>' AS [event_message]
										FROM [dbo].[vw_catalogInstanceNames]  cin
										INNER JOIN [health-check].[vw_statsDiskSpaceInfo]		dsi	ON dsi.[project_id] = cin.[project_id] AND dsi.[instance_id] = cin.[instance_id]
										LEFT  JOIN 
													(
														SELECT DISTINCT [project_id], [instance_id], [physical_drives] 
														FROM [health-check].[vw_statsDatabaseDetails]
													)   cdd ON cdd.[project_id] = cin.[project_id] AND cdd.[instance_id] = cin.[instance_id]
										LEFT JOIN [monitoring].[alertSkipRules] asr ON	asr.[category] = 'disk-space'
																						AND asr.[alert_name] IN ('Logical Disk: Free Disk Space (%)', 'Logical Disk: Free Disk Space (MB)')
																						AND asr.[active] = 1
																						AND (asr.[skip_value] = cin.[machine_name] OR asr.[skip_value]=cin.[instance_name])
																						AND (   asr.[skip_value2] IS NULL 
																							 OR (asr.[skip_value2] IS NOT NULL AND asr.[skip_value2] = ISNULL(dsi.[volume_mount_point], dsi.[logical_drive]))
																							)
										WHERE cin.[instance_active]=1
												AND cin.[project_id] = @projectID
												AND cin.[instance_name] LIKE @sqlServerName
												AND (    (	  dsi.[percent_available] IS NOT NULL 
															AND dsi.[percent_available] <= @warningFreeDiskMinPercent
															AND dsi.[percent_available] > @criticalFreeDiskMinPercent
															)
														OR 
														(	   dsi.[percent_available] IS NULL 
															AND dsi.[available_space_mb] IS NOT NULL 
															AND dsi.[available_space_mb] <= @warningFreeDiskMinSpaceMB
															AND dsi.[percent_available] > @criticalFreeDiskMinSpaceMB
														)
													)
												AND (dsi.[logical_drive] IN ('C') OR CHARINDEX(dsi.[logical_drive], cdd.[physical_drives])>0)
												AND asr.[id] IS NULL
												AND (@warningFreeDiskMinSpaceMB IS NOT NULL AND @warningFreeDiskMinPercent IS NOT NULL)

										UNION ALL

										SELECT  DISTINCT
												  cin.[instance_name]
												, ISNULL(dsi.[volume_mount_point], dsi.[logical_drive]) AS [object_name]
												, 'critical'			AS [severity]
												, 'low disk space'	AS [event_name]
												, '<alert><detail>' + 
													'<severity>critical</severity>' + 
													'<machine_name>' + cin.[machine_name] + '</machine_name>' + 
													'<counter_name>low disk space</counter_name><target_name>' + ISNULL(dsi.[volume_mount_point], dsi.[logical_drive]) + '</target_name>' + 
													'<measure_unit>MB</measure_unit>' + 
													'<current_value>' + CAST(dsi.[available_space_mb] AS [varchar]) +'</current_value>' + 
													'<current_percentage>' + CAST(dsi.[percent_available] AS [varchar]) + '</current_percentage>' + 
													'<refference_value>' + CAST(dsi.[total_size_mb] AS [varchar]) + '</refference_value>' + 
													'<threshold_value>' + CAST(@criticalFreeDiskMinSpaceMB AS [varchar]) + '</threshold_value>' + 
													'<threshold_percentage>' + CAST(@criticalFreeDiskMinPercent AS [varchar]) + '</threshold_percentage>' + 
													'<event_date_utc>' + CONVERT([varchar](20), dsi.[event_date_utc], 120) + '</event_date_utc>' + 
													'</detail></alert>' AS [event_message]
										FROM [dbo].[vw_catalogInstanceNames]  cin
										INNER JOIN [health-check].[vw_statsDiskSpaceInfo]		dsi	ON dsi.[project_id] = cin.[project_id] AND dsi.[instance_id] = cin.[instance_id]
										LEFT  JOIN 
													(
														SELECT DISTINCT [project_id], [instance_id], [physical_drives] 
														FROM [health-check].[vw_statsDatabaseDetails]
													)   cdd ON cdd.[project_id] = cin.[project_id] AND cdd.[instance_id] = cin.[instance_id]
										LEFT JOIN [monitoring].[alertSkipRules] asr ON	asr.[category] = 'disk-space'
																						AND asr.[alert_name] IN ('Logical Disk: Free Disk Space (%)', 'Logical Disk: Free Disk Space (MB)')
																						AND asr.[active] = 1
																						AND (asr.[skip_value] = cin.[machine_name] OR asr.[skip_value]=cin.[instance_name])
																						AND (   asr.[skip_value2] IS NULL 
																							 OR (asr.[skip_value2] IS NOT NULL AND asr.[skip_value2] = ISNULL(dsi.[volume_mount_point], dsi.[logical_drive]))
																							)
										WHERE cin.[instance_active]=1
												AND cin.[project_id] = @projectID
												AND cin.[instance_name] LIKE @sqlServerName
												AND (    (	  dsi.[percent_available] IS NOT NULL 
															AND dsi.[percent_available] < @criticalFreeDiskMinPercent
															)
														OR 
														(	   dsi.[percent_available] IS NULL 
															AND dsi.[available_space_mb] IS NOT NULL 
															AND dsi.[available_space_mb] < @criticalFreeDiskMinSpaceMB
														)
													)
												AND (dsi.[logical_drive] IN ('C') OR CHARINDEX(dsi.[logical_drive], cdd.[physical_drives])>0)
												AND asr.[id] IS NULL
												AND (@criticalFreeDiskMinSpaceMB IS NOT NULL AND @criticalFreeDiskMinPercent IS NOT NULL)
										
										ORDER BY [instance_name], [object_name]
OPEN crsDiskSpaceAlarms
FETCH NEXT FROM crsDiskSpaceAlarms INTO @instanceName, @objectName, @severity, @eventName, @eventMessage
WHILE @@FETCH_STATUS=0
	begin
		EXEC [dbo].[usp_logEventMessageAndSendEmail]	@projectCode			= @projectCode,
														@sqlServerName			= @instanceName,
														@dbName					= @severity,
														@objectName				= @objectName,
														@childObjectName		= NULL,
														@module					= 'health-check',
														@eventName				= @eventName,
														@parameters				= NULL,	
														@eventMessage			= @eventMessage,
														@dbMailProfileName		= NULL,
														@recipientsList			= NULL,
														@eventType				= 6,	/* 6 - alert-custom */
														@additionalOption		= 0

		FETCH NEXT FROM crsDiskSpaceAlarms INTO @instanceName, @objectName, @severity, @eventName, @eventMessage
	end
CLOSE crsDiskSpaceAlarms
DEALLOCATE crsDiskSpaceAlarms
GO


