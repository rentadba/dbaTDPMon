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
IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='jobExecutionQueue' AND COLUMN_NAME='database_name')
	ALTER TABLE [dbo].[jobExecutionQueue] ADD [database_name] [sysname] NULL
GO
IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='jobExecutionQueue' AND COLUMN_NAME='database_name' AND DATA_TYPE='nvarchar')
	ALTER TABLE [dbo].[jobExecutionQueue] ALTER COLUMN [database_name] [sysname] NULL
GO


/* merge code from external implementation for table: dbo.jobExecutionHistory */
IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='jobExecutionHistory' AND COLUMN_NAME='task_id')
	ALTER TABLE [dbo].[jobExecutionHistory] ADD [task_id]	[smallint] NULL
GO
IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='jobExecutionHistory' AND COLUMN_NAME='database_name')
	ALTER TABLE [dbo].[jobExecutionHistory] ADD [database_name] [sysname] NULL
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
IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='catalogProjects' AND COLUMN_NAME='db_filter')
	ALTER TABLE [dbo].[catalogProjects] ADD [db_filter] [sysname] NULL
GO

IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='catalogProjects' AND COLUMN_NAME='is_production')
begin
	EXEC ('ALTER TABLE [dbo].[catalogProjects] ADD [is_production] [bit] NULL');
	EXEC ('ALTER TABLE [dbo].[catalogProjects] ADD CONSTRAINT [DF_catalogProjects_isProduction] DEFAULT (0) FOR [is_production]');
	EXEC ('UPDATE [dbo].[catalogProjects] SET [is_production] = 0 WHERE [is_production] IS NULL');
	EXEC ('ALTER TABLE [dbo].[catalogProjects] ALTER COLUMN [is_production] [bit] NOT NULL');
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
