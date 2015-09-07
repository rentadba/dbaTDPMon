USE dbaTDPMon
GO

-----------------------------------------------------------------------------------------------------
--
-----------------------------------------------------------------------------------------------------
RAISERROR('Rename table: [dbo].[reportHTMLDailyHealthCheck] => [dbo].[reportHTML]', 10, 1) WITH NOWAIT
GO

IF EXISTS(SELECT * FROM sys.indexes WHERE [name] = 'IX_reportHTMLDailyHealthCheck_ProjecteID' AND [object_id] = OBJECT_ID('reportHTMLDailyHealthCheck'))	
	DROP INDEX [IX_reportHTMLDailyHealthCheck_ProjecteID] ON [dbo].[reportHTMLDailyHealthCheck]
GO
IF EXISTS(SELECT * FROM sys.foreign_keys WHERE [name] = 'FK_reportHTMLDailyHealthCheck_CatalogProjects'  AND [parent_object_id] = OBJECT_ID('reportHTMLDailyHealthCheck'))	
	ALTER TABLE [dbo].[reportHTMLDailyHealthCheck] DROP CONSTRAINT [FK_reportHTMLDailyHealthCheck_CatalogProjects]
GO
IF EXISTS(SELECT * FROM sys.key_constraints WHERE [name] = 'PK_reportHTMLDailyHealthCheck'  AND [parent_object_id] = OBJECT_ID('reportHTMLDailyHealthCheck'))	
	ALTER TABLE [dbo].[reportHTMLDailyHealthCheck] DROP CONSTRAINT [PK_reportHTMLDailyHealthCheck]
GO

IF NOT EXISTS(SELECT * FROM sys.tables WHERE [name] = 'reportHTMLDailyHealthCheck') AND NOT EXISTS(SELECT * FROM sys.tables WHERE [name] = 'reportHTML')
	CREATE TABLE [dbo].[reportHTML]
	(
		[id]										[int] IDENTITY (1, 1)NOT NULL,
		[project_id]								[smallint]			NOT NULL,
		[module]									[varchar](32)		NOT NULL,
		[instance_id]								[smallint]			NULL,
		[start_date]								[datetime]			NOT NULL,
		[flg_actions]								[int]				NOT NULL,
		[flg_options]								[int]				NOT NULL,
		[file_name]									[nvarchar](260)		NOT NULL,
		[file_path]									[nvarchar](260)		NOT NULL,
		[http_address]								[nvarchar](512)		NULL,
		[build_at]									[datetime]			NOT NULL,
		[build_duration]							[int]				NOT NULL,
		[html_content] 								[nvarchar](max)		NULL,
		[build_in_progress]							[bit]				NOT NULL CONSTRAINT [DF_reportHTML_BuildInProgress]  DEFAULT ((0)),
		[report_uid]								[uniqueidentifier]	NOT NULL CONSTRAINT [DF_reportHTML_ReportUID]  DEFAULT ((NEWID())),
		CONSTRAINT [PK_reportHTML] PRIMARY KEY  CLUSTERED 
		(
			[id]
		) ON [FG_Statistics_Data],
		CONSTRAINT [FK_reportHTML_CatalogProjects] FOREIGN KEY 
		(
			[project_id]
		) 
		REFERENCES [dbo].[catalogProjects] 
		(
			[id]
		),
		CONSTRAINT [FK_reportHTML_catalogInstanceNames] FOREIGN KEY 
		(
			[instance_id],
			[project_id]
		) 
		REFERENCES [dbo].[catalogInstanceNames] 
		(
			[id],
			[project_id]
		)

	) ON [FG_Statistics_Data]
GO

IF EXISTS(SELECT * FROM sys.tables WHERE [name] = 'reportHTMLDailyHealthCheck') AND NOT EXISTS(SELECT * FROM sys.tables WHERE [name] = 'reportHTML')
	EXEC sp_rename 'reportHTMLDailyHealthCheck', 'reportHTML'
GO

IF NOT EXISTS(SELECT * FROM sys.key_constraints WHERE [name] = 'PK_reportHTML'  AND [parent_object_id] = OBJECT_ID('reportHTML'))	
	ALTER TABLE [dbo].[reportHTML] ADD CONSTRAINT [PK_reportHTML] PRIMARY KEY  CLUSTERED 
		(
			[id]
		) ON [FG_Statistics_Data] 
	GO

IF NOT EXISTS(SELECT * FROM sys.foreign_keys WHERE [name] = 'FK_reportHTML_CatalogProjects'  AND [parent_object_id] = OBJECT_ID('reportHTML'))	
	ALTER TABLE [dbo].[reportHTML] ADD CONSTRAINT [FK_reportHTML_CatalogProjects] FOREIGN KEY 
		(
			[project_id]
		) 
		REFERENCES [dbo].[catalogProjects] 
		(
			[id]
		)
GO

IF NOT EXISTS(SELECT * FROM sys.indexes WHERE [name] = 'IX_reportHTML_ProjecteID' AND [object_id] = OBJECT_ID('reportHTML'))	
	CREATE INDEX [IX_reportHTML_ProjecteID] ON [dbo].[reportHTML]([project_id]) ON [FG_Statistics_Index]
GO


IF NOT EXISTS(SELECT * FROM sys.columns WHERE [name] = 'module' AND [object_id] = OBJECT_ID('reportHTML'))
	ALTER TABLE [dbo].[reportHTML] ADD [module]	[varchar](32) NULL
GO

UPDATE [dbo].[reportHTML] SET [module]='health-check'
GO

IF EXISTS(SELECT * FROM sys.columns WHERE [name] = 'module' AND [object_id] = OBJECT_ID('reportHTML'))
	ALTER TABLE [dbo].[reportHTML] ALTER COLUMN [module]	[varchar](32) NOT NULL
GO


IF NOT EXISTS(SELECT * FROM sys.columns WHERE [name] = 'instance_id' AND [object_id] = OBJECT_ID('reportHTML'))
	ALTER TABLE [dbo].[reportHTML] ADD [instance_id] [smallint]	NULL
GO

IF NOT EXISTS(SELECT * FROM sys.foreign_keys WHERE [name] = 'FK_reportHTML_catalogInstanceNames'  AND [parent_object_id] = OBJECT_ID('reportHTML'))	
	ALTER TABLE [dbo].[reportHTML] ADD CONSTRAINT [FK_reportHTML_catalogInstanceNames] FOREIGN KEY 
		(
			[instance_id],
			[project_id]
		) 
		REFERENCES [dbo].[catalogInstanceNames] 
		(
			[id],
			[project_id]
		)
GO

IF NOT EXISTS(SELECT * FROM sys.indexes WHERE [name] = 'IX_reportHTML_InstanceID' AND [object_id] = OBJECT_ID('reportHTML'))	
	CREATE INDEX [IX_reportHTML_InstanceID] ON [dbo].[reportHTML]([instance_id], [project_id]) ON [FG_Statistics_Index]
GO

