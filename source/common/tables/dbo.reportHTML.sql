-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 25.08.2010
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-----------------------------------------------------------------------------------------------------
--
-----------------------------------------------------------------------------------------------------
RAISERROR('Create table: [dbo].[reportHTML]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[reportHTML]') AND type in (N'U'))
DROP TABLE [dbo].[reportHTML]
GO
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


CREATE INDEX [IX_reportHTML_ProjecteID] ON [dbo].[reportHTML]([project_id]) ON [FG_Statistics_Index]
GO
CREATE INDEX [IX_reportHTML_InstanceID] ON [dbo].[reportHTML]([instance_id], [project_id]) ON [FG_Statistics_Index]
GO
