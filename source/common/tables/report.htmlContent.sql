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
RAISERROR('Create table: [report].[htmlContent]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[report].[htmlContent]') AND type in (N'U'))
DROP TABLE [report].[htmlContent]
GO
CREATE TABLE [report].[htmlContent]
(
	[id]										[int] IDENTITY (1, 1)NOT NULL,
	[project_id]								[smallint]			NULL,
	[module]									[varchar](32)		NOT NULL,
	[instance_id]								[smallint]			NULL,
	[start_date]								[datetime]			NOT NULL,
	[flg_actions]								[int]				NULL,
	[flg_options]								[int]				NULL,
	[file_name]									[nvarchar](260)		NOT NULL,
	[file_path]									[nvarchar](260)		NOT NULL,
	[http_address]								[nvarchar](512)		NULL,
	[build_at]									[datetime]			NOT NULL,
	[build_duration]							[int]				NOT NULL,
	[html_content] 								[nvarchar](max)		NULL,
	[build_in_progress]							[bit]				NOT NULL CONSTRAINT [DF_htmlContent_BuildInProgress]  DEFAULT ((0)),
	[report_uid]								[uniqueidentifier]	NOT NULL CONSTRAINT [DF_htmlContent_ReportUID]  DEFAULT ((NEWID())),
	CONSTRAINT [PK_htmlContent] PRIMARY KEY  CLUSTERED 
	(
		[id]
	),
	CONSTRAINT [FK_htmlContent_CatalogProjects] FOREIGN KEY 
	(
		[project_id]
	) 
	REFERENCES [dbo].[catalogProjects] 
	(
		[id]
	),
	CONSTRAINT [FK_htmlContent_catalogInstanceNames] FOREIGN KEY 
	(
		[instance_id],
		[project_id]
	) 
	REFERENCES [dbo].[catalogInstanceNames] 
	(
		[id],
		[project_id]
	)

)
GO


CREATE INDEX [IX_htmlContent_ProjecteID] ON [report].[htmlContent]([project_id])
GO
CREATE INDEX [IX_htmlContent_InstanceID] ON [report].[htmlContent]([instance_id], [project_id])
GO
