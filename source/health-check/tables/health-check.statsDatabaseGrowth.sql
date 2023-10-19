-- ============================================================================
-- Copyright (c) 2004-2023 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 26.07.2023
-- Module			 : Database Analysis & Performance health-check
-- ============================================================================

-----------------------------------------------------------------------------------------------------
--
-----------------------------------------------------------------------------------------------------
RAISERROR('Create table: [health-check].[statsDatabaseGrowth]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[health-check].[statsDatabaseGrowth]') AND type in (N'U'))
DROP TABLE [health-check].[statsDatabaseGrowth]
GO
CREATE TABLE [health-check].[statsDatabaseGrowth]
(
	[id]				[int] IDENTITY(1,1) NOT NULL,
	[instance_id]		[smallint] NOT NULL,
	[project_id]		[smallint] NOT NULL,
	[database_name]		[nvarchar](128) NOT NULL,
	[logical_name]		[nvarchar](255) NOT NULL,
	[current_size_kb]	[bigint] NULL,
	[file_type]			[nvarchar](10) NOT NULL,
	[growth_type]		[nvarchar](50) NOT NULL,
	[growth_kb]			[int] NOT NULL,
	[duration]			[int] NOT NULL,
	[start_time]		[datetime] NOT NULL,
	[end_time]			[datetime] NOT NULL,
	[session_id]		[smallint] NOT NULL,
	[login_name]		[sysname] NULL,
	[host_name]			[sysname] NULL,
	[application_name]	[sysname] NULL,
	[client_process_id]	[int] NULL
	CONSTRAINT [PK_statsDatabaseGrowth] PRIMARY KEY CLUSTERED 
	(
		[id]
	),
	CONSTRAINT [FK_statsDatabaseGrowth_catalogProjects] FOREIGN KEY 
	(
		[project_id]
	) 
	REFERENCES [dbo].[catalogProjects] 
	(
		[id]
	),
	CONSTRAINT [FK_statsDatabaseGrowth_catalogInstanceNames] FOREIGN KEY 
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

CREATE INDEX [IX_statsDatabaseGrowth_ProjectID] ON [health-check].[statsDatabaseGrowth] ([project_id], [instance_id])
GO
CREATE INDEX [IX_statsDatabaseGrowth_InstanceID] ON [health-check].[statsDatabaseGrowth] ([instance_id], [project_id], [database_name], [logical_name], [start_time]);
GO
