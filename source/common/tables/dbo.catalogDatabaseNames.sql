-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 05.12.2014
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-----------------------------------------------------------------------------------------------------
--catalog for database names
-----------------------------------------------------------------------------------------------------
RAISERROR('Create table: [dbo].[catalogDatabaseNames]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[catalogDatabaseNames]') AND type in (N'U'))
DROP TABLE [dbo].[catalogDatabaseNames]
GO

CREATE TABLE [dbo].[catalogDatabaseNames] 
(
	[id]					[int]			IDENTITY (1, 1)	NOT NULL,
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
	) 
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
) 
GO

CREATE INDEX [IX_catalogDatabaseNames_InstanceID] ON [dbo].[catalogDatabaseNames]([instance_id], [project_id]) 
GO
CREATE INDEX [IX_catalogDatabaseNames_ProjecteID] ON [dbo].[catalogDatabaseNames]([project_id]) 
GO
CREATE UNIQUE INDEX [UK_catalogDatabaseNames_Name] ON [dbo].[catalogDatabaseNames]([name], [instance_id]) INCLUDE ([project_id], [active])
GO
