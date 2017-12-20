-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 05.12.2014
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-----------------------------------------------------------------------------------------------------
--catalog for Machine Names
-----------------------------------------------------------------------------------------------------
RAISERROR('Create table: [dbo].[catalogMachineNames]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sysobjects WHERE id = OBJECT_ID(N'[dbo].[catalogMachineNames]') AND type in (N'U'))
DROP TABLE [dbo].[catalogMachineNames]
GO

CREATE TABLE [dbo].[catalogMachineNames] 
(
	[id]					[smallint] IDENTITY (1, 1)	NOT NULL,
	[project_id]			[smallint]		NOT NULL,
	[name]					[sysname]		NOT NULL,
	[domain]				[sysname]			NULL,
	[type]					[varchar](32)	NOT NULL CONSTRAINT [DF_catalogMachineNames_Type] DEFAULT ('SQLServer'),
	[host_platform]			[sysname]			NULL,
	CONSTRAINT [PK_catalogMachineNames] PRIMARY KEY  CLUSTERED 
	(
		[id],
		[project_id]
	) ON [PRIMARY],
	CONSTRAINT [UK_catalogMachineNames_Name] UNIQUE  NONCLUSTERED 
	(
		[name],
		[project_id]
	) ON [PRIMARY],
	CONSTRAINT [FK_catalogMachineNames_catalogProjects] FOREIGN KEY 
	(
		[project_id]
	) 
	REFERENCES [dbo].[catalogProjects] 
	(
		[id]
	)
)  ON [PRIMARY]
GO

CREATE INDEX [IX_catalogMachineNames_ProjectID] ON [dbo].[catalogMachineNames]([project_id]) ON [PRIMARY]
GO

