-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 05.12.2014
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-----------------------------------------------------------------------------------------------------
--Catalog for Projects
-----------------------------------------------------------------------------------------------------
RAISERROR('Create table: [dbo].[catalogProjects]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sysobjects WHERE id = OBJECT_ID(N'[dbo].[catalogProjects]') AND type in (N'U'))
DROP TABLE [dbo].[catalogProjects]
GO
CREATE TABLE [dbo].[catalogProjects]
(
	[id]			[smallint]				NOT NULL,
	[code]			[varchar](32)			NOT NULL,
	[name]			[nvarchar](128)			NOT NULL,
	[description]	[nvarchar](256)			NOT NULL,
	[solution_id]	[smallint]				NULL,
	[db_filter]		[sysname]				NULL,
	[is_production]	[bit]					NOT NULL CONSTRAINT [DF_catalogProjects_isProduction] DEFAULT (0),
	[active]		[bit]					NOT NULL CONSTRAINT [DF_catalogProjects_Active] DEFAULT (1),
	CONSTRAINT [PK_catalogProjects] PRIMARY KEY  CLUSTERED 
	(
		[code]
	) ,
	CONSTRAINT [UK_catalogProjects_Name] UNIQUE  NONCLUSTERED 
	(
		[name]
	) ,
	CONSTRAINT [UK_catalogProjects_ID] UNIQUE  NONCLUSTERED 
	(
		[id]
	) ,
	CONSTRAINT [FK_catalogProjects_catalogSolutions] FOREIGN KEY 
	(
		[solution_id]
	) 
	REFERENCES [dbo].[catalogSolutions] 
	(
		[id]
	)
) 
GO

CREATE INDEX [IX_catalogProjects_ProjectID_SolutionID] ON [dbo].[catalogProjects]([solution_id]) 
GO
