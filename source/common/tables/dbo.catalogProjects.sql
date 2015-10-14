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
	[active]		[bit]					NOT NULL CONSTRAINT [DF_catalogProjects_Active] DEFAULT (1),
	CONSTRAINT [PK_catalogProjects] PRIMARY KEY  CLUSTERED 
	(
		[code]
	) ON [PRIMARY] ,
	CONSTRAINT [UK_catalogProjects_Name] UNIQUE  NONCLUSTERED 
	(
		[name]
	) ON [PRIMARY],
	CONSTRAINT [UK_catalogProjects_ID] UNIQUE  NONCLUSTERED 
	(
		[id]
	) ON [PRIMARY]
) ON [PRIMARY]
GO

-----------------------------------------------------------------------------------------------------
RAISERROR('		...insert default data', 10, 1) WITH NOWAIT
GO
SET NOCOUNT ON
GO
INSERT	INTO [dbo].[catalogProjects]([id], [code], [name], [description], [active])
		SELECT 0, '$(projectCode)', '', '', 1
GO
