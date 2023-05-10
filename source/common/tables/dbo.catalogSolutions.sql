-- ============================================================================
-- Copyright (c) 2004-2018 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 05.04.2018
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-----------------------------------------------------------------------------------------------------
--Catalog for Solutions (includes multiple projects)
-----------------------------------------------------------------------------------------------------
RAISERROR('Create table: [dbo].[catalogSolutions]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sysobjects WHERE id = OBJECT_ID(N'[dbo].[catalogProjects]') AND type in (N'U'))
DROP TABLE [dbo].[catalogProjects]
GO
IF  EXISTS (SELECT * FROM sysobjects WHERE id = OBJECT_ID(N'[dbo].[catalogSolutions]') AND type in (N'U'))
DROP TABLE [dbo].[catalogSolutions]
GO
CREATE TABLE [dbo].[catalogSolutions]
(
	[id]			[smallint]				NOT NULL IDENTITY(1, 1),
	[name]			[nvarchar](128)			NOT NULL,
	[contact]		[nvarchar](256)			NULL,
	[details]		[nvarchar](512)			NULL,
	CONSTRAINT [PK_catalogSolutions] PRIMARY KEY  CLUSTERED 
	(
		[id]
	) ,
	CONSTRAINT [UK_catalogSolutions_Name] UNIQUE  NONCLUSTERED 
	(
		[name]
	) 
) 
GO

-----------------------------------------------------------------------------------------------------
RAISERROR('		...insert default data', 10, 1) WITH NOWAIT
GO
SET NOCOUNT ON
GO
INSERT	INTO [dbo].[catalogSolutions]([name])
		SELECT 'Default'
GO
