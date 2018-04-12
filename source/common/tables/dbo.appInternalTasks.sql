-- ============================================================================
-- Copyright (c) 2004-2018 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 11.04.2018
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-----------------------------------------------------------------------------------------------------
--
-----------------------------------------------------------------------------------------------------
RAISERROR('Create table: [dbo].[appInternalTasks]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sysobjects WHERE id = OBJECT_ID(N'[dbo].[appInternalTasks]') AND type in (N'U'))
DROP TABLE [dbo].[appInternalTasks]
GO

CREATE TABLE [dbo].[appInternalTasks]
(
	[id]					[bigint]		NOT NULL,
	[descriptor]			[varchar](256)	NOT NULL,
	[task_name]				[varchar](256)	NOT NULL,
	[flg_actions]			[smallint]		NULL,
	CONSTRAINT [PK_appInternalTasks] PRIMARY KEY  CLUSTERED 
	(
		[id]
	) ON [PRIMARY],
	CONSTRAINT [UK_appInternalTasks] UNIQUE
	(
		  [descriptor]
		, [task_name]
	) ON [PRIMARY]
) ON [PRIMARY]
GO
