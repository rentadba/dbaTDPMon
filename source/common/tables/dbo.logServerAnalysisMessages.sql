-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 05.12.2014
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-----------------------------------------------------------------------------------------------------
--log for discovery messages
-----------------------------------------------------------------------------------------------------
RAISERROR('Create table: [dbo].[logServerAnalysisMessages]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[logServerAnalysisMessages]') AND type in (N'U'))
DROP TABLE [dbo].[logServerAnalysisMessages]
GO

CREATE TABLE [dbo].[logServerAnalysisMessages]
(
	[id]					[int]	 IDENTITY (1, 1)	NOT NULL,
	[instance_id]			[smallint]		NOT NULL,
	[project_id]			[smallint]		NOT NULL,
	[event_date_utc]		[datetime]		NOT NULL,
	[descriptor]			[varchar](256)	NULL,
	[message]				[varchar](max)	NULL,
	CONSTRAINT [PK_logServerAnalysisMessages] PRIMARY KEY  CLUSTERED 
	(
		[id],
		[instance_id]
	) ON [FG_Statistics_Data],
	CONSTRAINT [FK_logServerAnalysisMessages_catalogProjects] FOREIGN KEY 
	(
		[project_id]
	) 
	REFERENCES [dbo].[catalogProjects] 
	(
		[id]
	),
	CONSTRAINT [FK_logServerAnalysisMessages_catalogInstanceNames] FOREIGN KEY 
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

CREATE INDEX [IX_logServerAnalysisMessages_InstanceID] ON [dbo].[logServerAnalysisMessages]([instance_id], [project_id]) ON [FG_Statistics_Index]
GO
CREATE INDEX [IX_logServerAnalysisMessages_ProjecteID] ON [dbo].[logServerAnalysisMessages]([project_id]) ON [FG_Statistics_Index]
GO

