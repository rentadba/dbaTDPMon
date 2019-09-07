-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 05.12.2014
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-----------------------------------------------------------------------------------------------------
--catalog for Instance Names
-----------------------------------------------------------------------------------------------------
RAISERROR('Create table: [dbo].[catalogInstanceNames]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sysobjects WHERE id = OBJECT_ID(N'[dbo].[catalogInstanceNames]') AND type in (N'U'))
DROP TABLE [dbo].[catalogInstanceNames]
GO

CREATE TABLE [dbo].[catalogInstanceNames] 
(
	[id]						[smallint] IDENTITY (1, 1)	NOT NULL,
	[machine_id]				[smallint]		NOT NULL,
	[project_id]				[smallint]		NOT NULL,
	[name]						[sysname]		NOT NULL,
	[version]					[varchar](30)	NULL,
	[edition]					[varchar](256)	NULL,
	[engine]					[int]			NULL,
	[active]					[bit]			NOT NULL CONSTRAINT [DF_catalogInstanceNames_Active] DEFAULT (1),
	[is_clustered]				[bit]			NOT NULL CONSTRAINT [DF_catalogInstanceNames_IsClustered] DEFAULT (0),
	[cluster_node_machine_id]	[smallint]		NULL,
	[last_refresh_date_utc]		[datetime]		NULL,
	CONSTRAINT [PK_catalogInstanceNames] PRIMARY KEY  CLUSTERED 
	(
		[id],
		[machine_id],
		[project_id]
	) ON [PRIMARY],
	CONSTRAINT [UK_catalogInstanceNames_Name] UNIQUE  NONCLUSTERED 
	(
		[name],
		[machine_id]
	) ON [PRIMARY],
	CONSTRAINT [UK_catalogInstanceNames_ID] UNIQUE  NONCLUSTERED 
	(
		[id],
		[project_id]
	) ON [PRIMARY],	
	CONSTRAINT [FK_catalogInstanceNames_catalogProjects] FOREIGN KEY 
	(
		[project_id]
	) 
	REFERENCES [dbo].[catalogProjects] 
	(
		[id]
	),
	CONSTRAINT [FK_catalogInstanceNames_catalogMachineNames] FOREIGN KEY 
	(
		[machine_id],
		[project_id]
	) 
	REFERENCES [dbo].[catalogMachineNames] 
	(
		[id],
		[project_id]
	),
	CONSTRAINT [FK_catalogInstanceNames_catalogMachineNames_Active] FOREIGN KEY 
	(
		[cluster_node_machine_id],
		[project_id]
	)
	REFERENCES [dbo].[catalogMachineNames] 
	(
		[id],
		[project_id]
	)
)  ON [PRIMARY]
GO

CREATE INDEX [IX_catalogInstanceNames_ProjectID] ON [dbo].[catalogInstanceNames]([project_id]) ON [PRIMARY]
GO
CREATE INDEX [IX_catalogInstanceNames_MachineID] ON [dbo].[catalogInstanceNames]([machine_id], [project_id]) ON [PRIMARY]
GO
CREATE INDEX [IX_catalogInstanceNames_ClusterNodeMachineID] ON [dbo].[catalogInstanceNames]([cluster_node_machine_id], [project_id]) ON [PRIMARY]
GO
