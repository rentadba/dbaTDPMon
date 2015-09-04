-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 04.09.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-----------------------------------------------------------------------------------------------------
--catalog for hardcoded filters
-----------------------------------------------------------------------------------------------------
RAISERROR('Create table: [dbo].[catalogHardcodedFilters]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sysobjects WHERE id = OBJECT_ID(N'[dbo].[catalogHardcodedFilters]') AND type in (N'U'))
DROP TABLE [dbo].[catalogHardcodedFilters]
GO

CREATE TABLE [dbo].[catalogHardcodedFilters] 
(
	[id]					[smallint] IDENTITY (1, 1)	NOT NULL,
	[module]				[varchar](32)	NOT NULL,
	[object_name]			[sysname]		NOT NULL,
	[filter_pattern]		[nvarchar](256)	NOT	NULL,
	[active]				[bit]			NOT NULL CONSTRAINT [DF_catalogHardcodedFilters_Active] DEFAULT (1),
	CONSTRAINT [PK_catalogHardcodedFilters] PRIMARY KEY  CLUSTERED 
	(
		[id]
	) ON [PRIMARY],
	CONSTRAINT [UK_catalogHardcodedFilters_Name] UNIQUE  NONCLUSTERED 
	(
		[module],
		[object_name],
		[filter_pattern]
	) ON [PRIMARY]
)  ON [PRIMARY]
GO

-----------------------------------------------------------------------------------------------------
RAISERROR('		...insert default data', 10, 1) WITH NOWAIT
GO
SET NOCOUNT ON
GO
INSERT	INTO [dbo].[catalogHardcodedFilters] ([module], [object_name], [filter_pattern], [active])
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%Attempting to cycle errorlog%', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%error%log has been reinitialized%', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%without errors%', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%found 0 errors and repaired 0 errors%', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%Log was backed up%', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%Log backed up%', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%Log was restored%', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%Database was backed up%', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%Database backed up%', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%Database differential changes backed up%', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%Database differential changes were backed up.%', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%BACKUP DATABASE WITH DIFFERENTIAL successfully%', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%BACKUP % successfully processed % pages%', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%This is an informational message%user action is required%', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%Analysis of database%complete (approximately%more seconds)%', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%Microsoft Corporation%', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%Microsoft SQL Server%', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%All rights reserved.%', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%Server process ID is%', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%System Manufacturer:%', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%Authentication mode is%', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%Logging SQL Server messages in file%', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%Registry startup parameters:%', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%Command Line Startup Parameters:%', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%SQL Server is%', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%FILESTREAM: effective level = %, configured level = %, file system access share name = %', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%Server name is %', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%Clearing tempdb database.%', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%A self-generated certificate was successfully loaded for encryption.%', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%SQL server listening %', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%Server is listening %', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%Server % provider is ready to accept connection%', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%Dedicated admin connection support was established for listening%', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%The SQL Server Network Interface library successfully registered%', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%Resource governor reconfiguration succeeded.%', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%The % protocol transport is disabled or not configured%', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%The % endpoint is in disabled or stopped state.%', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%Service Broker manager has started.%', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%Using conventional memory in the memory manager.%', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%Software Usage Metrics is disabled.%', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%Using % version %', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%CLR version % loaded%', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%Address Windowing Extensions enabled.%', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%SQL Trace ID 1 was started by login "sa".%', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%A new instance of the full-text filter daemon host process has been successfully started.%', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%Attempting to initialize Distributed Transaction Coordinator.%', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%Informational: No full-text supported languages found.%', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%Starting up database%', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%The database % is marked RESTORING and is in a state that does not allow recovery to be run.%', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%Setting database option % to % for database %', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%Configuration option % changed from % to %. Run the RECONFIGURE statement to install.%', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%SQL Server blocked access to procedure ''sys.xp_cmdshell'' of component ''xp_cmdshell''%', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%Error: 18456, Severity: 14, State: %', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%Login failed for user %', 1 UNION ALL
		SELECT 'health-check', 'dbo.statsSQLServerErrorlogDetails', '%SQL Trace%', 1 
		