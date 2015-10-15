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
RAISERROR('Create table: [report].[hardcodedFilters]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sysobjects WHERE id = OBJECT_ID(N'[report].[hardcodedFilters]') AND type in (N'U'))
DROP TABLE [report].[hardcodedFilters]
GO

CREATE TABLE [report].[hardcodedFilters] 
(
	[id]					[smallint] IDENTITY (1, 1)	NOT NULL,
	[module]				[varchar](32)	NOT NULL,
	[object_name]			[sysname]		NOT NULL,
	[filter_pattern]		[nvarchar](256)	NOT	NULL,
	[active]				[bit]			NOT NULL CONSTRAINT [DF_hardcodedFilters_Active] DEFAULT (1),
	CONSTRAINT [PK_hardcodedFilters] PRIMARY KEY  CLUSTERED 
	(
		[id]
	) ON [PRIMARY],
	CONSTRAINT [UK_hardcodedFilters_Name] UNIQUE  NONCLUSTERED 
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
INSERT	INTO [report].[hardcodedFilters] ([module], [object_name], [filter_pattern], [active])
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%Attempting to cycle errorlog%', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%error%log has been reinitialized%', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%without errors%', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%found 0 errors and repaired 0 errors%', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%Log was backed up%', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%Log backed up%', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%Log was restored%', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%Database was backed up%', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%Database backed up%', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%Database differential changes backed up%', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%Database differential changes were backed up.%', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%BACKUP DATABASE WITH DIFFERENTIAL successfully%', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%BACKUP % successfully processed % pages%', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%This is an informational message%user action is required%', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%Analysis of database%complete (approximately%more seconds)%', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%Microsoft Corporation%', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%Microsoft SQL Server%', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%All rights reserved.%', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%Server process ID is%', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%System Manufacturer:%', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%Authentication mode is%', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%Logging SQL Server messages in file%', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%Registry startup parameters:%', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%Command Line Startup Parameters:%', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%SQL Server is%', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%FILESTREAM: effective level = %, configured level = %, file system access share name = %', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%Server name is %', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%Clearing tempdb database.%', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%A self-generated certificate was successfully loaded for encryption.%', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%SQL server listening %', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%Server is listening %', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%Server % provider is ready to accept connection%', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%Dedicated admin connection support was established for listening%', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%The SQL Server Network Interface library successfully registered%', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%Resource governor reconfiguration succeeded.%', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%The % protocol transport is disabled or not configured%', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%The % endpoint is in disabled or stopped state.%', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%Service Broker manager has started.%', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%Using conventional memory in the memory manager.%', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%Software Usage Metrics is disabled.%', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%Using % version %', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%CLR version % loaded%', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%Address Windowing Extensions enabled.%', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%SQL Trace ID 1 was started by login "sa".%', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%A new instance of the full-text filter daemon host process has been successfully started.%', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%Attempting to initialize Distributed Transaction Coordinator.%', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%Informational: No full-text supported languages found.%', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%Starting up database%', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%The database % is marked RESTORING and is in a state that does not allow recovery to be run.%', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%Setting database option % to % for database %', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%Configuration option % changed from % to %. Run the RECONFIGURE statement to install.%', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%SQL Server blocked access to procedure ''sys.xp_cmdshell'' of component ''xp_cmdshell''%', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%Error: 18456, Severity: 14, State: %', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%Login failed for user %', 1 UNION ALL
		SELECT 'health-check', 'statsSQLServerErrorlogDetails', '%SQL Trace%', 1 
GO
