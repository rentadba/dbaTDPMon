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
	),
	CONSTRAINT [UK_hardcodedFilters_Name] UNIQUE  NONCLUSTERED 
	(
		[module],
		[object_name],
		[filter_pattern]
	)
)
GO

-----------------------------------------------------------------------------------------------------
RAISERROR('		...insert default data', 10, 1) WITH NOWAIT
GO
SET NOCOUNT ON
GO
INSERT	INTO [report].[hardcodedFilters] ([module], [object_name], [filter_pattern], [active])
		SELECT 'health-check', 'statsErrorlogDetails', '%Attempting to cycle errorlog%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%error%log has been reinitialized%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%without errors%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%found 0 errors and repaired 0 errors%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%Log was backed up%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%Log backed up%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%Log was restored%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%Database was backed up%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%Database backed up%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%Database differential changes backed up%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%Database differential changes were backed up.%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%BACKUP DATABASE WITH DIFFERENTIAL successfully%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%BACKUP % successfully processed % pages%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%This is an informational message%user action is required%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%Analysis of database%complete (approximately%more seconds)%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%Microsoft Corporation%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', 'Microsoft SQL Server%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%All rights reserved.%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%Server process ID is%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%System Manufacturer:%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%Authentication mode is%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%Logging SQL Server messages in file%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%Registry startup parameters:%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%Command Line Startup Parameters:%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%SQL Server is%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%FILESTREAM: effective level = %, configured level = %, file system access share name = %', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%Server name is %', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%Clearing tempdb database.%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%A self-generated certificate was successfully loaded for encryption.%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%SQL server listening %', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%Server is listening %', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%Server % provider is ready to accept connection%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%Dedicated admin connection support was established for listening%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%The SQL Server Network Interface library successfully registered%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%Resource governor reconfiguration succeeded.%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%The % protocol transport is disabled or not configured%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%The % endpoint is in disabled or stopped state.%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%Service Broker manager has started.%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%Using conventional memory in the memory manager.%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%Software Usage Metrics is disabled.%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%Using % version %', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%CLR version % loaded%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%Address Windowing Extensions enabled.%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%SQL Trace ID 1 was started by login "sa".%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%A new instance of the full-text filter daemon host process has been successfully started.%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%Attempting to initialize Distributed Transaction Coordinator.%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%Informational: No full-text supported languages found.%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%Starting up database%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%The database % is marked RESTORING and is in a state that does not allow recovery to be run.%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%Setting database option % to % for database %', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%Configuration option % changed from % to %. Run the RECONFIGURE statement to install.%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%SQL Server blocked access to procedure ''sys.xp_cmdshell'' of component ''xp_cmdshell''%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%Error: 18456, Severity: 14, State: %', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%Login failed for user %', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%SQL Trace%', 1  UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%UTC adjustment%', 1  UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%Default collation:%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%Error: 18470, Severity: 14, State: 1.%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%Error: 17892, Severity: 20, State: 1.%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%Logon failed for login ''%'' due to trigger execution.%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%The transaction ended in the trigger. The batch has been aborted.%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%Error: 18487, Severity: 14, State: 1.%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%Error: 3609, Severity: 16, State: 2.%', 1  UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%FlushCache: cleaned up % bufs with % writes in % ms%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%average writes per second%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%average throughput%I/O saturation%context switches%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%last target outstanding%avgWriteLatency%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%DbMgrPartnerCommitPolicy::SetSyncAndRecoveryPoint:%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%Process ID % was killed by hostname %, host process ID %.%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%Synchronize Database % with Resource Database.%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%Restore is complete on database %. The database is now available.%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%AppDomain % created.%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%AppDomain % is marked for unload due to common language runtime (CLR) or security data definition language (DDL) operations.%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%Buffer Pool scan took % seconds: database ID %, command ''DBCC TABLE CHECK''%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%Parallel redo is started for database % with worker pool size%', 1 UNION ALL
		SELECT 'health-check', 'statsErrorlogDetails', '%Parallel redo is shutdown for database % with worker pool size%', 1 UNION ALL

		SELECT 'health-check', 'statsOSEventLogs', '%Logon failed for login%', 1 UNION ALL
		SELECT 'health-check', 'statsOSEventLogs', '%Unable to retrieve steps for job%', 1 UNION ALL
		SELECT 'health-check', 'statsOSEventLogs', '%Unable to determine if the owner%of job%has server access%', 1
GO


