-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 19.08.2015
-- Module			 : Database Maintenance Plan 
-- Description		 : run DBCC CHECKDB for all databases on the server
-- ============================================================================
USE [$(dbName)]
GO
DECLARE @databaseName [sysname]

DECLARE crsDatabases CURSOR LOCAL FAST_FORWARD FOR	SELECT [name] 
							FROM master.dbo.sysdatabases
							WHERE [status] <> 0
								   AND CASE WHEN [status] & 32 = 32 THEN 'LOADING'
											WHEN [status] & 64 = 64 THEN 'PRE RECOVERY'
											WHEN [status] & 128 = 128 THEN 'RECOVERING'
											WHEN [status] & 256 = 256 THEN 'NOT RECOVERED'
											WHEN [status] & 512 = 512 THEN 'OFFLINE'
											WHEN [status] & 2097152 = 2097152 THEN 'STANDBY'
											WHEN [status] & 1024 = 1024 THEN 'READ ONLY'
											WHEN [status] & 2048 = 2048 THEN 'DBO USE ONLY'
											WHEN [status] & 4096 = 4096 THEN 'SINGLE USER'
											WHEN [status] & 32768 = 32768 THEN 'EMERGENCY MODE'
											WHEN [status] & 4194584 = 4194584 THEN 'SUSPECT'
											ELSE 'ONLINE'
									END IN ('ONLINE', 'READ ONLY')
OPEN crsDatabases
FETCH NEXT FROM crsDatabases INTO @databaseName
WHILE @@FETCH_STATUS=0
	begin
		EXEC [dbo].[usp_mpDatabaseConsistencyCheck]	@sqlServerName			= @@SERVERNAME,
													@dbName					= @databaseName,
													@tableSchema			= '%',
													@tableName				= '%',
													@flgActions				= 1,
													@flgOptions				= DEFAULT,
													@debugMode				= DEFAULT

		FETCH NEXT FROM crsDatabases INTO @databaseName
	end
CLOSE crsDatabases
DEALLOCATE crsDatabases
GO
