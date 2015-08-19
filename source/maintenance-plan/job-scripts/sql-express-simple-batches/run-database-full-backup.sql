-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 19.08.2015
-- Module			 : Database Maintenance Plan 
-- Description		 : performs full backup for all databases on the server
-- ============================================================================
USE [$(dbName)]
GO
DECLARE @databaseName [sysname]

DECLARE crsDatabases CURSOR LOCAL FAST_FORWARD FOR	SELECT [name] 
							FROM master.dbo.sysdatabases
OPEN crsDatabases
FETCH NEXT FROM crsDatabases INTO @databaseName
WHILE @@FETCH_STATUS=0
	begin
		EXEC [dbo].[usp_mpDatabaseBackup]	@sqlServerName		= @@SERVERNAME,
											@dbName				= @databaseName,
											@backupLocation		= DEFAULT,
											@flgActions			= 1,	
											@flgOptions			= DEFAULT,	
											@retentionDays		= DEFAULT,
											@executionLevel		= DEFAULT,
											@debugMode			= DEFAULT

		FETCH NEXT FROM crsDatabases INTO @databaseName
	end
CLOSE crsDatabases
DEALLOCATE crsDatabases
GO
	