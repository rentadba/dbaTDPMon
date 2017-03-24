-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 17.01.2015
-- Module			 : SQL Server 2000/2005/2008/2008R2/2012+
-- Description		 : 
-------------------------------------------------------------------------------
IF EXISTS(SELECT 1 FROM sysdatabases WHERE [name]='$(dbName)')
	begin
		PRINT 'Killing database connections...'
		DECLARE @queryToRun [nvarchar](4000)

		SET @queryToRun=N''

		SELECT @queryToRun = @queryToRun + 'KILL ' + CAST([spid] AS [nvarchar]) + ';'
		FROM sysprocesses
		WHERE [dbid]=DB_ID('$(dbName)')

		EXEC (@queryToRun)

		DROP DATABASE [$(dbName)]
		PRINT '"$(dbName)" database dropped.'
	end
GO	
