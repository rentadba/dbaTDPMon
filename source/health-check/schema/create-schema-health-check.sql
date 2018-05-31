-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 14.10.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

IF NOT EXISTS(SELECT * FROM sys.schemas WHERE [name] = 'health-check' AND [principal_id] IN (SELECT [principal_id] FROM sys.database_principals WHERE [name] = 'dbo'))
	begin
		RAISERROR('Create schema: [health-check]', 10, 1) WITH NOWAIT
		EXEC sp_executesql N'CREATE SCHEMA [health-check] AUTHORIZATION [dbo]'
	end
GO
