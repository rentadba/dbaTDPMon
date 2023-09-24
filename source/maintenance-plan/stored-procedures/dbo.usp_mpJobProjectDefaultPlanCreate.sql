RAISERROR('Create procedure: [dbo].[usp_mpJobProjectDefaultPlanCreate]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_mpJobProjectDefaultPlanCreate]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpJobProjectDefaultPlanCreate]
GO

CREATE PROCEDURE [dbo].[usp_mpJobProjectDefaultPlanCreate]
		@projectCode			[varchar](32),
		@sqlServerNameFilter	[sysname]='%',
		@enableJobs				[bit] = 1,
		@debugMode				[bit] = 0
/* WITH ENCRYPTION */
AS
SET NOCOUNT ON

-- ============================================================================
-- Copyright (c) 2004-2019 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 14.12.2019
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

DECLARE @jobName		[sysname]
		
SET @jobName = DB_NAME() + ' - Database Backup - Log - ' + @projectCode
EXEC [dbo].[usp_mpJobSQLAgentCreate]  @jobName = @jobName
									, @projectCode = @projectCode
									, @sqlServerNameFilter = @sqlServerNameFilter
									, @jobDescriptorList = 'dbo.usp_mpDatabaseBackup(Log)'
									, @flgActions = 8192
									, @skipDatabasesList = NULL
									, @recreateMode = 0
									, @enableJobs = 1
									, @debugMode = 0

SET @jobName = DB_NAME() + ' - Database Backup - Full and Diff - ' + @projectCode
EXEC [dbo].[usp_mpJobSQLAgentCreate]  @jobName = @jobName
									, @projectCode = @projectCode
									, @sqlServerNameFilter = @sqlServerNameFilter
									, @jobDescriptorList = 'dbo.usp_mpDatabaseBackup(Data)'
									, @flgActions = 7168
									, @skipDatabasesList = NULL
									, @recreateMode = 0
									, @enableJobs = 1
									, @debugMode = 0

SET @jobName = DB_NAME() + ' - Database Maintenance - User DBs - ' + @projectCode
EXEC [dbo].[usp_mpJobSQLAgentCreate]  @jobName = @jobName
									, @projectCode = @projectCode
									, @sqlServerNameFilter = @sqlServerNameFilter
									, @jobDescriptorList = 'dbo.usp_mpDatabaseConsistencyCheck,dbo.usp_mpDatabaseOptimize,dbo.usp_mpDatabaseShrink'
									, @flgActions = 1023
									, @skipDatabasesList = NULL
									, @recreateMode = 0
									, @enableJobs = 1
									, @debugMode = 0
GO
