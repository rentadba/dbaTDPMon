RAISERROR('Create procedure: [dbo].[usp_mpCheckAndRevertInternalActions]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sysobjects 
	     WHERE [id] = OBJECT_ID(N'[dbo].[usp_mpCheckAndRevertInternalActions]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpCheckAndRevertInternalActions]
GO

-----------------------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[usp_mpCheckAndRevertInternalActions]
		@sqlServerName			[sysname],
		@flgOptions				[int]	= 12941,
		@executionLevel			[tinyint]	=     0,
		@debugMode				[bit]		=     0
/* WITH ENCRYPTION */
AS

DECLARE   @crtDatabaseName			[sysname]
		, @crtSchemaName			[sysname]
		, @crtObjectName			[sysname]
		, @crtChildObjectName		[sysname]
		, @queryToRun				[nvarchar](1024)
		, @nestExecutionLevel		[tinyint]
		, @affectedDependentObjects	[nvarchar](max)

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 19.02.2015
-- Module			 : Database Maintenance Plan 
-- Description		 : Optimization and Maintenance Checks
-- ============================================================================
-----------------------------------------------------------------------------------------
-- Input Parameters:
--		@flgOptions		 1  - Compact large objects (LOB) (default)
--						 2  - Rebuild index by create with drop existing on (default)
--						 4  - Rebuild all non-clustered indexes when rebuild clustered indexes (default)
--						 8  - Disable non-clustered index before rebuild (save space) (default)
--						16  - Disable all foreign key constraints that reffered current table before rebuilding clustered indexes
--						32  - Stop execution if an error occurs. Default behaviour is to print error messages and continue execution
--					    64  - When enabling foreign key constraints, do no check values. Default behaviour is to enabled foreign key constraint with check option
--					  4096  - rebuild/reorganize indexes using ONLINE=ON, if applicable (default)
--		@debugMode		 1 - print dynamic SQL statements / 0 - no statements will be displayed
-----------------------------------------------------------------------------------------
-- Return : 
-- 1 : Succes  -1 : Fail 
-----------------------------------------------------------------------------------------
/*
	--usage sample
	EXEC [dbo].[usp_mpCheckAndRevertInternalActions]	@flgOptions				= DEFAULT,
														@debugMode				= DEFAULT
*/

SET NOCOUNT ON

---------------------------------------------------------------------------------------------
--get configuration values
---------------------------------------------------------------------------------------------
DECLARE @queryLockTimeOut [int]
SELECT	@queryLockTimeOut=[value] 
FROM	[dbo].[appConfigurations] 
WHERE	[name]='Default lock timeout (ms)'
		AND [module] = 'common'

--reset configuration value
UPDATE [dbo].[appConfigurations]
	SET [value]='-1'
WHERE	[name]='Default lock timeout (ms)'
		AND [module] = 'common'

-----------------------------------------------------------------------------------------
SET @queryToRun=N'Rebuilding previously disabled indexes...'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

SET @nestExecutionLevel = @executionLevel + 1
DECLARE crslogInternalAction CURSOR LOCAL FAST_FORWARD FOR	SELECT	[database_name], [schema_name], [object_name], [child_object_name]
															FROM	[maintenance-plan].[logInternalAction]
															WHERE	[name] = 'index-made-disable'
																	AND [server_name] = @sqlServerName
OPEN crslogInternalAction
FETCH NEXT FROM crslogInternalAction INTO @crtDatabaseName, @crtSchemaName, @crtObjectName, @crtChildObjectName
WHILE @@FETCH_STATUS=0
	begin
		EXEC [dbo].[usp_mpAlterTableIndexes]		@SQLServerName				= @sqlServerName,
													@DBName						= @crtDatabaseName,
													@TableSchema				= @crtSchemaName,
													@TableName					= @crtObjectName,
													@IndexName					= @crtChildObjectName,
													@IndexID					= NULL,
													@PartitionNumber			= DEFAULT,
													@flgAction					= 1,
													@flgOptions					= @flgOptions,
													@MaxDOP						= 1,
													@executionLevel				= @nestExecutionLevel,
													@affectedDependentObjects	= @affectedDependentObjects OUT,
													@debugMode					= @debugMode

		FETCH NEXT FROM crslogInternalAction INTO @crtDatabaseName, @crtSchemaName, @crtObjectName, @crtChildObjectName
	end
CLOSE crslogInternalAction
DEALLOCATE crslogInternalAction


-----------------------------------------------------------------------------------------
SET @queryToRun=N'Rebuilding previously disabled foreign key constraints...'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

DECLARE crslogInternalAction CURSOR LOCAL FAST_FORWARD FOR	SELECT	[database_name], [schema_name], [object_name], [child_object_name]
															FROM	[maintenance-plan].[logInternalAction]
															WHERE	[name] = 'foreign-key-made-disable'
																	AND [server_name] = @sqlServerName
OPEN crslogInternalAction
FETCH NEXT FROM crslogInternalAction INTO @crtDatabaseName, @crtSchemaName, @crtObjectName, @crtChildObjectName
WHILE @@FETCH_STATUS=0
	begin
		EXEC [dbo].[usp_mpAlterTableForeignKeys]	@SQLServerName		= @sqlServerName,
													@DBName				= @crtDatabaseName,
													@TableSchema		= @crtSchemaName,
													@TableName			= @crtObjectName,
													@ConstraintName		= @crtChildObjectName,
													@flgAction			= 1,
													@flgOptions			= @flgOptions,
													@executionLevel		= @nestExecutionLevel,
													@debugMode			= @debugMode
		FETCH NEXT FROM crslogInternalAction INTO @crtDatabaseName, @crtSchemaName, @crtObjectName, @crtChildObjectName
	end
CLOSE crslogInternalAction
DEALLOCATE crslogInternalAction


-----------------------------------------------------------------------------------------
--restore original configuration value
-----------------------------------------------------------------------------------------
UPDATE [dbo].[appConfigurations]
	SET [value]=@queryLockTimeOut
WHERE	[name]='Default lock timeout (ms)'
		AND [module] = 'common'

GO
