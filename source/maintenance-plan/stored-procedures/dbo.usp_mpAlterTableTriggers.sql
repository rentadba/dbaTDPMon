RAISERROR('Create procedure: [dbo].[usp_mpAlterTableTriggers]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_mpAlterTableTriggers]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpAlterTableTriggers]
GO

-----------------------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[usp_mpAlterTableTriggers]
		@sqlServerName		[sysname],
		@dbName				[sysname],
		@tableSchema		[sysname] = '%', 
		@tableName			[sysname] = '%',
		@triggerName		[sysname] = '%',
		@flgAction			[bit] = 1,
		@flgOptions			[int] = 2048,
		@executionLevel		[tinyint] = 0,
		@debugMode			[bit] = 0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 2004-2009
-- Module			 : Database Maintenance Scripts
-- ============================================================================

-----------------------------------------------------------------------------------------
-- Input Parameters:
--		@sqlServerName	- name of SQL Server instance to analyze
--		@dbName			- database to be analyzed
--		@tableSchema	- schema that current table belongs to
--		@tableName		- specify table name to be analyzed. default = %, all tables will be analyzed
--		@flgAction:		 1	- Enable Triggers (default)
--						 0	- Disable Triggers
--		@flgOptions:	 8  - Stop execution if an error occurs. Default behaviour is to print error messages and continue execution
--					  2048  - send email when a error occurs (default)
--		@debugMode:		 1 - print dynamic SQL statements 
--						 0 - no statements will be displayed (default)
-----------------------------------------------------------------------------------------
-- Return : 
-- 1 : Succes  -1 : Fail 
-----------------------------------------------------------------------------------------

DECLARE		@queryToRun   				[nvarchar](max),
			@objectName				[varchar](512),
			@childObjectName		[sysname],
			@crtTableSchema			[sysname],
			@crtTableName 			[sysname],
			@crtTriggerName			[sysname],
			@errorCode				[int],
			@tmpFlgOptions			[smallint],
			@nestedExecutionLevel	[tinyint]

SET NOCOUNT ON

-- { sql_statement | statement_block }
BEGIN TRY
		SET @errorCode	 = 0

		---------------------------------------------------------------------------------------------
		--get tables list	
		IF object_id('tempdb..#tmpTableList') IS NOT NULL DROP TABLE #tmpTableList
		CREATE TABLE #tmpTableList 
				(
					[table_schema]	[sysname],
					[table_name]	[sysname]
				)

		SET @queryToRun = CASE WHEN @sqlServerName=@@SERVERNAME THEN N'USE ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '; ' ELSE N'' END + N'
						SELECT TABLE_SCHEMA, TABLE_NAME 
						FROM ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'INFORMATION_SCHEMA.TABLES
						WHERE	TABLE_TYPE = ''BASE TABLE'' 
								AND TABLE_NAME LIKE ''' + [dbo].[ufn_getObjectQuoteName](@tableName, 'sql') + N''' 
								AND TABLE_SCHEMA LIKE ''' + [dbo].[ufn_getObjectQuoteName](@tableSchema, 'sql') + N''''
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		INSERT	INTO #tmpTableList ([table_schema], [table_name])
				EXEC sp_executesql  @queryToRun

		---------------------------------------------------------------------------------------------
		IF EXISTS(SELECT 1 FROM #tmpTableList)
			begin
				IF object_id('tempdb..#tmpTableToAlterTriggers') IS NOT NULL DROP TABLE #tmpTableToAlterTriggers
				CREATE TABLE #tmpTableToAlterTriggers 
							(
								[TriggerName]	[sysname]
							)

				DECLARE crsTableList CURSOR LOCAL FAST_FORWARD FOR	SELECT	[table_schema], [table_name]
																	FROM	#tmpTableList
																	ORDER BY [table_schema], [table_name]
				OPEN crsTableList
				FETCH NEXT FROM crsTableList INTO @crtTableSchema, @crtTableName

				WHILE @@FETCH_STATUS=0
					begin
						SET @queryToRun= CASE WHEN @flgAction=1  THEN 'Enable'
																ELSE 'Disable'
										END + ' triggers for: ' + [dbo].[ufn_getObjectQuoteName](@crtTableSchema, 'quoted') + N'.' + [dbo].[ufn_getObjectQuoteName](@crtTableName, 'quoted')
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						--if current action is to disable triggers, will get only enabled triggers
						--if current action is to enable triggers, will get only disabled triggers
						SET @queryToRun=CASE WHEN @sqlServerName=@@SERVERNAME THEN N'USE ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '; ' ELSE N'' END + N'
									SELECT DISTINCT st.[name]
									FROM ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[triggers] st
									INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[objects] so ON so.[object_id] = st.[parent_id] 
									INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[schemas] sch ON sch.[schema_id] = so.[schema_id] 
									WHERE	so.[name]=''' + [dbo].[ufn_getObjectQuoteName](@crtTableName, 'sql') + '''
											AND sch.[name] = ''' + [dbo].[ufn_getObjectQuoteName](@crtTableSchema, 'sql') + '''
											AND st.[is_disabled]=' + CAST(@flgAction AS [varchar]) + '
											AND st.[is_ms_shipped] = 0
											AND st.[name] LIKE ''' + [dbo].[ufn_getObjectQuoteName](@triggerName, 'sql') + ''''
						SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
						IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

						TRUNCATE TABLE #tmpTableToAlterTriggers
						INSERT	INTO #tmpTableToAlterTriggers([TriggerName])
								EXEC sp_executesql  @queryToRun
								
						DECLARE crsTableToAlterTriggers CURSOR	LOCAL FAST_FORWARD FOR	SELECT DISTINCT [TriggerName]
																						FROM #tmpTableToAlterTriggers
																						ORDER BY [TriggerName]
						OPEN crsTableToAlterTriggers
						FETCH NEXT FROM crsTableToAlterTriggers INTO @crtTriggerName
						WHILE @@FETCH_STATUS=0
							begin
								SET @queryToRun= @crtTriggerName
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

								SET @queryToRun=N'ALTER TABLE ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + N'.' + [dbo].[ufn_getObjectQuoteName](@crtTableSchema, 'quoted') + N'.' + [dbo].[ufn_getObjectQuoteName](@crtTableName, 'quoted') + 
													CASE WHEN @flgAction=1  THEN N'ENABLE'
																			ELSE N'DISABLE'
													END + N' TRIGGER ' + [dbo].[ufn_getObjectQuoteName](@crtTriggerName, 'quoted')
								IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

								--
								SET @objectName = [dbo].[ufn_getObjectQuoteName](@crtTableSchema, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](@crtTableName, 'quoted')
								SET @childObjectName = [dbo].[ufn_getObjectQuoteName](@crtTriggerName, 'quoted')
								SET @nestedExecutionLevel = @executionLevel + 1

								EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
																				@dbName			= @dbName,
																				@objectName		= @objectName,
																				@childObjectName= @childObjectName,
																				@module			= 'dbo.usp_mpAlterTableTriggers',
																				@eventName		= 'database maintenance - alter triggers',
																				@queryToRun  	= @queryToRun,
																				@flgOptions		= @flgOptions,
																				@executionLevel	= @nestedExecutionLevel,
																				@debugMode		= @debugMode

								FETCH NEXT FROM crsTableToAlterTriggers INTO @crtTriggerName
							end
						CLOSE crsTableToAlterTriggers
						DEALLOCATE crsTableToAlterTriggers
											
						FETCH NEXT FROM crsTableList INTO @crtTableSchema, @crtTableName
					end
				CLOSE crsTableList
				DEALLOCATE crsTableList
			end

		---------------------------------------------------------------------------------------------
		--delete all temporary tables
		IF object_id('#tmpTableList') IS NOT NULL DROP TABLE #tmpTableList
END TRY

BEGIN CATCH
DECLARE 
        @ErrorMessage    NVARCHAR(4000),
        @ErrorNumber     INT,
        @ErrorSeverity   INT,
        @ErrorState      INT,
        @ErrorLine       INT,
        @ErrorProcedure  NVARCHAR(200);
    -- Assign variables to error-handling functions that 
    -- capture information for RAISERROR.
	SET @errorCode = -1

    SELECT 
        @ErrorNumber = ERROR_NUMBER(),
        @ErrorSeverity = ERROR_SEVERITY(),
        @ErrorState = CASE WHEN ERROR_STATE() BETWEEN 1 AND 127 THEN ERROR_STATE() ELSE 1 END ,
        @ErrorLine = ERROR_LINE(),
        @ErrorProcedure = ISNULL(ERROR_PROCEDURE(), '-');
	-- Building the message string that will contain original
    -- error information.
    SELECT @ErrorMessage = 
        N'Error %d, Level %d, State %d, Procedure %s, Line %d, ' + 
            'Message: '+ ERROR_MESSAGE();
    -- Raise an error: msg_str parameter of RAISERROR will contain
    -- the original error information.
    RAISERROR 
        (
        @ErrorMessage, 
        @ErrorSeverity, 
        @ErrorState,               
        @ErrorNumber,    -- parameter: original error number.
        @ErrorSeverity,  -- parameter: original error severity.
        @ErrorState,     -- parameter: original error state.
        @ErrorProcedure, -- parameter: original error procedure name.
        @ErrorLine       -- parameter: original error line number.
        );

        -- Test XACT_STATE:
        -- If 1, the transaction is committable.
        -- If -1, the transaction is uncommittable and should 
        --     be rolled back.
        -- XACT_STATE = 0 means that there is no transaction and
        --     a COMMIT or ROLLBACK would generate an error.

    -- Test if the transaction is uncommittable.
    IF (XACT_STATE()) = -1
    BEGIN
        PRINT
            N'The transaction is in an uncommittable state.' +
            'Rolling back transaction.'
        ROLLBACK TRANSACTION 
   END;

END CATCH

RETURN @errorCode
GO
