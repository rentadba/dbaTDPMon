RAISERROR('Create procedure: [dbo].[usp_mpAlterTableForeignKeys]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_mpAlterTableForeignKeys]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpAlterTableForeignKeys]
GO

-----------------------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[usp_mpAlterTableForeignKeys]
		@sqlServerName		[sysname],
		@dbName				[sysname],
		@tableSchema		[sysname] = '%', 
		@tableName			[sysname] = '%',
		@constraintName		[sysname] = '%',
		@flgAction			[bit] = 1,
		@flgOptions			[int] = 2049,
		@executionLevel		[tinyint] = 0,
		@debugMode			[bit] = 0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 06.01.2010
-- Module			 : Database Maintenance Scripts
-- ============================================================================

-----------------------------------------------------------------------------------------
-- Input Parameters:
--		@sqlServerName	- name of SQL Server instance to analyze
--		@dbName			- database to be analyzed
--		@tableSchema	- schema that current table belongs to
--		@tableName		- specify table name to be analyzed. default = %, all tables will be analyzed
--		@constraintName	- specify constraint name to be enabled/disabled. default all
--		@flgAction:		 1	- Enable Constraints (default)
--						 0	- Disable Constraints
--		@flgOptions:	 1	- Use tables that have foreign key constraints that reffer current table (default)
--						 2	- Use tables that current table foreign key constraints reffer  
--						 4  - Enable constraints with NOCHECK. Default is to enable constraints using CHECK option
--						 8  - Stop execution if an error occurs. Default behaviour is to print error messages and continue execution
--					  2048  - send email when a error occurs (default)
--		@debugMode:		 1 - print dynamic SQL statements 
--						 0 - no statements will be displayed (default)
-----------------------------------------------------------------------------------------
-- Return : 
-- 1 : Succes  -1 : Fail 
-----------------------------------------------------------------------------------------

DECLARE		@queryToRun    				[nvarchar](max),
			@crtTableSchema 		[sysname],
			@crtTableName 			[sysname],
			@tmpSchemaName			[sysname],
			@tmpTableName			[sysname],
			@objectName				[nvarchar](512),
			@childObjectName		[sysname],
			@tmpConstraintName		[sysname],
			@errorCode				[int],
			@tmpFlgAction			[smallint],
			@nestedExecutionLevel	[tinyint]

SET NOCOUNT ON

-- { sql_statement | statement_block }
BEGIN TRY
		SET @errorCode = 0

		---------------------------------------------------------------------------------------------
		--get tables list	
		IF object_id('tempdb..#tmpTableList') IS NOT NULL DROP TABLE #tmpTableList
		CREATE TABLE #tmpTableList 
				(
					[table_schema] [sysname],
					[table_name] [sysname]
				)

		SET @queryToRun = CASE WHEN @sqlServerName=@@SERVERNAME THEN N'USE ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '; ' ELSE N'' END + N'
						SELECT TABLE_SCHEMA, TABLE_NAME 
						FROM ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'INFORMATION_SCHEMA.TABLES 
						WHERE	TABLE_TYPE = ''BASE TABLE'' 
								AND TABLE_NAME LIKE ''' + [dbo].[ufn_getObjectQuoteName](@tableName, 'sql') + ''' 
								AND TABLE_SCHEMA LIKE ''' + [dbo].[ufn_getObjectQuoteName](@tableSchema, 'sql') + ''''
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		INSERT	INTO #tmpTableList ([table_schema], [table_name])
				EXEC (@queryToRun)

		---------------------------------------------------------------------------------------------
		IF EXISTS(SELECT 1 FROM #tmpTableList)
			begin
				IF object_id('tempdb..#tmpTableToAlterConstraints') IS NOT NULL DROP TABLE #tmpTableToAlterConstraints
				CREATE TABLE #tmpTableToAlterConstraints 
							(
								[TableSchema]		[sysname]
							  , [TableName]			[sysname]
							  , [ConstraintName]	[sysname]
							)

				DECLARE crsTableList CURSOR LOCAL FAST_FORWARD FOR	SELECT [table_schema], [table_name]
																	FROM #tmpTableList
																	ORDER BY [table_schema], [table_name]
				OPEN crsTableList
				FETCH NEXT FROM crsTableList INTO @crtTableSchema, @crtTableName
				WHILE @@FETCH_STATUS=0
					begin
						SET @queryToRun= CASE WHEN @flgAction=1	THEN 'Enable'
																ELSE 'Disable'
										END + ' foreign key constraints for: ' + [dbo].[ufn_getObjectQuoteName](@crtTableSchema, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](@crtTableName, 'quoted')
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						--if current action is to disable foreign key constraint, will get only enabled constraints
						--if current action is to enable foreign key constraint, will get only disabled constraints
						IF (@flgOptions & 1 = 1)
							begin
								--list all tables that have foreign key constraints that reffers current table					
								SET @queryToRun=CASE WHEN @sqlServerName=@@SERVERNAME THEN N'USE ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '; ' ELSE N'' END + N'
												SELECT DISTINCT sch.[name] AS [schema_name], so.[name] AS [table_name], sfk.[name] AS [constraint_name]
												FROM ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[objects] so
												INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[schemas]		sch  ON sch.[schema_id] = so.[schema_id]
												INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[foreign_keys]	sfk  ON so.[object_id] = sfk.[parent_object_id]
												INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[objects]		so2  ON sfk.[referenced_object_id] = so2.[object_id]
												INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[schemas]		sch2 ON sch2.[schema_id] = so2.[schema_id]
												WHERE	so2.[name]=''' + [dbo].[ufn_getObjectQuoteName](@crtTableName, 'sql') + '''
														AND sch2.[name] = ''' + [dbo].[ufn_getObjectQuoteName](@crtTableSchema, 'sql') + '''
														AND sfk.[is_disabled]=' + CAST(@flgAction AS [varchar]) + '
														AND sfk.[name] LIKE ''' + [dbo].[ufn_getObjectQuoteName](@constraintName, 'sql') + ''''
								SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
								IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

								INSERT	INTO #tmpTableToAlterConstraints([TableSchema], [TableName], [ConstraintName])
										EXEC (@queryToRun)
							end

						IF (@flgOptions & 2 = 2)
							begin
								--list all tables that current table foreign key constraints reffers 
								SET @queryToRun=CASE WHEN @sqlServerName=@@SERVERNAME THEN N'USE ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '; ' ELSE N'' END + N'
												SELECT DISTINCT sch2.[name] AS [schema_name], so2.[name] AS [table_name], sfk.[name] AS [constraint_name]
												FROM ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[objects] so
												INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[schemas]		sch  ON sch.[schema_id] = so.[schema_id]
												INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[foreign_keys]	sfk ON so.[object_id] = sfk.[referenced_object_id]
												INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[objects]		so2 ON sfk.[parent_object_id] = so2.[object_id]
												INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[schemas]		sch2 ON sch.[schema_id] = so2.[schema_id]
												WHERE	so2.[name]=''' + [dbo].[ufn_getObjectQuoteName](@crtTableName, 'sql') + '''
														AND sch2.[name] = ''' + [dbo].[ufn_getObjectQuoteName](@crtTableSchema, 'sql') + '''
														AND sfk.[is_disabled]=' + CAST(@flgAction AS [varchar])+ '
														AND sfk.[name] LIKE ''' + [dbo].[ufn_getObjectQuoteName](@constraintName, 'sql') + ''''

								SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
								IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

								INSERT	INTO #tmpTableToAlterConstraints ([TableSchema], [TableName], [ConstraintName])
										EXEC (@queryToRun)
							end

						DECLARE crsTableToAlterConstraints CURSOR LOCAL FAST_FORWARD FOR	SELECT DISTINCT [TableSchema], [TableName], [ConstraintName]
																							FROM #tmpTableToAlterConstraints
																							ORDER BY [TableName]						
						OPEN crsTableToAlterConstraints
						FETCH NEXT FROM crsTableToAlterConstraints INTO @tmpSchemaName, @tmpTableName, @tmpConstraintName
						WHILE @@FETCH_STATUS=0
							begin
								SET @queryToRun= [dbo].[ufn_getObjectQuoteName](@tmpSchemaName, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](@tmpTableName, 'quoted')
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

								--enable/disable foreign key constraints
								SET @queryToRun='ALTER TABLE ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](@tmpSchemaName, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](@tmpTableName, 'quoted') +  
												CASE WHEN @flgAction=1	
													 THEN ' WITH ' + 
															CASE WHEN @flgOptions & 4 = 4	THEN 'NOCHECK'
																							ELSE 'CHECK'
															END + ' CHECK '	
													 ELSE ' NOCHECK '
												END + 'CONSTRAINT ' + [dbo].[ufn_getObjectQuoteName](@tmpConstraintName, 'quoted')
								IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

								--
								SET @objectName = [dbo].[ufn_getObjectQuoteName](@tmpSchemaName, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName]( @tmpTableName, 'quoted')
								SET @childObjectName = [dbo].[ufn_getObjectQuoteName](@tmpConstraintName, 'quoted')
								SET @nestedExecutionLevel = @executionLevel + 1

								EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
																				@dbName			= @dbName,
																				@objectName		= @objectName,
																				@childObjectName= @childObjectName,
																				@module			= 'dbo.usp_mpAlterTableForeignKeys',
																				@eventName		= 'database maintenance - alter constraints',
																				@queryToRun  	= @queryToRun,
																				@flgOptions		= @flgOptions,
																				@executionLevel	= @nestedExecutionLevel,
																				@debugMode		= @debugMode

								IF @errorCode=0	
									begin
										/* 0 disable FK -> insert action 1 */
										/* 1 enable FK  -> delete action 2 */
										SET @tmpFlgAction = CASE WHEN @flgAction=1 THEN 2 ELSE 1 END
										EXEC [dbo].[usp_mpMarkInternalAction]		@actionName			= N'foreign-key-made-disable',
																					@flgOperation		= @tmpFlgAction,
																					@server_name		= @sqlServerName,
																					@database_name		= @dbName,
																					@schema_name		= @tmpSchemaName,
																					@object_name		= @tmpTableName,
																					@child_object_name	= @tmpConstraintName
									end
						
								FETCH NEXT FROM crsTableToAlterConstraints INTO @tmpSchemaName, @tmpTableName, @tmpConstraintName
							end
						CLOSE crsTableToAlterConstraints
						DEALLOCATE crsTableToAlterConstraints
						
						FETCH NEXT FROM crsTableList INTO @crtTableSchema, @crtTableName
					end
				CLOSE crsTableList
				DEALLOCATE crsTableList
			end

		---------------------------------------------------------------------------------------------
		--delete all temporary tables
		IF object_id('#tmpTableList') IS NOT NULL DROP TABLE #tmpTableList
		IF object_id('#tmpTableToAlterConstraints') IS NOT NULL DROP TABLE #tmpTableToAlterConstraints
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
