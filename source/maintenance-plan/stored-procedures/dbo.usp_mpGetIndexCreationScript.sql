RAISERROR('Create procedure: [dbo].[usp_mpGetIndexCreationScript]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_mpGetIndexCreationScript]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpGetIndexCreationScript]
GO

-----------------------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[usp_mpGetIndexCreationScript]
		@SQLServerName		[sysname]=@@SERVERNAME,
		@DBName				[sysname],
		@TableSchema		[sysname]='dbo',
		@TableName			[sysname],
		@IndexName			[sysname],
		@IndexID			[int],
		@flgOptions			[int] = 4099,
		@sqlIndexCreate		[nvarchar](max) OUTPUT,
		@executionLevel		[tinyint] = 0,
		@DebugMode			[bit] = 0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date: 07.01.2010
-- Module     : Database Maintenance Scripts
-- ============================================================================

-----------------------------------------------------------------------------------------
-- Input Parameters:
--		@SQLServerName	- name of SQL Server instance to analyze
--		@DBName			- database to be analyzed
--		@TableSchema	- schema that current table belongs to
--		@TableName		- specify table name to be analyzed
--		@IndexName		- name of the index to be analyzed
--		@IndexID		- id of the index to be analyzed. to may specify either index name or id. 
--						  if you specify both, index name will be taken into consideration
--		@flgOptions:	1 - get also indexes that are created by a table constraint (primary or unique key) (default)
--						2 - use drop existing to recreate the index (default)
--					 4096 - use ONLINE=ON, if applicable (default)
--		@DebugMode:		1 - print dynamic SQL statements 
--						0 - no statements will be displayed (default)
-----------------------------------------------------------------------------------------
-- Output Parameters:
--		@sqlIndexCreate	- sql statement that will create the index
-----------------------------------------------------------------------------------------
-- Return : 
-- 1 : Succes  -1 : Fail 
-----------------------------------------------------------------------------------------

DECLARE @tmpSQL				[nvarchar](max),
		@sqlIndexInclude	[nvarchar](max),
		@sqlIndexWithClause [nvarchar](max),
		@sqlScriptOnline	[nvarchar](512),
		@crtIndexName		[sysname],
		@IndexType			[tinyint],
		@FillFactor			[tinyint],
		@IsUniqueConstraint	[int],
		@IsPadded			[int],
		@AllowRowLocks		[int],
		@AllowPageLocks		[int],
		@IgnoreDupKey		[int],
		@KeyOrdinal			[int],
		@IndexColumnID		[int],
		@IsIncludedColumn	[bit],
		@IsDescendingKey	[bit],
		@ColumnName			[sysname],
		@FileGroupName		[sysname],
		@ReturnValue		[int],
		@nestExecutionLevel	[tinyint]

DECLARE @IndexDetails TABLE	(
								[IndexName]			[sysname]	NULL,
								[IndexType]			[tinyint]	NULL,
								[FillFactor]		[tinyint]	NULL,
								[FileGroupName]		[sysname]	NULL,
								[IsUniqueConstraint][bit]		NULL,
								[IsPadded]			[bit]		NULL,
								[AllowRowLocks]		[bit]		NULL,
								[AllowPageLocks]	[bit]		NULL,
								[IgnoreDupKey]		[bit]		NULL
							)

DECLARE @IndexColumnDetails TABLE
							(
								[KeyOrdinal]		[int]		NULL,
								[IndexColumnID]		[int]		NULL,
								[IsIncludedColumn]	[bit]		NULL,
								[IsDescendingKey]	[bit]		NULL,
								[ColumnName]		[sysname]	NULL
							)

SET NOCOUNT ON

-- { sql_statement | statement_block }
BEGIN TRY
		SET @ReturnValue	 = 1

		--get current index properties
		SET @tmpSQL = N''
		SET @tmpSQL = @tmpSQL + N'SELECT  idx.[name]
										, idx.[type]
										, idx.[fill_factor]
										, dSp.[name] AS [file_group_name]
										, idx.[is_unique]
										, idx.[is_padded]
										, idx.[allow_row_locks]
										, idx.[allow_page_locks]
										, idx.[ignore_dup_key]
									FROM [' + @DBName + '].[sys].[indexes]				idx
									INNER JOIN [' + @DBName + '].[sys].[objects]		obj ON  idx.[object_id] = obj.[object_id]
									INNER JOIN [' + @DBName + '].[sys].[schemas]		sch ON	sch.[schema_id] = obj.[schema_id]
									INNER JOIN [' + @DBName + '].[sys].[data_spaces]	dSp	ON  idx.[data_space_id] = dSp.[data_space_id]
									WHERE	obj.[name] = ''' + @TableName + '''
											AND sch.[name] = ''' + @TableSchema + '''' + 
											CASE	WHEN @IndexName IS NOT NULL 
													THEN ' AND idx.[name] = ''' + @IndexName + ''''
													ELSE ' AND idx.[index_id] = ' + CAST(@IndexID AS [nvarchar])
											END + 
											CASE WHEN @flgOptions & 1 <> 1
												 THEN '	AND NOT EXISTS	(
																			SELECT 1
																			FROM [' + @DBName + '].[INFORMATION_SCHEMA].[TABLE_CONSTRAINTS]
																			WHERE [CONSTRAINT_TYPE]=''PRIMARY KEY''
																					AND [CONSTRAINT_CATALOG]=''' + @DBName + '''
																					AND [TABLE_NAME]=''' + @TableName + '''
																					AND [TABLE_SCHEMA] = ''' + @TableSchema + '''
																					AND [CONSTRAINT_NAME]=''' + @IndexName + '''
																		)'
												ELSE ''
											END
		SET @tmpSQL = [dbo].[ufn_formatSQLQueryForLinkedServer](@SQLServerName, @tmpSQL)
		IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @tmpSQL, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		DELETE FROM @IndexDetails
		INSERT INTO @IndexDetails ([IndexName], [IndexType], [FillFactor], [FileGroupName], [IsUniqueConstraint], [IsPadded], [AllowRowLocks], [AllowPageLocks], [IgnoreDupKey])
			EXEC (@tmpSQL)

		--get index fill factor and file group
		SELECT	  @crtIndexName		= ISNULL(@IndexName, [IndexName])
				, @IndexType		= [IndexType]
				, @FillFactor		= [FillFactor]
				, @FileGroupName	= [FileGroupName]
				, @IsUniqueConstraint = [IsUniqueConstraint]
				, @IsPadded			= [IsPadded]
				, @AllowRowLocks	= [AllowRowLocks]
				, @AllowPageLocks	= [AllowPageLocks]
				, @IgnoreDupKey		= [IgnoreDupKey]
		FROM @IndexDetails
		
		--get current index key columns and include columns and their properties
		SET @tmpSQL = N''
		SET @tmpSQL = @tmpSQL + N'SELECT    
										  idxCol.[key_ordinal]
										, idxCol.[index_column_id]
										, idxCol.[is_included_column]
										, idxCol.[is_descending_key]
										, col.[name] AS [column_name]
								FROM [' + @DBName + '].[sys].[indexes] idx
								INNER JOIN [' + @DBName + '].[sys].[index_columns] idxCol ON	idx.[object_id] = idxCol.[object_id]
																								AND idx.[index_id] = idxCol.[index_id]
								INNER JOIN [' + @DBName + '].[sys].[columns]		 col	ON	idxCol.[object_id] = col.[object_id]
																								AND idxCol.[column_id] = col.[column_id]
								INNER JOIN [' + @DBName + '].[sys].[objects]		 obj	ON  idx.[object_id] = obj.[object_id]
								INNER JOIN [' + @DBName + '].[sys].[schemas]		 sch	ON	sch.[schema_id] = obj.[schema_id]
								WHERE	obj.[name] = ''' + @TableName + '''
										AND sch.[name] = ''' + @TableSchema + '''' + 
										CASE	WHEN @IndexName IS NOT NULL 
												THEN ' AND idx.[name] = ''' + @IndexName + ''''
												ELSE ' AND idx.[index_id] = ' + CAST(@IndexID AS [nvarchar])
										END + 
										CASE WHEN @flgOptions & 1 <> 1
											 THEN '	AND NOT EXISTS	(
																		SELECT 1
																		FROM [' + @DBName + '].[INFORMATION_SCHEMA].[TABLE_CONSTRAINTS]
																		WHERE [CONSTRAINT_TYPE]=''PRIMARY KEY''
																				AND [CONSTRAINT_CATALOG]=''' + @DBName + '''
																				AND [TABLE_NAME]=''' + @TableName + '''
																				AND [TABLE_SCHEMA]=''' + @TableSchema + '''
																				AND [CONSTRAINT_NAME]=''' + @IndexName + '''
																	)'
											ELSE ''
										END
		SET @tmpSQL = [dbo].[ufn_formatSQLQueryForLinkedServer](@SQLServerName, @tmpSQL)
		IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @tmpSQL, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		DELETE FROM @IndexColumnDetails
		INSERT INTO @IndexColumnDetails ([KeyOrdinal], [IndexColumnID], [IsIncludedColumn], [IsDescendingKey], [ColumnName])
			EXEC (@tmpSQL)

		SET @sqlIndexCreate=N''
		IF EXISTS (SELECT 1 FROM @IndexColumnDetails)
			begin
				-- check for online operation mode, for reorganize/rebuild
				SET @nestExecutionLevel = @executionLevel + 1
				EXEC [dbo].[usp_mpCheckIndexOnlineOperation]	@sqlServerName		= @SQLServerName,
																@dbName				= @DBName,
																@tableSchema		= @TableSchema,
																@tableName			= @TableName,
																@indexName			= @IndexName,
																@indexID			= @IndexID,
																@partitionNumber	= 1,
																@sqlScriptOnline	= @sqlScriptOnline OUT,
																@flgOptions			= @flgOptions,
																@executionLevel		= @nestExecutionLevel,
																@debugMode			= @DebugMode

				SET @sqlIndexCreate = @sqlIndexCreate + N'CREATE'
				SET @sqlIndexCreate = @sqlIndexCreate +	 CASE	WHEN @IsUniqueConstraint=1	
																THEN ' UNIQUE' 
																ELSE ''
														 END 
				SET @sqlIndexCreate = @sqlIndexCreate +	 CASE	WHEN @IndexType=1	
																THEN ' CLUSTERED' 
																ELSE ''
														 END 
				SET @sqlIndexCreate = @sqlIndexCreate +	 ' INDEX [' + @crtIndexName + '] ON [' + @TableSchema + '].[' + @TableName + '] ('
				--index key columns
				DECLARE crsIndexKey CURSOR LOCAL FAST_FORWARD FOR	SELECT [ColumnName], [IsDescendingKey]
																	FROM @IndexColumnDetails
																	WHERE [IsIncludedColumn] = 0
																	ORDER BY [KeyOrdinal]
				OPEN crsIndexKey
				FETCH NEXT FROM crsIndexKey INTO @ColumnName, @IsDescendingKey
				WHILE @@FETCH_STATUS=0
					begin
						SET @sqlIndexCreate = @sqlIndexCreate + '[' + @ColumnName + ']' + 
												CASE WHEN @IsDescendingKey=1	THEN ' DESC'
																				ELSE '' END + ', '
						FETCH NEXT FROM crsIndexKey INTO @ColumnName, @IsDescendingKey
					end
				CLOSE  crsIndexKey
				DEALLOCATE crsIndexKey
				IF LEN(@sqlIndexCreate)<>0
					SET @sqlIndexCreate = SUBSTRING(@sqlIndexCreate, 1, LEN(@sqlIndexCreate)-1) + ')'

				--index include columns
				SET @sqlIndexInclude = N''
				DECLARE crsIndexInclude CURSOR LOCAL FAST_FORWARD FOR	SELECT [ColumnName]
																		FROM @IndexColumnDetails
																		WHERE [IsIncludedColumn] = 1
																		ORDER BY [IndexColumnID]
				OPEN crsIndexInclude
				FETCH NEXT FROM crsIndexInclude INTO @ColumnName
				WHILE @@FETCH_STATUS=0
					begin
						SET @sqlIndexInclude = @sqlIndexInclude + '[' + @ColumnName + '], '
						FETCH NEXT FROM crsIndexInclude INTO @ColumnName
					end
				CLOSE  crsIndexInclude
				DEALLOCATE crsIndexInclude
				IF LEN(@sqlIndexInclude)<>0
					SET @sqlIndexInclude = SUBSTRING(@sqlIndexInclude, 1, LEN(@sqlIndexInclude)-1)


				IF LEN(@sqlIndexInclude)<>0
					SET @sqlIndexCreate = @sqlIndexCreate + N' INCLUDE(' + @sqlIndexInclude + ')'

				--index options
				SET @sqlIndexWithClause = N''
				SET @sqlIndexWithClause = @sqlIndexWithClause + 
										CASE	WHEN LEN(@sqlIndexWithClause)>0 
												THEN ', '
												ELSE ''
									    END + N'PAD_INDEX = ' + CASE WHEN @IsPadded=1 THEN 'ON' ELSE 'OFF' END
				SET @sqlIndexWithClause = @sqlIndexWithClause + 
										CASE	WHEN LEN(@sqlIndexWithClause)>0 
												THEN ', '
												ELSE ''
									    END + N'ALLOW_ROW_LOCKS = ' + CASE WHEN @AllowRowLocks=1 THEN 'ON' ELSE 'OFF' END
				SET @sqlIndexWithClause = @sqlIndexWithClause + 
										CASE	WHEN LEN(@sqlIndexWithClause)>0 
												THEN ', '
												ELSE ''
									    END + N'ALLOW_PAGE_LOCKS = ' + CASE WHEN @AllowPageLocks=1 THEN 'ON' ELSE 'OFF' END
				SET @sqlIndexWithClause = @sqlIndexWithClause + 
										CASE	WHEN LEN(@sqlIndexWithClause)>0 
												THEN ', '
												ELSE ''
									    END + N'IGNORE_DUP_KEY = ' + CASE WHEN @IgnoreDupKey=1 THEN 'ON' ELSE 'OFF' END
				SET @sqlIndexWithClause = @sqlIndexWithClause + 
										CASE	WHEN LEN(@sqlIndexWithClause)>0 
												THEN ', '
												ELSE ''
									    END + @sqlScriptOnline
				SET @sqlIndexWithClause = @sqlIndexWithClause + 
										CASE	WHEN LEN(@sqlIndexWithClause)>0 
												THEN ', '
												ELSE ''
									    END + N'SORT_IN_TEMPDB = ON'
				SET @sqlIndexWithClause = @sqlIndexWithClause + 
										CASE	WHEN LEN(@sqlIndexWithClause)>0 
												THEN ', '
												ELSE ''
									    END + N'STATISTICS_NORECOMPUTE = OFF'
				SET @sqlIndexWithClause = @sqlIndexWithClause + 
										CASE	WHEN LEN(@sqlIndexWithClause)>0 
												THEN ', '
												ELSE ''
									    END + N'MAXDOP = 1'
				SET @sqlIndexWithClause = @sqlIndexWithClause + 
										CASE WHEN @FillFactor<>0	
											 THEN CASE	WHEN LEN(@sqlIndexWithClause)>0 
														THEN ', '
														ELSE ''
												  END + N'FILLFACTOR=' + CAST(@FillFactor AS [nvarchar])
											 ELSE ''
										END
				SET @sqlIndexWithClause = @sqlIndexWithClause + 
										CASE	WHEN LEN(@sqlIndexWithClause)>0 
												THEN ', '
												ELSE ''
									    END + 
										CASE WHEN @flgOptions & 2 = 2 
											 THEN N'DROP_EXISTING = ON'
											 ELSE ''
										END
				--index storage filegroup
				SET @sqlIndexCreate = @sqlIndexCreate + 
										CASE WHEN LEN(@sqlIndexWithClause)>0
											 THEN N' WITH (' + @sqlIndexWithClause + ')'
											 ELSE ''
										END + N' ON [' + @FileGroupName + ']'
			end
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
	SET @ReturnValue = -1

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

RETURN @ReturnValue
GO
