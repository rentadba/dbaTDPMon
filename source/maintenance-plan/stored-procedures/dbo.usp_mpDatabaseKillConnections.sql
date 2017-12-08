RAISERROR('Create procedure: [dbo].[usp_mpDatabaseKillConnections]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_mpDatabaseKillConnections]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpDatabaseKillConnections]
GO

-----------------------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[usp_mpDatabaseKillConnections]
		@sqlServerName		[sysname],
		@dbName				[sysname] = NULL,
		@flgOptions			[int] = 2,
		@executionLevel		[tinyint] = 0,
		@debugMode			[bit] = 0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date: 05.03.2010
-- Module     : Database Maintenance Scripts
-- ============================================================================

-----------------------------------------------------------------------------------------
-- Input Parameters:
--		@sqlServerName	- name of SQL Server instance to analyze
--		@dbName			- database to be analyzed
--		@flgOptions		- 1 - normal connections
--						  2 - orphan connections
--		@debugMode:		 1 - print dynamic SQL statements 
--						 0 - no statements will be displayed (default)
-----------------------------------------------------------------------------------------
-- Return : 
-- 1 : Succes  -1 : Fail 
-----------------------------------------------------------------------------------------

DECLARE @queryToRun			[nvarchar](MAX),
		@serverToRun		[varchar](256),
		@databaseName		[sysname],
		@StartTime			[datetime],
		@MaxWaitTime		[int],
		@ConnectionsLeft	[int],
		@LocksLeft			[int],
		@ReturnValue		[int],
		@spid				[int],
		@uow				[uniqueidentifier]

DECLARE @DatabaseList	TABLE (	[dbname] [sysname] )

DECLARE @RowCount		TABLE (	[rowcount] [int] )

DECLARE @SessionDetails	TABLE (
								[spid]	[int]				NULL,
								[uow]	[uniqueidentifier]	NULL
							  )

SET NOCOUNT ON

-- { sql_statement | statement_block }
BEGIN TRY
		SET @ReturnValue	 = 1
 
		SET @queryToRun= 'Checking database active connections...'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		SET @MaxWaitTime = 180 --3 minutes 
		SET @StartTime = GETUTCDATE()

		SET @serverToRun = N''
		SET @serverToRun = @serverToRun + N'[' + @sqlServerName + '].[master].[dbo].[sp_executesql]'

		------------------------------------------------------------------------------
		--get database list that will be analyzed
		------------------------------------------------------------------------------
		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'SELECT [name]
										FROM (
												SELECT DISTINCT DB_NAME(ISNULL(resource_database_id,1)) [name]
												FROM [master].sys.dm_exec_connections	ec 
												LEFT JOIN [master].sys.dm_exec_requests	er on	ec.connection_id = er.connection_id 
												LEFT JOIN [master].sys.dm_tran_locks	tl on	tl.request_session_id = ec.session_id 
																								and resource_type=''DATABASE'' 
												WHERE	ec.session_id <> @@SPID
														AND (   (ec.session_id <> -2 and ' + CAST(@flgOptions AS [nvarchar](max)) + N' & 1 = 1)
															 or (ec.session_id = -2  and ' + CAST(@flgOptions AS [nvarchar](max)) + N' & 2 = 2)
															)

												UNION		

												SELECT DB_NAME(rsc_dbid) [name]
												FROM [master].dbo.syslockinfo
												WHERE	req_spid=-2
														and req_transactionuow <> ''00000000-0000-0000-0000-000000000000''
											 )x
										WHERE [name] LIKE ''' + CASE WHEN @dbName IS NULL THEN '%' ELSE [dbo].[ufn_getObjectQuoteName](@dbName, 'sql') END + ''''
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		DELETE FROM @DatabaseList
		INSERT	INTO @DatabaseList([dbname])
				EXEC (@queryToRun)


		DECLARE crsDatabases CURSOR LOCAL FAST_FORWARD FOR	SELECT [dbname]
															FROM @DatabaseList
		OPEN crsDatabases
		FETCH NEXT FROM crsDatabases INTO @databaseName
		WHILE @@FETCH_STATUS=0
			begin
				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + N'
							SELECT COUNT(*) [row_count]
							FROM [master].sys.dm_exec_connections	ec 
							LEFT JOIN [master].sys.dm_exec_requests	er on	ec.connection_id = er.connection_id 
							LEFT JOIN [master].sys.dm_tran_locks	tl on	tl.request_session_id = ec.session_id 
																			and resource_type=''DATABASE''
							WHERE	DB_NAME(ISNULL(resource_database_id,1)) = ''' + [dbo].[ufn_getObjectQuoteName](@databaseName, 'sql') + '''
									AND ec.session_id <> @@SPID
									AND (   (ec.session_id<>-2 and ' + CAST(@flgOptions AS [nvarchar](max)) + N' & 1 = 1)
										 or (ec.session_id=-2  and ' + CAST(@flgOptions AS [nvarchar](max)) + N' & 2 = 2)
										)'
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

				DELETE FROM @RowCount
				INSERT	INTO @RowCount([rowcount])
						EXEC (@queryToRun)
				
				SELECT @ConnectionsLeft = [rowcount] FROM @RowCount


				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + N'				
							SELECT COUNT(*) [row_count]
							FROM (
									SELECT DISTINCT req_transactionuow
									FROM [master].dbo.syslockinfo
									WHERE	rsc_dbid=DB_ID(''' + [dbo].[ufn_getObjectQuoteName](@databaseName, 'sql') + ''')
											AND req_spid=-2
											AND req_transactionuow <> ''00000000-0000-0000-0000-000000000000''
								 )y'

				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

				DELETE FROM @RowCount
				INSERT	INTO @RowCount([rowcount])
						EXEC (@queryToRun)
				
				SELECT @LocksLeft = [rowcount] FROM @RowCount

				WHILE	(@ConnectionsLeft + @LocksLeft)>0 AND DATEDIFF(ss, @StartTime, GETUTCDATE())<=@MaxWaitTime
					begin
						IF @ConnectionsLeft>0
							begin
								DELETE FROM @SessionDetails
								
								IF @flgOptions & 1 = 1
									begin
										SET @queryToRun= 'Get connections for database: ' + [dbo].[ufn_getObjectQuoteName](@databaseName, 'quoted')
										EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
																			
										------------------------------------------------------------------------------
										--get "normal" connections to database
										------------------------------------------------------------------------------
										SET @queryToRun = N''
										SET @queryToRun = @queryToRun + N'
															SELECT ec.session_id
															FROM [master].sys.dm_exec_connections	ec 
															LEFT JOIN [master].sys.dm_exec_requests	er on	ec.connection_id = er.connection_id 
															LEFT JOIN [master].sys.dm_tran_locks	tl on	tl.request_session_id = ec.session_id 
																											and resource_type=''DATABASE''
															WHERE	DB_NAME(ISNULL(resource_database_id,1)) = ''' + [dbo].[ufn_getObjectQuoteName](@databaseName, 'sql') + '''
																	AND ec.session_id <> @@SPID
																	AND ec.session_id<>-2'

										SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
										IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

										INSERT	INTO @SessionDetails([spid])
												EXEC (@queryToRun)
								end

								IF @flgOptions & 2 = 2
									begin									
										SET @queryToRun= 'Get orphan connections for database: ' + [dbo].[ufn_getObjectQuoteName](@databaseName, 'quoted') + ' (sys.dm_tran_locks)'
										EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

									
										------------------------------------------------------------------------------
										--get orphan connections to database
										------------------------------------------------------------------------------
										SET @queryToRun = N''
										SET @queryToRun = @queryToRun + N'
															SELECT tl.request_owner_guid
															FROM [master].sys.dm_exec_connections	ec 
															LEFT JOIN [master].sys.dm_exec_requests	er on	ec.connection_id = er.connection_id 
															LEFT JOIN [master].sys.dm_tran_locks	tl on	tl.request_session_id = ec.session_id 
																											and resource_type=''DATABASE''
															WHERE	DB_NAME(ISNULL(resource_database_id,1)) = ''' + [dbo].[ufn_getObjectQuoteName](@databaseName, 'sql') + '''
																	AND ec.session_id=-2'

										SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
										IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

										INSERT	INTO @SessionDetails([uow])
												EXEC (@queryToRun)
									end
							end

						IF @LocksLeft>0
							begin
								IF @flgOptions & 2 = 2
									begin									
										SET @queryToRun= 'Get orphan connections for database: ' + [dbo].[ufn_getObjectQuoteName](@databaseName, 'quoted') + ' (syslockinfo)'
										EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

										------------------------------------------------------------------------------
										--get orphan connections to database - locks
										------------------------------------------------------------------------------
										SET @queryToRun = N''
										SET @queryToRun = @queryToRun + N'
															SELECT req_transactionuow
															FROM (
																	SELECT DISTINCT req_transactionuow
																	FROM [master].dbo.syslockinfo
																	WHERE	rsc_dbid=DB_ID(''' + [dbo].[ufn_getObjectQuoteName](@databaseName, 'sql') + ''')
																			AND req_spid=-2
																			AND req_transactionuow <> ''00000000-0000-0000-0000-000000000000''
																 )x'

										SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
										IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

										INSERT	INTO @SessionDetails([uow])
												EXEC (@queryToRun)
									end
							end

						IF @flgOptions & 1 = 1
							begin
								------------------------------------------------------------------------------
								--kill connections to database
								------------------------------------------------------------------------------
								SET @queryToRun= 'Kill connections for database: ' + [dbo].[ufn_getObjectQuoteName](@databaseName, 'quoted')
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
								
								DECLARE crsSPIDList CURSOR LOCAL FAST_FORWARD FOR SELECT DISTINCT [spid] FROM @SessionDetails WHERE [spid] IS NOT NULL
								OPEN crsSPIDList
								FETCH NEXT FROM crsSPIDList INTO @spid
								WHILE @@FETCH_STATUS=0
									begin
										SET @queryToRun = N''
										SET @queryToRun = @queryToRun + N'KILL ' + CAST(@spid AS [nvarchar](max))
										EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0
										
										BEGIN TRY
											EXEC @serverToRun @queryToRun
										END TRY
										BEGIN CATCH
											SET @queryToRun = ERROR_MESSAGE()
											EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0
										END CATCH
										
										FETCH NEXT FROM crsSPIDList INTO @spid
									end
								CLOSE crsSPIDList
								DEALLOCATE crsSPIDList
							end
							
						IF @flgOptions & 2 = 2
							begin
								------------------------------------------------------------------------------
								--kill orphan connections to database
								------------------------------------------------------------------------------
								SET @queryToRun= 'Kill orphan connections for database: ' + [dbo].[ufn_getObjectQuoteName](@databaseName, 'quoted')
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
								
								DECLARE crsUOWList CURSOR LOCAL FAST_FORWARD FOR SELECT DISTINCT [uow] FROM @SessionDetails WHERE [uow] IS NOT NULL
								OPEN crsUOWList
								FETCH NEXT FROM crsUOWList INTO @uow
								WHILE @@FETCH_STATUS=0
									begin
										SET @queryToRun = N''
										SET @queryToRun = @queryToRun + N'KILL ''' + CAST(@uow AS [nvarchar](max)) + ''''
										EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0
										
										BEGIN TRY
											EXEC @serverToRun @queryToRun
										END TRY
										BEGIN CATCH
											SET @queryToRun = ERROR_MESSAGE()
											EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0
										END CATCH
										
										FETCH NEXT FROM crsUOWList INTO @uow
									end
								CLOSE crsUOWList
								DEALLOCATE crsUOWList
							end						

						
						SET @queryToRun = N''
						SET @queryToRun = @queryToRun + N'
									SELECT COUNT(*) [row_count]
									FROM [master].sys.dm_exec_connections	ec 
									LEFT JOIN [master].sys.dm_exec_requests	er on	ec.connection_id = er.connection_id 
									LEFT JOIN [master].sys.dm_tran_locks	tl on	tl.request_session_id = ec.session_id 
																					and resource_type=''DATABASE''
									WHERE	DB_NAME(ISNULL(resource_database_id,1)) = ''' + [dbo].[ufn_getObjectQuoteName](@databaseName, 'sql') + '''
											AND ec.session_id <> @@SPID
											AND (   (ec.session_id<>-2 and ' + CAST(@flgOptions AS [nvarchar](max)) + N' & 1 = 1)
												 or (ec.session_id=-2  and ' + CAST(@flgOptions AS [nvarchar](max)) + N' & 2 = 2)
												)'
						SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
						IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						DELETE FROM @RowCount
						INSERT	INTO @RowCount([rowcount])
								EXEC (@queryToRun)
						
						SELECT @ConnectionsLeft = [rowcount] FROM @RowCount


						SET @queryToRun = N''
						SET @queryToRun = @queryToRun + N'				
									SELECT COUNT(*) [row_count]
									FROM (
											SELECT DISTINCT req_transactionuow
											FROM [master].dbo.syslockinfo
											WHERE	rsc_dbid=DB_ID(''' + [dbo].[ufn_getObjectQuoteName](@databaseName, 'sql') + ''')
													AND req_spid=-2
													AND req_transactionuow <> ''00000000-0000-0000-0000-000000000000''
										 )y'

						SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
						IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						DELETE FROM @RowCount
						INSERT	INTO @RowCount([rowcount])
								EXEC (@queryToRun)
						
						SELECT @LocksLeft = [rowcount] FROM @RowCount
					end

				--check if all connections have been killed
				IF @ConnectionsLeft>0 
					begin 
						SET @queryToRun= 'Cannot kill all connections to database ' +  [dbo].[ufn_getObjectQuoteName](@databaseName, 'quoted') + '. There are ' + CAST(@ConnectionsLeft AS VARCHAR) + ' active connection(s) left. Operation failed.'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

					end
				IF @LocksLeft>0 
					begin 
						SET @queryToRun= 'Cannot kill all connections to database ' +  [dbo].[ufn_getObjectQuoteName](@databaseName, 'quoted') + '. There are ' + CAST(@LocksLeft AS VARCHAR) + ' active lock(s) left. Operation failed.'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
					end

				FETCH NEXT FROM crsDatabases INTO @databaseName
			end
		CLOSE crsDatabases
		DEALLOCATE crsDatabases

		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'				
					SELECT COUNT(*) [row_count]
					FROM (
							SELECT DISTINCT req_transactionuow
							FROM [master].dbo.syslockinfo
							WHERE	rsc_dbid=DB_ID(''' + [dbo].[ufn_getObjectQuoteName](@databaseName, 'sql') + ''')
									AND req_spid=-2
									AND req_transactionuow = ''00000000-0000-0000-0000-000000000000''
						 )y'

		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		DELETE FROM @RowCount
		INSERT	INTO @RowCount([rowcount])
				EXEC (@queryToRun)
		
		SELECT @LocksLeft = [rowcount] FROM @RowCount
			
		IF @LocksLeft>0
			EXEC [dbo].[usp_logPrintMessage] @customMessage = 'You need to restart the MSDTC service. There are orphan {00000000-0000-0000-0000-000000000000 transactions} left. Operation failed.', @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
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
