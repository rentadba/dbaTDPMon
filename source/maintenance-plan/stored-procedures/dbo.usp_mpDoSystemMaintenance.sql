RAISERROR('Create procedure: [dbo].[usp_mpDoSystemMaintenance]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_mpDoSystemMaintenance]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpDoSystemMaintenance]
GO

-----------------------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[usp_mpDoSystemMaintenance]
	@sqlServerName	[sysname] = '',
	@doAllSteps		[bit] = 0,
	@debugMode		[bit] = 0
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	IF @sqlServerName = ''
	BEGIN
		SET @sqlServerName = @@SERVERNAME
	END

	DECLARE   @queryToRun			[nvarchar](max)
			, @serverToRun			[nvarchar](512)
			, @compatibilityLevel	[tinyint]
			, @executionLevel		[tinyint]
			, @hasDistribution		[bit]

	SET @executionLevel = 1
	SET @hasDistribution = 0

	SET @serverToRun = '[' + @sqlServerName + '].master.dbo.sp_executesql'

	SET @queryToRun = 'SELECT database_id FROM sys.databases WHERE name = ''distribution'''
	SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
	IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

	DECLARE @results TABLE (id int)
	INSERT INTO @results
	EXEC(@queryToRun)

	SELECT @hasDistribution = CASE WHEN id > 0 THEN 1 ELSE 1 END FROM @results

	-- master - Cycle errorlog file (daily)

	SET @queryToRun = 'DBCC errorlog'
	IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @serverToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
	IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

	EXEC @serverToRun @queryToRun

	-- master - Consistency Checks (weekly)

	/* only once a week on Saturday */
	IF DATENAME(weekday, GETDATE()) = 'Saturday' OR @doAllSteps  = 1
	BEGIN
		EXEC [dbo].[usp_mpDatabaseConsistencyCheck]	@sqlServerName			= @sqlServerName,
													@dbName					= 'master',
													@tableSchema			= '%',
													@tableName				= '%',
													@flgActions				= 1,
													@flgOptions				= 0,
													@debugMode				= @debugMode
	END

	/* only once a week on Saturday */
	IF DATENAME(weekday, GETDATE()) = 'Saturday' OR @doAllSteps = 1
	BEGIN
		EXEC [dbo].[usp_mpDatabaseConsistencyCheck]	@sqlServerName			= @sqlServerName,
													@dbName					= 'msdb',
													@tableSchema			= '%',
													@tableName				= '%',
													@flgActions				= 1,
													@flgOptions				= 0,
													@debugMode				= @debugMode
	END

	-- model - Consistency Checks (weekly)
	/* only once a week on Saturday */
	IF DATENAME(weekday, GETDATE()) = 'Saturday' OR @doAllSteps = 1
	BEGIN
		EXEC [dbo].[usp_mpDatabaseConsistencyCheck]	@sqlServerName			= @sqlServerName,
													@dbName					= 'model',
													@tableSchema			= '%',
													@tableName				= '%',
													@flgActions				= 1,
													@flgOptions				= 0,
													@debugMode				= @debugMode
	END

	-- distribution - Consistency Checks (weekly)
	/* only once a week on Saturday */
	IF (DATENAME(weekday, GETDATE()) = 'Saturday' OR @doAllSteps = 1)  AND @hasDistribution = 1
	-- AND EXISTS (SELECT * FROM sys.databases WHERE [name]='distribution')
	BEGIN
		EXEC [dbo].[usp_mpDatabaseConsistencyCheck]	@sqlServerName			= @sqlServerName,
													@dbName					= 'distribution',
													@tableSchema			= '%',
													@tableName				= '%',
													@flgActions				= 1,
													@flgOptions				= 0,
													@debugMode				= @debugMode
	END		

	-- msdb - Backup History Retention (3 months)
	/* keep only last 6 months of backup history */
	SET @queryToRun = '
							DECLARE		@oldestDate	[datetime],
										@str		[varchar](32)

							SELECT @oldestDate=MIN([backup_finish_date])
							FROM [msdb].[dbo].[backupset]

							WHILE @oldestDate <= DATEADD(month, -1, GETDATE())
							begin
								SET @oldestDate=DATEADD(day, 1, @oldestDate)
								SET @str=CONVERT([varchar](20), @oldestDate, 120)

								RAISERROR(@str, 10, 1) WITH NOWAIT

								EXEC msdb.dbo.sp_delete_backuphistory @oldest_date = @oldestDate
							end'
	IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @serverToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
	IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
	EXEC @serverToRun @queryToRun
		
	-- msdb - Job History Retention (3 months)
	SET @queryToRun = 'DECLARE   @oldestDate	[datetime]
	SET @oldestDate=DATEADD(month, -3, GETDATE())
	EXEC msdb.dbo.sp_purge_jobhistory @oldest_date = @oldestDate'
	IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @serverToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
	IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
	EXEC @serverToRun @queryToRun

	-- msdb - Maintenance Plan History Retention (3 months)
	SET @queryToRun = 'DECLARE   @oldestDate	[datetime]
	SET @oldestDate=DATEADD(month, -3, GETDATE())
	EXECUTE msdb.dbo.sp_maintplan_delete_log null, null, @oldestDate
	DELETE FROM msdb.dbo.sysdbmaintplan_history WHERE end_time < @oldestDate  
	DELETE FROM msdb.dbo.sysmaintplan_logdetail WHERE end_time < @oldestDate'
	IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @serverToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
	IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
	EXEC @serverToRun @queryToRun

	-- msdb - Maintenance Plan History Retention - 3 days for highly executed jobsd
	IF @sqlServerName = @@servername
	BEGIN
		SET @queryToRun = 'DECLARE @SqlTxt nvarchar(max)
							DECLARE @JobName sysname

							DECLARE JobCursor CURSOR LOCAL FORWARD_ONLY FOR
								select JobName from master.dbo.agentJobsInfo
								where occurrence = ''daily'' and freq_seconds < 600 and freq_seconds > 0
								and jobisenabled = ''Yes''
								order by jobname
							OPEN JobCursor
							FETCH NEXT FROM JobCursor INTO  @JobName
							WHILE @@FETCH_STATUS = 0
							BEGIN
								SET @SqlTxt = ''
									DECLARE @OldestDate DATETIME
									SET @OldestDate = DATEADD(DAY, -3, GETDATE())
									EXEC msdb.dbo.sp_purge_jobhistory  
									@job_name = N'''' + @JobName + '''',
									 @oldest_date = @OldestDate		
									''
								 PRINT @SqlTxt
								EXEC sp_executesql @SqlTxt
	
								FETCH NEXT FROM JobCursor INTO  @JobName
							END
							CLOSE JobCursor
							DEALLOCATE JobCursor'
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @serverToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
		BEGIN TRY
			EXEC @serverToRun @queryToRun
		END TRY
		BEGIN CATCH
			-- do nothing
		END CATCH
	END
	-- msdb - Purge Old Mail Items (3 months)
	SET @queryToRun = '/* delete old mail items; especially, if you are sending attachements */
	/* keep only last 6 months of history */
	DECLARE   @oldestDate	[datetime]

	SET @oldestDate=DATEADD(month, -1, GETDATE())
	EXEC msdb.dbo.sysmail_delete_mailitems_sp @sent_before = @oldestDate'
	IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @serverToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
	IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
	EXEC @serverToRun @queryToRun

	-- msdb - Purge Old Mail Logs (3 months)
	SET @queryToRun = 'DECLARE   @oldestDate	[datetime]
	SET @oldestDate=DATEADD(month, -1, GETDATE())
	EXEC msdb.dbo.sysmail_delete_log_sp @logged_before = @oldestDate'
	IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @serverToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
	IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
	EXEC @serverToRun @queryToRun

	-- msdb -	 (3 months)
	IF @hasDistribution = 1
	BEGIN
		SET @queryToRun = 'DELETE FROM msdb.dbo.sysreplicationalerts WHERE time <= DATEADD(month, -3, GETDATE())'
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @serverToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
		EXEC @serverToRun @queryToRun
	END

	-- master - Index & Statistics Maintenance (weekly)
	IF DATENAME(weekday, GETDATE()) = 'Sunday'  OR @doAllSteps = 1
	BEGIN
		EXEC [dbo].[usp_mpDatabaseOptimize]		@sqlServerName			= @sqlServerName,
												@dbName					= 'master',
												@tableSchema			= '%',
												@tableName				= '%',
												@flgActions				= 11,
												@flgOptions				= DEFAULT,
												@defragIndexThreshold	= DEFAULT,
												@rebuildIndexThreshold	= DEFAULT,
												@statsSamplePercent		= DEFAULT,
												@statsAgeDays			= DEFAULT,
												@statsChangePercent		= DEFAULT,
												@debugMode				= @debugMode
	END

	-- msdb - Index & Statistics Maintenance (weekly)
	IF DATENAME(weekday, GETDATE()) = 'Sunday'  OR @doAllSteps = 1
	BEGIN
		EXEC [dbo].[usp_mpDatabaseOptimize]		@sqlServerName			= @sqlServerName,
												@dbName					= 'msdb',
												@tableSchema			= '%',
												@tableName				= '%',
												@flgActions				= 11,
												@flgOptions				= DEFAULT,
												@defragIndexThreshold	= DEFAULT,
												@rebuildIndexThreshold	= DEFAULT,
												@statsSamplePercent		= DEFAULT,
												@statsAgeDays			= DEFAULT,
												@statsChangePercent		= DEFAULT,
												@debugMode				= @debugMode
	END

	-- Weekly: Shrink Database (TRUNCATEONLY)
	IF DATENAME(weekday, GETDATE()) = 'Monday'  OR @doAllSteps = 1
	BEGIN
		EXEC [dbo].[usp_mpDatabaseShrink]	@sqlServerName		= @sqlServerName,
											@dbName				= 'master',
											@flgActions			= 2,	
											@flgOptions			= 1,
											@executionLevel		= DEFAULT,
											@debugMode			= @debugMode
				
		EXEC [dbo].[usp_mpDatabaseShrink]	@sqlServerName		= @sqlServerName,
											@dbName				= 'msdb',
											@flgActions			= 2,	
											@flgOptions			= 1,
											@executionLevel		= DEFAULT,
											@debugMode			= @debugMode
				

		EXEC [dbo].[usp_mpDatabaseShrink]	@sqlServerName		= @sqlServerName,
											@dbName				= 'model',
											@flgActions			= 2,	
											@flgOptions			= 1,
											@executionLevel		= DEFAULT,
											@debugMode			= @debugMode

		IF @hasDistribution = 1 
		BEGIN
			EXEC [dbo].[usp_mpDatabaseShrink]	@sqlServerName		= @sqlServerName,
												@dbName				= 'distribution',
												@flgActions			= 2,	
												@flgOptions			= 1,
												@executionLevel		= DEFAULT,
												@debugMode			= @debugMode
		END																							
										
	END

	-- Monthly: Shrink Log File
	IF (DATENAME(weekday, GETDATE()) = 'Saturday' OR @doAllSteps = 1) AND DATEPART(dd, GETUTCDATE())<=7  
	begin 
		EXEC [dbo].[usp_mpDatabaseShrink]	@sqlServerName		= @sqlServerName,
											@dbName				= 'master',
											@flgActions			= 1,	
											@flgOptions			= 0,
											@executionLevel		= DEFAULT,
											@debugMode			= @debugMode
													
		EXEC [dbo].[usp_mpDatabaseShrink]	@sqlServerName		= @sqlServerName,
											@dbName				= 'model',
											@flgActions			= 1,	
											@flgOptions			= 0,
											@executionLevel		= DEFAULT,
											@debugMode			= @debugMode			

		EXEC [dbo].[usp_mpDatabaseShrink]	@sqlServerName		= @sqlServerName,
											@dbName				= 'msdb',
											@flgActions			= 1,	
											@flgOptions			= 0,
											@executionLevel		= DEFAULT,
											@debugMode			= @debugMode			

		EXEC [dbo].[usp_mpDatabaseShrink]	@sqlServerName		= @sqlServerName,
											@dbName				= 'tempdb',
											@flgActions			= 1,	
											@flgOptions			= 0,
											@executionLevel		= DEFAULT,
											@debugMode			= @debugMode			
		IF @hasDistribution = 1
		BEGIN
			EXEC [dbo].[usp_mpDatabaseShrink]	@sqlServerName		= @sqlServerName,
												@dbName				= 'distribution',
												@flgActions			= 1,	
												@flgOptions			= 0,
												@executionLevel		= DEFAULT,
												@debugMode			= @debugMode			

		END
	end
END


