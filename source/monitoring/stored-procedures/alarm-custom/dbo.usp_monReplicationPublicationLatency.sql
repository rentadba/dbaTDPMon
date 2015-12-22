RAISERROR('Create procedure: [dbo].[usp_monReplicationPublicationLatency]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_monReplicationPublicationLatency]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_monReplicationPublicationLatency]
GO

CREATE PROCEDURE [dbo].[usp_monReplicationPublicationLatency]
		  @projectCode			[varchar](32)=NULL
		, @sqlServerNameFilter	[sysname]='%'
		, @iterations			[int] = 2
		, @iterationDelay		[varchar](10) = N'00:00:01'
		, @debugMode			[bit]=0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 13.10.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================
-- Change Date	: 
-- Description	: 
-----------------------------------------------------------------------------------------

SET NOCOUNT ON 
		
DECLARE   @sqlServerName		[sysname]
		, @projectID			[smallint]
		, @strMessage			[nvarchar](512)
		, @queryToRun			[nvarchar](max)
		, @serverToRun			[nvarchar](512)
		, @eventMessageData		[nvarchar](max)
		, @runStartTime			[datetime]
		, @replicationDelay		[varchar](10)

------------------------------------------------------------------------------------------------------------------------------------------
--get default project code
IF @projectCode IS NULL
	SELECT	@projectCode = [value]
	FROM	[dbo].[appConfigurations]
	WHERE	[name] = 'Default project code'
			AND [module] = 'common'

SELECT @projectID = [id]
FROM [dbo].[catalogProjects]
WHERE [code] = @projectCode 

IF @projectID IS NULL
	begin
		SET @strMessage=N'The value specifief for Project Code is not valid.'
		RAISERROR(@strMessage, 16, 1) WITH NOWAIT
	end

------------------------------------------------------------------------------------------------------------------------------------------
--get value for critical alert threshold
DECLARE   @alertThresholdCriticalReplicationLatencySec [int]
		, @alertThresholdWarningReplicationLatencySec [int] 
		

SELECT	@alertThresholdCriticalReplicationLatencySec = [critical_limit]
FROM	[monitoring].[alertThresholds]
WHERE	[alert_name] = 'Replication Latency'
		AND [category] = 'replication'
		AND [is_critical_limit_enabled]=1
SET @alertThresholdCriticalReplicationLatencySec = ISNULL(@alertThresholdCriticalReplicationLatencySec, 20)


SELECT	@alertThresholdWarningReplicationLatencySec = [warning_limit]
FROM	[monitoring].[alertThresholds]
WHERE	[alert_name] = 'Replication Latency'
		AND [category] = 'replication'
		AND [is_warning_limit_enabled]=1
SET @alertThresholdWarningReplicationLatencySec = ISNULL(@alertThresholdWarningReplicationLatencySec, 15)

SET @replicationDelay = REPLICATE('0', 2 - LEN(CAST(@alertThresholdCriticalReplicationLatencySec / 3600 AS [varchar]))) + CAST(@alertThresholdCriticalReplicationLatencySec / 3600 AS [varchar]) + ':' + 
						REPLICATE('0', 2 - LEN(CAST((@alertThresholdCriticalReplicationLatencySec % 3600) / 60 AS [varchar]))) + CAST((@alertThresholdCriticalReplicationLatencySec % 3600) / 60 AS [varchar]) + ':' + 
						REPLICATE('0', 2 - LEN(CAST((@alertThresholdCriticalReplicationLatencySec % 60) AS [varchar]))) + CAST((@alertThresholdCriticalReplicationLatencySec % 60) AS [varchar])

------------------------------------------------------------------------------------------------------------------------------------------
RAISERROR('--Delete existing information....', 10, 1) WITH NOWAIT

DELETE srl
FROM [monitoring].[statsReplicationLatency]		srl
WHERE srl.[project_id] = @projectID
	AND [publisher_server] LIKE @sqlServerNameFilter


------------------------------------------------------------------------------------------------------------------------------------------
RAISERROR('--Get Publications & Subscriptions Information....', 10, 1) WITH NOWAIT
SET @runStartTime = GETUTCDATE()

--replication distribution servers
DECLARE crsReplicationDistributorServers CURSOR FAST_FORWARD READ_ONLY FOR	SELECT [instance_name]
																			FROM [dbo].[vw_catalogDatabaseNames] 
																			WHERE [project_id] = @projectID
																					AND [active] = 1
																					AND [database_name] = 'distribution'
																					AND [instance_name] LIKE @sqlServerNameFilter
OPEN crsReplicationDistributorServers
FETCH NEXT FROM crsReplicationDistributorServers INTO @sqlServerName
WHILE @@FETCH_STATUS=0
	begin
		SET @strMessage='--	Analyzing server: ' + @sqlServerName
		RAISERROR(@strMessage, 10, 1) WITH NOWAIT

		--publications and subscriptions
		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'
							SELECT   ' + CAST(@projectID AS [nvarchar]) + N' AS [project_id]
									, @@SERVERNAME		AS [distributor_server]
									, p.[publication]	AS [publication_name]
									, p.[publication_type]
									, srv.[srvname]		AS [publisher_server]
									, p.[publisher_db]
									, ss.[srvname]		AS [subscriber_server]
									, s.[subscriber_db] 
									, s.[status]		AS [subscription_status]
									, s.[subscription_type]
									, COUNT(DISTINCT s.[article_id]) AS [subscription_articles]
							FROM distribution..MSpublications p 
							JOIN distribution..MSsubscriptions s ON p.[publication_id] = s.[publication_id] 
							JOIN master..sysservers ss ON s.[subscriber_id] = ss.[srvid]
							JOIN master..sysservers srv ON srv.[srvid] = p.[publisher_id]
							JOIN distribution..MSdistribution_agents da ON da.[publisher_id] = p.[publisher_id] AND da.[subscriber_id] = s.[subscriber_id] 
							GROUP BY p.[publication]
									, srv.[srvname]
									, p.[publisher_db]
									, ss.[srvname]
									, s.[subscriber_db] 
									, s.[status]
									, p.[publication_type]
									, s.[subscription_type]'
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=0

		INSERT	INTO [monitoring].[statsReplicationLatency]([project_id], [distributor_server], [publication_name], [publication_type], [publisher_server], [publisher_db], [subscriber_server], [subscriber_db], [subscription_status], [subscription_type], [subscription_articles])
				EXEC (@queryToRun)

		FETCH NEXT FROM crsReplicationDistributorServers INTO @sqlServerName
	end
CLOSE crsReplicationDistributorServers
DEALLOCATE crsReplicationDistributorServers

		
------------------------------------------------------------------------------------------------------------------------------------------
--generate 21074 errors: The subscription(s) have been marked inactive and must be reinitialized.
------------------------------------------------------------------------------------------------------------------------------------------
RAISERROR('--Generate 21074 errors: The subscription(s) have been marked inactive and must be reinitialized.....', 10, 1) WITH NOWAIT

DECLARE   @publicationName		[sysname]
		, @publicationServer	[sysname]
		, @publisherDB			[sysname]
		, @subcriptionServer	[sysname]
		, @subscriptionDB		[sysname]
		, @distributorServer	[sysname]
		, @subscriptionArticles	[int]

DECLARE crsInactiveSubscriptions CURSOR FAST_FORWARD READ_ONLY FOR	SELECT [publication_name], [publisher_server], [publisher_db], [subscriber_server], [subscriber_db], [subscription_articles], [distributor_server]
																	FROM [monitoring].[statsReplicationLatency]
																	WHERE [subscription_status] = 0
OPEN crsInactiveSubscriptions
FETCH NEXT FROM crsInactiveSubscriptions INTO @publicationName, @publicationServer, @publisherDB, @subcriptionServer, @subscriptionDB, @subscriptionArticles, @distributorServer
WHILE @@FETCH_STATUS=0
	begin
		SET @queryToRun = 'Publication: [' + @publicationName + '] / Subscriber [' + @subcriptionServer + '].[' + @subscriptionDB + '] / Publisher: [' + @publicationServer + '].[' + @publisherDB + '] / Distributor: [' + @distributorServer + '] / Articles: ' + CAST(@subscriptionArticles as [nvarchar])
		RAISERROR(@queryToRun, 10, 1) WITH NOWAIT

		SET @eventMessageData = '<alert><detail>' + 
								'<error_code>21074</error_code>' + 
								'<error_string>The subscription(s) have been marked inactive and must be reinitialized.</error_string>' + 
								'<query_executed>' + @queryToRun + '</query_executed>' + 
								'<duration_seconds>' + CAST(ISNULL(DATEDIFF(ss, @runStartTime, GETUTCDATE()), 0) AS [nvarchar]) + '</duration_seconds>' + 
								'</detail></alert>'

		EXEC [dbo].[usp_logEventMessageAndSendEmail]	@projectCode			= @projectCode,
														@sqlServerName			= @publicationServer,
														@dbName					= @publicationName,
														@objectName				= @subcriptionServer,
														@childObjectName		= @subscriptionDB,
														@module					= 'monitoring',
														@eventName				= 'subscription marked inactive',
														@parameters				= NULL,			/* may contain the attach file name */
														@eventMessage			= @eventMessageData,
														@dbMailProfileName		= NULL,
														@recipientsList			= NULL,
														@eventType				= 1,	
														@additionalOption		= 0

		FETCH NEXT FROM crsInactiveSubscriptions INTO @publicationName, @publicationServer, @publisherDB, @subcriptionServer, @subscriptionDB, @subscriptionArticles, @distributorServer
	end
CLOSE crsInactiveSubscriptions
DEALLOCATE crsInactiveSubscriptions


------------------------------------------------------------------------------------------------------------------------------------------
RAISERROR('--Deploy temporary objects for Replication Latency analysis...', 10, 1) WITH NOWAIT

DECLARE crsActivePublications CURSOR FAST_FORWARD READ_ONLY FOR	SELECT DISTINCT [publisher_server]
																FROM [monitoring].[statsReplicationLatency]
																WHERE [subscription_status] <> 0
																		AND [publication_type] = 0 /* only transactional publications */
OPEN crsActivePublications
FETCH NEXT FROM crsActivePublications INTO @publicationServer
WHILE @@FETCH_STATUS=0
	begin
		SET @strMessage='--	running on server: ' + @publicationServer
		RAISERROR(@strMessage, 10, 1) WITH NOWAIT

		IF @publicationServer<>@@SERVERNAME
			SET @serverToRun = '[' + @publicationServer + '].tempdb.dbo.sp_executesql'
		ELSE
			SET @serverToRun = 'tempdb.dbo.sp_executesql'

		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'
			IF EXISTS(SELECT * FROM sysobjects WHERE [name] = ''usp_ReplicationGetPublicationLatency'' AND [type]=''P'')
				DROP PROCEDURE dbo.usp_ReplicationGetPublicationLatency'

		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @serverToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=0
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=0
		EXEC @serverToRun @queryToRun

		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'			
			CREATE PROCEDURE dbo.usp_ReplicationGetPublicationLatency
				  @publisherDB			[sysname]
				, @publicationName		[sysname]
				, @replicationDelay		[varchar](10) = N''00:00:15''
				, @iterations			[int] = 1
				, @iterationDelay		[varchar](10) = N''00:00:05''
			AS
			/*
				original code source:
				Name:       dba_replicationLatencyGet_sp
				Author:     Michelle F. Ufford
			*/
			SET NOCOUNT ON
			DECLARE   @currentIteration [int]
					, @tokenID			[bigint]
					, @currentDateTime	[smalldatetime]
					, @queryToRun		[nvarchar](4000)
					, @queryParam		[nvarchar](512)

			IF NOT EXISTS(SELECT * FROM sysobjects WHERE [name]=''replicationTokenResults'' AND [type]=''U'')
				CREATE TABLE [dbo].[replicationTokenResults]
					(
						  [publisher_db]		[sysname] NULL
						, [publication]			[sysname] NULL
						, [iteration]			[int] NULL
						, [tracer_id]			[int] NULL
						, [distributor_latency]	[int] NULL
						, [subscriber]			[sysname] NULL
						, [subscriber_db]		[sysname] NULL
						, [subscriber_latency]	[int] NULL
						, [overall_latency]		[int] NULL
					)
			ELSE
				DELETE FROM [dbo].[replicationTokenResults] WHERE [publication] = @publicationName AND [publisher_db] = @publisherDB

			DECLARE @temptokenresult TABLE 
				(
					  [tracer_id]			[int] NULL
					, [distributor_latency] [int] NULL
					, [subscriber]			[sysname] NULL
					, [subscriber_db]		[sysname] NULL
					, [subscriber_latency]	[int] NULL
					, [overall_latency]		[int] NULL
				);

			SET @currentIteration = 0
			SET @currentDateTime  = GETDATE()

			WHILE @currentIteration < @iterations
				begin
					/* Insert a new tracer token in the publication database */
					SET @queryToRun = N''EXECUTE ['' + @publisherDB + N''].sys.sp_postTracerToken @publication = @publicationName, @tracer_token_id = @tokenID OUTPUT''
					SET @queryParam = N''@publicationName [sysname], @tokenID [bigint] OUTPUT''
					
					PRINT @queryToRun
					EXEC sp_executesql @queryToRun, @queryParam , @publicationName = @publicationName
																, @tokenID = @tokenID OUTPUT

					/* Give a few seconds to allow the record to reach the subscriber */
					WAITFOR DELAY @replicationDelay

					SET @queryToRun = N''EXECUTE ['' + @publisherDB + N''].sys.sp_helpTracerTokenHistory @publicationName, @tokenID'' 
					PRINT @queryToRun

					/* Store our results in a temp table for retrieval later */
					INSERT	INTO @temptokenResult ([distributor_latency], [subscriber], [subscriber_db], [subscriber_latency], [overall_latency])
							EXEC sp_executesql @queryToRun, @queryParam , @publicationName = @publicationName
																		, @tokenID = @tokenID

					INSERT	[dbo].[replicationTokenResults] ([publisher_db], [publication], [distributor_latency], [subscriber], [subscriber_db], [subscriber_latency], [overall_latency])
							SELECT    @publisherDB
									, @publicationName
									, distributor_latency
									, subscriber
									, subscriber_db
									, subscriber_latency
									, overall_latency
							FROM @temptokenResult

					/* Assign the iteration and token id to the results for easier investigation */
					UPDATE [dbo].[replicationTokenResults]
					SET   [iteration] = @currentIteration + 1
						, [tracer_id] = @tokenID
					WHERE [iteration] IS NULL;

					DELETE FROM @temptokenresult

					/* Wait for the specified time period before creating another token */
					WAITFOR DELAY @iterationDelay

					SET @currentIteration = @currentIteration + 1;
				end;

			/* perform cleanup */
			SET @queryToRun = N''EXECUTE ['' + @publisherDB + N''].sys.sp_deleteTracerTokenHistory @publication = @publicationName, @cutoff_date = @currentDateTime''
			SET @queryParam = N''@publicationName [sysname], @currentDateTime [datetime]''
			PRINT @queryToRun

			EXEC sp_executesql @queryToRun, @queryParam , @publicationName = @publicationName
														, @currentDateTime = @currentDateTime

			/* SELECT * FROM [dbo].[replicationTokenResults] */'

		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @serverToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=0
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=0
		EXEC @serverToRun @queryToRun

		FETCH NEXT FROM crsActivePublications INTO @publicationServer
	end
CLOSE crsActivePublications
DEALLOCATE crsActivePublications


------------------------------------------------------------------------------------------------------------------------------------------
RAISERROR('--Generate Replication Latency check internal jobs..', 10, 1) WITH NOWAIT
------------------------------------------------------------------------------------------------------------------------------------------

DECLARE   @publisherInstanceID	[int]
		, @currentInstanceID	[int]

SELECT	@currentInstanceID = [id]
FROM	[dbo].[catalogInstanceNames] cin
WHERE	cin.[active] = 1
		AND cin.[project_id] = @projectID
		AND cin.[name] = @@SERVERNAME

DELETE FROM [dbo].[jobExecutionQueue]
WHERE [project_id] = @projectID
		AND [instance_id] = @currentInstanceID
		AND [module] = 'monitoring'
		AND [descriptor] = 'usp_monReplicationPublicationLatency'

DECLARE crsActivePublishers	CURSOR FAST_FORWARD READ_ONLY FOR	SELECT DISTINCT cin.[id], [publisher_server]
																FROM	[monitoring].[statsReplicationLatency] srl
																INNER JOIN [dbo].[catalogInstanceNames] cin ON srl.[publisher_server] = cin.[name]
																WHERE	[subscription_status] <> 0
																		AND [publication_type] = 0 /* only transactional publications */
																		AND cin.[active] = 1
																		AND cin.[project_id] = @projectID
OPEN crsActivePublishers
FETCH NEXT FROM crsActivePublishers INTO @publisherInstanceID, @publicationServer
WHILE @@FETCH_STATUS=0
	begin
		SET @strMessage='--	Analyzing server: ' + @publicationServer
		RAISERROR(@strMessage, 10, 1) WITH NOWAIT

		DECLARE crsActivePublications CURSOR FAST_FORWARD READ_ONLY FOR	SELECT DISTINCT [publication_name], [publisher_db]
																		FROM	[monitoring].[statsReplicationLatency] srl																		
																		WHERE	[subscription_status] <> 0
																				AND srl.[publisher_server] = @publicationServer
																				AND [publication_type] = 0 /* only transactional publications */
																			
		OPEN crsActivePublications
		FETCH NEXT FROM crsActivePublications INTO @publicationName, @publisherDB
		WHILE @@FETCH_STATUS=0
			begin

				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + N'EXEC [' + @publicationServer + '].tempdb.dbo.usp_ReplicationGetPublicationLatency @publisherDB = ''' + @publisherDB + N''', @publicationName = ''' + @publicationName + N''', @replicationDelay = ''' + @replicationDelay + N''', @iterations = ' + CAST(@iterations  AS [nvarchar]) + N', @iterationDelay = ''' + @iterationDelay + N''';'

				INSERT	INTO [dbo].[jobExecutionQueue](	[instance_id], [project_id], [module], [descriptor], [filter], [for_instance_id],
														[job_name], [job_step_name], [job_database_name], [job_command])
						SELECT	@currentInstanceID, @projectID, 'monitoring', 'usp_monReplicationPublicationLatency', @publicationName + ' - ' + @publisherDB, @publisherInstanceID,
								'dbaTDPMon - usp_monReplicationPublicationLatency(1) - ' + REPLACE(@publicationServer, '\', '_') + ' - ' + @publicationName + ' - ' + @publisherDB, 'Run Analysis', 'tempdb', @queryToRun
				
				FETCH NEXT FROM crsActivePublications INTO @publicationName, @publisherDB
			end
		CLOSE crsActivePublications
		DEALLOCATE crsActivePublications

		FETCH NEXT FROM crsActivePublishers INTO @publisherInstanceID, @publicationServer
	end
CLOSE crsActivePublishers
DEALLOCATE crsActivePublishers


------------------------------------------------------------------------------------------------------------------------------------------
RAISERROR('--Running jobs to compute replication latency..', 10, 1) WITH NOWAIT
------------------------------------------------------------------------------------------------------------------------------------------
EXEC dbo.usp_jobQueueExecute	@projectCode		= @projectCode,
								@moduleFilter		= 'monitoring',
								@descriptorFilter	= 'usp_monReplicationPublicationLatency',
								@waitForDelay		= '00:00:10',
								@debugMode			= @debugMode

UPDATE srl
	SET srl.[state] = 1	/* analysis job executed successfully */
FROM [monitoring].[statsReplicationLatency] srl
INNER JOIN [dbo].[jobExecutionQueue] jeq ON jeq.[filter] = srl.[publication_name] + ' - ' + srl.[publisher_db]
											AND jeq.[job_name] = 'dbaTDPMon - usp_monReplicationPublicationLatency(1) - ' + REPLACE(srl.[publisher_server], '\', '_') + ' - ' + srl.[publication_name] + ' - ' + srl.[publisher_db]
											AND jeq.[job_step_name] = 'Run Analysis'
WHERE	jeq.[module] = 'monitoring'
		AND jeq.[descriptor] = 'usp_monReplicationPublicationLatency'
		AND jeq.[status] = 1 /* succedded */

------------------------------------------------------------------------------------------------------------------------------------------
RAISERROR('--Generate Replication Latency getdata internal jobs..', 10, 1) WITH NOWAIT
------------------------------------------------------------------------------------------------------------------------------------------

DECLARE crsActivePublishers	CURSOR FAST_FORWARD READ_ONLY FOR	SELECT DISTINCT cin.[id], [publisher_server]
																FROM	[monitoring].[statsReplicationLatency] srl
																INNER JOIN [dbo].[catalogInstanceNames] cin ON srl.[publisher_server] = cin.[name]
																WHERE	[subscription_status] <> 0
																		AND [publication_type] = 0 /* only transactional publications */
																		AND cin.[active] = 1
																		AND cin.[project_id] = @projectID
OPEN crsActivePublishers
FETCH NEXT FROM crsActivePublishers INTO @publisherInstanceID, @publicationServer
WHILE @@FETCH_STATUS=0
	begin
		SET @strMessage='--	Analyzing server: ' + @publicationServer
		RAISERROR(@strMessage, 10, 1) WITH NOWAIT
		
		DECLARE crsActivePublications CURSOR FAST_FORWARD READ_ONLY FOR	SELECT DISTINCT [publication_name], [publisher_db]
																		FROM	[monitoring].[statsReplicationLatency] srl																		
																		WHERE	[subscription_status] <> 0
																				AND srl.[publisher_server] = @publicationServer
																				AND [publication_type] = 0 /* only transactional publications */
		OPEN crsActivePublications
		FETCH NEXT FROM crsActivePublications INTO @publicationName, @publisherDB
		WHILE @@FETCH_STATUS=0
			begin
				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + N'SELECT * FROM tempdb.[dbo].[replicationTokenResults] WHERE [publication]=''' + @publicationName + N''' AND [publisher_db] = ''' + @publisherDB + N''''
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@publicationServer, @queryToRun)

				SET @queryToRun = 'UPDATE srl
										SET   srl.[distributor_latency] = x.[distributor_latency]
											, srl.[subscriber_latency] = x.[subscriber_latency]
											, srl.[overall_latency] = x.[overall_latency]
											, srl.[event_date_utc] = GETUTCDATE()
									FROM [monitoring].[statsReplicationLatency] srl
									INNER JOIN (
													SELECT    [publisher_db], [publication], [subscriber], [subscriber_db]
															, MAX(ISNULL([distributor_latency],  2147483647))	AS [distributor_latency]
															, MAX(ISNULL([subscriber_latency],  2147483647))	AS [subscriber_latency]
															, MAX(ISNULL([overall_latency],  2147483647))		AS [overall_latency]
													FROM (' + @queryToRun + ')y
													GROUP BY [publisher_db], [publication], [subscriber], [subscriber_db]
												)x ON	srl.[publisher_db] = x.[publisher_db] 
													and srl.[publication_name] = x.[publication] 
													AND srl.[subscriber_server] = x.[subscriber] 
													AND srl.[subscriber_db] = x.[subscriber_db]
													AND srl.[publisher_server] = ''' + @publicationServer + N''''
		
				INSERT	INTO [dbo].[jobExecutionQueue](	[instance_id], [project_id], [module], [descriptor], [filter], [for_instance_id],
														[job_name], [job_step_name], [job_database_name], [job_command])
						SELECT	@currentInstanceID, @projectID, 'monitoring', 'usp_monReplicationPublicationLatency', @publicationName + ' - ' + @publisherDB , @publisherInstanceID,
								'dbaTDPMon - usp_monReplicationPublicationLatency(2) - ' + REPLACE(@publicationServer, '\', '_') + ' - ' + @publicationName + ' - ' + @publisherDB, 'Get Latency', DB_NAME(), @queryToRun

				FETCH NEXT FROM crsActivePublications INTO @publicationName, @publisherDB
			end
		CLOSE crsActivePublications
		DEALLOCATE crsActivePublications

		FETCH NEXT FROM crsActivePublishers INTO @publisherInstanceID, @publicationServer
	end
CLOSE crsActivePublishers
DEALLOCATE crsActivePublishers


------------------------------------------------------------------------------------------------------------------------------------------
RAISERROR('--Running GetData jobs..', 10, 1) WITH NOWAIT
------------------------------------------------------------------------------------------------------------------------------------------
EXEC dbo.usp_jobQueueExecute	@projectCode		= @projectCode,
								@moduleFilter		= 'monitoring',
								@descriptorFilter	= 'usp_monReplicationPublicationLatency',
								@waitForDelay		= '00:00:10',
								@debugMode			= @debugMode

UPDATE [monitoring].[statsReplicationLatency] SET [distributor_latency] = NULL	WHERE [distributor_latency] = 2147483647
UPDATE [monitoring].[statsReplicationLatency] SET [subscriber_latency] = NULL	WHERE [subscriber_latency] = 2147483647
UPDATE [monitoring].[statsReplicationLatency] SET [overall_latency] = NULL		WHERE [overall_latency] = 2147483647

UPDATE srl
	SET srl.[state] = 2	/* getdate job executed successfully */
FROM [monitoring].[statsReplicationLatency] srl
INNER JOIN [dbo].[jobExecutionQueue] jeq ON jeq.[filter] = srl.[publication_name] + ' - ' + srl.[publisher_db]
											AND jeq.[job_name] = 'dbaTDPMon - usp_monReplicationPublicationLatency(2) - ' + REPLACE(srl.[publisher_server], '\', '_') + ' - ' + srl.[publication_name] + ' - ' + srl.[publisher_db]
											AND jeq.[job_step_name] = 'Get Latency'
WHERE	jeq.[module] = 'monitoring'
		AND jeq.[descriptor] = 'usp_monReplicationPublicationLatency'
		AND jeq.[status] = 1 /* succedded */


------------------------------------------------------------------------------------------------------------------------------------------
RAISERROR('--Perform cleanup...', 10, 1) WITH NOWAIT

DECLARE crsActivePublications CURSOR FAST_FORWARD READ_ONLY FOR	SELECT DISTINCT [publisher_server]
																FROM	[monitoring].[statsReplicationLatency]
																WHERE	[subscription_status] <> 0
																		AND [publication_type] = 0 /* only transactional publications */
OPEN crsActivePublications
FETCH NEXT FROM crsActivePublications INTO @publicationServer
WHILE @@FETCH_STATUS=0
	begin
		SET @strMessage='--	running on server: ' + @publicationServer
		RAISERROR(@strMessage, 10, 1) WITH NOWAIT

		IF @publicationServer<>@@SERVERNAME
			SET @serverToRun = '[' + @publicationServer + '].tempdb.dbo.sp_executesql'
		ELSE
			SET @serverToRun = 'tempdb.dbo.sp_executesql'

		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'
			IF EXISTS(SELECT * FROM sysobjects WHERE [name] = ''usp_ReplicationGetPublicationLatency'' AND [type]=''P'')
				DROP PROCEDURE dbo.usp_ReplicationGetPublicationLatency'

		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @serverToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=0
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=0
		EXEC @serverToRun @queryToRun

		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'
			IF EXISTS(SELECT * FROM sysobjects WHERE [name] = ''replicationTokenResults'' AND [type]=''U'')
				DROP TABLE dbo.replicationTokenResults'

		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @serverToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=0
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=0
		EXEC @serverToRun @queryToRun

		FETCH NEXT FROM crsActivePublications INTO @publicationServer
	end
CLOSE crsActivePublications
DEALLOCATE crsActivePublications


------------------------------------------------------------------------------------------------------------------------------------------
--generate alerts: Replication latency exceeds thresold
------------------------------------------------------------------------------------------------------------------------------------------
RAISERROR('--generate alerts: Replication latency exceeds thresold...', 10, 1) WITH NOWAIT

DECLARE   @instanceName		[sysname]
		, @objectName		[nvarchar](512)
		, @eventName		[sysname]
		, @severity			[sysname]
		, @eventMessage		[nvarchar](max)


DECLARE crsReplicationAlarms CURSOR FOR	SELECT  DISTINCT
												  srl.[publisher_server] AS [instance_name]
												, 'Publication: ' + srl.[publication_name] + ' - Subscriber:' + srl.[subscriber_server] + '.' + srl.[subscriber_db] AS [object_name]
												, 'critical'			AS [severity]
												, 'replication latency'	AS [event_name]
												, '<alert><detail>' + 
													'<severity>critical</severity>' + 
													'<machine_name>' + cin.[machine_name] + '</machine_name>' + 
													'<counter_name>replication latency</counter_name>
													<target_name>Publication: ' + srl.[publication_name] + ' / Subscriber: [' + srl.[subscriber_server] + '].[' + srl.[subscriber_db] + '] / Publisher: [' + srl.[publisher_server] + '].[' + srl.[publisher_db] + '] / Distributor: [' + srl.[distributor_server] + ']</target_name>' + 
													'<measure_unit>sec</measure_unit>' + 
													'<current_value>' + ISNULL(CAST(srl.[overall_latency] AS [nvarchar]), '-1') +'</current_value>' + 
													'<threshold_value>' + CAST(@alertThresholdCriticalReplicationLatencySec AS [varchar]) + '</threshold_value>' + 
													'<event_date_utc>' + CONVERT([varchar](20), srl.[event_date_utc], 120) + '</event_date_utc>' + 
													'</detail></alert>' AS [event_message]
										FROM [dbo].[vw_catalogInstanceNames]  cin
										INNER JOIN [monitoring].[statsReplicationLatency] srl ON srl.[project_id] = cin.[project_id] AND srl.[publisher_server] = cin.[instance_name]
										LEFT JOIN [monitoring].[alertSkipRules] asr ON	asr.[category] = 'replication latency'
																						AND asr.[alert_name] IN ('')
																						AND asr.[active] = 1
																						AND (asr.[skip_value] = cin.[machine_name] OR asr.[skip_value]=cin.[instance_name])
																						AND (   asr.[skip_value2] IS NULL 
																							 OR (asr.[skip_value2] IS NOT NULL AND asr.[skip_value2] = (srl.[subscriber_server] + '.' + srl.[subscriber_db]))
																							)
										WHERE cin.[instance_active]=1
												AND cin.[project_id] = @projectID
												AND cin.[instance_name] LIKE @sqlServerNameFilter
												AND (srl.[overall_latency] IS NULL OR srl.[overall_latency]>=@alertThresholdCriticalReplicationLatencySec)									
												AND srl.[subscription_status] <> 0
												AND srl.[state] = 2 /* run analysis and get data jobs completed successfully */
										ORDER BY [instance_name], [object_name]
OPEN crsReplicationAlarms
FETCH NEXT FROM crsReplicationAlarms INTO @instanceName, @objectName, @severity, @eventName, @eventMessage
WHILE @@FETCH_STATUS=0
	begin
		EXEC [dbo].[usp_logEventMessageAndSendEmail]	@projectCode			= @projectCode,
														@sqlServerName			= @instanceName,
														@dbName					= @severity,
														@objectName				= @objectName,
														@childObjectName		= NULL,
														@module					= 'monitoring',
														@eventName				= @eventName,
														@parameters				= NULL,	
														@eventMessage			= @eventMessage,
														@dbMailProfileName		= NULL,
														@recipientsList			= NULL,
														@eventType				= 6,	/* 6 - alert-custom */
														@additionalOption		= 0

		FETCH NEXT FROM crsReplicationAlarms INTO @instanceName, @objectName, @severity, @eventName, @eventMessage
	end
CLOSE crsReplicationAlarms
DEALLOCATE crsReplicationAlarms
GO
