RAISERROR('Create procedure: [dbo].[usp_monAlarmCustomReplicationLatency]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_monAlarmCustomReplicationLatency]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_monAlarmCustomReplicationLatency]
GO

CREATE PROCEDURE [dbo].[usp_monAlarmCustomReplicationLatency]
		  @projectCode			[varchar](32)=NULL
		, @sqlServerNameFilter	[sysname]='%'
		, @operationDelay		[varchar](10) = N'00:00:05'
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
		, @additionalRecipients	[nvarchar](1024)
		, @eventName			[sysname]
		, @publicationDetails	[sysname]

------------------------------------------------------------------------------------------------------------------------------------------
IF OBJECT_ID('tempdb..#statsReplicationLatency') IS NOT NULL DROP TABLE #statsReplicationLatency
CREATE TABLE #statsReplicationLatency
(
	[distributor_server]		[sysname]	NOT NULL,
	[publication_name]			[sysname]	NOT NULL,
	[publication_type]			[int]		NOT NULL,
	[publisher_server]			[sysname]	NOT NULL,
	[publisher_db]				[sysname]	NOT NULL,
	[subscriber_server]			[sysname]	NOT NULL,
	[subscriber_db]				[sysname]	NOT NULL,
	[subscription_type]			[int]		NOT NULL,
	[subscription_status]		[tinyint]	NOT NULL,
	[subscription_articles]		[int]		NULL
)

------------------------------------------------------------------------------------------------------------------------------------------
--get default projectCode
IF @projectCode IS NULL
	SET @projectCode = [dbo].[ufn_getProjectCode](NULL, NULL)

SELECT @projectID = [id]
FROM [dbo].[catalogProjects]
WHERE [code] = @projectCode 

IF @projectID IS NULL
	begin
		SET @strMessage=N'The value specifief for Project Code is not valid.'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=1
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

---------------------------------------------------------------------------------------------
--get configuration values
---------------------------------------------------------------------------------------------
DECLARE @queryLockTimeOut [int]
SELECT	@queryLockTimeOut=[value] 
FROM	[dbo].[appConfigurations] 
WHERE	[name] = 'Default lock timeout (ms)'
		AND [module] = 'common'
				
------------------------------------------------------------------------------------------------------------------------------------------
SET @strMessage = 'Delete existing information....'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0


DELETE srl
FROM [monitoring].[statsReplicationLatency]	srl
INNER JOIN [dbo].[vw_catalogDatabaseNames] cdn ON	cdn.[instance_name] = srl.[publisher_server] 
													AND cdn.[database_name] = srl.[publisher_db]
													AND cdn.[project_id] = srl.[project_id]	
WHERE cdn.[project_id] = @projectID
	AND cdn.[instance_name] LIKE @sqlServerNameFilter


------------------------------------------------------------------------------------------------------------------------------------------
SET @strMessage='Get Publications & Subscriptions Information....'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

SET @runStartTime = GETUTCDATE()

--replication distribution servers
DECLARE crsReplicationDistributorServers CURSOR LOCAL FAST_FORWARD  FOR	SELECT	[instance_name]
																		FROM	[dbo].[vw_catalogDatabaseNames] 
																		WHERE	[active] = 1
																				AND [database_name] = 'distribution'
																				
OPEN crsReplicationDistributorServers
FETCH NEXT FROM crsReplicationDistributorServers INTO @sqlServerName
WHILE @@FETCH_STATUS=0
	begin
		SET @strMessage='	Analyzing server: ' + @sqlServerName
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 1, @messageTreelevel = 1, @stopExecution=0

		--publications and subscriptions
		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'
							SELECT    @@SERVERNAME		AS [distributor_server]
									, p.[publication]	AS [publication_name]
									, p.[publication_type]
									, srv.[srvname]		AS [publisher_server]
									, p.[publisher_db]
									, ss.[srvname]		AS [subscriber_server]
									, s.[subscriber_db] 
									, s.[status]		AS [subscription_status]
									, s.[subscription_type]
									, COUNT(DISTINCT s.[article_id]) AS [subscription_articles]
							FROM [distribution].[dbo].MSpublications p 
							JOIN [distribution].[dbo].MSsubscriptions s ON p.[publication_id] = s.[publication_id] 
							JOIN [distribution].[dbo].[MSreplservers] ss ON s.[subscriber_id] = ss.[srvid]
							JOIN [distribution].[dbo].[MSreplservers] srv ON srv.[srvid] = p.[publisher_id]
							JOIN [distribution].[dbo].MSdistribution_agents da ON da.[publisher_id] = p.[publisher_id] AND da.[subscriber_id] = s.[subscriber_id] 
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
		
		BEGIN TRY
			INSERT	INTO #statsReplicationLatency([distributor_server], [publication_name], [publication_type], [publisher_server], [publisher_db], [subscriber_server], [subscriber_db], [subscription_status], [subscription_type], [subscription_articles])
					EXEC sp_executesql @queryToRun
		END TRY
		BEGIN CATCH
			SET @strMessage = ERROR_MESSAGE()
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
		END CATCH

		FETCH NEXT FROM crsReplicationDistributorServers INTO @sqlServerName
	end
CLOSE crsReplicationDistributorServers
DEALLOCATE crsReplicationDistributorServers

INSERT	INTO [monitoring].[statsReplicationLatency]([project_id], [distributor_server], [publication_name], [publication_type], [publisher_server], [publisher_db], [subscriber_server], [subscriber_db], [subscription_status], [subscription_type], [subscription_articles])
		SELECT    cdn.[project_id], srl.[distributor_server], srl.[publication_name], srl.[publication_type], srl.[publisher_server], srl.[publisher_db]
				, srl.[subscriber_server], srl.[subscriber_db], srl.[subscription_status], srl.[subscription_type], srl.[subscription_articles]
		FROM #statsReplicationLatency srl
		INNER JOIN [dbo].[vw_catalogDatabaseNames] cdn ON	cdn.[instance_name] = srl.[publisher_server] COLLATE DATABASE_DEFAULT
													AND cdn.[database_name] = srl.[publisher_db] COLLATE DATABASE_DEFAULT
		WHERE cdn.[project_id] = @projectID
			AND cdn.[instance_name] LIKE @sqlServerNameFilter


------------------------------------------------------------------------------------------------------------------------------------------
--generate 21074 errors: The subscription(s) have been marked inactive and must be reinitialized.
------------------------------------------------------------------------------------------------------------------------------------------
SET @strMessage = 'Generate 21074 errors: The subscription(s) have been marked inactive and must be reinitialized.....'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

DECLARE   @publicationName		[sysname]
		, @publicationServer	[sysname]
		, @publisherDB			[sysname]
		, @subcriptionServer	[sysname]
		, @subscriptionDB		[sysname]
		, @distributorServer	[sysname]
		, @subscriptionArticles	[int]

DECLARE crsInactiveSubscriptions CURSOR LOCAL FAST_FORWARD FOR	SELECT DISTINCT
																		  srl.[publication_name], srl.[publisher_server], srl.[publisher_db]
																		, srl.[subscriber_server], srl.[subscriber_db], srl.[subscription_articles], srl.[distributor_server]
																FROM [monitoring].[statsReplicationLatency] srl
																INNER JOIN [dbo].[vw_catalogDatabaseNames] cdn ON	cdn.[instance_name] = srl.[publisher_server] 
																													AND cdn.[database_name] = srl.[publisher_db]
																													AND cdn.[project_id] = srl.[project_id]	
																LEFT JOIN [monitoring].[alertSkipRules] asr ON	asr.[category] = 'replication'
																											AND asr.[alert_name] IN ('subscription marked inactive')
																											AND asr.[active] = 1
																											AND (asr.[skip_value] = ('[' + srl.[publisher_server] + '].[' + srl.[publisher_db] + '](' + srl.[publication_name] + ')'))
																											AND (    asr.[skip_value2] IS NULL 
																													OR (asr.[skip_value2] IS NOT NULL AND asr.[skip_value2] = ('[' + srl.[subscriber_server] + '].[' + srl.[subscriber_db] + ']'))
																												)
																WHERE	srl.[subscription_status] = 0 /* inactive subscriptions */
																		AND cdn.[project_id] = @projectID
																		AND cdn.[instance_name] LIKE @sqlServerNameFilter
																		AND asr.[id] IS NULL
OPEN crsInactiveSubscriptions
FETCH NEXT FROM crsInactiveSubscriptions INTO @publicationName, @publicationServer, @publisherDB, @subcriptionServer, @subscriptionDB, @subscriptionArticles, @distributorServer
WHILE @@FETCH_STATUS=0
	begin
		SET @queryToRun = 'Publication: ' + [dbo].[ufn_getObjectQuoteName](@publicationName, 'quoted') + ' / Subscriber ' + [dbo].[ufn_getObjectQuoteName](@subcriptionServer, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](@subscriptionDB, 'quoted') + ' / Publisher: ' + [dbo].[ufn_getObjectQuoteName](@publicationServer, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](@publisherDB, 'quoted') + ' / Distributor: ' + [dbo].[ufn_getObjectQuoteName](@distributorServer, 'quoted') + ' / Articles: ' + CAST(@subscriptionArticles as [nvarchar])
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = 1, @messageTreelevel = 1, @stopExecution=0

		SET @eventMessageData = '<alert><detail>' + 
								'<error_code>21074</error_code>' + 
								'<error_string>The subscription(s) have been marked inactive and must be reinitialized.</error_string>' + 
								'<query_executed>' + [dbo].[ufn_getObjectQuoteName](@queryToRun, 'xml') + '</query_executed>' + 
								'<duration_seconds>' + CAST(ISNULL(DATEDIFF(ss, @runStartTime, GETUTCDATE()), 0) AS [nvarchar]) + '</duration_seconds>' + 
								'<event_date_utc>' + CONVERT([varchar](20), GETUTCDATE(), 120) + '</event_date_utc>' + 
								'</detail></alert>'

		SET @eventName = 'subscription marked inactive'

		/* check for additional receipients for the alert */		
		SET @publicationDetails = 'Publication: ' + @publicationName + ' - Subscriber: ' + @subcriptionServer + '.' + @subscriptionDB
		SET @additionalRecipients = [dbo].[ufn_monGetAdditionalAlertRecipients](@projectID, @publicationServer, @eventName, @publicationDetails)

		EXEC [dbo].[usp_logEventMessageAndSendEmail]	@sqlServerName			= @publicationServer,
														@dbName					= @publicationName,
														@objectName				= @subcriptionServer,
														@childObjectName		= @subscriptionDB,
														@module					= 'monitoring',
														@eventName				= @eventName,
														@parameters				= NULL,			/* may contain the attach file name */
														@eventMessage			= @eventMessageData,
														@dbMailProfileName		= NULL,
														@recipientsList			= @additionalRecipients,
														@eventType				= 1,	
														@additionalOption		= 0

		FETCH NEXT FROM crsInactiveSubscriptions INTO @publicationName, @publicationServer, @publisherDB, @subcriptionServer, @subscriptionDB, @subscriptionArticles, @distributorServer
	end
CLOSE crsInactiveSubscriptions
DEALLOCATE crsInactiveSubscriptions


------------------------------------------------------------------------------------------------------------------------------------------
-- Subscribed but not active subscriptions
------------------------------------------------------------------------------------------------------------------------------------------
SET @strMessage='Check for subscribed but not active subscriptions....'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

DECLARE crsInactiveSubscriptions CURSOR LOCAL FAST_FORWARD FOR	SELECT DISTINCT
																		  srl.[publication_name], srl.[publisher_server], srl.[publisher_db], srl.[subscriber_server]
																		, srl.[subscriber_db], srl.[subscription_articles], srl.[distributor_server]
																FROM [monitoring].[statsReplicationLatency] srl
																INNER JOIN [dbo].[vw_catalogDatabaseNames] cdn ON	cdn.[instance_name] = srl.[publisher_server] 
																													AND cdn.[database_name] = srl.[publisher_db]
																													AND cdn.[project_id] = srl.[project_id]	
																LEFT JOIN [monitoring].[alertSkipRules] asr ON	asr.[category] = 'replication'
																											AND asr.[alert_name] IN ('subscription not active')
																											AND asr.[active] = 1
																											AND (asr.[skip_value] = ('[' + srl.[publisher_server] + '].[' + srl.[publisher_db] + '](' + srl.[publication_name] + ')'))
																											AND (    asr.[skip_value2] IS NULL 
																													OR (asr.[skip_value2] IS NOT NULL AND asr.[skip_value2] = ('[' + srl.[subscriber_server] + '].[' + srl.[subscriber_db] + ']'))
																												)
																WHERE	srl.[subscription_status] = 1 /* subscribed subscriptions */
																		AND cdn.[project_id] = @projectID
																		AND cdn.[instance_name] LIKE @sqlServerNameFilter
																		AND asr.[id] IS NULL
OPEN crsInactiveSubscriptions
FETCH NEXT FROM crsInactiveSubscriptions INTO @publicationName, @publicationServer, @publisherDB, @subcriptionServer, @subscriptionDB, @subscriptionArticles, @distributorServer
WHILE @@FETCH_STATUS=0
	begin
		SET @queryToRun = 'Publication: ' + [dbo].[ufn_getObjectQuoteName](@publicationName, 'quoted') + ' / Subscriber ' + [dbo].[ufn_getObjectQuoteName](@subcriptionServer, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](@subscriptionDB, 'quoted') + ' / Publisher: ' + [dbo].[ufn_getObjectQuoteName](@publicationServer, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](@publisherDB, 'quoted') + ' / Distributor: ' + [dbo].[ufn_getObjectQuoteName](@distributorServer, 'quoted') + ' / Articles: ' + CAST(@subscriptionArticles as [nvarchar])
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = 1, @messageTreelevel = 1, @stopExecution=0

		SET @eventMessageData = '<alert><detail>' + 
								'<error_code>21488</error_code>' + 
								'<error_string>The subscription is not active. Subscription must have active in order to post a tracer token.</error_string>' + 
								'<query_executed>' + @queryToRun + '</query_executed>' + 
								'<duration_seconds>' + CAST(ISNULL(DATEDIFF(ss, @runStartTime, GETUTCDATE()), 0) AS [nvarchar]) + '</duration_seconds>' + 
								'<event_date_utc>' + CONVERT([varchar](20), GETUTCDATE(), 120) + '</event_date_utc>' + 
								'</detail></alert>'

		SET @eventName = 'subscription not active'

		/* check for additional receipients for the alert */		
		SET @publicationDetails = 'Publication: ' + @publicationName + ' - Subscriber: ' + @subcriptionServer + '.' + @subscriptionDB
		SET @additionalRecipients = [dbo].[ufn_monGetAdditionalAlertRecipients](@projectID, @publicationServer, @eventName, @publicationDetails)

		EXEC [dbo].[usp_logEventMessageAndSendEmail]	@sqlServerName			= @publicationServer,
														@dbName					= @publicationName,
														@objectName				= @subcriptionServer,
														@childObjectName		= @subscriptionDB,
														@module					= 'monitoring',
														@eventName				= @eventName,
														@parameters				= NULL,			/* may contain the attach file name */
														@eventMessage			= @eventMessageData,
														@dbMailProfileName		= NULL,
														@recipientsList			= @additionalRecipients,
														@eventType				= 1,	
														@additionalOption		= 0

		FETCH NEXT FROM crsInactiveSubscriptions INTO @publicationName, @publicationServer, @publisherDB, @subcriptionServer, @subscriptionDB, @subscriptionArticles, @distributorServer
	end
CLOSE crsInactiveSubscriptions
DEALLOCATE crsInactiveSubscriptions

------------------------------------------------------------------------------------------------------------------------------------------
SET @strMessage='Deploy temporary objects for Replication Latency analysis...'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

DECLARE crsActivePublications CURSOR LOCAL FAST_FORWARD FOR	SELECT DISTINCT srl.[publisher_server]
															FROM [monitoring].[statsReplicationLatency] srl
															INNER JOIN [dbo].[vw_catalogDatabaseNames] cdn ON	cdn.[instance_name] = srl.[publisher_server] 
																												AND cdn.[database_name] = srl.[publisher_db]
																												AND cdn.[project_id] = srl.[project_id]	
															LEFT JOIN [monitoring].[alertSkipRules] asr ON	asr.[category] = 'replication'
																										AND asr.[alert_name] IN ('subscription marked inactive', 'subscription not active')
																										AND asr.[active] = 1
																										AND (asr.[skip_value] = ('[' + srl.[publisher_server] + '].[' + srl.[publisher_db] + '](' + srl.[publication_name] + ')'))
																										AND (    asr.[skip_value2] IS NULL 
																												OR (asr.[skip_value2] IS NOT NULL AND asr.[skip_value2] = ('[' + srl.[subscriber_server] + '].[' + srl.[subscriber_db] + ']'))
																											)

															WHERE srl.[subscription_status] = 2 /* active subscriptions */
																	AND srl.[publication_type] = 0 /* only transactional publications */
																	AND cdn.[project_id] = @projectID
																	AND cdn.[instance_name] LIKE @sqlServerNameFilter
																	AND asr.[id] IS NULL
OPEN crsActivePublications
FETCH NEXT FROM crsActivePublications INTO @publicationServer
WHILE @@FETCH_STATUS=0
	begin
		SET @strMessage='running on server: ' + @publicationServer
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 1, @messageTreelevel = 1, @stopExecution=0

		IF @publicationServer<>@@SERVERNAME
			SET @serverToRun = '[' + @publicationServer + '].tempdb.dbo.sp_executesql'
		ELSE
			SET @serverToRun = 'tempdb.dbo.sp_executesql'

		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'
			IF EXISTS(SELECT * FROM sysobjects WHERE [name] = ''usp_monGetReplicationLatency'' AND [type]=''P'')
				DROP PROCEDURE dbo.usp_monGetReplicationLatency'

		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @serverToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=0
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=0
		BEGIN TRY
			EXEC @serverToRun @queryToRun
		END TRY
		BEGIN CATCH
			SET @strMessage = ERROR_MESSAGE()
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
		END CATCH

		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'
			CREATE PROCEDURE dbo.usp_monGetReplicationLatency
				  @publisherDB			[sysname]
				, @publicationName		[sysname]
				, @replicationDelay		[int] = 15
				, @operationDelay		[varchar](10) = N''00:00:05''
			AS
			/*
				original code source:
				Name:       dba_replicationLatencyGet_sp
				Author:     Michelle F. Ufford
				http://sqlfool.com/2008/11/checking-replication-latency-with-t-sql/
			*/
			SET NOCOUNT ON
			DECLARE   @currentIteration [int]
					, @tokenID			[bigint]
					, @currentDateTime	[smalldatetime]
					, @tokenStartTime	[datetime]
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

			DECLARE @tempTokenResult TABLE 
				(
					  [tracer_id]			[int] NULL
					, [distributor_latency] [int] NULL
					, [subscriber]			[sysname] NULL
					, [subscriber_db]		[sysname] NULL
					, [subscriber_latency]	[int] NULL
					, [overall_latency]		[int] NULL
				);

			SET @currentIteration = 1
			SET @currentDateTime  = GETDATE()

			WHILE @currentIteration <= 2
				begin
					/* Insert a new tracer token in the publication database */
					SET @queryToRun = N''EXECUTE ['' + @publisherDB + N''].sys.sp_posttracertoken @publication = @publicationName, @tracer_token_id = @tokenID OUTPUT''
					SET @queryParam = N''@publicationName [sysname], @tokenID [bigint] OUTPUT''
					
					PRINT @queryToRun
					SET @tokenStartTime = GETDATE()
					EXEC sp_executesql @queryToRun, @queryParam , @publicationName = @publicationName
																, @tokenID = @tokenID OUTPUT

					/* Give a few seconds to allow the record to reach the subscriber */
					WHILE GETDATE() <= DATEADD(ss, @replicationDelay, @tokenStartTime)
						begin
							/* Give a few seconds to allow the record to reach the subscriber */
							WAITFOR DELAY @operationDelay

							SET @queryToRun = N''EXECUTE ['' + @publisherDB + N''].sys.sp_helptracertokenhistory @publicationName, @tokenID'' 
							PRINT @queryToRun

							/* Store our results in a temp table for retrieval later */
							INSERT	INTO @tempTokenResult ([distributor_latency], [subscriber], [subscriber_db], [subscriber_latency], [overall_latency])
									EXEC sp_executesql @queryToRun, @queryParam , @publicationName = @publicationName
																				, @tokenID = @tokenID

							IF NOT EXISTS(	SELECT * FROM @tempTokenResult 
											WHERE [subscriber_latency] IS NULL OR [overall_latency] IS NULL OR [distributor_latency] IS NULL
										 )													
								BREAK
							ELSE
								DELETE FROM @tempTokenResult							
						end										

					INSERT	[dbo].[replicationTokenResults] ([publisher_db], [publication], [distributor_latency], [subscriber], [subscriber_db], [subscriber_latency], [overall_latency])
							SELECT    @publisherDB
									, @publicationName
									, distributor_latency
									, subscriber
									, subscriber_db
									, subscriber_latency
									, overall_latency
							FROM @tempTokenResult

					/* Assign the iteration and token id to the results for easier investigation */
					UPDATE [dbo].[replicationTokenResults]
					SET   [iteration] = @currentIteration
						, [tracer_id] = @tokenID
					WHERE [iteration] IS NULL;

					DELETE FROM @tempTokenResult		
					
					/* add retry mechanism for 1st iteration */
					IF	@currentIteration=1
						AND EXISTS(	SELECT * FROM [dbo].[replicationTokenResults] 
									WHERE	[publication] = @publicationName AND [publisher_db] = @publisherDB 
											AND ([overall_latency] IS NULL OR [distributor_latency] IS NULL OR [subscriber_latency] IS NULL)
									)
						begin
							DELETE FROM [dbo].[replicationTokenResults] 
							WHERE [publication] = @publicationName AND [publisher_db] = @publisherDB
							
							SET @currentIteration = @currentIteration + 1;					
						end
					ELSE
						SET @currentIteration = 3;	
				end;

			/* perform cleanup */
			SET @queryToRun = N''EXECUTE ['' + @publisherDB + N''].sys.sp_deletetracertokenhistory @publication = @publicationName, @cutoff_date = @currentDateTime''
			SET @queryParam = N''@publicationName [sysname], @currentDateTime [datetime]''
			PRINT @queryToRun

			EXEC sp_executesql @queryToRun, @queryParam , @publicationName = @publicationName
														, @currentDateTime = @currentDateTime

			/* SELECT * FROM [dbo].[replicationTokenResults]  */'

		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @serverToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=0
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=0
		BEGIN TRY
			EXEC @serverToRun @queryToRun
		END TRY
		BEGIN CATCH
			SET @strMessage = ERROR_MESSAGE()
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
		END CATCH


		FETCH NEXT FROM crsActivePublications INTO @publicationServer
	end
CLOSE crsActivePublications
DEALLOCATE crsActivePublications


------------------------------------------------------------------------------------------------------------------------------------------
SET @strMessage='Generate Replication Latency check internal jobs..'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
------------------------------------------------------------------------------------------------------------------------------------------

DECLARE   @publisherInstanceID	[int]
		, @currentInstanceID	[int]

SELECT	TOP 1 @currentInstanceID = [id]
FROM	[dbo].[catalogInstanceNames] cin
WHERE	cin.[active] = 1
		AND cin.[name] = @@SERVERNAME
		--AND cin.[project_id] = @projectID
ORDER BY [id]

/* save the previous executions statistics */
EXEC [dbo].[usp_jobExecutionSaveStatistics]	@projectCode		= @projectCode,
											@moduleFilter		= 'monitoring',
											@descriptorFilter	= 'dbo.usp_monAlarmCustomReplicationLatency'

/* save the execution history */
INSERT	INTO [dbo].[jobExecutionHistory]([instance_id], [project_id], [module], [descriptor], [filter], [for_instance_id], 
										 [job_name], [job_id], [job_step_name], [job_database_name], [job_command], [execution_date], 
										 [running_time_sec], [log_message], [status], [event_date_utc], [task_id], [database_name])
		SELECT	[instance_id], [project_id], [module], [descriptor], [filter], [for_instance_id], 
				[job_name], [job_id], [job_step_name], [job_database_name], [job_command], [execution_date], 
				[running_time_sec], [log_message], [status], [event_date_utc], [task_id], [database_name]
		FROM [dbo].[jobExecutionQueue]
		WHERE [project_id] = @projectID
				AND [instance_id] = @currentInstanceID
				AND [module] = 'monitoring'
				AND [descriptor] = 'dbo.usp_monAlarmCustomReplicationLatency'
				AND [status] <> -1

DELETE FROM [dbo].[jobExecutionQueue]
WHERE [project_id] = @projectID
		AND [instance_id] = @currentInstanceID
		AND [module] = 'monitoring'
		AND [descriptor] = 'dbo.usp_monAlarmCustomReplicationLatency'

DECLARE crsActivePublishers	CURSOR LOCAL FAST_FORWARD FOR	SELECT DISTINCT cin.[id], [publisher_server]
															FROM	[monitoring].[statsReplicationLatency] srl
															INNER JOIN [dbo].[vw_catalogDatabaseNames] cdn ON	cdn.[instance_name] = srl.[publisher_server] 
																												AND cdn.[database_name] = srl.[publisher_db]
																												AND cdn.[project_id] = srl.[project_id]	
															INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[project_id] = cdn.[project_id]
																											AND cin.[id] = cdn.[instance_id]
															WHERE	srl.[subscription_status] = 2 /* active subscriptions */
																	AND srl.[publication_type] = 0 /* only transactional publications */
																	AND cdn.[project_id] = @projectID
																	AND cdn.[instance_name] LIKE @sqlServerNameFilter
																	AND cin.[active] = 1
OPEN crsActivePublishers
FETCH NEXT FROM crsActivePublishers INTO @publisherInstanceID, @publicationServer
WHILE @@FETCH_STATUS=0
	begin
		SET @strMessage='Analyzing server: ' + @publicationServer
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 1, @messageTreelevel = 1, @stopExecution=0

		DECLARE crsActivePublications CURSOR LOCAL FAST_FORWARD FOR	SELECT DISTINCT srl.[publication_name], srl.[publisher_db]
																	FROM	[monitoring].[statsReplicationLatency] srl		
																	INNER JOIN [dbo].[vw_catalogDatabaseNames] cdn ON	cdn.[instance_name] = srl.[publisher_server] 
																													AND cdn.[database_name] = srl.[publisher_db]
																													AND cdn.[project_id] = srl.[project_id]	
																
																	WHERE	srl.[subscription_status] = 2 /* active subscriptions */
																			AND srl.[publisher_server] = @publicationServer
																			AND srl.[publication_type] = 0 /* only transactional publications */
																			AND cdn.[project_id] = @projectID
																			AND cdn.[instance_name] LIKE @sqlServerNameFilter
		OPEN crsActivePublications
		FETCH NEXT FROM crsActivePublications INTO @publicationName, @publisherDB
		WHILE @@FETCH_STATUS=0
			begin

				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + N'SELECT * FROM sys.databases WHERE name=''' + [dbo].[ufn_getObjectQuoteName](@publisherDB, 'sql') + N''' AND state_desc=''ONLINE'' AND 1=1'
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@publicationServer, @queryToRun)

				SET @queryToRun = N'IF EXISTS(' + @queryToRun + N')
					EXEC [' + @publicationServer + '].tempdb.dbo.usp_monGetReplicationLatency @publisherDB = ''' + [dbo].[ufn_getObjectQuoteName](@publisherDB, 'sql') + N''', @publicationName = ''' + [dbo].[ufn_getObjectQuoteName](@publicationName, 'sql') + N''', @replicationDelay = ' + CAST(@alertThresholdCriticalReplicationLatencySec AS [nvarchar]) + N', @operationDelay = ''' + @operationDelay + N''';'

				INSERT	INTO [dbo].[jobExecutionQueue](	[instance_id], [project_id], [module], [descriptor], [task_id],
														[filter], [for_instance_id],
														[job_name], [job_step_name], [job_database_name], [job_command])
						SELECT	@currentInstanceID, @projectID, 'monitoring', 'dbo.usp_monAlarmCustomReplicationLatency', 
								(SELECT it.[id] FROM [dbo].[appInternalTasks] it WHERE it.[descriptor] = 'dbo.usp_monAlarmCustomReplicationLatency'),
								@publicationName + ' - ' + @publisherDB, @publisherInstanceID,
								'dbaTDPMon - usp_monAlarmCustomReplicationLatency(1) - ' + REPLACE(@publicationServer, '\', '$') + ' - ' + @publicationName + ' - ' + @publisherDB, 'Run Analysis', 'tempdb', @queryToRun
				
				FETCH NEXT FROM crsActivePublications INTO @publicationName, @publisherDB
			end
		CLOSE crsActivePublications
		DEALLOCATE crsActivePublications

		FETCH NEXT FROM crsActivePublishers INTO @publisherInstanceID, @publicationServer
	end
CLOSE crsActivePublishers
DEALLOCATE crsActivePublishers


------------------------------------------------------------------------------------------------------------------------------------------
SET @strMessage='Running jobs to compute replication latency..'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

------------------------------------------------------------------------------------------------------------------------------------------
EXEC dbo.usp_jobQueueExecute	@projectCode		= @projectCode,
								@moduleFilter		= 'monitoring',
								@descriptorFilter	= 'dbo.usp_monAlarmCustomReplicationLatency',
								@waitForDelay		= DEFAULT,
								@debugMode			= @debugMode

UPDATE srl
	SET srl.[state] = 1	/* analysis job executed successfully */
FROM [monitoring].[statsReplicationLatency] srl
INNER JOIN [dbo].[jobExecutionQueue] jeq ON jeq.[filter] = srl.[publication_name] + ' - ' + srl.[publisher_db]
											AND jeq.[job_name] = 'dbaTDPMon - usp_monAlarmCustomReplicationLatency(1) - ' + REPLACE(srl.[publisher_server], '\', '$') + ' - ' + srl.[publication_name] + ' - ' + srl.[publisher_db]
											AND jeq.[job_step_name] = 'Run Analysis'
WHERE	jeq.[module] = 'monitoring'
		AND jeq.[descriptor] = 'dbo.usp_monAlarmCustomReplicationLatency'
		AND jeq.[status] = 1 /* succedded */

------------------------------------------------------------------------------------------------------------------------------------------
SET @strMessage='Generate Replication Latency getdata internal jobs..'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
------------------------------------------------------------------------------------------------------------------------------------------

DECLARE crsActivePublishers	CURSOR LOCAL FAST_FORWARD FOR	SELECT DISTINCT cin.[id], [publisher_server]
															FROM	[monitoring].[statsReplicationLatency] srl
															INNER JOIN [dbo].[vw_catalogDatabaseNames] cdn ON	cdn.[instance_name] = srl.[publisher_server] 
																												AND cdn.[database_name] = srl.[publisher_db]
																												AND cdn.[project_id] = srl.[project_id]	
															INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[project_id] = cdn.[project_id]
																											AND cin.[id] = cdn.[instance_id]
															WHERE	[subscription_status] = 2 /* active subscriptions */
																	AND [publication_type] = 0 /* only transactional publications */
																	AND cdn.[project_id] = @projectID
																	AND cdn.[instance_name] LIKE @sqlServerNameFilter
																	AND cin.[active] = 1
OPEN crsActivePublishers
FETCH NEXT FROM crsActivePublishers INTO @publisherInstanceID, @publicationServer
WHILE @@FETCH_STATUS=0
	begin
		SET @strMessage='Analyzing server: ' + @publicationServer
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 1, @messageTreelevel = 1, @stopExecution=0
		
		DECLARE crsActivePublications CURSOR LOCAL FAST_FORWARD FOR	SELECT DISTINCT [publication_name], [publisher_db]
																	FROM	[monitoring].[statsReplicationLatency] srl		
																	INNER JOIN [dbo].[vw_catalogDatabaseNames] cdn ON	cdn.[instance_name] = srl.[publisher_server] 
																												AND cdn.[database_name] = srl.[publisher_db]
																												AND cdn.[project_id] = srl.[project_id]																	
																	WHERE	[subscription_status] = 2 /* active subscriptions */
																			AND srl.[publisher_server] = @publicationServer
																			AND [publication_type] = 0 /* only transactional publications */
																			AND cdn.[project_id] = @projectID
																			AND cdn.[instance_name] LIKE @sqlServerNameFilter
		OPEN crsActivePublications
		FETCH NEXT FROM crsActivePublications INTO @publicationName, @publisherDB
		WHILE @@FETCH_STATUS=0
			begin
				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + N'SELECT * FROM tempdb.[dbo].[replicationTokenResults] 
													WHERE [publication]=''' + [dbo].[ufn_getObjectQuoteName](@publicationName, 'sql') + N''' 
														  AND [publisher_db] = ''' + [dbo].[ufn_getObjectQuoteName](@publisherDB, 'sql') + N''''
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@publicationServer, @queryToRun)

				SET @queryToRun = N'UPDATE srl
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
													AND srl.[publisher_server] = ''' + @publicationServer + N'''
									WHERE srl.[publisher_db]=''' + [dbo].[ufn_getObjectQuoteName](@publisherDB, 'sql') + N'''
										AND srl.[publication_name]=''' + [dbo].[ufn_getObjectQuoteName](@publicationName, 'sql') + N''''

				SET @queryToRun = N'SET QUOTED_IDENTIFIER ON; SET LOCK_TIMEOUT ' + CAST(@queryLockTimeOut AS [nvarchar]) + N'; ' + @queryToRun
		
				INSERT	INTO [dbo].[jobExecutionQueue](	[instance_id], [project_id], [module], [descriptor], [task_id],
														[filter], [for_instance_id],
														[job_name], [job_step_name], [job_database_name], [job_command])
						SELECT	@currentInstanceID, @projectID, 'monitoring', 'dbo.usp_monAlarmCustomReplicationLatency', 
								(SELECT it.[id] FROM [dbo].[appInternalTasks] it WHERE it.[descriptor] = 'dbo.usp_monAlarmCustomReplicationLatency'),
								@publicationName + ' - ' + @publisherDB , @publisherInstanceID,
								'dbaTDPMon - usp_monAlarmCustomReplicationLatency(2) - ' + REPLACE(@publicationServer, '\', '$') + ' - ' + @publicationName + ' - ' + @publisherDB, 'Get Latency', DB_NAME(), @queryToRun

				FETCH NEXT FROM crsActivePublications INTO @publicationName, @publisherDB
			end
		CLOSE crsActivePublications
		DEALLOCATE crsActivePublications

		FETCH NEXT FROM crsActivePublishers INTO @publisherInstanceID, @publicationServer
	end
CLOSE crsActivePublishers
DEALLOCATE crsActivePublishers


------------------------------------------------------------------------------------------------------------------------------------------
SET @strMessage='Running GetData jobs..'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
------------------------------------------------------------------------------------------------------------------------------------------
EXEC dbo.usp_jobQueueExecute	@projectCode		= @projectCode,
								@moduleFilter		= 'monitoring',
								@descriptorFilter	= 'dbo.usp_monAlarmCustomReplicationLatency',
								@waitForDelay		= DEFAULT,
								@debugMode			= @debugMode

UPDATE [monitoring].[statsReplicationLatency] SET [distributor_latency] = NULL	WHERE [distributor_latency] = 2147483647
UPDATE [monitoring].[statsReplicationLatency] SET [subscriber_latency] = NULL	WHERE [subscriber_latency] = 2147483647
UPDATE [monitoring].[statsReplicationLatency] SET [overall_latency] = NULL		WHERE [overall_latency] = 2147483647

UPDATE srl
	SET srl.[state] = 2	/* getdate job executed successfully */
FROM [monitoring].[statsReplicationLatency] srl
INNER JOIN [dbo].[jobExecutionQueue] jeq ON jeq.[filter] = srl.[publication_name] + ' - ' + srl.[publisher_db]
											AND jeq.[job_name] = 'dbaTDPMon - usp_monAlarmCustomReplicationLatency(2) - ' + REPLACE(srl.[publisher_server], '\', '$') + ' - ' + srl.[publication_name] + ' - ' + srl.[publisher_db]
											AND jeq.[job_step_name] = 'Get Latency'
WHERE	jeq.[module] = 'monitoring'
		AND jeq.[descriptor] = 'dbo.usp_monAlarmCustomReplicationLatency'
		AND jeq.[status] = 1 /* succedded */


------------------------------------------------------------------------------------------------------------------------------------------
SET @strMessage='Perform cleanup...'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

DECLARE crsActivePublications CURSOR LOCAL FAST_FORWARD FOR	SELECT DISTINCT srl.[publisher_server], srl.[publication_name], srl.[publisher_db]
															FROM [monitoring].[statsReplicationLatency] srl
															INNER JOIN [dbo].[vw_catalogDatabaseNames] cdn ON	cdn.[instance_name] = srl.[publisher_server] 
																												AND cdn.[database_name] = srl.[publisher_db]
																												AND cdn.[project_id] = srl.[project_id]	
															LEFT JOIN [monitoring].[alertSkipRules] asr ON	asr.[category] = 'replication'
																										AND asr.[alert_name] IN ('subscription marked inactive', 'subscription not active')
																										AND asr.[active] = 1
																										AND (asr.[skip_value] = ('[' + srl.[publisher_server] + '].[' + srl.[publisher_db] + '](' + srl.[publication_name] + ')'))
																										AND (    asr.[skip_value2] IS NULL 
																												OR (asr.[skip_value2] IS NOT NULL AND asr.[skip_value2] = ('[' + srl.[subscriber_server] + '].[' + srl.[subscriber_db] + ']'))
																											)

															WHERE srl.[subscription_status] = 2 /* active subscriptions */
																	AND srl.[publication_type] = 0 /* only transactional publications */
																	AND cdn.[project_id] = @projectID
																	AND cdn.[instance_name] LIKE @sqlServerNameFilter
																	AND asr.[id] IS NULL
OPEN crsActivePublications
FETCH NEXT FROM crsActivePublications INTO @publicationServer, @publicationName, @publisherDB
WHILE @@FETCH_STATUS=0
	begin
		SET @strMessage='running on server: ' + @publicationServer
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 1, @messageTreelevel = 1, @stopExecution=0

		IF @publicationServer<>@@SERVERNAME
			SET @serverToRun = '[' + @publicationServer + '].tempdb.dbo.sp_executesql'
		ELSE
			SET @serverToRun = 'tempdb.dbo.sp_executesql'

		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'
			IF EXISTS(SELECT * FROM sysobjects WHERE [name] = ''usp_monGetReplicationLatency'' AND [type]=''P'')
				DROP PROCEDURE dbo.usp_monGetReplicationLatency'

		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @serverToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=0
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=0
		BEGIN TRY
			EXEC @serverToRun @queryToRun
		END TRY
		BEGIN CATCH
			SET @strMessage = ERROR_MESSAGE()
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
		END CATCH

		/*
		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'
			IF EXISTS(SELECT * FROM sysobjects WHERE [name] = ''replicationTokenResults'' AND [type]=''U'')
				DELETE FROM dbo.replicationTokenResults WHERE [publisher_db]=''' + @publisherDB + N''' AND [publication] = ''' + @publicationName + N''''

		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @serverToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=0
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=0
		EXEC @serverToRun @queryToRun
		*/
		FETCH NEXT FROM crsActivePublications INTO @publicationServer, @publicationName, @publisherDB
	end
CLOSE crsActivePublications
DEALLOCATE crsActivePublications


------------------------------------------------------------------------------------------------------------------------------------------
--generate alerts: Replication latency exceeds thresold
------------------------------------------------------------------------------------------------------------------------------------------
SET @strMessage='generate alerts: Replication latency exceeds thresold...'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

DECLARE   @instanceName		[sysname]
		, @objectName		[nvarchar](512)
		, @severity			[sysname]
		, @eventMessage		[nvarchar](max)


DECLARE crsReplicationAlarms CURSOR LOCAL FAST_FORWARD FOR	SELECT  DISTINCT
																	  srl.[publisher_server] AS [instance_name]
																	, 'Publication: ' + srl.[publication_name] + ' - Subscriber: ' + srl.[subscriber_server] + '.' + srl.[subscriber_db] AS [object_name]
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
															FROM [monitoring].[statsReplicationLatency] srl
															INNER JOIN [dbo].[vw_catalogDatabaseNames] cdn ON	cdn.[instance_name] = srl.[publisher_server] 
																												AND cdn.[database_name] = srl.[publisher_db]
																												AND cdn.[project_id] = srl.[project_id]	
															INNER JOIN [dbo].[vw_catalogInstanceNames] cin ON cin.[project_id] = cdn.[project_id]
																											AND cin.[instance_id] = cdn.[instance_id]
															LEFT JOIN [monitoring].[alertSkipRules] asr ON	asr.[category] = 'replication'
																											AND asr.[alert_name] IN ('replication latency')
																											AND asr.[active] = 1
																											AND (asr.[skip_value] = ('[' + srl.[publisher_server] + '].[' + srl.[publisher_db] + '](' + srl.[publication_name] + ')'))
																											AND (    asr.[skip_value2] IS NULL 
																													OR (asr.[skip_value2] IS NOT NULL AND asr.[skip_value2] = ('[' + srl.[subscriber_server] + '].[' + srl.[subscriber_db] + ']'))
																												)
															WHERE cin.[instance_active]=1
																	AND cdn.[project_id] = @projectID
																	AND cdn.[instance_name] LIKE @sqlServerNameFilter
																	AND (srl.[overall_latency] IS NULL OR srl.[overall_latency]>=@alertThresholdCriticalReplicationLatencySec)									
																	AND srl.[subscription_status] = 2 /* active subscriptions */
																	AND srl.[state] = 2 /* run analysis and get data jobs completed successfully */
																	AND asr.[id] IS NULL
															ORDER BY [instance_name], [object_name]
OPEN crsReplicationAlarms
FETCH NEXT FROM crsReplicationAlarms INTO @instanceName, @objectName, @severity, @eventName, @eventMessage
WHILE @@FETCH_STATUS=0
	begin
		/* check for additional receipients for the alert */		
		SET @publicationDetails = 'Publication: ' + @publicationName + ' - Subscriber: ' + @subcriptionServer + '.' + @subscriptionDB
		SET @additionalRecipients = [dbo].[ufn_monGetAdditionalAlertRecipients](@projectID, @publicationServer, @eventName, @publicationDetails)

		EXEC [dbo].[usp_logEventMessageAndSendEmail]	@sqlServerName			= @instanceName,
														@dbName					= @severity,
														@objectName				= @objectName,
														@childObjectName		= NULL,
														@module					= 'monitoring',
														@eventName				= @eventName,
														@parameters				= NULL,	
														@eventMessage			= @eventMessage,
														@dbMailProfileName		= NULL,
														@recipientsList			= @additionalRecipients,
														@eventType				= 6,	/* 6 - alert-custom */
														@additionalOption		= 0

		FETCH NEXT FROM crsReplicationAlarms INTO @instanceName, @objectName, @severity, @eventName, @eventMessage
	end
CLOSE crsReplicationAlarms
DEALLOCATE crsReplicationAlarms
GO
