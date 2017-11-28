RAISERROR('Create procedure: [dbo].[usp_sqlExecuteAndLog]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sysobjects 
	     WHERE id = OBJECT_ID(N'[dbo].[usp_sqlExecuteAndLog]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_sqlExecuteAndLog]
GO

-----------------------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[usp_sqlExecuteAndLog]
		@sqlServerName			[sysname],
		@dbName					[sysname] = NULL,
		@objectName				[nvarchar](512) = NULL,
		@childObjectName		[sysname] = NULL,
		@module					[sysname] = NULL,
		@eventName				[nvarchar](256) = NULL,
		@queryToRun  			[nvarchar](4000) = NULL,
		@flgOptions				[int]=32,
		@executionLevel			[tinyint]= 0,
		@debugMode				[bit]=0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 23.03.2015
-- Module			 : Database Maintenance Plan 
--					 : SQL Server 2000/2005/2008/2008R2/2012+
-- Description		 : run SQL command and log action
-- ============================================================================

DECLARE		@queryParameters				[nvarchar](512),
			@tmpSQL		  					[nvarchar](2048),
			@tmpServer						[varchar](256),
			@ReturnValue					[int]

DECLARE		@projectID						[smallint],
			@instanceID						[smallint],
			@errorCode						[int],
			@durationSeconds				[bigint],
			@eventData						[varchar](8000)
			
SET NOCOUNT ON


---------------------------------------------------------------------------------------------
--get default project id / instance id
SELECT	@projectID = [id]
FROM	[dbo].[catalogProjects]
WHERE	[code] IN ( 
					SELECT	[value]
					FROM	[dbo].[appConfigurations]
					WHERE	[name] = 'Default project code'
							AND [module] = 'common'
				  )

SELECT  @instanceID = [id] 
FROM	[dbo].[catalogInstanceNames]  
WHERE	[name] = @sqlServerName
		AND [project_id] = @projectID

---------------------------------------------------------------------------------------------
DECLARE @logEventActions	[nvarchar](32)

SELECT	@logEventActions = LOWER([value])
FROM	[dbo].[appConfigurations]
WHERE	[name]='Log action events'
		AND [module] = 'common'


---------------------------------------------------------------------------------------------
--get destination server running version/edition
DECLARE		@serverEdition					[sysname],
			@serverVersionStr				[sysname],
			@serverVersionNum				[numeric](9,6),
			@nestedExecutionLevel			[tinyint]

SET @nestedExecutionLevel = @executionLevel + 1

EXEC [dbo].[usp_getSQLServerVersion]	@sqlServerName			= @sqlServerName,
										@serverEdition			= @serverEdition OUT,
										@serverVersionStr		= @serverVersionStr OUT,
										@serverVersionNum		= @serverVersionNum OUT,
										@executionLevel			= @nestedExecutionLevel,
										@debugMode				= @debugMode

--------------------------------------------------------------------------------------------------
SET @tmpServer= [dbo].[ufn_getObjectQuoteName](@sqlServerName, NULL) + '.' + [dbo].[ufn_getObjectQuoteName](ISNULL(@dbName, 'master'), NULL) + '.[dbo].[sp_executesql]'

IF @serverVersionNum >= 9
	SET @tmpSQL = N'DECLARE @startTime [datetime]

					BEGIN TRY
						SET @startTime = GETDATE()
						
						EXEC @tmpServer @queryToRun

						SET @errorCode = 0
						SET @durationSeconds=DATEDIFF(ss, @startTime, GETDATE())
					END TRY

					BEGIN CATCH
						DECLARE   @flgRaiseErrorAndStop [bit]
								, @errorString			[nvarchar](max)
								, @eventMessageData		[varchar](8000)

						SET @errorString = ERROR_MESSAGE()
						SET @errorCode = ERROR_NUMBER()
						SET @durationSeconds=DATEDIFF(ss, @startTime, GETDATE())

						IF LEFT(@errorString, 2)=''--'' 
							SET @errorString = LTRIM(SUBSTRING(@errorString, 3, LEN(@errorString)))

						SET @flgRaiseErrorAndStop = CASE WHEN @flgOptions & 32 = 32 THEN 1 ELSE 0 END
						
						SET @eventMessageData = ''<alert><detail>'' + 
												''<error_code>'' + CAST(@errorCode AS [varchar](32)) + ''</error_code>'' + 
												''<error_string>'' + [dbo].[ufn_getObjectQuoteName](@errorString, ''xml'') + ''</error_string>'' + 
												''<query_executed>'' + [dbo].[ufn_getObjectQuoteName](@queryToRun, ''xml'') + ''</query_executed>'' + 
												''<duration_seconds>'' + CAST(@durationSeconds AS [varchar](32)) + ''</duration_seconds>'' + 
												''<event_date_utc>'' + CONVERT([varchar](20), GETUTCDATE(), 120) + ''</event_date_utc>'' + 
												''</detail></alert>''

						EXEC [dbo].[usp_logEventMessageAndSendEmail]	@sqlServerName			= @sqlServerName,
																		@dbName					= @dbName,
																		@objectName				= @objectName,
																		@childObjectName		= @childObjectName,
																		@module					= @module,
																		@eventName				= @eventName,
																		@eventMessage			= @eventMessageData,
																		@eventType				= 1

						EXEC [dbo].[usp_logPrintMessage] @customMessage = @errorString, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=@flgRaiseErrorAndStop
					END CATCH'
ELSE
	SET @tmpSQL = N'DECLARE   @startTime			[datetime]
					
					SET @startTime = GETDATE()
					
					EXEC @tmpServer @queryToRun
					
					SET @errorCode=@@ERROR
					SET @durationSeconds=DATEDIFF(ss, @startTime, GETDATE())

					IF @errorCode<>0
						begin
							DECLARE   @flgRaiseErrorAndStop [bit]
									, @errorString			[nvarchar](255)
									, @eventData			[varchar](8000)

							SELECT @errorString = [description]
							FROM master.dbo.sysmessages 
							WHERE [error] = @errorCode

							SET @flgRaiseErrorAndStop = CASE WHEN @flgOptions & 32 = 32 THEN 1 ELSE 0 END
							
							SET @eventData = ''<alert><detail>'' + 
												''<error_code>'' + CAST(@errorCode AS [varchar](32)) + ''</error_code>'' + 
												''<error_string>'' + [dbo].[ufn_getObjectQuoteName](ISNULL(@errorString, ''''), ''xml'') + ''</error_string>'' + 
												''<query_executed>'' + [dbo].[ufn_getObjectQuoteName](@queryToRun, ''xml'') + ''</query_executed>'' + 
												''<duration_seconds>'' + CAST(@durationSeconds AS [varchar](32)) + ''</duration_seconds>'' + 
											''</detail></alert>''

							EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
																@dbName			= @dbName,
																@objectName		= @objectName,
																@childObjectName= @childObjectName,
																@module			= @module,
																@eventName		= @eventName,
																@eventMessage	= @eventData,
																@eventType		= 1

							EXEC [dbo].[usp_logPrintMessage] @customMessage = @errorString, @stopExecution=0
							EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @stopExecution=@flgRaiseErrorAndStop
						end'

SET @queryParameters=N'@tmpServer [nvarchar](512), @queryToRun [nvarchar](2048), @flgOptions [int], @module [sysname], @eventName [nvarchar](512), @sqlServerName [sysname], @dbName [sysname], @objectName [nvarchar](512), @childObjectName [sysname], @errorCode [int] OUTPUT, @durationSeconds [bigint] OUTPUT'


--------------------------------------------------------------------------------------------------
--running action
SET @errorCode=0
IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @tmpServer, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @tmpSQL, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @objectName, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @childObjectName, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

EXEC sp_executesql @tmpSQL, @queryParameters, @tmpServer		= @tmpServer
											, @queryToRun		= @queryToRun
											, @flgOptions		= @flgOptions
											, @eventName		= @eventName
											, @module			= @module
											, @sqlServerName	= @sqlServerName
											, @dbName			= @dbName
											, @objectName		= @objectName
											, @childObjectName	= @childObjectName
											, @errorCode		= @errorCode OUT
											, @durationSeconds	= @durationSeconds OUT

--------------------------------------------------------------------------------------------------
--logging action
IF @logEventActions = 'true'
	begin
		SET @eventData = '<action><detail>' + 
							CASE WHEN @dbName IS NOT NULL THEN '<database_name>' + [dbo].[ufn_getObjectQuoteName](@dbName, 'xml') + '</database_name>' ELSE N'' END + 
							CASE WHEN @eventName IS NOT NULL THEN '<event_name>' + @eventName + '</event_name>' ELSE N'' END + 
							CASE WHEN @objectName IS NOT NULL THEN '<object_name>' + [dbo].[ufn_getObjectQuoteName](@objectName, 'xml') + '</object_name>' ELSE N'' END + 
							CASE WHEN @childObjectName IS NOT NULL THEN '<child_object_name>' + [dbo].[ufn_getObjectQuoteName](@childObjectName, 'xml') + '</child_object_name>' ELSE N'' END + 
							'<query_executed>' + [dbo].[ufn_getObjectQuoteName](@queryToRun, 'xml') + '</query_executed>' + 
							'<duration>' + REPLICATE('0', 2-LEN(CAST(@durationSeconds / 3600 AS [varchar]))) + CAST(@durationSeconds / 3600 AS [varchar]) + 'h'
												+ ' ' + REPLICATE('0', 2-LEN(CAST((@durationSeconds / 60) % 60 AS [varchar]))) + CAST((@durationSeconds / 60) % 60 AS [varchar]) + 'm'
												+ ' ' + REPLICATE('0', 2-LEN(CAST(@durationSeconds % 60 AS [varchar]))) + CAST(@durationSeconds % 60 AS [varchar]) + 's' + '</duration>' + 
							'<error_code>' + CAST(@errorCode AS [varchar](32) )+ '</error_code>' + 
							'</detail></action>'

		EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
											@dbName			= @dbName,
											@objectName		= @objectName,
											@childObjectName= @childObjectName,
											@module			= @module,
											@eventName		= @eventName,
											@eventMessage	= @eventData,
											@eventType		= 4 /* action */
	end

RETURN @errorCode
GO
