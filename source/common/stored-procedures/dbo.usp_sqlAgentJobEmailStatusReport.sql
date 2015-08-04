RAISERROR('Create procedure: [dbo].[usp_sqlAgentJobEmailStatusReport]', 10, 1) WITH NOWAIT
GO---
IF  EXISTS (
	    SELECT * 
	      FROM sysobjects 
	     WHERE id = OBJECT_ID(N'[dbo].[usp_sqlAgentJobEmailStatusReport]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_sqlAgentJobEmailStatusReport]
GO

CREATE PROCEDURE [dbo].[usp_sqlAgentJobEmailStatusReport]
		@jobName				[nvarchar](256),
		@logFileLocation		[nvarchar](512),
		@module					[varchar](32),
		@sendLogAsAttachment	[bit]=1,
		@eventType				[smallint]=2
/* WITH ENCRYPTION */
AS

SET NOCOUNT ON

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 17.03.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

DECLARE @eventMessageData	[varchar](8000),
		@jobID				[uniqueidentifier],
		@strMessage			[nvarchar](512)

-----------------------------------------------------------------------------------------------------
--get job id
SELECT	@jobID = [job_id] 
FROM	[msdb].[dbo].[sysjobs] 
WHERE	[name]=@jobName 

-----------------------------------------------------------------------------------------------------
SET @eventMessageData = '<job-history><job-step>'

SELECT @eventMessageData = @eventMessageData + [job_step_detail]
FROM (
		SELECT	'<step_id>' + CAST([step_id] AS [varchar](32)) + '</step_id>' + 
				'<step_name>' + [step_name] + '</step_name>' + 
				'<run_status>' + [run_status] + '</run_status>' + 
				'<run_date>' + [run_date] + '</run_date>' + 
				'<run_time>' + [run_time] + '</run_time>' + 
				'<duration>' + [duration] + '</duration>' AS [job_step_detail]
		FROM (
				SELECT	  [step_id]
						, [step_name]
						, [run_status]
						, SUBSTRING([run_date], 1, 4) + '-' + SUBSTRING([run_date], 5 ,2) + '-' + SUBSTRING([run_date], 7 ,2) AS [run_date]
						, SUBSTRING([run_time], 1,2) + ':' + SUBSTRING([run_time], 3,2) + ':' + SUBSTRING([run_time], 5,2) AS [run_time]
						, SUBSTRING([run_duration], 1,2) + 'h ' + SUBSTRING([run_duration], 3,2) + 'm ' + SUBSTRING([run_duration], 5,2) + 's' AS [duration]
				FROM (		
						SELECT	  h.[step_id]
								, h.[step_name]
								, CASE h.[run_status]	WHEN '0' THEN 'Failed'
														WHEN '1' THEN 'Succeded'	
														WHEN '2' THEN 'Retry'
														WHEN '3' THEN 'Canceled'
														WHEN '4' THEN 'In progress'
														ELSE 'Unknown'
									END [run_status]
								, CAST(h.[run_date] AS varchar) AS [run_date]
								, REPLICATE('0', 6-LEN(CAST(h.[run_time] AS varchar))) + CAST(h.[run_time] AS varchar) AS [run_time]
								, REPLICATE('0', 6-LEN(CAST(h.[run_duration] AS varchar))) + CAST(h.[run_duration] AS varchar) AS [run_duration]
								, h.[instance_id]
						FROM [msdb].[dbo].[sysjobs] j 
						RIGHT JOIN [msdb].[dbo].[sysjobhistory] h	 ON j.[job_id] = h.[job_id] 
						WHERE j.[job_id] = @jobID
							AND	h.[instance_id]>ISNULL((SELECT MAX(h.[instance_id])
														FROM [msdb].[dbo].[sysjobs] j 
														RIGHT JOIN [msdb].[dbo].[sysjobhistory] h ON j.[job_id] = h.[job_id] 
														WHERE	j.[job_id] = @jobID
																AND h.[step_name] ='(Job outcome)'
														),0)	
					)A
				)x										
	)xmlData

SET @eventMessageData = @eventMessageData + '</job-step></job-history>'

IF @sendLogAsAttachment=0
	SET @logFileLocation = NULL


--if one of the job steps failed, will fail the job
DECLARE @failedSteps [int]

SELECT @failedSteps = COUNT(*)
FROM [msdb].[dbo].[sysjobs] j 
RIGHT JOIN [msdb].[dbo].[sysjobhistory] h	 ON j.[job_id] = h.[job_id] 
WHERE j.[job_id] = @jobID
	AND	h.[instance_id]>ISNULL( (SELECT MAX(h.[instance_id])
								 FROM	[msdb].[dbo].[sysjobs] j 
								 RIGHT JOIN [msdb].[dbo].[sysjobhistory] h ON j.[job_id] = h.[job_id] 
								 WHERE	j.[job_id] = @jobID
										AND h.[step_name] ='(Job outcome)'
								),0)
	AND h.[run_status]=0 /* Failed */

EXEC [dbo].[usp_logEventMessageAndSendEmail] @projectCode		= NULL,
											 @sqlServerName		= @@SERVERNAME,
											 @objectName		= @jobName,
											 @module			= @module,
											 @eventName			= 'sql agent job status',
											 @parameters		= @logFileLocation,
											 @eventMessage		= @eventMessageData,
											 @recipientsList	= NULL,
											 @eventType			= @eventType,
											 @additionalOption	= @failedSteps

IF @failedSteps <> 0
	begin
		SET @strMessage = 'Job execution failed. See individual steps status.'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=1
	end

GO
