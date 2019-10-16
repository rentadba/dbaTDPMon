RAISERROR('Create procedure: [dbo].[usp_logEventMessage]', 10, 1) WITH NOWAIT
GO---
IF  EXISTS (
	    SELECT * 
	      FROM sysobjects 
	     WHERE id = OBJECT_ID(N'[dbo].[usp_logEventMessage]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_logEventMessage]
GO

GO
CREATE PROCEDURE [dbo].[usp_logEventMessage]
		@projectCode			[sysname]=NULL,
		@sqlServerName			[sysname]=NULL,
		@dbName					[sysname] = NULL,
		@objectName				[nvarchar](512) = NULL,
		@childObjectName		[sysname] = NULL,
		@module					[sysname],
		@eventName				[nvarchar](256) = NULL,
		@parameters				[nvarchar](512) = NULL,			/* may contain the attach file name */
		@eventMessage			[varchar](max) = NULL,
		@eventType				[smallint]=1,	/*	0 - info
													1 - alert 
													2 - job-history
													3 - report-html
													4 - action
													5 - backup-job-history
													6 - alert-custom
												*/
		@recipientsList			[nvarchar](1024) = NULL,
		@isEmailSent			[bit]=0,
		@isFloodControl			[bit]=0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 05.11.2014
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SET NOCOUNT ON

DECLARE @projectID					[smallint],
		@instanceID					[smallint]

-----------------------------------------------------------------------------------------------------
-- try to get project code by database name / or get the default project value
IF @projectCode IS NULL
	SET @projectCode = [dbo].[ufn_getProjectCode](@sqlServerName, @dbName)

SELECT @projectID = [id]
FROM [dbo].[catalogProjects]
WHERE [code] = @projectCode 

-----------------------------------------------------------------------------------------------------
SELECT  @instanceID = [id] 
FROM	[dbo].[catalogInstanceNames]  
WHERE	[name] = @sqlServerName
		AND [project_id] = @projectID

-----------------------------------------------------------------------------------------------------
/*
--xml corrections
SET @eventMessage = REPLACE(@eventMessage, CHAR(38), CHAR(38) + 'amp;')
IF @objectName IS NOT NULL
	begin
		IF CHARINDEX('<', @objectName) <> 0 AND CHARINDEX('>', @objectName) <> 0
			SET @eventMessage = REPLACE(@eventMessage, @objectName, REPLACE(REPLACE(@objectName, '<', '&lt;'), '>', '&gt;'))
		ELSE
			IF CHARINDEX('<', @objectName) <> 0 AND CHARINDEX('>', @objectName) <> 0
				SET @eventMessage = REPLACE(@eventMessage, @objectName, REPLACE(@objectName, '<', '&lt;'))
			IF CHARINDEX('>', @objectName) <> 0
				SET @eventMessage = REPLACE(@eventMessage, @objectName, REPLACE(@objectName, '>', '&gt;'))
	end

IF @childObjectName IS NOT NULL
	begin
		IF CHARINDEX('<', @childObjectName) <> 0 AND CHARINDEX('>', @childObjectName) <> 0
			SET @eventMessage = REPLACE(@eventMessage, @childObjectName, REPLACE(REPLACE(@childObjectName, '<', '&lt;'), '>', '&gt;'))
		ELSE
			IF CHARINDEX('<', @childObjectName) <> 0 AND CHARINDEX('>', @childObjectName) <> 0
				SET @eventMessage = REPLACE(@eventMessage, @childObjectName, REPLACE(@childObjectName, '<', '&lt;'))
			IF CHARINDEX('>', @childObjectName) <> 0
				SET @eventMessage = REPLACE(@eventMessage, @childObjectName, REPLACE(@childObjectName, '>', '&gt;'))
	end
*/

-----------------------------------------------------------------------------------------------------
INSERT	INTO [dbo].[logEventMessages]([project_id], [instance_id], [event_date_utc], [module], [parameters], [event_name], [database_name], [object_name], [child_object_name], [message], [send_email_to], [event_type], [is_email_sent], [flood_control])
		SELECT @projectID, @instanceID, GETUTCDATE(), @module, @parameters, @eventName, @dbName, @objectName, @childObjectName, @eventMessage, @recipientsList, @eventType, @isEmailSent, @isFloodControl

RETURN 0
GO

