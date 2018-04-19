RAISERROR('Create function: [dbo].[ufn_monGetAdditionalAlertRecipients]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sysobjects WHERE id = OBJECT_ID(N'[dbo].[ufn_monGetAdditionalAlertRecipients]') AND xtype in (N'FN', N'IF', N'TF', N'FS', N'FT'))
DROP FUNCTION [dbo].[ufn_monGetAdditionalAlertRecipients]
GO

CREATE FUNCTION [dbo].[ufn_monGetAdditionalAlertRecipients]
(		
	@projectID			[smallint],
	@instanceName		[sysname],
	@eventName			[sysname],
	@objectName			[sysname]
)
RETURNS [nvarchar](1024)
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2018 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 2018.04.19
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

begin
	DECLARE @additionalRecipients	[nvarchar](1024)

	/* check for additional receipients for the alert */		
	SET @additionalRecipients = N''
	SELECT @additionalRecipients = @additionalRecipients + [recipients] + N';'
	FROM (
			SELECT DISTINCT adr.[recipients]
			FROM [monitoring].[alertAdditionalRecipients] adr
			INNER JOIN [dbo].[vw_catalogInstanceNames] cin ON cin.[instance_id] = adr.[instance_id]
															AND cin.[project_id] = adr.[project_id]
			WHERE cin.[project_id] = @projectID
					AND cin.[instance_name] = @instanceName
					AND adr.[active] = 1
					AND adr.[event_name] = @eventName
					AND adr.[object_name] = @objectName
		)x
		
	IF @additionalRecipients <> N''
		begin
			SELECT @additionalRecipients = [value] + N';' + @additionalRecipients
			FROM [dbo].[appConfigurations]
			WHERE [name] = 'Default recipients list - Alerts (semicolon separated)'
					AND [module] = 'common'
		end
	ELSE
		SET @additionalRecipients = NULL

	RETURN @additionalRecipients
end
GO
