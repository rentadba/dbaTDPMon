RAISERROR('Create procedure: [dbo].[usp_logPrintMessage]', 10, 1) WITH NOWAIT
GO---
IF  EXISTS (
	    SELECT * 
	      FROM sysobjects 
	     WHERE id = OBJECT_ID(N'[dbo].[usp_logPrintMessage]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_logPrintMessage]
GO

CREATE PROCEDURE [dbo].[usp_logPrintMessage]
		@customMessage			[nvarchar](4000),
		@raiseErrorAsPrint		[bit]=0,
		@messagRootLevel		[tinyint]=0,
		@messageTreelevel		[tinyint]=1,
		@stopExecution			[bit]=0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 05.02.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SET NOCOUNT ON

DECLARE @messageHead [nvarchar](4000)

SET @messageHead = '--' + REPLICATE(CHAR(9), (@messagRootLevel + @messageTreelevel))

IF @customMessage='<separator-line>'
	SET @customMessage= '*' + REPLICATE('-', 98-LEN(@messageHead)) + '*'

SET @customMessage = @messageHead + @customMessage

IF @stopExecution=0
	begin	
		IF @raiseErrorAsPrint=1 AND CHARINDEX('%', @customMessage)=0
			RAISERROR(@customMessage, 10, 1) WITH NOWAIT
		ELSE
			PRINT @customMessage
	end
ELSE
			RAISERROR(@customMessage, 16, 1) WITH NOWAIT
GO
