RAISERROR('Create function: [dbo].[ufn_reportHTMLPrepareText]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ufn_reportHTMLPrepareText]') AND type in (N'FN', N'IF', N'TF', N'FS', N'FT'))
DROP FUNCTION [dbo].[ufn_reportHTMLPrepareText]
GO

CREATE FUNCTION [dbo].[ufn_reportHTMLPrepareText]
(		
	  @sqlText			[nvarchar] (max)
	, @maxStrLength		[int]
)
RETURNS [nvarchar](max)
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 19.10.2010
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-- { sql_statement | statement_block }
begin
	DECLARE   @fmtString	[nvarchar](max)
	
	IF ISNULL(@maxStrLength, 0) <> 0
		SET @fmtString = LEFT(@sqlText, @maxStrLength) + CASE WHEN LEN(@sqlText) > @maxStrLength THEN ' [...]' ELSE N'' END
	ELSE
		SET @fmtString = @sqlText

	SET @fmtString = REPLACE(REPLACE(@fmtString, '<', '&lt;'), '>', '&gt;')
	RETURN @fmtString
end

GO
