RAISERROR('Create function: [dbo].[ufn_reportHTMLGetAnchorName]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ufn_reportHTMLGetAnchorName]') AND type in (N'FN', N'IF', N'TF', N'FS', N'FT'))
DROP FUNCTION [dbo].[ufn_reportHTMLGetAnchorName]
GO

CREATE FUNCTION [dbo].[ufn_reportHTMLGetAnchorName]
(		
	@sqlText			[nvarchar] (max)
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
			, @charToRemove	[nvarchar](128)
			, @idx			[int]
	
	SELECT   @charToRemove=': -,()/\%#'
			, @idx = 0
			, @fmtString = @sqlText
	WHILE @idx<LEN(@charToRemove)
		begin
			SET @fmtString = REPLACE(@fmtString, SUBSTRING(@charToRemove, @idx + 1, 1), '')
			SET @idx = @idx + 1
		end
	RETURN @fmtString
end

GO
