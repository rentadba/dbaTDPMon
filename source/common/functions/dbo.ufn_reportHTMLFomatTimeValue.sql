RAISERROR('Create function: [dbo].[ufn_reportHTMLFormatTimeValue]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ufn_reportHTMLFormatTimeValue]') AND type in (N'FN', N'IF', N'TF', N'FS', N'FT'))
DROP FUNCTION [dbo].[ufn_reportHTMLFormatTimeValue]
GO

CREATE FUNCTION [dbo].[ufn_reportHTMLFormatTimeValue]
(		
	@valueInMS	[bigint]
)
RETURNS [nvarchar](64)
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
	DECLARE @timeValue	[varchar](32),
			@crtValue	[varchar](3)

	SELECT    @timeValue = ''
			, @valueInMS = ISNULL(@valueInMS, 0)
	
	SELECT    @crtValue = CAST(@valueInMS / (1000 * 60 * 60 * 24) AS [varchar])
			, @valueInMS = @valueInMS % (1000 * 60 * 60 * 24)

	SET @timeValue = @timeValue + CASE WHEN @crtValue>0 THEN @crtValue  + 'd ' ELSE '' END

	SELECT    @crtValue = CAST(@valueInMS / (1000 * 60 * 60) AS [varchar])
			, @valueInMS = @valueInMS % (1000 * 60 * 60)

	SET @timeValue = @timeValue + REPLICATE('0', 2-CASE WHEN LEN(@crtValue) < 2 THEN LEN(@crtValue) ELSE 2 END) + @crtValue + ':'

	SELECT    @crtValue = CAST(@valueInMS / (1000 * 60) AS [varchar])
			, @valueInMS = @valueInMS % (1000 * 60)

	SET @timeValue = @timeValue + REPLICATE('0', 2-CASE WHEN LEN(@crtValue) < 2 THEN LEN(@crtValue) ELSE 2 END) + @crtValue + ':'

	SELECT    @crtValue = CAST(@valueInMS / (1000) AS [varchar])
			, @valueInMS = @valueInMS % (1000)

	SET @timeValue = @timeValue + REPLICATE('0', 2-CASE WHEN LEN(@crtValue) < 2 THEN LEN(@crtValue) ELSE 2 END) + @crtValue + '.'

	SELECT    @crtValue = CAST(@valueInMS AS [varchar])

	SET @timeValue = @timeValue + REPLICATE('0', 3-CASE WHEN LEN(@crtValue) < 3 THEN LEN(@crtValue) ELSE 3 END) + @crtValue

	RETURN @timeValue
end

GO
