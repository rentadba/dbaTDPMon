RAISERROR('Create function: [dbo].[ufn_reportHTMLGetImage]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ufn_reportHTMLGetImage]') AND type in (N'FN', N'IF', N'TF', N'FS', N'FT'))
DROP FUNCTION [dbo].[ufn_reportHTMLGetImage]
GO

CREATE FUNCTION [dbo].[ufn_reportHTMLGetImage]
(		
	  @imageType		[nvarchar](32)
)
RETURNS [nvarchar](max)
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 13.04.2011
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-- { sql_statement | statement_block }
begin
	DECLARE   @base64			[nvarchar](max)

	SELECT @base64 = CASE	WHEN [reference_url] IS NOT NULL OR [tooltip] IS NOT NULL	
							THEN N'<A HREF="' 
								 + CASE WHEN [reference_url] IS NOT NULL THEN [reference_url] ELSE N'#' END + N'"'
								 + N' TARGET="_blank"'
								 + CASE WHEN [tooltip] IS NOT NULL THEN N' class="tooltip"' ELSE N'' END 
								 + N'>'
							ELSE N'' 
					 END
					+ [image_data_base64]					 
	FROM 
		(
			SELECT *
			FROM (
					SELECT    *
							, ROW_NUMBER() OVER(ORDER BY [id]) AS [row_no]
					FROM [dbo].[catalogReportHTMLGraphics]
					WHERE [name]=@imageType
				)xbase64
			WHERE [row_no] = 1
		)a
		
	RETURN @base64
end

GO
