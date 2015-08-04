RAISERROR('Create function: [dbo].[ufn_convertLSNToNumeric]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sysobjects WHERE id = OBJECT_ID(N'[dbo].[ufn_convertLSNToNumeric]') AND type in (N'FN', N'IF', N'TF', N'FS', N'FT'))
DROP FUNCTION [dbo].[ufn_convertLSNToNumeric]
GO

CREATE FUNCTION [dbo].[ufn_convertLSNToNumeric]
(		
	@LSN		[varchar](22)
)
RETURNS [numeric](25)
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 26.05.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

/*
	http://www.sqlskills.com/blogs/paul/using-fn_dblog-fn_dump_dblog-and-restoring-with-stopbeforemark-to-an-lsn/
	Take the rightmost 4 characters (2-byte log record number) and convert to a 5-character decimal number, including leading zeroes, to get stringA
	Take the middle number (4-byte log block number) and convert to a 10-character decimal number, including leading zeroes, to get stringB
	Take the leftmost number (4-byte VLF sequence number) and convert to a decimal number, with no leading zeroes, to get stringC
	The LSN string we need is stringC + stringB + stringA

	00000001:00000001:0001
*/
begin
	RETURN	CAST(CAST(CAST(CAST(CONVERT(VARBINARY, '0x' + RIGHT(REPLICATE('0', 8) + LEFT(@LSN, 8), 8), 1) As int)  AS VARCHAR(32)) as varchar(8)) + 
			CAST(RIGHT(REPLICATE('0', 10) + CAST(CAST(CONVERT(VARBINARY, '0x' + RIGHT(REPLICATE('0', 8) + SUBSTRING(@LSN, 10, 8), 8), 1) As int)  AS VARCHAR(32)), 10) as varchar(10)) + 
			CAST(RIGHT(REPLICATE('0', 5) + CAST(CAST(CONVERT(VARBINARY, '0x' + RIGHT(REPLICATE('0', 8) + RIGHT(@LSN, 4), 8), 1) As int)  AS VARCHAR(32)), 5) as varchar(5)) AS [numeric](25))
end

GO


