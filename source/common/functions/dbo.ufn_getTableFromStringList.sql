RAISERROR('Create function: [dbo].[ufn_getTableFromStringList]', 10, 1) WITH NOWAIT
GO
if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[ufn_getTableFromStringList]') and xtype in (N'FN', N'IF', N'TF'))
drop function [dbo].[ufn_getTableFromStringList]
GO

SET QUOTED_IDENTIFIER ON 
GO
SET ANSI_NULLS ON 
GO

/* http://stackoverflow.com/questions/17481479/parse-comma-separated-string-to-make-in-list-of-strings-in-the-where-clause */
CREATE FUNCTION [dbo].[ufn_getTableFromStringList]
(
	@listWithValues		nvarchar(4000), 
	@valueDelimiter		char(1)
)
RETURNS @result TABLE 
	(	[id]	[int],
		[value] [nvarchar](4000)
	)
AS
begin
	SET @listWithValues = @listWithValues + @valueDelimiter
	;WITH lst AS
	(
		SELECT	CAST(1 AS [int]) StartPos, 
				CHARINDEX(@valueDelimiter, @listWithValues) EndPos, 
				1 AS [id]
		UNION ALL
		SELECT	EndPos + 1,
				CHARINDEX(@valueDelimiter, @listWithValues, EndPos + 1), 
				[id] + 1
		FROM lst
		WHERE CHARINDEX(@valueDelimiter, @listWithValues, EndPos + 1) > 0
	)
	INSERT @result ([id], [value])
	SELECT [id], LTRIM(RTRIM(SUBSTRING(@listWithValues, StartPos, EndPos - StartPos)))
	FROM lst
	--OPTION (MAXRECURSION 0)

	RETURN
end
GO
