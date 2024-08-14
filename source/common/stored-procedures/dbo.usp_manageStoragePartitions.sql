RAISERROR('Create procedure: [dbo].[usp_manageStoragePartitions]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_manageStoragePartitions]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_manageStoragePartitions]
GO

-----------------------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[usp_manageStoragePartitions]
		@monthDate		[date], 
		@debugMode		[bit] = 1
/* WITH ENCRYPTION */
AS
SET NOCOUNT ON
-- ============================================================================
-- Copyright (c) 2004-2023 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 03.11.2023
-- Module			 : Database Maintenance Scripts
-- ============================================================================

DECLARE	  @queryToRun			[nvarchar](max)

DECLARE @monthDays TABLE
	(
		[DayNo]		[tinyint],
		[MonthDay]	[date]
	)

/* generate the date's month days list */
;WITH cteCurrentMonthDays ([DayNo], [MonthDay])
AS
(
	SELECT 1 AS DayNo, DATEADD(month,-1, DATEADD(day, 1, EOMONTH(@monthDate))) AS [MonthDay]
	UNION ALL
	SELECT DayNo + 1, DATEADD(day, DayNo, DATEADD(month,-1, DATEADD(day, 1, EOMONTH(@monthDate)))) AS [MonthDay]
	FROM cteCurrentMonthDays
	WHERE DayNo < DATENAME(day, EOMONTH(@monthDate))
)
INSERT	INTO @monthDays([DayNo], [MonthDay])
		SELECT [DayNo], [MonthDay]
		FROM cteCurrentMonthDays

/* create the partition function */
IF NOT EXISTS(SELECT * FROM sys.partition_functions WHERE [name] = 'pf_PartitionByDay')
	begin
		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'CREATE PARTITION FUNCTION [pf_PartitionByDay] (DATETIME) AS RANGE RIGHT FOR VALUES ('

		SELECT @queryToRun = @queryToRun + N'''' + CONVERT([varchar](10), [MonthDay], 120) + N''','
		FROM @monthDays
		ORDER BY [MonthDay]

		SELECT @queryToRun = SUBSTRING(@queryToRun, 1, LEN(@queryToRun)-1) + N');'

		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=0
		EXEC sp_executesql @queryToRun
	end

/* create the partition scheme */
IF NOT EXISTS(SELECT * FROM sys.partition_schemes WHERE [name] = 'ps_PartitionByDay')
	begin
		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'CREATE PARTITION SCHEME [ps_PartitionByDay] AS PARTITION [pf_PartitionByDay] TO ( '

		SELECT @queryToRun = @queryToRun + N'''PRIMARY'','
		FROM @monthDays
		ORDER BY [MonthDay]

		SELECT @queryToRun = SUBSTRING(@queryToRun, 1, LEN(@queryToRun)-1) + N', ''PRIMARY'');'

		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=0
		EXEC sp_executesql @queryToRun
	end

/* alter partition function and schema, if needed */
SET @queryToRun = N''
SELECT @queryToRun = @queryToRun + N'ALTER PARTITION SCHEME [ps_PartitionByDay] NEXT USED ''PRIMARY'';
ALTER PARTITION FUNCTION [pf_PartitionByDay]() SPLIT RANGE (''' + CONVERT([varchar](10), [MonthDay], 120) + N''');'
FROM @monthDays
WHERE [MonthDay] NOT IN (
							SELECT CAST([value] AS [date])
							FROM sys.partition_range_values prv WITH (NOLOCK)
							INNER JOIN sys.partition_functions pf WITH (NOLOCK) ON pf.[function_id] = prv.[function_id]
							WHERE pf.[name] = 'pf_PartitionByDay'
						)
ORDER BY [MonthDay]
		
IF @queryToRun <> N''
	begin
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=0
		EXEC sp_executesql @queryToRun
	end
GO

EXEC [dbo].[usp_manageStoragePartitions] '2023-11-01'
