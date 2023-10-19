/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
*/
SET NOCOUNT ON
DECLARE   @compressionType	[varchar](30)
		, @indexID			[smallint]
		, @fullTableName	[nvarchar](560)
		, @indexName		[sysname]
		, @sqlText			[nvarchar](max)
SET @compressionType = 'PAGE'
-----------------------------------------------------------------------------------------
/* get the tables and indexes list */
IF OBJECT_ID('tempdb..#indexesList') IS NOT NULL DROP TABLE #indexesList;

CREATE TABLE #indexesList
	(
			[index_id]		[smallint]
		, [schema_name]		[sysname]
		, [table_name]		[sysname]
		, [index_name]		[sysname] NULL
	)

INSERT	INTO #indexesList([index_id], [schema_name], [table_name], [index_name])
		SELECT  DISTINCT  si.[index_id]
				, OBJECT_SCHEMA_NAME(si.[object_id]) AS [schema_name]
				, OBJECT_NAME(si.[object_id]) AS [table_name]
				, si.[name] AS [index_name]
		FROM sys.partitions sp WITH (NOLOCK)
		INNER JOIN sys.indexes si WITH (NOLOCK) ON sp.[object_id] = si.[object_id] AND sp.[index_id] = si.[index_id]
		INNER JOIN sys.objects so WITH (NOLOCK) ON sp.[object_id] = so.[object_id]
		WHERE	so.[is_ms_shipped] = 0
			AND sp.[data_compression_desc] = 'NONE'

-----------------------------------------------------------------------------------------
/* processing tables and indexes*/
DECLARE crsObjects CURSOR READ_ONLY FAST_FORWARD FOR SELECT   QUOTENAME([schema_name]) + '.' + QUOTENAME([table_name])  AS [full_table_name]
															, [index_name]
														FROM #indexesList
														ORDER BY [full_table_name], [index_id]
OPEN crsObjects
FETCH NEXT FROM crsObjects INTO @fullTableName, @indexName
WHILE @@FETCH_STATUS = 0
	begin
		SET @sqlText = N'-- processing: ' + @fullTableName + CASE WHEN @indexName IS NOT NULL THEN N' / index: ' + @indexName + N')' ELSE N'' END
		RAISERROR(@sqlText, 10, 1) WITH NOWAIT

		/* generate the index/table rebuild SQL statement */
		SET @sqlText = N'ALTER ' + CASE WHEN @indexName IS NOT NULL 
										THEN N'INDEX ' + QUOTENAME(@indexName) + ' ON '
										ELSE N'TABLE' END + 
						@fullTableName + N' REBUILD WITH (MAXDOP=1, DATA_COMPRESSION=' + @compressionType + N');'

		BEGIN TRY
			EXECUTE (@sqlText)
		END TRY
		BEGIN CATCH
			PRINT ERROR_MESSAGE()
		END CATCH

		FETCH NEXT FROM crsObjects INTO @fullTableName, @indexName
	end
CLOSE crsObjects
DEALLOCATE crsObjects
GO