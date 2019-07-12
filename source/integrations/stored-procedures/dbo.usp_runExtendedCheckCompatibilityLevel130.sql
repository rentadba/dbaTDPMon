SET QUOTED_IDENTIFIER ON 
GO
SET ANSI_NULLS ON 
GO

RAISERROR('Create procedure: [dbo].[usp_runExtendedCheckCompatibilityLevel130]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_runExtendedCheckCompatibilityLevel130]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_runExtendedCheckCompatibilityLevel130]
GO

CREATE PROCEDURE [dbo].[usp_runExtendedCheckCompatibilityLevel130]
		@dbName			[sysname],
		@analyzeOnly	[bit] = 1
AS
SET NOCOUNT ON
/* 
	perform metadata analysis, as described under 
	https://support.microsoft.com/en-gb/help/4010261/sql-server-and-azure-sql-database-improvements-in-handling-data-types 
*/

DECLARE @sql	[nvarchar](max);

DECLARE @runExtendedChecks TABLE
	(
	 [id]			[int] not null identity(1,1),
	 [sql_script]	[nvarchar](max)
	)

INSERT	INTO @runExtendedChecks([sql_script])
		SELECT 'DBCC TRACEON(139,-1); ' ;

SET @sql = N'USE [' + @dbName + N'];
SELECT N''USE ['' + DB_NAME() + '']; DBCC CHECKTABLE (N'''''' + object_for_checktable + N'''''') WITH EXTENDED_LOGICAL_CHECKS, NO_INFOMSGS, TABLERESULTS; ''
FROM
(
	--indexed views
	SELECT DISTINCT QUOTENAME(SCHEMA_NAME(o.schema_id)) + N''.'' + QUOTENAME(o.name) AS ''object_for_checktable''
	FROM sys.sql_expression_dependencies AS sed
	 INNER JOIN sys.objects AS o ON sed.referencing_id = o.object_id AND o.type = N''V''
	 INNER JOIN sys.indexes AS i ON o.object_id = i.object_id
	 INNER JOIN sys.sql_modules AS s ON s.object_id = o.object_id
	 INNER JOIN sys.columns AS c ON sed.referenced_id = c.object_id AND sed.referenced_minor_id = c.column_id
	 INNER JOIN sys.types AS t ON c.system_type_id = t.system_type_id

	WHERE referencing_class = 1 AND referenced_class=1 
		 AND (c.system_type_id IN 
	(  59 --real
	 , 62 --float
	 , 58 --smalldatetime
	 , 61 --datetime
	 , 60 --money
	 , 122 --smallmoney
	 , 106 --decimal
	 , 108 --numeric
	 , 56 --int
	 , 48 --tinyint
	 , 52 -- smallint
	 , 41 --time
	 , 127 --bigint
	) OR s.[definition] LIKE N''%DATEDIFF%''
	  OR s.[definition] LIKE N''%CONVERT%''
	  OR s.[definition] LIKE N''%CAST%''
	  OR s.[definition] LIKE N''%DATEPART%''
	  OR s.[definition] LIKE N''%DEGREES%'')

	UNION

	--persisted computed columns
	SELECT DISTINCT QUOTENAME(sed.referenced_schema_name) + N''.'' + QUOTENAME(sed.referenced_entity_name) AS ''object_for_checktable''
	FROM sys.sql_expression_dependencies AS sed
	INNER JOIN sys.computed_columns AS c1 ON sed.referencing_id = c1.object_id AND sed.referencing_minor_id = c1.column_id
	INNER JOIN sys.columns AS c2 ON sed.referenced_id=c2.object_id AND sed.referenced_minor_id = c2.column_id
	INNER JOIN sys.types AS t ON c2.system_type_id = t.system_type_id
	WHERE referencing_class = 1 AND referenced_class = 1 
		AND (c2.system_type_id IN
	(  59 --real
	 , 62 --float
	 , 58 --smalldatetime
	 , 61 --datetime
	 , 60 --money
	 , 122 --smallmoney
	 , 106 --decimal
	 , 108 --numeric
	 , 56 --int
	 , 48 --tinyint
	 , 52 -- smallint
	 , 41 --time
	 , 127 --bigint
	) OR c1.[definition] LIKE N''%DATEDIFF%''
	  OR c1.[definition] LIKE N''%CONVERT%''
	  OR c1.[definition] LIKE N''%DATEPART%''
	  OR c1.[definition] LIKE N''%DEGREES%'')
	AND (
	-- the column is persisted
	c1.is_persisted = 1 
	-- OR the column is included in an index
	OR EXISTS (SELECT 1 FROM sys.index_columns AS ic 
	WHERE ic.object_id = c1.object_id AND ic.column_id=c1.column_id)
	)

	UNION

	--indexed views
	SELECT DISTINCT QUOTENAME(sed.referenced_schema_name) + N''.'' + QUOTENAME(sed.referenced_entity_name) AS ''object_for_checktable''
	FROM sys.sql_expression_dependencies AS sed 
	INNER JOIN sys.indexes AS i ON sed.referencing_id = i.object_id AND sed.referencing_minor_id = i.index_id
	INNER JOIN sys.columns AS c ON sed.referenced_id = c.object_id AND sed.referenced_minor_id = c.column_id 
	INNER JOIN sys.types AS t ON c.system_type_id = t.system_type_id
	WHERE referencing_class = 7 AND referenced_class = 1 AND i.has_filter = 1
	AND c.system_type_id IN ( 
	 59 --real
	 , 62 --float
	 , 58 --smalldatetime
	 , 61 --datetime
	 , 60 --money
	 , 122 --smallmoney
	 , 106 --decimal
	 , 108 --numeric
	 , 56 --int
	 , 48 --tinyint
	 , 52 -- smallint
	 , 41 --time
	 , 127 --bigint
	)
) AS a

UNION ALL

SELECT  N''USE ['' + DB_NAME() + '']; DBCC CHECKCONSTRAINTS (N'''''' + object_for_checkconstraints + N''''''); ''
FROM
(
	SELECT DISTINCT QUOTENAME(sed.referenced_schema_name) + N''.'' + QUOTENAME(sed.referenced_entity_name) AS ''object_for_checkconstraints''
	FROM sys.sql_expression_dependencies AS sed 
	INNER JOIN sys.check_constraints AS c ON sed.referencing_id = c.object_id AND sed.referencing_class = 1
	INNER JOIN sys.columns AS col ON sed.referenced_id = col.object_id AND sed.referenced_minor_id = col.column_id
	INNER JOIN sys.types AS t ON col.system_type_id = t.system_type_id
	WHERE referencing_class = 1 AND referenced_class = 1 AND (col.system_type_id IN 
	(  59 --real
	 , 62 --float
	 , 58 --smalldatetime
	 , 61 --datetime
	 , 60 --money
	 , 122 --smallmoney
	 , 106 --decimal
	 , 108 --numeric
	 , 56 --int
	 , 48 --tinyint
	 , 52 -- smallint
	 , 41 --time
	 , 127 --bigint
	) OR c.[definition] LIKE N''%DATEDIFF%''
	  OR c.[definition] LIKE N''%CONVERT%''
	  OR c.[definition] LIKE N''%DATEPART%''
	  OR c.[definition] LIKE N''%DEGREES%'')
) a'

INSERT	INTO @runExtendedChecks([sql_script])
		EXEC (@sql)

INSERT	INTO @runExtendedChecks([sql_script])
		SELECT  N'DBCC TRACEOFF(139,-1);' ;

IF (SELECT COUNT(*) FROM @runExtendedChecks) > 2
	begin
		IF @analyzeOnly = 1
			begin
				SELECT * FROM @runExtendedChecks ORDER BY [id]
			end
		ELSE
			begin
				DECLARE crsExtendedChecks CURSOR LOCAL FAST_FORWARD FOR SELECT [sql_script] FROM @runExtendedChecks ORDER BY [id]
				OPEN crsExtendedChecks
				FETCH NEXT FROM crsExtendedChecks INTO @sql
				WHILE @@FETCH_STATUS = 0
					begin
						RAISERROR(@sql, 10, 1) WITH NOWAIT

						EXEC (@sql)

						FETCH NEXT FROM crsExtendedChecks INTO @sql
					end
				CLOSE crsExtendedChecks
				DEALLOCATE crsExtendedChecks
			end
	end
GO

/*
--sample run
EXEC [dbo].[usp_dbExtendedCheckCompatibilityLevel130] @dbName = 'msdb', @analyzeOnly = 0
*/
