RAISERROR('Create procedure: [dbo].[usp_mpRestoreDatabaseAs]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sysobjects 
	     WHERE id = OBJECT_ID(N'[dbo].[usp_mpRestoreDatabaseAs]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpRestoreDatabaseAs]
GO

SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[usp_mpRestoreDatabaseAs]
		  @forDatabaseName				[sysname]
		, @toDatabaseName				[sysname]
		, @restorePathForData			[nvarchar](260)
		, @restorePathForLog			[nvarchar](260)
		, @addTimeStampSuffixToFiles	[bit] = 0
AS

SET NOCOUNT ON
DECLARE   @queryToRun		[nvarchar](max)
		, @BackupFileName	[nvarchar](260)
		, @LogicalName		[sysname]
		, @logicalFileType	[char](1)
		, @PhysicalName		[nvarchar](260)
		, @timeStampSuffix	[nvarchar](20)

IF OBJECT_ID('tempdb..#mostRecentBackupFile') IS NOT NULL DROP TABLE #mostRecentBackupFile
CREATE TABLE #mostRecentBackupFile
	(
		  [file_name]	[nvarchar](260)
		, [time_stamp]	[nvarchar](20)
		, [type]		[sysname]	NULL
	)
		
SET @queryToRun = N''
SET @queryToRun = @queryToRun + N'
DECLARE	  @serverToRun			[sysname]
		, @forSQLServerName		[sysname]
		, @backupLocation		[nvarchar](512)
		, @backupType			[nvarchar](32)
		, @nameConvention		[nvarchar](32)


SELECT    @serverToRun = [RunSQLFrom]
		, @forSQLServerName = [ServerName]
		, @backupLocation = [BackupLocation]
		, @backupType = ''full''
		, @nameConvention = [BackupMode]
FROM DbaAdmin.refresh.ClientSourceData
WHERE [DatabaseName] = ''' + @forDatabaseName + '''

EXEC dbaTDPMon.[dbo].[usp_mpDatabaseGetMostRecentBackupFromLocation]	  @serverToRun		= @serverToRun
																		, @forSQLServerName = @forSQLServerName
																		, @forDatabaseName	= ''' + @forDatabaseName + '''
																		, @backupLocation	= @backupLocation
																		, @backupType		= @backupType
																		, @nameConvention	= @nameConvention
																		, @debugMode		= 0
'

SET @queryToRun = N'SELECT * FROM OPENQUERY([AWISMONDB1], ''SET FMTONLY OFF; EXEC(''''' + REPLACE(@queryToRun, '''', '''''''''') + ''''') WITH RESULT SETS(([file_name] [nvarchar](260), [time_stamp] [nvarchar](20)))'')'

/* get latest full database backup */
--PRINT @queryToRun
INSERT	INTO #mostRecentBackupFile([file_name], [time_stamp])
		EXEC (@queryToRun)

UPDATE #mostRecentBackupFile SET [type]='full' WHERE [type] IS NULL

/* get latest differential database backup */
SET @queryToRun = REPLACE(@queryToRun, '@backupType = ''''''''full''''''''', '@backupType = ''''''''diff''''''''')

--PRINT @queryToRun
INSERT	INTO #mostRecentBackupFile([file_name], [time_stamp])
		EXEC (@queryToRun)

UPDATE #mostRecentBackupFile SET [type]='diff' WHERE [type] IS NULL

DELETE FROM #mostRecentBackupFile
WHERE	[type]='diff' 
		AND CAST(REPLACE([time_stamp], '_', '') AS [bigint]) < (
																SELECT CAST(REPLACE([time_stamp], '_', '') AS [bigint]) 
																FROM #mostRecentBackupFile 
																WHERE [type]='full'
																)

IF @addTimeStampSuffixToFiles=1
	SELECT @timeStampSuffix = MAX([time_stamp])
	FROM #mostRecentBackupFile

		
IF OBJECT_ID('tempdb..#dbFileList') IS NOT NULL DROP TABLE #dbFileList;
CREATE TABLE #dbFileList
(
	  [LogicalName]				nvarchar(128)
	, [PhysicalName]			nvarchar(260)
	, [Type]					char(1)
	, [FileGroupName]			nvarchar(128)
	, [Size]					numeric(20,0)
	, [MaxSize]					numeric(20,0)
	, [FileID]					bigint
	, [CreateLSN]				numeric(25,0)
	, [DropLSN]					numeric(25,0) NULL
	, [UniqueID]				uniqueidentifier
	, [ReadOnlyLSN]				numeric(25,0) NULL
	, [ReadWriteLSN]			numeric(25,0) NULL
	, [BackupSizeInBytes]		bigint
	, [SourceBlockSize]			int
	, [FileGroupID]				int
	, [LogGroupGUID]			uniqueidentifier NULL
	, [DifferentialBaseLSN]		numeric(25,0) NULL
	, [DifferentialBaseGUID]	uniqueidentifier
	, [IsReadOnly]				bit
	, [IsPresent]				bit
	, [TDEThumbprint]			varbinary(32)
)

/* restore last full database backup */
SELECT @BackupFileName = [file_name] FROM #mostRecentBackupFile WHERE [type]='full'

SET @queryToRun = N'RESTORE FILELISTONLY FROM DISK = ''' + @BackupFileName + ''';';
INSERT	INTO #dbFileList 
		EXEC (@queryToRun)

SET @queryToRun = N'RESTORE DATABASE [' + @toDatabaseName + N'] FROM DISK = ''' + @BackupFileName + N''' WITH STATS=1, REPLACE, NORECOVERY';
			
DECLARE crsdbFileList CURSOR FOR	SELECT [LogicalName], [PhysicalName], [Type] 
									FROM #dbFileList;
OPEN crsdbFileList;
FETCH NEXT FROM crsdbFileList INTO @LogicalName, @PhysicalName, @logicalFileType;
WHILE @@FETCH_STATUS = 0
	begin
		SET @PhysicalName = RIGHT(@PhysicalName, CHARINDEX('\', REVERSE(@PhysicalName)) - 1)
		IF @addTimeStampSuffixToFiles=1
			SET @PhysicalName = SUBSTRING(@PhysicalName, 1, LEN(@PhysicalName)-4) + 
								'_' + @timeStampSuffix + 
								'_' + REPLACE(REPLACE(REPLACE(CONVERT([varchar](19), GETDATE(), 121), '-', ''), ':', ''), ' ', '_') + 
								SUBSTRING(@PhysicalName, LEN(@PhysicalName)-3, 4)

		SET @queryToRun = @queryToRun + N', MOVE ''' + @LogicalName + ''' TO ''' + 
							CASE	WHEN @logicalFileType='D' THEN @restorePathForData
									WHEN @logicalFileType='L' THEN @restorePathForLog
							END  + @PhysicalName + '''';
				
		FETCH NEXT FROM crsdbFileList INTO @LogicalName, @PhysicalName, @logicalFileType;
	end
		
CLOSE crsdbFileList;
DEALLOCATE crsdbFileList;			

/* kill existing database connections*/
EXEC [dbaTDPMon].[dbo].[usp_mpDatabaseKillConnections]	@SQLServerName	= @@SERVERNAME,
														@DBName			= @toDatabaseName,
														@flgOptions		= 3

PRINT '--' + @queryToRun
EXEC sp_executesql @queryToRun


/* restore last differential backup */
SET @BackupFileName=NULL
SELECT @BackupFileName = [file_name] FROM #mostRecentBackupFile WHERE [type]='diff'

IF @BackupFileName IS NOT NULL
	begin
		SET @queryToRun = N'RESTORE DATABASE [' + @toDatabaseName + N'] FROM DISK = ''' + @BackupFileName + N''' WITH STATS=1, REPLACE, NORECOVERY';

		PRINT '--' + @queryToRun
		EXEC sp_executesql @queryToRun
	end


SET @queryToRun = N'RESTORE DATABASE [' + @toDatabaseName + N'] WITH RECOVERY';
PRINT '--' + @queryToRun
EXEC sp_executesql @queryToRun
