RAISERROR('Create procedure: [dbo].[usp_mpDatabaseGetMostRecentBackupFromLocation]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sysobjects 
	     WHERE id = OBJECT_ID(N'[dbo].[usp_mpDatabaseGetMostRecentBackupFromLocation]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpDatabaseGetMostRecentBackupFromLocation]
GO

SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[usp_mpDatabaseGetMostRecentBackupFromLocation]
		  @serverToRun			[sysname]
		, @forSQLServerName		[sysname] = '%'
		, @forDatabaseName		[sysname]
		, @backupLocation		[nvarchar](512)
		, @backupType			[nvarchar](32) = 'full' /* options available: FULL, DIFF, LOG */
		, @nameConvention		[nvarchar](32) = 'dbaTDPMon' /*options available: dbaTDPMon, Ola */
		, @debugMode			[bit]=0
/* WITH ENCRYPTION */
WITH RECOMPILE
AS

-- ============================================================================
-- Copyright (c) 2004-2016 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 18.05.2016
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SET NOCOUNT ON

DECLARE   @queryToRun				[nvarchar](1024)

DECLARE	  @serverEdition			[sysname]
		, @serverVersionStr			[sysname]
		, @serverVersionNum			[numeric](9,6)
		, @optionXPValue			[int]

SET NOCOUNT ON

/*-------------------------------------------------------------------------------------------------------------------------------*/
IF object_id('tempdb..#serverPropertyConfig') IS NOT NULL DROP TABLE #serverPropertyConfig
CREATE TABLE #serverPropertyConfig
			(
				[value]			[sysname]	NULL
			)

-----------------------------------------------------------------------------------------
--get destination server running version/edition
EXEC [dbo].[usp_getSQLServerVersion]	@sqlServerName			= @serverToRun,
										@serverEdition			= @serverEdition OUT,
										@serverVersionStr		= @serverVersionStr OUT,
										@serverVersionNum		= @serverVersionNum OUT,
										@executionLevel			= 0,
										@debugMode				= @debugMode

/*-------------------------------------------------------------------------------------------------------------------------------*/
SET @optionXPValue = 0

IF @serverVersionNum>=9
	begin
		/* enable xp_cmdshell configuration option */
		EXEC [dbo].[usp_changeServerOption_xp_cmdshell]   @serverToRun	 = @serverToRun
														, @flgAction	 = 1			-- 1=enable | 0=disable
														, @optionXPValue = @optionXPValue OUTPUT
														, @debugMode	 = @debugMode
	end

/*-------------------------------------------------------------------------------------------------------------------------------*/
	IF OBJECT_ID('tempdb..#backupFiles') IS NOT NULL DROP TABLE #backupFiles;
	CREATE TABLE #backupFiles
	(
		  [id]			[int] IDENTITY(1, 1)
		, [file_name]	[nvarchar](260)
		, [depth]		[int]
		, [is_file]		[int]
		, [create_time]	[varchar](20)
	)

IF RIGHT(@backupLocation, 1)<>'\' 
	SET @backupLocation = @backupLocation + '\'
IF @forSQLServerName <> '%' 
	SET @backupLocation = @backupLocation + 
							CASE	WHEN @nameConvention='dbaTDPMon' THEN @forSQLServerName
									WHEN @nameConvention='Ola' THEN REPLACE(@forSQLServerName, '\', '$')
							END + '\' + 
							@forDatabaseName + '\' + 
							CASE	WHEN @nameConvention='dbaTDPMon' THEN ''
									WHEN @nameConvention='Ola' THEN @backupType + '\'
							END
	
SET @queryToRun = N'EXEC xp_dirtree ''' + @backupLocation + ''', 8, 1';
IF @serverToRun<>@@SERVERNAME
	begin
		IF @serverVersionNum < 11
			SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @serverToRun + '], ''SET FMTONLY OFF; EXEC(''''' + REPLACE(@queryToRun, '''', '''''''''') + ''''')'')'
		ELSE
			SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @serverToRun + '], ''SET FMTONLY OFF; EXEC(''''' + REPLACE(@queryToRun, '''', '''''''''') + ''''') WITH RESULT SETS(([subdirectory] [nvarchar](260), [depth] [int], [file] [int]))'')'
	end

IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

DELETE FROM #backupFiles
INSERT INTO #backupFiles([file_name], [depth], [is_file])
		EXEC (@queryToRun)


IF @nameConvention = 'dbaTDPMon'
	begin
		SET @backupType = CASE	WHEN LOWER(@backupType) = 'full' THEN '_full.BAK'
								WHEN LOWER(@backupType) = 'diff' THEN '_diff.BAK'
								WHEN LOWER(@backupType) = 'log' THEN '_log.TRN'
						  END
		UPDATE #backupFiles
			SET [create_time] = SUBSTRING([file_name], CHARINDEX('_' + @forDatabaseName + '_', [file_name], 1) + LEN(@forDatabaseName) + 2, 15)
		WHERE	CHARINDEX('_' + @forDatabaseName + '_', [file_name], 1) <> 0
				AND CHARINDEX(@backupType, [file_name], 1) <> 0
	end

IF @nameConvention = 'Ola'
	begin
		SET @backupType = CASE	WHEN LOWER(@backupType) = 'full' THEN '_full_'
								WHEN LOWER(@backupType) = 'diff' THEN '_diff_'
								WHEN LOWER(@backupType) = 'log' THEN '_log_'
						  END
		UPDATE #backupFiles
			SET [create_time] = SUBSTRING([file_name], CHARINDEX( @backupType, [file_name], 1) + LEN(@backupType) , 15)
		WHERE	CHARINDEX('_' + @forDatabaseName + '_', [file_name], 1) <> 0
				AND CHARINDEX(@backupType, [file_name], 1) <> 0

	end

/*-------------------------------------------------------------------------------------------------------------------------------*/
/* get last backup type with full path */
DECLARE   @fileName		[nvarchar](512)
		, @depth		[int]
		, @id			[int]
		, @timeStamp	[varchar](20)

SELECT TOP 1 
	   @id = [id]
	 , @fileName = [file_name]
	 , @depth = [depth]
	 , @timeStamp = [create_time]
FROM #backupFiles 
WHERE [create_time] IS NOT NULL
ORDER BY [create_time] DESC

WHILE @depth>0
	begin
		SELECT TOP 1
			@fileName = [file_name] + '\' + @fileName
		FROM #backupFiles
		WHERE [id] < @id
			AND [depth] = @depth -1
		ORDER BY [id] DESC

		SET @depth = @depth -1
	end

SET @fileName = @backupLocation + @fileName
SELECT @fileName AS [file_name], @timeStamp AS [time_stamp]

/*-------------------------------------------------------------------------------------------------------------------------------*/
IF @serverVersionNum>=9
	begin
		/* disable xp_cmdshell configuration option */
		EXEC [dbo].[usp_changeServerOption_xp_cmdshell]   @serverToRun	 = @serverToRun
														, @flgAction	 = 0			-- 1=enable | 0=disable
														, @optionXPValue = @optionXPValue OUTPUT
														, @debugMode	 = @debugMode
	end
GO
