-- ============================================================================
-- Copyright (c) 2004-2017 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
SET QUOTED_IDENTIFIER ON
GO
SET NOCOUNT ON
GO

DECLARE   @dataFilePath		[nvarchar](260)
        , @logFilePath		[nvarchar](260)
		, @serverVersionStr	[sysname]
		, @serverVersionNum	[numeric](9,6)
		, @hostPlatform		[sysname]
		, @queryToRun		[varchar](512)

SELECT @serverVersionStr = CAST(SERVERPROPERTY('ProductVersion') AS [sysname]) 
PRINT 'productVersion=' + CAST(SERVERPROPERTY('ProductVersion') AS [sysname]) 
PRINT 'engineVersion=' + SUBSTRING(@serverVersionStr, 1, CHARINDEX('.', @serverVersionStr)-1)
SET @serverVersionNum=SUBSTRING(@serverVersionStr, 1, CHARINDEX('.', @serverVersionStr)-1) + '.' + REPLACE(SUBSTRING(@serverVersionStr, CHARINDEX('.', @serverVersionStr)+1, LEN(@serverVersionStr)), '.', '') 

IF @serverVersionNum >= 14
	begin
		SET @queryToRun = N'SELECT [host_platform] FROM sys.dm_os_host_info'

		IF object_id('tempdb..#tmpOutput') IS NOT NULL 
		DROP TABLE #tmpOutput

		CREATE TABLE #tmpOutput
		(
			[output] [nvarchar](512) NULL
		)

		INSERT	INTO #tmpOutput([output])
				EXEC (@queryToRun)

		SELECT @hostPlatform = LOWER([output])
		FROM #tmpOutput
	end
	
SET @dataFilePath = REPLACE('$(data_files_path)', '"', '')
SET @logFilePath =  REPLACE('$(data_files_path)', '"', '')

IF NOT (@serverVersionNum >= 14 AND @hostPlatform='linux' ) 
	begin
		/* try to read default data and log file location from registry */
		IF ISNULL(@dataFilePath, '')=''
			EXEC master.dbo.xp_instance_regread   N'HKEY_LOCAL_MACHINE'
												, N'Software\Microsoft\MSSQLServer\MSSQLServer'
												, N'DefaultData'
												, @dataFilePath output;

		IF ISNULL(@logFilePath, '')=''
			EXEC master.dbo.xp_instance_regread	  N'HKEY_LOCAL_MACHINE'
												, N'Software\Microsoft\MSSQLServer\MSSQLServer'
												, N'DefaultLog'
												, @logFilePath output;
	end

IF ISNULL(@dataFilePath, '')='' OR ISNULL(@logFilePath, '')=''
	begin
		RAISERROR('*-----------------------------------------------------------------------------*', 10, 1) WITH NOWAIT
		RAISERROR('Database Default Locations are not set for current SQL Server instance. You must provide them to the install utility.', 16, 1) WITH NOWAIT
	end
ELSE
	begin
		IF RIGHT(@dataFilePath, 1)<>'\' SET @dataFilePath = @dataFilePath + '\'
		IF RIGHT(@logFilePath, 1)<>'\'	SET @logFilePath = @logFilePath + '\'

		PRINT 'dataFilePath="' + @dataFilePath + '"'
		PRINT 'logFilePath="' + @logFilePath + '"'
	end
GO
