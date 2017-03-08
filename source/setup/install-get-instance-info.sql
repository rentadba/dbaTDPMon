-- ============================================================================
-- Copyright (c) 2004-2017 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
SET QUOTED_IDENTIFIER ON
GO

DECLARE   @dataFilePath [nvarchar](260)
        , @logFilePath	[nvarchar](260)
		, @serverVersionStr	[sysname]

SET @dataFilePath = REPLACE('$(data_files_path)', '"', '')
SET @logFilePath =  REPLACE('$(data_files_path)', '"', '')

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

		SELECT @serverVersionStr = CAST(SERVERPROPERTY('ProductVersion') AS [sysname]) 
		PRINT 'productVersion=' + CAST(SERVERPROPERTY('ProductVersion') AS [sysname]) 
		PRINT 'engineVersion=' + SUBSTRING(@serverVersionStr, 1, CHARINDEX('.', @serverVersionStr)-1)
	end
GO
