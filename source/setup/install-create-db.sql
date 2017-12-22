-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
SET QUOTED_IDENTIFIER ON
GO

DECLARE   @dataFilePath [nvarchar](260)
        , @logFilePath	[nvarchar](260)
		, @queryToRun	[nvarchar](4000)

SET @dataFilePath = '$(data_files_path)'
SET @logFilePath = '$(data_files_path)'

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

IF RIGHT(@dataFilePath, 1)<>'\' SET @dataFilePath = @dataFilePath + '\'
IF RIGHT(@logFilePath, 1)<>'\'	SET @logFilePath = @logFilePath + '\'

SET @queryToRun = N'CREATE DATABASE [$(dbName)] ON  PRIMARY 
( NAME = N''Primary'', FILENAME = ''' + @dataFilePath + N'$(dbName)_primary.mdf'' , SIZE = 8MB , MAXSIZE = UNLIMITED, FILEGROWTH = 1024MB ), 
 FILEGROUP [FG_Snapshots_Data] 
( NAME = N''Snapshots_Data_1'', FILENAME = ''' + @dataFilePath + N'$(dbName)_data_snapshots_1.ndf'' , SIZE = 8MB , MAXSIZE = UNLIMITED, FILEGROWTH = 1024MB ), 
 FILEGROUP [FG_Snapshots_Index] 
( NAME = N''Snapshots_Index_1'', FILENAME = ''' + @dataFilePath + N'$(dbName)_index_snapshots_1.ndf'' , SIZE = 8MB , MAXSIZE = UNLIMITED, FILEGROWTH = 1024MB ),
 FILEGROUP [FG_Statistics_Data] 
( NAME = N''Statistics_Data_1'', FILENAME = ''' + @dataFilePath + N'$(dbName)_data_statistics_1.ndf'' , SIZE = 8MB , MAXSIZE = UNLIMITED, FILEGROWTH = 1024MB ), 
 FILEGROUP [FG_Statistics_Index] 
( NAME = N''Statistics_Index_1'', FILENAME = ''' + @dataFilePath + N'$(dbName)_index_statistics_1.ndf'' , SIZE = 8MB , MAXSIZE = UNLIMITED, FILEGROWTH = 1024MB )
 LOG ON 
( NAME = N''Log_1'', FILENAME = ''' + @logFilePath + N'$(dbName)_log_1.ldf'' , SIZE = 32MB , MAXSIZE = UNLIMITED , FILEGROWTH = 1024MB)'

EXEC (@queryToRun)
GO

ALTER DATABASE [$(dbName)] SET ANSI_NULL_DEFAULT OFF 
GO

ALTER DATABASE [$(dbName)] SET ANSI_NULLS OFF 
GO

ALTER DATABASE [$(dbName)] SET ANSI_PADDING OFF 
GO

ALTER DATABASE [$(dbName)] SET ANSI_WARNINGS OFF 
GO

ALTER DATABASE [$(dbName)] SET ARITHABORT OFF 
GO

ALTER DATABASE [$(dbName)] SET AUTO_CLOSE OFF 
GO

ALTER DATABASE [$(dbName)] SET AUTO_CREATE_STATISTICS ON 
GO

ALTER DATABASE [$(dbName)] SET AUTO_SHRINK OFF 
GO

ALTER DATABASE [$(dbName)] SET AUTO_UPDATE_STATISTICS ON 
GO

ALTER DATABASE [$(dbName)] SET CURSOR_CLOSE_ON_COMMIT OFF 
GO

ALTER DATABASE [$(dbName)] SET CURSOR_DEFAULT  GLOBAL 
GO

ALTER DATABASE [$(dbName)] SET CONCAT_NULL_YIELDS_NULL OFF 
GO

ALTER DATABASE [$(dbName)] SET NUMERIC_ROUNDABORT OFF 
GO

ALTER DATABASE [$(dbName)] SET QUOTED_IDENTIFIER OFF 
GO

ALTER DATABASE [$(dbName)] SET RECURSIVE_TRIGGERS OFF 
GO

ALTER DATABASE [$(dbName)] SET READ_WRITE 
GO

ALTER DATABASE [$(dbName)] SET RECOVERY SIMPLE 
GO

ALTER DATABASE [$(dbName)] SET MULTI_USER 
GO


---------------------------------------------------------------------------------------------
--get SQL Server running major version
---------------------------------------------------------------------------------------------
DECLARE @SQLMajorVersion [int]
SELECT @SQLMajorVersion = REPLACE(LEFT(ISNULL(CAST(SERVERPROPERTY('ProductVersion') AS [varchar](32)), ''), 2), '.', '') 

DECLARE @queryToRun [nvarchar](1024)

IF @SQLMajorVersion > 8
	begin
		SET @queryToRun=N'ALTER DATABASE [$(dbName)] SET DISABLE_BROKER'
		EXEC (@queryToRun)

		SET @queryToRun=N'ALTER DATABASE [$(dbName)] SET AUTO_UPDATE_STATISTICS_ASYNC OFF'
		EXEC (@queryToRun)

		SET @queryToRun=N'ALTER DATABASE [$(dbName)] SET DATE_CORRELATION_OPTIMIZATION OFF'
		EXEC (@queryToRun)

		SET @queryToRun=N'ALTER DATABASE [$(dbName)] SET TRUSTWORTHY OFF'
		EXEC (@queryToRun)

		SET @queryToRun=N'ALTER DATABASE [$(dbName)] SET ALLOW_SNAPSHOT_ISOLATION ON'
		EXEC (@queryToRun)

		SET @queryToRun=N'ALTER DATABASE [$(dbName)] SET PARAMETERIZATION SIMPLE'
		EXEC (@queryToRun)

		SET @queryToRun=N'ALTER DATABASE [$(dbName)] SET READ_COMMITTED_SNAPSHOT ON'
		EXEC (@queryToRun)

		SET @queryToRun=N'ALTER DATABASE [$(dbName)] SET PAGE_VERIFY CHECKSUM'
		EXEC (@queryToRun)

		SET @queryToRun=N'ALTER DATABASE [$(dbName)] SET DB_CHAINING OFF'
		EXEC (@queryToRun)
	end

PRINT '"$(dbName)" database created.'
GO
