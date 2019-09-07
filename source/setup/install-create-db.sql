-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
SET QUOTED_IDENTIFIER ON
GO

DECLARE   @dataFilePath [nvarchar](260)
        , @logFilePath	[nvarchar](260)
		, @queryToRun	[nvarchar](4000)
		, @engineEdition [int]

SET @dataFilePath = '$(data_files_path)'
SET @logFilePath = '$(data_files_path)'
SET @engineEdition = CAST(SERVERPROPERTY('EngineEdition') AS [int])

/* try to read default data and log file location from registry */
IF ISNULL(@dataFilePath, '')='' AND @engineEdition NOT IN (5, 6, 8)
	EXEC master.dbo.xp_instance_regread   N'HKEY_LOCAL_MACHINE'
										, N'Software\Microsoft\MSSQLServer\MSSQLServer'
										, N'DefaultData'
										, @dataFilePath output;

IF ISNULL(@logFilePath, '')='' AND @engineEdition NOT IN (5, 6, 8)
	EXEC master.dbo.xp_instance_regread	  N'HKEY_LOCAL_MACHINE'
										, N'Software\Microsoft\MSSQLServer\MSSQLServer'
										, N'DefaultLog'
										, @logFilePath output;

IF RIGHT(@dataFilePath, 1)<>'\' SET @dataFilePath = @dataFilePath + '\'
IF RIGHT(@logFilePath, 1)<>'\'	SET @logFilePath = @logFilePath + '\'

IF @engineEdition NOT IN (5, 6, 8)
	SET @queryToRun = N'CREATE DATABASE [$(dbName)] ON PRIMARY 
	( NAME = N''$(dbName)_data'', FILENAME = ''' + @dataFilePath + N'$(dbName)_data.mdf'' , SIZE = 32MB , MAXSIZE = UNLIMITED, FILEGROWTH = 256MB )
	 LOG ON 
	( NAME = N''$(dbName)_log'', FILENAME = ''' + @logFilePath + N'$(dbName)_log.ldf'' , SIZE = 32MB , MAXSIZE = UNLIMITED , FILEGROWTH = 256MB)'
ELSE
	SET @queryToRun = N'CREATE DATABASE [$(dbName)]'
EXEC sp_executesql @queryToRun
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
DECLARE @queryToRun [nvarchar](1024)

SET @queryToRun=N'ALTER DATABASE [$(dbName)] SET DISABLE_BROKER'
EXEC sp_executesql  @queryToRun

SET @queryToRun=N'ALTER DATABASE [$(dbName)] SET AUTO_UPDATE_STATISTICS_ASYNC OFF'
EXEC sp_executesql  @queryToRun

SET @queryToRun=N'ALTER DATABASE [$(dbName)] SET DATE_CORRELATION_OPTIMIZATION OFF'
EXEC sp_executesql  @queryToRun

SET @queryToRun=N'ALTER DATABASE [$(dbName)] SET TRUSTWORTHY OFF'
EXEC sp_executesql  @queryToRun

SET @queryToRun=N'ALTER DATABASE [$(dbName)] SET ALLOW_SNAPSHOT_ISOLATION ON'
EXEC sp_executesql  @queryToRun

SET @queryToRun=N'ALTER DATABASE [$(dbName)] SET PARAMETERIZATION SIMPLE'
EXEC sp_executesql  @queryToRun

SET @queryToRun=N'ALTER DATABASE [$(dbName)] SET READ_COMMITTED_SNAPSHOT ON'
EXEC sp_executesql  @queryToRun

SET @queryToRun=N'ALTER DATABASE [$(dbName)] SET PAGE_VERIFY CHECKSUM'
EXEC sp_executesql  @queryToRun

SET @queryToRun=N'ALTER DATABASE [$(dbName)] SET DB_CHAINING OFF'
EXEC sp_executesql  @queryToRun

PRINT '"$(dbName)" database created.'
GO
