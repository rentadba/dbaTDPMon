-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
SET QUOTED_IDENTIFIER ON
GO

IF NOT EXISTS(SELECT * FROM sys.databases WHERE [name] = '$(dbName)')
	begin
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

		IF @engineEdition NOT IN (5, 6, 8)
			begin
				SET @queryToRun=N'ALTER DATABASE [$(dbName)] SET RECOVERY SIMPLE'
				EXEC sp_executesql  @queryToRun

				SET @queryToRun=N'ALTER DATABASE [$(dbName)] SET ALLOW_SNAPSHOT_ISOLATION ON'
				EXEC sp_executesql  @queryToRun

				SET @queryToRun=N'ALTER DATABASE [$(dbName)] SET READ_COMMITTED_SNAPSHOT ON'
				EXEC sp_executesql  @queryToRun
			end
		PRINT '"$(dbName)" database created.'
	end
GO
