-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 05.12.2014
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-----------------------------------------------------------------------------------------------------
--
-----------------------------------------------------------------------------------------------------
RAISERROR('Create table: [dbo].[appConfigurations]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sysobjects WHERE id = OBJECT_ID(N'[dbo].[appConfigurations]') AND type in (N'U'))
DROP TABLE [dbo].[appConfigurations]
GO
CREATE TABLE [dbo].[appConfigurations]
(
	[id]			[smallint] IDENTITY (1, 1)NOT NULL,
	[name] 			[nvarchar](128)			NOT NULL,
	[value]			[nvarchar](128)			NULL,
	CONSTRAINT [PK_appConfigurations] PRIMARY KEY  CLUSTERED 
	(
		[id]
	) ON [PRIMARY],
	CONSTRAINT [IX_appConfigurations_Name] UNIQUE  NONCLUSTERED 
	(
		[name]
	) ON [PRIMARY]
) ON [PRIMARY]
GO

-----------------------------------------------------------------------------------------------------
RAISERROR('		...insert default data', 10, 1) WITH NOWAIT
GO
SET NOCOUNT ON
GO
INSERT	INTO [dbo].[appConfigurations] ([name], [value])
		  SELECT 'Default project code'															AS [name], NULL			AS [value]		UNION ALL
		  SELECT 'Alert repeat interval (minutes)'												AS [name], '60'			AS [value]		UNION ALL
		  SELECT 'Default lock timeout (ms)'													AS [name], '5000'		AS [value]		UNION ALL
		  SELECT 'Default backup location'														AS [name], NULL			AS [value]		UNION ALL
		  SELECT 'Default backup retention (days)'												AS [name], '7'			AS [value]		UNION ALL
		  SELECT 'Database Mail profile name to use for sending emails'							AS [name], NULL			AS [value]		UNION ALL
		  SELECT 'Default recipients list - Reports (semicolon separated)'						AS [name], NULL			AS [value]		UNION ALL
		  SELECT 'Default recipients list - Job Status (semicolon separated)'					AS [name], NULL			AS [value]		UNION ALL
		  SELECT 'Default recipients list - Alerts (semicolon separated)'						AS [name], NULL			AS [value]		UNION ALL
		  SELECT 'Local storage path for HTML reports'											AS [name], NULL			AS [value]		UNION ALL
		  SELECT 'HTTP address for report files'												AS [name], NULL			AS [value]		UNION ALL
		  SELECT 'Notify job status only for Failed jobs'										AS [name], 'true'		AS [value]		UNION ALL
		  SELECT 'Log action events'															AS [name], 'true'		AS [value]		UNION ALL
		  SELECT 'Log events retention (days)'													AS [name], '15'			AS [value]		UNION ALL
		  SELECT 'Ignore alerts for: Error 1222 - Lock request time out period exceeded'		AS [name], 'true'		AS [value]		UNION ALL
		  SELECT 'Change retention policy from RetentionDays to RetentionBackupsCount'			AS [name], 'true'		AS [value]		UNION ALL
		  SELECT 'Force cleanup of ghost records'												AS [name], 'false'		AS [value]		UNION ALL
		  SELECT 'Ghost records cleanup threshold'												AS [name], '131072'		AS [value]		UNION ALL
		  SELECT 'Collect SQL Agent jobs step details (health-check)'							AS [name], 'false'		AS [value]
GO

---------------------------------------------------------------------------------------------
--get SQL Server running major version
---------------------------------------------------------------------------------------------
DECLARE @SQLMajorVersion [int]
SELECT @SQLMajorVersion = REPLACE(LEFT(ISNULL(CAST(SERVERPROPERTY('ProductVersion') AS [varchar](32)), ''), 2), '.', '') 

DECLARE @queryToRun [nvarchar](1024)

IF @SQLMajorVersion>8
	begin
		SET @queryToRun=N'UPDATE [dbo].[appConfigurations] SET [value]=(select top 1 [name] from msdb.dbo.sysmail_profile) WHERE [name]=''Database Mail profile name to use for sending emails'''
		EXEC (@queryToRun)
	end
GO


---------------------------------------------------------------------------------------------
--get SQL Server instance default backup location
---------------------------------------------------------------------------------------------
DECLARE @defaultBackupDirectory [nvarchar](260)

EXEC master.dbo.xp_instance_regread N'HKEY_LOCAL_MACHINE',N'Software\Microsoft\MSSQLServer\MSSQLServer',
									N'BackupDirectory',
									@defaultBackupDirectory OUTPUT, 
									'no_output'

IF @defaultBackupDirectory IS NOT NULL
	UPDATE [dbo].[appConfigurations] SET [value] = @defaultBackupDirectory WHERE [name] = 'Default backup location'
GO
