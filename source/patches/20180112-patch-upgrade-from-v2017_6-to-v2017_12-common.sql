SET NOCOUNT ON
RAISERROR('*-----------------------------------------------------------------------------*', 10, 1) WITH NOWAIT
RAISERROR('* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *', 10, 1) WITH NOWAIT
RAISERROR('* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *', 10, 1) WITH NOWAIT
RAISERROR('*-----------------------------------------------------------------------------*', 10, 1) WITH NOWAIT
RAISERROR('* Patch script: from version 2017.6 to 2017.12 (2017.12.22)					 *', 10, 1) WITH NOWAIT
RAISERROR('*-----------------------------------------------------------------------------*', 10, 1) WITH NOWAIT

DECLARE @appVersion [sysname]
SELECT @appVersion = [value] FROM [dbo].[appConfigurations] WHERE [module] = 'common' AND [name] = 'Application Version'
PRINT 'Detected dbaTDPMon version: ' + @appVersion

GO
UPDATE [dbo].[appConfigurations] SET [value] = N'2017.17.22' WHERE [module] = 'common' AND [name] = 'Application Version'
GO


/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: common																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('Patching module: COMMON', 10, 1) WITH NOWAIT

IF NOT EXISTS(SELECT * FROM [dbo].[appConfigurations] WHERE [name] = 'Maximum SQL Agent jobs started per minute (KB306457)' and [module] = 'common')
	INSERT	INTO [dbo].[appConfigurations] ([module], [name], [value])
		  SELECT 'common' AS [module], 'Maximum SQL Agent jobs started per minute (KB306457)' AS [name], '60' AS [value]
GO

IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE [TABLE_SCHEMA]='dbo' AND [TABLE_NAME]='catalogMachineNames' AND [COLUMN_NAME]='host_platform')
	ALTER TABLE [dbo].[catalogMachineNames] ADD [host_platform]	[sysname] NULL
GO


/*---------------------------------------------------------------------------------------------------------------------*/
DECLARE @appVersion [sysname]
SELECT @appVersion = [value] FROM [dbo].[appConfigurations] WHERE [module] = 'common' AND [name] = 'Application Version'
PRINT 'Updated  dbaTDPMon version: ' + @appVersion

RAISERROR('* Done *', 10, 1) WITH NOWAIT
GO
