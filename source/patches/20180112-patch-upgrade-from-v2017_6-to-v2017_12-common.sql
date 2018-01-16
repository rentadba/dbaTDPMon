SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2017.6 to 2017.12 (2017.12.22)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: common																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20180112-patch-upgrade-from-v2017_6-to-v2017_12-common.sql', 10, 1) WITH NOWAIT

IF NOT EXISTS(SELECT * FROM [dbo].[appConfigurations] WHERE [name] = 'Maximum SQL Agent jobs started per minute (KB306457)' and [module] = 'common')
	INSERT	INTO [dbo].[appConfigurations] ([module], [name], [value])
		  SELECT 'common' AS [module], 'Maximum SQL Agent jobs started per minute (KB306457)' AS [name], '60' AS [value]
GO

IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE [TABLE_SCHEMA]='dbo' AND [TABLE_NAME]='catalogMachineNames' AND [COLUMN_NAME]='host_platform')
	ALTER TABLE [dbo].[catalogMachineNames] ADD [host_platform]	[sysname] NULL
GO
