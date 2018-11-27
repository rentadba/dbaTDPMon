SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2018.10 to 2018.11 (2018.11.27)				  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: common																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20181127-patch-upgrade-from-v2018_10-to-v2018_11-common.sql', 10, 1) WITH NOWAIT

IF NOT EXISTS(SELECT * FROM [dbo].[appConfigurations] WHERE [name] = 'Ignore alerts for: Maximum SQL Agent jobs running limit reached' and [module] = 'common')
	INSERT	INTO [dbo].[appConfigurations] ([module], [name], [value])
			SELECT 'common' AS [module], 'Ignore alerts for: Maximum SQL Agent jobs running limit reached'	AS [name], '0' AS [value]
GO
