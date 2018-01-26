SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2017.4 to 2017.5 (2017.04.28)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: commons																							   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20170428-patch-upgrade-from-v2017_4-to-v2017_5-common.sql', 10, 1) WITH NOWAIT

IF NOT EXISTS(SELECT * FROM [dbo].[appConfigurations] WHERE [name] = 'Ignore alerts for: Error 15281 - SQL Server blocked access to procedure' and [module] = 'maintenance-plan')
	INSERT	INTO [dbo].[appConfigurations] ([module], [name], [value])
		  SELECT 'maintenance-plan' AS [module], 'Ignore alerts for: Error 15281 - SQL Server blocked access to procedure' AS [name], 'true' AS [value]
GO

