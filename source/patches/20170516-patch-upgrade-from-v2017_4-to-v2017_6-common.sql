SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2017.4 to 2017.6 (2017.05.16)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: commons																							   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20170516-patch-upgrade-from-v2017_4-to-v2017_6-common.sql', 10, 1) WITH NOWAIT

IF NOT EXISTS(SELECT * FROM [dbo].[appConfigurations] WHERE [name] = 'WAIT_AT_LOW_PRIORITY max duration (min)' and [module] = 'maintenance-plan')
	INSERT	INTO [dbo].[appConfigurations] ([module], [name], [value])
		  SELECT 'maintenance-plan' AS [module], 'WAIT_AT_LOW_PRIORITY max duration (min)' AS [name], '1' AS [value]
GO
