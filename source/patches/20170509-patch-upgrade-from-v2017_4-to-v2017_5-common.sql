SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2017.4 to 2017.5 (2017.05.09)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: commons																							   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20170509-patch-upgrade-from-v2017_4-to-v2017_5-common.sql', 10, 1) WITH NOWAIT

IF NOT EXISTS(SELECT * FROM [dbo].[appConfigurations] WHERE [name] = 'Flood control: maximum alerts in 5 minutes' and [module] = 'common')
	INSERT	INTO [dbo].[appConfigurations] ([module], [name], [value])
		  SELECT 'common' AS [module], 'Flood control: maximum alerts in 5 minutes' AS [name], '50' AS [value]
GO
