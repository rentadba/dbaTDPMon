SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2018.12 to 2019.01 (2019.01.09)				  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: common																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20190109-patch-upgrade-from-v2018_12-to-v2019_01-common.sql', 10, 1) WITH NOWAIT

IF NOT EXISTS(SELECT * FROM [dbo].[appConfigurations] WHERE [name] = 'SMART default changes threshold' and [module] = 'maintenance-plan')
	INSERT	INTO [dbo].[appConfigurations] ([module], [name], [value])
			SELECT 'maintenance-plan' AS [module], 'SMART default changes threshold'	AS [name], '50' AS [value]
GO
