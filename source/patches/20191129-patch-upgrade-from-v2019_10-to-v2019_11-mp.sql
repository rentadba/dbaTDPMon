SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2019.10 to 2019.11 (2019.11.29)				  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: maintenance-plan																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20191129-patch-upgrade-from-v2019_10-to-v2019_11-mp.sql', 10, 1) WITH NOWAIT

IF NOT EXISTS(SELECT * FROM [dbo].[appConfigurations] WHERE [name] = 'Do not create SQL Agent jobs for non-readable secondary replicas (AlwaysOn)' and [module] = 'maintenance-plan')
	INSERT	INTO [dbo].[appConfigurations] ([module], [name], [value])
			SELECT 'maintenance-plan' AS [module], 'Do not create SQL Agent jobs for non-readable secondary replicas (AlwaysOn)' AS [name], 'true' AS [value]
GO
