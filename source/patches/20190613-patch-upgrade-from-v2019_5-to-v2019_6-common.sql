SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2019.5 to 2019.6 (2019.06.13)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: common																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20190613-patch-upgrade-from-v2019_5-to-v2019_6-common.sql', 10, 1) WITH NOWAIT

IF NOT EXISTS(SELECT * FROM [report].[htmlOptions] WHERE [name] = N'Analyze backup size (GB) in the last days' and [module] = 'health-check' )
	INSERT	INTO [report].[htmlOptions] ([module], [name], [value], [description])
		SELECT 'health-check' AS [module], N'Analyze backup size (GB) in the last days' AS [name], '7' AS [value], 'analyze the size used by backups taken with this utility (full/diff/log) in the last X days' AS [description]
GO

IF NOT EXISTS(SELECT * FROM [report].[htmlOptions] WHERE [name] = N'Minimum Index Size (pages)' and [module] = 'health-check' )
	INSERT	INTO [report].[htmlOptions] ([module], [name], [value], [description])
		SELECT 'health-check' AS [module], N'Minimum Index Size (pages)' AS [name], '50000' AS [value], 'report only fragmented indexes having the minimum size in pages as' AS [description]
GO

IF NOT EXISTS(SELECT * FROM [report].[htmlOptions] WHERE [name] = N'Minimum Index fill-factor' and [module] = 'health-check' )
	INSERT	INTO [report].[htmlOptions] ([module], [name], [value], [description])
		SELECT 'health-check' AS [module], N'Minimum Index fill-factor' AS [name], '90' AS [value], 'report only fragmented indexes with fill-factor greater than' AS [description]
GO