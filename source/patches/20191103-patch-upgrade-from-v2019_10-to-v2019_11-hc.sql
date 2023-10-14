SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2019.10 to 2019.11 (2019.11.03)				  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: health-check																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20191103-patch-upgrade-from-v2019_10-to-v2019_11-hc.sql', 10, 1) WITH NOWAIT

IF NOT EXISTS(SELECT * FROM [report].[htmlOptions] WHERE [name] = N'Analyze database(s) growth in the last days' and [module] = 'health-check' )
	INSERT	INTO [report].[htmlOptions] ([module], [name], [value], [description])
			SELECT 'health-check' AS [module], N'Analyze database(s) growth in the last days' AS [name], '30' AS [value], 'analyze the database growth in the last X days' AS [description]
GO

IF NOT EXISTS(SELECT * FROM [report].[htmlOptions] WHERE [name] = N'Analyze backup size (GB) in the last days' and [module] = 'health-check' )
	INSERT	INTO [report].[htmlOptions] ([module], [name], [value], [description])
			SELECT 'health-check' AS [module], N'Analyze backup size (GB) in the last days' AS [name], '7' AS [value], 'analyze the size used by backups taken with this utility (full/diff/log) in the last X days' AS [description]
GO

IF NOT EXISTS(SELECT * FROM [report].[htmlOptions] WHERE [name] = N'Minimum database(s) growth percent' and [module] = 'health-check' )
	INSERT	INTO [report].[htmlOptions] ([module], [name], [value], [description])
			SELECT 'health-check' AS [module], N'Minimum database(s) growth percent' AS [name], '10' AS [value], 'report only databases having growth in the last X days at least Y percentage' AS [description]
GO

IF NOT EXISTS(SELECT * FROM [report].[htmlOptions] WHERE [name] = N'Minimum database(s) growth size (mb)' and [module] = 'health-check' )
	INSERT	INTO [report].[htmlOptions] ([module], [name], [value], [description])
			SELECT 'health-check' AS [module], N'Minimum database(s) growth size (mb)' AS [name], '32768' AS [value], 'report only databases having growth in the last X days at least Y MB' AS [description]
GO


