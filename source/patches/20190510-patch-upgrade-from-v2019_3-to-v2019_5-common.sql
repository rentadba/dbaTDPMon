SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2019.3 to 2019.5 (2019.05.10)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: common																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20190510-patch-upgrade-from-v2019_3-to-v2019_5-common.sql', 10, 1) WITH NOWAIT

IF NOT EXISTS(SELECT * FROM [report].[htmlOptions] WHERE [name] = N'Minimum Disk space to reclaim (mb)' and [module] = 'health-check' )
	INSERT	INTO [report].[htmlOptions] ([module], [name], [value], [description])
			  SELECT 'health-check' AS [module], N'Minimum Disk space to reclaim (mb)' AS [name], '10240' AS [value], 'minimum disk space to reclaim when reporting data and log space available' AS [description]
GO