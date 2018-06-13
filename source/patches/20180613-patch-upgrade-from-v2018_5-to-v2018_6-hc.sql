SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2018.5 to 2018.6 (2018.06.13)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: health-check																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20180613-patch-upgrade-from-v2018_5-to-v2018_6-hc.sql', 10, 1) WITH NOWAIT

IF NOT EXISTS(SELECT * FROM [report].[htmlOptions] WHERE [name] = 'OS Event Messages in last hours' and [module] = 'health-check')
	INSERT	INTO [report].[htmlOptions] ([module], [name], [value], [description])
		  SELECT 'health-check' AS [module], N'OS Event Messages in last hours'	AS [name], '24'	AS [value], 'report OS messages in the last hours; default 24' AS [description]
GO

IF NOT EXISTS(SELECT * FROM [report].[htmlOptions] WHERE [name] = 'Online Instance Get Databases Size per Projects' and [module] = 'health-check')
	INSERT	INTO [report].[htmlOptions] ([module], [name], [value], [description])
		  SELECT 'health-check' AS [module], N'Online Instance Get Databases Size per Project'	AS [name], 'false'	AS [value], 'get only project databases size for an instance; default get all dbs' AS [description]
GO
