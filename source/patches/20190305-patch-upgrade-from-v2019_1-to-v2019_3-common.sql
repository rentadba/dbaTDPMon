SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2019.1 to 2019.3 (2019.03.05)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: common																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20190305-patch-upgrade-from-v2019_1-to-v2019_3-common.sql', 10, 1) WITH NOWAIT

IF NOT EXISTS(SELECT * FROM [report].[hardcodedFilters] WHERE [filter_pattern] = '%Process ID % was killed by hostname %, host process ID %.%' and [module] = 'health-check' and [object_name] = 'statsErrorlogDetails')
	INSERT	INTO [report].[hardcodedFilters]([module], [object_name], [filter_pattern], [active])
			SELECT 'health-check', 'statsErrorlogDetails', '%Process ID % was killed by hostname %, host process ID %.%', 1
GO
IF NOT EXISTS(SELECT * FROM [report].[hardcodedFilters] WHERE [filter_pattern] = '%Synchronize Database % with Resource Database.%' and [module] = 'health-check' and [object_name] = 'statsErrorlogDetails')
	INSERT	INTO [report].[hardcodedFilters]([module], [object_name], [filter_pattern], [active])
			SELECT 'health-check', 'statsErrorlogDetails', '%Synchronize Database % with Resource Database.%', 1
GO
IF NOT EXISTS(SELECT * FROM [report].[hardcodedFilters] WHERE [filter_pattern] = '%Restore is complete on database %. The database is now available.%' and [module] = 'health-check' and [object_name] = 'statsErrorlogDetails')
	INSERT	INTO [report].[hardcodedFilters]([module], [object_name], [filter_pattern], [active])
			SELECT 'health-check', 'statsErrorlogDetails', '%Restore is complete on database %. The database is now available.%', 1
GO
IF NOT EXISTS(SELECT * FROM [report].[hardcodedFilters] WHERE [filter_pattern] = '%AppDomain % created.%' and [module] = 'health-check' and [object_name] = 'statsErrorlogDetails')
	INSERT	INTO [report].[hardcodedFilters]([module], [object_name], [filter_pattern], [active])
			SELECT 'health-check', 'statsErrorlogDetails', '%AppDomain % created.%', 1
GO
IF NOT EXISTS(SELECT * FROM [report].[hardcodedFilters] WHERE [filter_pattern] = '%AppDomain % is marked for unload due to common language runtime (CLR) or security data definition language (DDL) operations.%' and [module] = 'health-check' and [object_name] = 'statsErrorlogDetails')
	INSERT	INTO [report].[hardcodedFilters]([module], [object_name], [filter_pattern], [active])
			SELECT 'health-check', 'statsErrorlogDetails', '%AppDomain % is marked for unload due to common language runtime (CLR) or security data definition language (DDL) operations.%', 1
GO

IF NOT EXISTS(SELECT * FROM [report].[htmlSkipRules] WHERE [rule_name] = 'Failed Login Attempts - Issues Detected' and [module] = 'health-check' and [rule_id] = 536870912)
	INSERT	INTO [report].[htmlSkipRules] ([module], [rule_id], [rule_name], [skip_value], [active])
			SELECT 'health-check', 536870912, 'Failed Login Attempts - Issues Detected', NULL, 0
GO

IF NOT EXISTS(SELECT * FROM [report].[htmlOptions] WHERE [name] = N'Minimum Failed Login Attempts' and [module] = 'health-check' )
	INSERT	INTO [report].[htmlOptions] ([module], [name], [value], [description])
			  SELECT 'health-check' AS [module], N'Minimum Failed Login Attempts' AS [name], '50' AS [value], 'minimum failed login attempts per interval to be reported' AS [description]
GO