SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2022.02 to 2023.05 (2023.05.21)				  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: health-check																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20230521-patch-upgrade-from-v2022_02-to-v2023_05-hc.sql', 10, 1) WITH NOWAIT
IF NOT EXISTS(SELECT * FROM [report].[hardcodedFilters] WHERE [filter_pattern] ='%Buffer Pool scan took % seconds: database ID %, command ''DBCC TABLE CHECK''%')
	INSERT	INTO [report].[hardcodedFilters] ([module], [object_name], [filter_pattern], [active])
			SELECT 'health-check', 'statsErrorlogDetails', '%Buffer Pool scan took % seconds: database ID %, command ''DBCC TABLE CHECK''%', 1

IF NOT EXISTS(SELECT * FROM [report].[hardcodedFilters] WHERE [filter_pattern] ='%Parallel redo is started for database % with worker pool size%')
	INSERT	INTO [report].[hardcodedFilters] ([module], [object_name], [filter_pattern], [active])
			SELECT 'health-check', 'statsErrorlogDetails', '%Parallel redo is started for database % with worker pool size%', 1

IF NOT EXISTS(SELECT * FROM [report].[hardcodedFilters] WHERE [filter_pattern] ='%Parallel redo is shutdown for database % with worker pool size%')
	INSERT	INTO [report].[hardcodedFilters] ([module], [object_name], [filter_pattern], [active])
			SELECT 'health-check', 'statsErrorlogDetails', '%Parallel redo is shutdown for database % with worker pool size%', 1
GO


