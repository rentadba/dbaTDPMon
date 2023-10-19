SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2023.07 to 2023.10 (2023.10.15)				  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: health-check																						   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20231015-patch-upgrade-from-v2023_07-to-v2023_10-hc.sql', 10, 1) WITH NOWAIT

IF EXISTS (SELECT * FROM sys.indexes WHERE [object_id] = OBJECT_ID('health-check.statsDatabaseGrowth') AND [name] = 'IX_statsDatabaseGrowth_InstanceID') 
	DROP INDEX [IX_statsDatabaseGrowth_InstanceID] ON [health-check].[statsDatabaseGrowth];
CREATE INDEX [IX_statsDatabaseGrowth_InstanceID] ON [health-check].[statsDatabaseGrowth] ([instance_id], [project_id], [database_name], [logical_name], [start_time]);
GO
