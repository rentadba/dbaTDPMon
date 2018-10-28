SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2018.9 to 2018.10 (2018.10.17)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: monitoring																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20181017-patch-upgrade-from-v2018_9-to-v2018_10-mon.sql', 10, 1) WITH NOWAIT

IF EXISTS (SELECT * FROM sys.indexes WHERE [object_id] = OBJECT_ID(N'monitoring.statsSQLAgentJobs') AND [name] = N'IX_statsSQLAgentJobs_InstanceID') 
	DROP INDEX [IX_statsSQLAgentJobs_InstanceID] ON [monitoring].[statsSQLAgentJobs]
GO
CREATE INDEX [IX_statsSQLAgentJobs_InstanceID] ON [monitoring].[statsSQLAgentJobs]
		([instance_id], [project_id], [job_name])
	INCLUDE
		([last_completion_time])
	ON [FG_Statistics_Index]
GO
