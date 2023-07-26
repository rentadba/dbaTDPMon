SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under MIT licence model			  *
*-----------------------------------------------------------------------------*
* Patch script: from version 2022.02 to 2023.05 (2023.05.15)				  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: maintenance-plan																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20230515-patch-upgrade-from-v2022_02-to-v2023_05-mp.sql', 10, 1) WITH NOWAIT

IF EXISTS(SELECT * FROM sys.indexes WHERE [name]='IX_jobExecutionQueue_Descriptor' AND [object_id]=OBJECT_ID('[dbo].[jobExecutionQueue]'))
	DROP INDEX [IX_jobExecutionQueue_Descriptor] ON [dbo].[jobExecutionQueue]
GO
CREATE INDEX [IX_jobExecutionQueue_Descriptor] ON [dbo].[jobExecutionQueue]([descriptor], [module], [project_id], [task_id], [status]) INCLUDE ([instance_id], [for_instance_id], [job_name], [execution_date]) 
GO
