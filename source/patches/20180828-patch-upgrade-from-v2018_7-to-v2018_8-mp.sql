SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2018.7 to 2018.8 (2018.08.28)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: maintenance-plan																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20180828-patch-upgrade-from-v2018_7-to-v2018_8-mp.sql', 10, 1) WITH NOWAIT

UPDATE [dbo].[appInternalTasks]
	SET [is_resource_intensive] = 1
WHERE [task_name] IN ('Allocation Consistency Check')
	AND [is_resource_intensive] = 0
GO
