SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2018.1 to 2018.3 (2018.03.01)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: maintenance-plan																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20180301-patch-upgrade-from-v2018_1-to-v2018_3-mp.sql', 10, 1) WITH NOWAIT

UPDATE [maintenance-plan].[internalScheduler] SET [active] = 0 WHERE [task_id] IN (256, 512)
GO 