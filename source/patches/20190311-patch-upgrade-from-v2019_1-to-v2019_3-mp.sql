SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2019.1 to 2019.3 (2019.03.11)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: maintenance-plan																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20190311-patch-upgrade-from-v2019_1-to-v2019_3-mp.sql', 10, 1) WITH NOWAIT


IF EXISTS(SELECT * FROM sys.indexes WHERE [name] = 'IX_MaintenancePlan_objectSkipList_DatabaseName_TaskID_ObjectType' AND [object_id]=OBJECT_ID('[maintenance-plan].[objectSkipList]'))
	DROP INDEX [IX_MaintenancePlan_objectSkipList_DatabaseName_TaskID_ObjectType] ON [maintenance-plan].[objectSkipList]
GO
