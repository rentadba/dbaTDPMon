SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2018.7 to 2018.8 (2018.08.18)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: maintenance-plan																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20180818-patch-upgrade-from-v2018_7-to-v2018_8-mp.sql', 10, 1) WITH NOWAIT

IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='maintenance-plan' AND TABLE_NAME='objectSkipList' AND COLUMN_NAME='active')
begin
	EXEC ('ALTER TABLE [maintenance-plan].[objectSkipList] ADD [active] [bit] NULL');
	EXEC ('ALTER TABLE [maintenance-plan].[objectSkipList] ADD CONSTRAINT [DF_objectSkipList_Active] DEFAULT (1) FOR [active]');
	EXEC ('UPDATE [maintenance-plan].[objectSkipList] SET [active] = 1 WHERE [active] IS NULL');
	EXEC ('ALTER TABLE [maintenance-plan].[objectSkipList] ALTER COLUMN [active] [bit] NOT NULL');
end
GO
