SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2018.6 to 2018.7 (2018.07.11)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: common																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20180711-patch-upgrade-from-v2018_6-to-v2018_7-common.sql', 10, 1) WITH NOWAIT

IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_SCHEMA='dbo' AND CONSTRAINT_NAME='UK_catalogDatabaseNames_Name')
	ALTER TABLE [dbo].[catalogDatabaseNames] DROP CONSTRAINT [UK_catalogDatabaseNames_Name]
GO
IF NOT EXISTS(SELECT * FROM sys.indexes WHERE [name]='UK_catalogDatabaseNames_Name' AND [object_id]=OBJECT_ID('[dbo].[catalogDatabaseNames]'))
	CREATE UNIQUE INDEX [UK_catalogDatabaseNames_Name] ON [dbo].[catalogDatabaseNames]([name], [instance_id]) INCLUDE ([project_id], [active])
GO
