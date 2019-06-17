SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2019.5 to 2019.6 (2019.06.17)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: health-check																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20190617-patch-upgrade-from-v2019_5-to-v2019_6-hc.sql', 10, 1) WITH NOWAIT


IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='health-check' AND TABLE_NAME='statsDatabaseAlwaysOnDetails' AND COLUMN_NAME='readable_secondary_replica')
begin
	EXEC ('ALTER TABLE [health-check].[statsDatabaseAlwaysOnDetails] ADD [readable_secondary_replica] [nvarchar](60) NULL');
end
GO
