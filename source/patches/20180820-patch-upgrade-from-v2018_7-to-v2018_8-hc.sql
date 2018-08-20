SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2018.7 to 2018.8 (2018.08.20)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: health-check																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20180820-patch-upgrade-from-v2018_7-to-v2018_8-hc.sql', 10, 1) WITH NOWAIT

IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='health-check' AND TABLE_NAME='statsDatabaseAlwaysOnDetails' AND COLUMN_NAME='data_loss_sec')
	ALTER TABLE [health-check].[statsDatabaseAlwaysOnDetails] DROP COLUMN [data_loss_sec]
GO


