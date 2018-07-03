SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2018.6 to 2018.7 (2018.07.03)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: health-check																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20180703-patch-upgrade-from-v2018_6-to-v2018_7-hc.sql', 10, 1) WITH NOWAIT


IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='health-check' AND TABLE_NAME='statsDatabaseDetails' AND COLUMN_NAME='volume_mount_point')
	ALTER TABLE [health-check].[statsDatabaseDetails] ADD [volume_mount_point] [nvarchar](512) NULL
GO
IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='health-check' AND TABLE_NAME='statsDatabaseDetails' AND COLUMN_NAME='volume_mount_point')
	AND EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='health-check' AND TABLE_NAME='statsDatabaseDetails' AND COLUMN_NAME='physical_drives')
		EXEC ('UPDATE [health-check].[statsDatabaseDetails] SET [volume_mount_point] = [physical_drives]')
GO
IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='health-check' AND TABLE_NAME='statsDatabaseDetails' AND COLUMN_NAME='physical_drives')
	ALTER TABLE [health-check].[statsDatabaseDetails] DROP COLUMN [physical_drives]
GO

IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='health-check' AND TABLE_NAME='statsDatabaseUsageHistory' AND COLUMN_NAME='volume_mount_point')
	ALTER TABLE [health-check].[statsDatabaseUsageHistory] ADD [volume_mount_point] [nvarchar](512) NULL
GO
IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='health-check' AND TABLE_NAME='statsDatabaseUsageHistory' AND COLUMN_NAME='volume_mount_point')
	AND EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='health-check' AND TABLE_NAME='statsDatabaseUsageHistory' AND COLUMN_NAME='physical_drives')
		EXEC ('UPDATE [health-check].[statsDatabaseUsageHistory] SET [volume_mount_point] = [physical_drives]')
GO
IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='health-check' AND TABLE_NAME='statsDatabaseUsageHistory' AND COLUMN_NAME='physical_drives')
	ALTER TABLE [health-check].[statsDatabaseUsageHistory] DROP COLUMN [physical_drives]
GO
