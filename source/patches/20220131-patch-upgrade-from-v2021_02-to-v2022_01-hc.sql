SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2021.01 to 2022.01 (2022.01.31)				  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: health-check																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20220131-patch-upgrade-from-v2021_02-to-v2022_01-hc.sql', 10, 1) WITH NOWAIT
IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='health-check' AND TABLE_NAME='statsDatabaseAlwaysOnDetails' AND COLUMN_NAME='replica_join_state_desc')
	begin
		EXEC ('ALTER TABLE [health-check].[statsDatabaseAlwaysOnDetails] ADD [replica_join_state_desc] [nvarchar](60) NULL');
	end
GO
IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='health-check' AND TABLE_NAME='statsDatabaseAlwaysOnDetails' AND COLUMN_NAME='replica_connected_state_desc')
	begin
		EXEC ('ALTER TABLE [health-check].[statsDatabaseAlwaysOnDetails] ADD [replica_connected_state_desc] [nvarchar](60) NULL');
	end
GO
IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='health-check' AND TABLE_NAME='statsDatabaseAlwaysOnDetails' AND COLUMN_NAME='failover_mode_desc')
	begin
		EXEC ('ALTER TABLE [health-check].[statsDatabaseAlwaysOnDetails] ADD [failover_mode_desc] [nvarchar](60) NULL');
	end
GO
IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='health-check' AND TABLE_NAME='statsDatabaseAlwaysOnDetails' AND COLUMN_NAME='availability_mode_desc')
	begin
		EXEC ('ALTER TABLE [health-check].[statsDatabaseAlwaysOnDetails] ADD [availability_mode_desc] [nvarchar](60) NULL');
	end
GO
IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='health-check' AND TABLE_NAME='statsDatabaseAlwaysOnDetails' AND COLUMN_NAME='suspend_reason_desc')
	begin
		EXEC ('ALTER TABLE [health-check].[statsDatabaseAlwaysOnDetails] ADD [suspend_reason_desc] [nvarchar](60) NULL');
	end
GO
