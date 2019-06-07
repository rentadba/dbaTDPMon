SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2019.5 to 2019.6 (2019.06.07)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: health-check																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20190607-patch-upgrade-from-v2019_5-to-v2019_6-hc.sql', 10, 1) WITH NOWAIT


IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='health-check' AND TABLE_NAME='statsDatabaseDetails' AND COLUMN_NAME='is_snapshot')
begin
	EXEC ('ALTER TABLE [health-check].[statsDatabaseDetails] ADD [is_snapshot] [bit] NULL');
	EXEC ('ALTER TABLE [health-check].[statsDatabaseDetails] ADD CONSTRAINT [DF_health_check_statsDatabaseDetails_IsSnapshot] DEFAULT (0) FOR [is_snapshot]');
	EXEC ('UPDATE [health-check].[statsDatabaseDetails] SET [is_snapshot] = 0 WHERE [is_snapshot] IS NULL');
	EXEC ('ALTER TABLE [health-check].[statsDatabaseDetails] ALTER COLUMN [is_snapshot] [bit] NOT NULL');
end
GO

IF NOT EXISTS(SELECT * FROM [report].[htmlOptions] WHERE [name] = N'Exclude Database Snapshots for Backup/DBCC checks' and [module] = 'health-check' )
	INSERT	INTO [report].[htmlOptions] ([module], [name], [value], [description])
			  SELECT 'health-check' AS [module], N'Exclude Database Snapshots for Backup/DBCC checks'AS [name], 'true'	AS [value], 'do not check for outdated backups/dbcc for database snapshot(s)' AS [description]
GO
