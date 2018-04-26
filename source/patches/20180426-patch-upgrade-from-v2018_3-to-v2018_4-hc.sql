SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2018.3 to 2018.4 (2018.04.26)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: health-check																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20180426-patch-upgrade-from-v2018_3-to-v2018_4-hc.sql', 10, 1) WITH NOWAIT

IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_SCHEMA='health-check' AND CONSTRAINT_NAME='PK_statsDatabaseUsageHistory')
	ALTER TABLE [health-check].[statsDatabaseUsageHistory] DROP CONSTRAINT [PK_statsDatabaseUsageHistory]
GO
IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_SCHEMA='health-check' AND CONSTRAINT_NAME='FK_statsDatabaseUsageHistory_catalogDatabaseNames')
	ALTER TABLE [health-check].[statsDatabaseUsageHistory] DROP CONSTRAINT [FK_statsDatabaseUsageHistory_catalogDatabaseNames]
GO
IF EXISTS(SELECT * FROM sys.indexes WHERE [name]='IX_statsDatabaseUsageHistory_CatalogDatabaseID' AND [object_id]=OBJECT_ID('[health-check].[statsDatabaseUsageHistory]'))
	DROP INDEX [IX_statsDatabaseUsageHistory_CatalogDatabaseID] ON [health-check].[statsDatabaseUsageHistory]
GO

IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_SCHEMA='health-check' AND CONSTRAINT_NAME='PK_statsDatabaseDetails')
	ALTER TABLE [health-check].[statsDatabaseDetails] DROP CONSTRAINT [PK_statsDatabaseDetails]
GO
IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_SCHEMA='health-check' AND CONSTRAINT_NAME='FK_statsDatabaseDetails_catalogDatabaseNames')
	ALTER TABLE [health-check].[statsDatabaseDetails] DROP CONSTRAINT [FK_statsDatabaseDetails_catalogDatabaseNames]
GO
IF EXISTS(SELECT * FROM sys.indexes WHERE [name]='IX_statsDatabaseDetails_CatalogDatabaseID' AND [object_id]=OBJECT_ID('[health-check].[statsDatabaseDetails]'))
	DROP INDEX [IX_statsDatabaseDetails_CatalogDatabaseID] ON [health-check].[statsDatabaseDetails]
GO

IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_SCHEMA='health-check' AND CONSTRAINT_NAME='PK_statsDatabaseAlwaysOnDetails')
	ALTER TABLE [health-check].[statsDatabaseAlwaysOnDetails] DROP CONSTRAINT [PK_statsDatabaseAlwaysOnDetails]
GO
IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_SCHEMA='health-check' AND CONSTRAINT_NAME='FK_statsDatabaseAlwaysOnDetails_catalogDatabaseNames')
	ALTER TABLE [health-check].[statsDatabaseAlwaysOnDetails] DROP CONSTRAINT [FK_statsDatabaseAlwaysOnDetails_catalogDatabaseNames] 
GO
IF EXISTS(SELECT * FROM sys.indexes WHERE [name]='IX_statsDatabaseAlwaysOnDetails_CatalogDatabaseID' AND [object_id]=OBJECT_ID('[health-check].[statsDatabaseAlwaysOnDetails]'))
	DROP INDEX [IX_statsDatabaseAlwaysOnDetails_CatalogDatabaseID] ON [health-check].[statsDatabaseAlwaysOnDetails]
GO

IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_SCHEMA='dbo' AND CONSTRAINT_NAME='PK_catalogDatabaseNames')
	ALTER TABLE [dbo].[catalogDatabaseNames] DROP CONSTRAINT [PK_catalogDatabaseNames]
GO
IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='catalogDatabaseNames' AND COLUMN_NAME='id' AND DATA_TYPE='int')
	ALTER TABLE [dbo].[catalogDatabaseNames] ALTER COLUMN [id] [int] NOT NULL
GO
IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_SCHEMA='dbo' AND CONSTRAINT_NAME='PK_catalogDatabaseNames')
	ALTER TABLE [dbo].[catalogDatabaseNames] ADD 
			CONSTRAINT [PK_catalogDatabaseNames] PRIMARY KEY  CLUSTERED 
			(
				[id],
				[instance_id]
			) ON [PRIMARY]
GO

IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='health-check' AND TABLE_NAME='statsDatabaseUsageHistory' AND COLUMN_NAME='catalog_database_id' AND DATA_TYPE='int')
	ALTER TABLE [health-check].[statsDatabaseUsageHistory] ALTER COLUMN [catalog_database_id] [int] NOT NULL
GO
IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_SCHEMA='health-check' AND CONSTRAINT_NAME='PK_statsDatabaseUsageHistory')
	ALTER TABLE [health-check].[statsDatabaseUsageHistory] ADD 
		CONSTRAINT [PK_statsDatabaseUsageHistory] PRIMARY KEY  CLUSTERED 
		(
			[id],
			[catalog_database_id]
		) ON [FG_Statistics_Data]
GO
IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_SCHEMA='health-check' AND CONSTRAINT_NAME='FK_statsDatabaseUsageHistory_catalogDatabaseNames')
	ALTER TABLE [health-check].[statsDatabaseUsageHistory] ADD 
		CONSTRAINT [FK_statsDatabaseUsageHistory_catalogDatabaseNames] FOREIGN KEY 
		(
			  [catalog_database_id]
			, [instance_id]
		) 
		REFERENCES [dbo].[catalogDatabaseNames] 
		(
			  [id]
			, [instance_id]
		)
GO
IF NOT EXISTS(SELECT * FROM sys.indexes WHERE [name]='IX_statsDatabaseUsageHistory_CatalogDatabaseID' AND [object_id]=OBJECT_ID('[health-check].[statsDatabaseUsageHistory]'))
	CREATE INDEX [IX_statsDatabaseUsageHistory_CatalogDatabaseID] ON [health-check].[statsDatabaseUsageHistory]( [catalog_database_id], [instance_id]) ON [FG_Statistics_Index]
GO

IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='health-check' AND TABLE_NAME='statsDatabaseDetails' AND COLUMN_NAME='catalog_database_id' AND DATA_TYPE='int')
	ALTER TABLE [health-check].[statsDatabaseDetails] ALTER COLUMN [catalog_database_id] [int] NOT NULL
GO
IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_SCHEMA='health-check' AND CONSTRAINT_NAME='PK_statsDatabaseDetails')
	ALTER TABLE [health-check].[statsDatabaseDetails] ADD 
		CONSTRAINT [PK_statsDatabaseDetails] PRIMARY KEY  CLUSTERED 
		(
			[id],
			[catalog_database_id]
		) ON [FG_Statistics_Data]
GO
IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_SCHEMA='health-check' AND CONSTRAINT_NAME='FK_statsDatabaseDetails_catalogDatabaseNames')
	ALTER TABLE [health-check].[statsDatabaseDetails] ADD 
			CONSTRAINT [FK_statsDatabaseDetails_catalogDatabaseNames] FOREIGN KEY 
			(
				  [catalog_database_id]
				, [instance_id]
			) 
			REFERENCES [dbo].[catalogDatabaseNames] 
			(
				  [id]
				, [instance_id]
			)
GO
IF NOT EXISTS(SELECT * FROM sys.indexes WHERE [name]='IX_statsDatabaseDetails_CatalogDatabaseID' AND [object_id]=OBJECT_ID('[health-check].[statsDatabaseDetails]'))
	CREATE INDEX [IX_statsDatabaseDetails_CatalogDatabaseID] ON [health-check].[statsDatabaseDetails]( [catalog_database_id], [instance_id]) ON [FG_Statistics_Index]
GO


IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='health-check' AND TABLE_NAME='statsDatabaseAlwaysOnDetails' AND COLUMN_NAME='catalog_database_id' AND DATA_TYPE='int')
	ALTER TABLE [health-check].[statsDatabaseAlwaysOnDetails] ALTER COLUMN [catalog_database_id] [int] NOT NULL
GO
IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_SCHEMA='health-check' AND CONSTRAINT_NAME='PK_statsDatabaseAlwaysOnDetails')
	ALTER TABLE [health-check].[statsDatabaseAlwaysOnDetails]  ADD	
		CONSTRAINT [PK_statsDatabaseAlwaysOnDetails] PRIMARY KEY  CLUSTERED 
		(
			[id],
			[catalog_database_id]
		) ON [FG_Statistics_Data]
GO
IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_SCHEMA='health-check' AND CONSTRAINT_NAME='FK_statsDatabaseAlwaysOnDetails_catalogDatabaseNames')
	ALTER TABLE [health-check].[statsDatabaseAlwaysOnDetails]
		ADD CONSTRAINT [FK_statsDatabaseAlwaysOnDetails_catalogDatabaseNames] FOREIGN KEY 
		(
			  [catalog_database_id]
			, [instance_id]
		) 
		REFERENCES [dbo].[catalogDatabaseNames] 
		(
			  [id]
			, [instance_id]
		)
GO
IF NOT EXISTS(SELECT * FROM sys.indexes WHERE [name]='IX_statsDatabaseAlwaysOnDetails_CatalogDatabaseID' AND [object_id]=OBJECT_ID('[health-check].[statsDatabaseAlwaysOnDetails]'))
	CREATE INDEX [IX_statsDatabaseAlwaysOnDetails_CatalogDatabaseID] ON [health-check].[statsDatabaseAlwaysOnDetails] ([catalog_database_id], [instance_id]) ON [FG_Statistics_Index]
GO

