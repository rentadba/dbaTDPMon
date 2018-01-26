SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2017.4 to 2017.6 (2017.05.26)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: health-check																						   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20170526-patch-upgrade-from-v2017_4-to-v2017_6-hc.sql', 10, 1) WITH NOWAIT

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[health-check].[statsDatabaseAlwaysOnDetails]') AND type in (N'U'))
	begin
		RAISERROR('	Create table: [health-check].[statsDatabaseAlwaysOnDetails]', 10, 1) WITH NOWAIT
				
		CREATE TABLE [health-check].[statsDatabaseAlwaysOnDetails]
		(
			[id]							[int]	 IDENTITY (1, 1)	NOT NULL,
			[catalog_database_id]			[smallint]		NOT NULL,
			[instance_id]					[smallint]		NOT NULL,
			[cluster_name]					[sysname]		NOT NULL,
			[ag_name]						[sysname]		NOT NULL,
			[role_desc]						[nvarchar](60)	NULL,
			[synchronization_health_desc]	[nvarchar](60)	NULL,
			[synchronization_state_desc]	[nvarchar](60)	NULL,
			[data_loss_sec]					[int]			NULL,
			[event_date_utc]				[datetime]		NOT NULL,
			CONSTRAINT [PK_statsDatabaseAlwaysOnDetails] PRIMARY KEY  CLUSTERED 
			(
				[id],
				[catalog_database_id]
			) ON [FG_Statistics_Data],
			CONSTRAINT [FK_statsDatabaseAlwaysOnDetails_catalogDatabaseNames] FOREIGN KEY 
			(
				  [catalog_database_id]
				, [instance_id]
			) 
			REFERENCES [dbo].[catalogDatabaseNames] 
			(
				  [id]
				, [instance_id]
			)
		)ON [FG_Statistics_Data];

		CREATE INDEX [IX_statsDatabaseAlwaysOnDetails_CatalogDatabaseID] ON [health-check].[statsDatabaseAlwaysOnDetails] ([catalog_database_id], [instance_id]) ON [FG_Statistics_Index];
		CREATE INDEX [IX_statsDatabaseAlwaysOnDetails_InstanceID] ON [health-check].[statsDatabaseAlwaysOnDetails] ([instance_id]) ON [FG_Statistics_Index];
	end
GO
