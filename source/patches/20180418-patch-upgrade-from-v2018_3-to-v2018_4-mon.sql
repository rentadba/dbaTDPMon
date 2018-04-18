SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2018.3 to 2018.4 (2018.04.18)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: monitoring																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20180418-patch-upgrade-from-v2018_3-to-v2018_4-mon.sql', 10, 1) WITH NOWAIT

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[monitoring].[alertAdditionalRecipients]') AND type in (N'U'))
	begin
		RAISERROR('	Create table: [monitoring].[alertAdditionalRecipients]', 10, 1) WITH NOWAIT
		CREATE TABLE [monitoring].[alertAdditionalRecipients]
		(
			[id]						[int] IDENTITY (1, 1) NOT NULL,
			[instance_id]				[smallint]		NOT NULL,
			[project_id]				[smallint]		NOT NULL,
			[event_name]				[sysname]		NOT NULL,
			[object_name]				[sysname]		NULL,
			[recipients]				[nvarchar](256)	NULL,
			[active]					[bit]			NOT NULL CONSTRAINT [DF_Monitoring_alertAdditionalRecipients_Active] DEFAULT (1),
			CONSTRAINT [PK_Monitoring_alertAdditionalRecipients] PRIMARY KEY CLUSTERED 
			(
				[id]
			)  ON [FG_Statistics_Data],
			CONSTRAINT [FK_Monitoring_alertAdditionalRecipients_catalogInstanceNames] FOREIGN KEY 
			(
				[instance_id],
				[project_id]
			) 
			REFERENCES [dbo].[catalogInstanceNames] 
			(
				[id],
				[project_id]
			)
		) ON [FG_Statistics_Data];

		RAISERROR('		...insert default data', 10, 1) WITH NOWAIT
		INSERT	INTO [monitoring].[alertAdditionalRecipients]([project_id], [instance_id], [event_name], [object_name], [active])
				SELECT DISTINCT cin.[project_id], cin.[id] AS [instance_id], ev.[event_name], ev.[object_name], ev.[active]
				FROM 
					(	
						SELECT TOP 1 [project_id], [id]
						FROM [dbo].[catalogInstanceNames]
						WHERE [name] = @@SERVERNAME
						ORDER BY [project_id]
					) cin
				INNER JOIN
					(
						SELECT 'running transaction' AS [event_name], '<database_name>' AS [object_name], 0 AS [active] UNION ALL
						SELECT 'uncommitted transaction' AS [event_name], '<database_name>' AS [object_name], 0 AS [active] UNION ALL
						SELECT 'blocked transaction' AS [event_name], '<database_name>' AS [object_name], 0 AS [active] UNION ALL
						SELECT 'long session request' AS [event_name], '<database_name>' AS [object_name], 0 AS [active] UNION ALL
						SELECT 'tempdb space' AS [event_name], NULL AS [object_name], 0 AS [active] UNION ALL
						SELECT 'replication latency' AS [event_name], 'Publication: <publication_name> - Subscriber:<subscriber_server>.<subscriber_db>' AS [object_name], 0 AS [active] UNION ALL
						SELECT 'subscription marked inactive' AS [event_name] , 'Publication: <publication_name> - Subscriber:<subscriber_server>.<subscriber_db>' AS [object_name], 0 AS [active] UNION ALL
						SELECT 'subscription not active' AS [event_name], 'Publication: <publication_name> - Subscriber:<subscriber_server>.<subscriber_db>' AS [object_name], 0 AS [active] UNION ALL
						SELECT 'low disk space', '<mount_point>' AS [object_name], 0 AS [active]
					)ev ON 1=1;
	end

IF NOT EXISTS(SELECT * FROM sys.indexes WHERE [name]='IX_Monitoring_alertAdditionalRecipients_ProjectID' AND [object_id]=OBJECT_ID('[monitoring].[alertAdditionalRecipients]'))
	CREATE INDEX [IX_Monitoring_alertAdditionalRecipients_ProjectID] ON [monitoring].[alertAdditionalRecipients] ([instance_id], [project_id]) ON [FG_Statistics_Index]
GO
IF NOT EXISTS(SELECT * FROM sys.indexes WHERE [name]='IX_Monitoring_alertAdditionalRecipients_PublicationName' AND [object_id]=OBJECT_ID('[monitoring].[alertAdditionalRecipients]'))
	CREATE INDEX [IX_Monitoring_alertAdditionalRecipients_PublicationName] ON [monitoring].[alertAdditionalRecipients]([event_name], [active], [instance_id], [project_id], [object_name]) ON [FG_Statistics_Index]
GO
GO
