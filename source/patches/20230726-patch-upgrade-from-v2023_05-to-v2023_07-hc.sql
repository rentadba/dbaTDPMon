SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2022.05 to 2023.07 (2023.07.26)				  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: health-check																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20230521-patch-upgrade-from-v2023_05-to-v2023_07-hc.sql', 10, 1) WITH NOWAIT
INSERT	INTO [dbo].[appInternalTasks] ([id], [descriptor], [task_name], [flg_actions], [priority])
		SELECT S.[id], S.[descriptor], S.[task_name], S.[flg_actions], S.[priority]
		FROM (
				SELECT   16384 AS [id], 'dbo.usp_hcCollectDatabaseDetails' AS [descriptor], 'Collect Database Details' AS [task_name], NULL AS [flg_actions], 1 AS [priority] UNION ALL
				SELECT   32768, 'dbo.usp_hcCollectDiskSpaceUsage', 'Collect Disk Space Usage', NULL, 2 AS [priority] UNION ALL
				SELECT   65536, 'dbo.usp_hcCollectErrorlogMessages', 'Collect SQL Server errorlog Messages', NULL, 3 AS [priority] UNION ALL
				SELECT  131072, 'dbo.usp_hcCollectOSEventLogs', 'Collect OS Event Logs', NULL, 4 AS [priority] UNION ALL
				SELECT  262144, 'dbo.usp_hcCollectSQLServerAgentJobsStatus', 'Collect SQL Server Agent Jobs Status', NULL, 5 AS [priority] UNION ALL
				SELECT  524288, 'dbo.usp_hcCollectEventMessages', 'Collect Internal Event Messages', NULL , 6 AS [priority] UNION ALL
				SELECT 8388608, 'dbo.usp_hcCollectDatabaseGrowth', 'Collect Database Growth Events', NULL , 7 AS [priority]
			)S
		LEFT JOIN [dbo].[appInternalTasks] ait ON S.[id] = ait.[id]
		WHERE ait.[id] IS NULL
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[health-check].[statsDatabaseGrowth]') AND type in (N'U'))
	CREATE TABLE [health-check].[statsDatabaseGrowth]
	(
		[id]				[int] IDENTITY(1,1) NOT NULL,
		[instance_id]		[smallint] NOT NULL,
		[project_id]		[smallint] NOT NULL,
		[database_name]		[nvarchar](128) NOT NULL,
		[logical_name]		[nvarchar](255) NOT NULL,
		[current_size_kb]	[bigint] NULL,
		[file_type]			[nvarchar](10) NOT NULL,
		[growth_type]		[nvarchar](50) NOT NULL,
		[growth_kb]			[int] NOT NULL,
		[duration]			[int] NOT NULL,
		[start_time]		[datetime] NOT NULL,
		[end_time]			[datetime] NOT NULL,
		[session_id]		[smallint] NOT NULL,
		[login_name]		[sysname] NULL,
		[host_name]			[sysname] NULL,
		[application_name]	[sysname] NULL,
		[client_process_id]	[int] NULL
		CONSTRAINT [PK_statsDatabaseGrowth] PRIMARY KEY CLUSTERED 
		(
			[id]
		),
		CONSTRAINT [FK_statsDatabaseGrowth_catalogProjects] FOREIGN KEY 
		(
			[project_id]
		) 
		REFERENCES [dbo].[catalogProjects] 
		(
			[id]
		),
		CONSTRAINT [FK_statsDatabaseGrowth_catalogInstanceNames] FOREIGN KEY 
		(
			[instance_id],
			[project_id]
		) 
		REFERENCES [dbo].[catalogInstanceNames] 
		(
			[id],
			[project_id]
		)
	)
	GO

IF EXISTS(SELECT * FROM sys.indexes WHERE [name]='IX_statsDatabaseGrowth_ProjectID' AND [object_id]=OBJECT_ID('[health-check].[statsDatabaseGrowth]'))
	DROP INDEX [IX_statsDatabaseGrowth_ProjectID] ON [health-check].[statsDatabaseGrowth]
GO
CREATE INDEX [IX_statsDatabaseGrowth_ProjectID] ON [health-check].[statsDatabaseGrowth] ([project_id], [instance_id])
GO

IF EXISTS(SELECT * FROM sys.indexes WHERE [name]='IX_statsDatabaseGrowth_InstanceID' AND [object_id]=OBJECT_ID('[health-check].[statsDatabaseGrowth]'))
	DROP INDEX [IX_statsDatabaseGrowth_InstanceID] ON [health-check].[statsDatabaseGrowth]
GO
CREATE INDEX [IX_statsDatabaseGrowth_InstanceID] ON [health-check].[statsDatabaseGrowth]([instance_id], [project_id]) INCLUDE ([start_time])
GO
