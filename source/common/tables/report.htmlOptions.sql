-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 29.12.2014
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-----------------------------------------------------------------------------------------------------
--report HTML options
-----------------------------------------------------------------------------------------------------
RAISERROR('Create table: [report].[htmlOptions]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[report].[htmlOptions]') AND type in (N'U'))
DROP TABLE [report].[htmlOptions]
GO
CREATE TABLE [report].[htmlOptions]
(
	[id]					[int]	 IDENTITY (1, 1)	NOT NULL,
	[module]				[varchar](32)			NOT NULL,
	[name]					[nvarchar](256)	NOT NULL,
	[value]					[sysname]		NULL,
	[description]			[nvarchar](256) NULL,
	CONSTRAINT [PK_htmlOptions] PRIMARY KEY  CLUSTERED 
	(
		[id]
	),
	CONSTRAINT [UK_htmlOptions] UNIQUE 
	(
		[name], 
		[module]
	)
)
GO

-----------------------------------------------------------------------------------------------------
RAISERROR('		...insert default data', 10, 1) WITH NOWAIT
GO
SET NOCOUNT ON
GO
INSERT	INTO [report].[htmlOptions] ([module], [name], [value], [description])
		  SELECT 'health-check' AS [module], N'Database online admitted state'					AS [name], 'ONLINE, READ ONLY'	AS [value], 'comma separated, default ONLINE, READ ONLY'								AS [description] UNION ALL
		  SELECT 'health-check' AS [module], N'Database max size (mb) - master'					AS [name], '32'					AS [value], 'maximum allowed size for master database; default 32'						AS [description] UNION ALL
		  SELECT 'health-check' AS [module], N'Database max size (mb) - msdb'					AS [name], '1024'				AS [value], 'maximum allowed size for msdb database; default 1024'						AS [description] UNION ALL
		  SELECT 'health-check' AS [module], N'SQL Agent Job - Failures in last hours'			AS [name], '24'					AS [value], 'report job failured in the last hours; default 24'							AS [description] UNION ALL
		  SELECT 'health-check' AS [module], N'Database Min Size for Analysis (mb)'				AS [name], '512'				AS [value], 'minimum size of the database to be analyzed; default 512'					AS [description] UNION ALL
		  SELECT 'health-check' AS [module], N'Database Max Log Size (mb)'						AS [name], '32768'				AS [value], 'maximum allowed size for log file; default 32768'							AS [description] UNION ALL
		  SELECT 'health-check' AS [module], N'Database Min Data Usage (percent)'				AS [name], '50'					AS [value], 'minimum allowed percent for data space usage; default 50'					AS [description] UNION ALL
		  SELECT 'health-check' AS [module], N'Database Max Log Usage (percent)'				AS [name], '50'					AS [value], 'maximum allowed percent for log space usage; default 50'					AS [description] UNION ALL
		  SELECT 'health-check' AS [module], N'Database Log vs. Data Size (percent)'			AS [name], '90'					AS [value], 'maximum allowed percent between log and data allocated size; default 90'	AS [description] UNION ALL
		  SELECT 'health-check' AS [module], N'User Database BACKUP Age (days)'					AS [name], '2'					AS [value], 'maximum allowed age in days for outdated backups; default 2'				AS [description] UNION ALL
		  SELECT 'health-check' AS [module], N'System Database BACKUP Age (days)'				AS [name], '7'					AS [value], 'maximum allowed age in days for outdated backups; default 7'				AS [description] UNION ALL
		  SELECT 'health-check' AS [module], N'User Database DBCC CHECKDB Age (days)'			AS [name], '14'					AS [value], 'maximum allowed age in days for outdated dbcc checkdb; default 30'			AS [description] UNION ALL
		  SELECT 'health-check' AS [module], N'System Database DBCC CHECKDB Age (days)'			AS [name], '14'					AS [value], 'maximum allowed age in days for outdated dbcc checkdb; default 30'			AS [description] UNION ALL
		  SELECT 'health-check' AS [module], N'Free Disk Space Min Percent (percent)'			AS [name], '10'					AS [value], 'minimum allowed percent for free disk space, default 10'					AS [description] UNION ALL
		  SELECT 'health-check' AS [module], N'Free Disk Space Min Space (mb)'					AS [name], '3000'				AS [value], 'minimum allowed free disk space in mb, default 3000'						AS [description] UNION ALL
		  SELECT 'health-check' AS [module], N'Errorlog Messages in last hours'					AS [name], '24'					AS [value], 'report errorlog messages in the last hours; default 24'				    AS [description] UNION ALL
		  SELECT 'health-check' AS [module], N'Errorlog Messages Limit to Max'					AS [name], '500'				AS [value], 'limit errorlog messages to a maximum number; default 500'				    AS [description] UNION ALL
		  SELECT 'health-check' AS [module], N'OS Event Messages in last hours'					AS [name], '24'					AS [value], 'report OS messages in the last hours; default 24'							AS [description] UNION ALL
		  SELECT 'health-check' AS [module], N'OS Event Messages Limit to Max'					AS [name], '500'				AS [value], 'limit os event messages to a maximum number; default 500'				    AS [description] UNION ALL
		  SELECT 'health-check' AS [module], N'Minimum Index Maintenance Frequency (days)'		AS [name], '2'					AS [value], 'interval between 2 index maintenance operations for the same HoBT; default 2' AS [description] UNION ALL
		  SELECT 'health-check' AS [module], N'Analyze Index Maintenance Operation'				AS [name], 'REBUILD'			AS [value], 'which index maintenance operation to analyze (REBUILD and/or REORGANIZE)'	AS [description] UNION ALL
		  SELECT 'health-check' AS [module], N'Analyze Only Messages from the last hours'		AS [name], '24'					AS [value], 'analyze only messages raised in the last hours; default 24'				AS [description] UNION ALL
		  SELECT 'health-check' AS [module], N'Minimum Index Size (pages)'						AS [name], '50000'				AS [value], 'report only fragmented indexes having the minimum size in pages as'		AS [description] UNION ALL
		  SELECT 'health-check' AS [module], N'Minimum Index fill-factor'						AS [name], '90'					AS [value], 'report only fragmented indexes with fill-factor greater than'				AS [description] UNION ALL
		  SELECT 'health-check' AS [module], N'SQL Agent Job - Maximum Running Time (hours)'	AS [name], '3'					AS [value], 'maximum accepted job running time; default 3'								AS [description] UNION ALL
		  SELECT 'health-check' AS [module], N'Online Instance Get Databases Size per Project'	AS [name], 'false'				AS [value], 'get only project databases size for an instance; default get all dbs'		AS [description] UNION ALL
		  SELECT 'health-check' AS [module], N'Minimum Failed Login Attempts'					AS [name], '50'					AS [value], 'minimum failed login attempts per interval to be reported'					AS [description] UNION ALL
		  SELECT 'health-check' AS [module], N'Minimum Disk space to reclaim (mb)'				AS [name], '10240'				AS [value], 'minimum disk space to reclaim when reporting data and log space available'	AS [description] UNION ALL
		  SELECT 'health-check' AS [module], N'Exclude Database Snapshots for Backup/DBCC checks'AS [name], 'true'				AS [value], 'do not check for outdated backups/dbcc for database snapshot(s)'			AS [description] UNION ALL
		  SELECT 'health-check' AS [module], N'Analyze backup size (GB) in the last days'		AS [name], '7'					AS [value], 'analyze the size used by backups taken with this utility (full/diff/log) in the last X days' AS [description] UNION ALL
		  SELECT 'health-check' AS [module], N'Analyze database(s) growth in the last days'		AS [name], '30'					AS [value], 'analyze the database growth in the last X days' AS [description] UNION ALL
		  SELECT 'health-check' AS [module], N'Minimum database(s) growth percent'				AS [name], '10'					AS [value], 'report only databases having growth in the last X days at least Y percentage' AS [description] UNION ALL
		  SELECT 'health-check' AS [module], N'Minimum database(s) growth size (mb)'			AS [name], '32768'				AS [value], 'report only databases having growth in the last X days at least Y MB' AS [description]
GO
