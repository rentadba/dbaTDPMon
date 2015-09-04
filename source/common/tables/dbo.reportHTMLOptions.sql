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
RAISERROR('Create table: [dbo].[reportHTMLOptions]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[reportHTMLOptions]') AND type in (N'U'))
DROP TABLE [dbo].[reportHTMLOptions]
GO
CREATE TABLE [dbo].[reportHTMLOptions]
(
	[id]					[int]	 IDENTITY (1, 1)	NOT NULL,
	[module]				[varchar](32)			NOT NULL,
	[name]					[nvarchar](256)	NOT NULL,
	[value]					[sysname]		NULL,
	[description]			[nvarchar](256) NULL,
	CONSTRAINT [PK_reportHTMLOptions] PRIMARY KEY  CLUSTERED 
	(
		[id]
	) ON [PRIMARY],
	CONSTRAINT [UK_reportHTMLOptions] UNIQUE 
	(
		[name], 
		[module]
	) ON [PRIMARY],
)ON [PRIMARY]
GO

-----------------------------------------------------------------------------------------------------
RAISERROR('		...insert default data', 10, 1) WITH NOWAIT
GO
SET NOCOUNT ON
GO
INSERT	INTO [dbo].[reportHTMLOptions] ([module], [name], [value], [description])
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
		  SELECT 'health-check' AS [module], N'Errorlog Messages in last hours'					AS [name], '24'					AS [value], 'report errorlog messaged in the last hours; default 24'				    AS [description] UNION ALL
		  SELECT 'health-check' AS [module], N'Errorlog Messages Limit to Max'					AS [name], '500'				AS [value], 'limit errorlog messages to a maximum number; default 500'				    AS [description] UNION ALL
		  SELECT 'health-check' AS [module], N'Minimum Index Maintenance Frequency (days)'		AS [name], '2'					AS [value], 'interval between 2 index maintenance operations for the same HoBT; default 2' AS [description] UNION ALL
		  SELECT 'health-check' AS [module], N'Analyze Index Maintenance Operation'				AS [name], 'REBUILD'			AS [value], 'which index maintenance operation to analyze (REBUILD and/or REORGANIZE)'	AS [description] UNION ALL
		  SELECT 'health-check' AS [module], N'Analyze Only Messages from the last hours'		AS [name], '24'					AS [value], 'analyze only messages raised in the last hours; default 24'				AS [description] UNION ALL
		  SELECT 'health-check' AS [module], N'SQL Agent Job - Maximum Running Time (hours)'	AS [name], '3'					AS [value], 'maximum accepted job running time; default 3'								AS [description]
GO
