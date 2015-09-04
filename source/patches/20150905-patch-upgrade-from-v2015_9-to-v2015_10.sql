USE dbaTDPMon
GO

-----------------------------------------------------------------------------------------------------
--
-----------------------------------------------------------------------------------------------------
RAISERROR('Alter table: [dbo].[appConfigurations]', 10, 1) WITH NOWAIT
GO

IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='appConfigurations' AND COLUMN_NAME='module')
	ALTER TABLE [dbo].[appConfigurations] ADD [module] [varchar](32) NULL
GO

UPDATE [dbo].[appConfigurations]
	SET [module] = 'common'
WHERE [name] IN (
					N'Default project code',
					N'Alert repeat interval (minutes)',
					N'Default lock timeout (ms)',
					N'Database Mail profile name to use for sending emails',
					N'Default recipients list - Reports (semicolon separated)',
					N'Default recipients list - Job Status (semicolon separated)',
					N'Default recipients list - Alerts (semicolon separated)',
					N'Local storage path for HTML reports',
					N'HTTP address for report files',
					N'Notify job status only for Failed jobs',
					N'Log action events',
					N'Log events retention (days)',
					N'Ignore alerts for: Error 1222 - Lock request time out period exceeded'
				)
GO

UPDATE [dbo].[appConfigurations]
	SET [module] = 'maintenance-plan'
WHERE [name] IN (
					N'Default backup location',
					N'Default backup retention (days)',
					N'Change retention policy from RetentionDays to RetentionBackupsCount',
					N'Force cleanup of ghost records',
					N'Ghost records cleanup threshold'
				)
GO

UPDATE [dbo].[appConfigurations]
	SET [module] = 'health-check',
		[name] = N'Collect SQL Agent jobs step details'
WHERE [name] =N'Collect SQL Agent jobs step details (health-check)'
GO

UPDATE [dbo].[appConfigurations]
	SET [module] = 'health-check',
		[name] = N'Collect Information OS Events'
WHERE [name] =N'Collect Information OS Events (health-check)'
GO

UPDATE [dbo].[appConfigurations]
	SET [module] = 'health-check',
		[name] = N'Collect OS Events timeout (seconds)'
WHERE [name] =N'Collect OS Events timeout (seconds) (health-check)'
GO

UPDATE [dbo].[appConfigurations]
	SET [module] = 'health-check'
WHERE [name] IN (
					N'Collect SQL Agent jobs step details',
					N'Collect Information OS Events',
					N'Collect OS Events timeout (seconds)',
					N'Collect OS Events from last hours'
				)
GO

IF NOT EXISTS(SELECT * FROM [dbo].[appConfigurations] WHERE [name] = 'Collect SQL Agent jobs step details')
	INSERT	INTO [dbo].[appConfigurations] ([module], [name], [value])
			SELECT 'health-check'		AS [module], 'Collect SQL Agent jobs step details'										AS [name], 'false'		AS [value]
GO

IF NOT EXISTS(SELECT * FROM [dbo].[appConfigurations] WHERE [name] = 'Collect Information OS Events')
INSERT	INTO [dbo].[appConfigurations] ([module], [name], [value])
		  SELECT 'health-check'		AS [module], 'Collect Information OS Events'												AS [name], 'false'		AS [value]
GO

IF NOT EXISTS(SELECT * FROM [dbo].[appConfigurations] WHERE [name] = 'Collect OS Events timeout (seconds)')
INSERT	INTO [dbo].[appConfigurations] ([module], [name], [value])
		  SELECT 'health-check'		AS [module], 'Collect OS Events timeout (seconds)'											AS [name], '600'		AS [value]
GO

IF NOT EXISTS(SELECT * FROM [dbo].[appConfigurations] WHERE [name] = 'Collect OS Events from last hours')
INSERT	INTO [dbo].[appConfigurations] ([module], [name], [value])
		  SELECT 'health-check'		AS [module], 'Collect OS Events from last hours'											AS [name], '24'			AS [value]
GO

SELECT * FROM [dbo].[appConfigurations] WHERE [module] IS NULL
GO

DELETE FROM [dbo].[appConfigurations] WHERE [module] IS NULL
GO

IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='appConfigurations' AND CONSTRAINT_NAME='IX_appConfigurations_Name')
	ALTER TABLE [dbo].[appConfigurations] DROP CONSTRAINT [IX_appConfigurations_Name]
GO

IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='appConfigurations' AND CONSTRAINT_NAME='UK_appConfigurations_Name')
ALTER TABLE [dbo].[appConfigurations] ADD CONSTRAINT [UK_appConfigurations_Name] UNIQUE  NONCLUSTERED 
	(
		[module], 
		[name]
	) ON [PRIMARY]
GO

