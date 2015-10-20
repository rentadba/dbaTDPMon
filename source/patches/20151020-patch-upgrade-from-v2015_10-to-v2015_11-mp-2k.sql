USE [dbaTDPMon]
GO

UPDATE [dbo].[appConfigurations] SET [value] = N'2015.10.20' WHERE [module] = 'common' AND [name] = 'Application Version'
GO

IF NOT EXISTS(SELECT * FROM [dbo].[appConfigurations] WHERE [name] = 'Fail master job if any queued job fails' AND [module] = 'common')
	INSERT	INTO [dbo].[appConfigurations] ([module], [name], [value])
			SELECT 'common'		AS [module], 'Fail master job if any queued job fails'	AS [name], 'false' AS [value]
GO

UPDATE [dbo].[appConfigurations] SET [module]='common'
WHERE	[name] IN ('Parallel Data Collecting Jobs', 'Maximum number of retries at failed job', 'Fail master job if any queued job fails')
		AND [module] = 'health-check'
GO
