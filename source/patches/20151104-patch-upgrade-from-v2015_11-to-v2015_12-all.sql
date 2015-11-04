USE [dbaTDPMon]
GO

SELECT * FROM [dbo].[appConfigurations] WHERE [module] = 'common' AND [name] = 'Application Version'
GO
UPDATE [dbo].[appConfigurations] SET [value] = N'2015.11.04' WHERE [module] = 'common' AND [name] = 'Application Version'
GO

ALTER TABLE [report].[htmlSkipRules] DROP CONSTRAINT [UK_htmlSkipRules_Name]
GO
ALTER TABLE [report].[htmlSkipRules] ADD
	CONSTRAINT [UK_htmlSkipRules_Name] UNIQUE  NONCLUSTERED 
	(
		[module],
		[rule_id],
		[skip_value],
		[skip_value2]
	)
GO

SELECT * FROM [dbo].[appConfigurations] WHERE [module] = 'common' AND [name] = 'Application Version'
GO
