USE [dbaTDPMon]
GO

SELECT * FROM [dbo].[appConfigurations] WHERE [module] = 'common' AND [name] = 'Application Version'
GO
UPDATE [dbo].[appConfigurations] SET [value] = N'2015.11.09' WHERE [module] = 'common' AND [name] = 'Application Version'
GO

IF EXISTS(SELECT * FROM sys.indexes WHERE [object_id] = OBJECT_ID('dbo.jobExecutionQueue') AND [name] = 'UK_jobExecutionQueue' AND [is_unique_constraint]=1)
	ALTER TABLE [dbo].[jobExecutionQueue] DROP CONSTRAINT [UK_jobExecutionQueue]
GO
ALTER TABLE [dbo].[jobExecutionQueue] ADD
	CONSTRAINT [UK_jobExecutionQueue] UNIQUE
		(
			[module],
			[for_instance_id],
			[project_id],
			[instance_id],
			[job_name],
			[job_step_name],
			[filter]
		)
GO

SELECT * FROM [dbo].[appConfigurations] WHERE [module] = 'common' AND [name] = 'Application Version'
GO
