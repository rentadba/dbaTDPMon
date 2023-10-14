SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2023.07 to 2023.10 (2023.10.12)				  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: common																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20231012-patch-upgrade-from-v2023_07-to-v2023_10-common.sql', 10, 1) WITH NOWAIT

IF EXISTS(SELECT * FROM sys.indexes WHERE [name]='IX_jobExecutionHistory' AND [object_id]=OBJECT_ID('[dbo].[jobExecutionHistory]'))
	DROP INDEX [IX_jobExecutionHistory] ON [dbo].[jobExecutionHistory]
GO
CREATE INDEX [IX_jobExecutionHistory] ON [dbo].[jobExecutionHistory]([module], [for_instance_id], [project_id], [instance_id], [job_name], [job_step_name], [filter]);
GO

IF NOT EXISTS(SELECT * FROM sys.foreign_keys WHERE [parent_object_id] = OBJECT_ID('dbo.jobExecutionHistory') AND [name] = 'FK_jobExecutionHistory_InstanceID_catalogInstanceNames')
	begin
		ALTER TABLE [dbo].[jobExecutionHistory] WITH NOCHECK
			ADD CONSTRAINT [FK_jobExecutionHistory_InstanceID_catalogInstanceNames] 
			FOREIGN KEY ([instance_id], [project_id]) 
			REFERENCES [dbo].[catalogInstanceNames] ([id], [project_id]);
		ALTER TABLE [dbo].[jobExecutionHistory] WITH CHECK CHECK CONSTRAINT [FK_jobExecutionHistory_InstanceID_catalogInstanceNames];
	end
GO

IF NOT EXISTS(SELECT * FROM[report].[htmlSkipRules] WHERE [module] = 'health-check' AND [rule_id] = 1073741824)
	INSERT INTO [report].[htmlSkipRules] ([module], [rule_id], [rule_name], [skip_value], [skip_value2], [active]) 
	VALUES (N'health-check', 1073741824, N'Databases(s) Growth - Issues Detected', NULL, NULL, 0)
GO
