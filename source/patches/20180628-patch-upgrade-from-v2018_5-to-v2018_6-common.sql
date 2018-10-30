SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2018.5 to 2018.6 (2018.06.28)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: common																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20180628-patch-upgrade-from-v2018_5-to-v2018_6-common.sql', 10, 1) WITH NOWAIT

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE [object_id] = OBJECT_ID(N'dbo.jobExecutionQueue') AND [name] = N'IX_jobExecutionQueue_JobQueue') 
	CREATE INDEX [IX_jobExecutionQueue_JobQueue] ON [dbo].[jobExecutionQueue]
			([for_instance_id], [project_id], [instance_id], [job_name], [module], [descriptor], [job_step_name], [job_database_name]) 
		INCLUDE
			([status], [event_date_utc], [priority])
		ON [FG_Statistics_Index]
	GO

DECLARE	  @serverEdition			[sysname]
		, @serverVersionStr			[sysname]
		, @serverVersionNum			[numeric](9,6)

-----------------------------------------------------------------------------------------
--get destination server running version/edition
EXEC [dbo].[usp_getSQLServerVersion]	@sqlServerName			= @@SERVERNAME,
										@serverEdition			= @serverEdition OUT,
										@serverVersionStr		= @serverVersionStr OUT,
										@serverVersionNum		= @serverVersionNum OUT,
										@executionLevel			= 0,
										@debugMode				= 0

IF @serverVersionNum >=10
	begin
		EXEC ('IF EXISTS (SELECT * FROM sys.tables WHERE [object_id] = OBJECT_ID(''dbo.jobExecutionQueue'') AND [lock_escalation_desc]=''TABLE'')
				ALTER TABLE [dbo].[jobExecutionQueue] SET (LOCK_ESCALATION = DISABLE)')
	end
GO

IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='report' AND TABLE_NAME='htmlContent' AND COLUMN_NAME='project_id' AND IS_NULLABLE='YES')
	ALTER TABLE [report].[htmlContent] ALTER COLUMN [project_id] [smallint]	NULL
GO
IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='report' AND TABLE_NAME='htmlContent' AND COLUMN_NAME='flg_actions' AND IS_NULLABLE='YES')
	ALTER TABLE [report].[htmlContent] ALTER COLUMN [flg_actions] [int]	NULL
GO
IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='report' AND TABLE_NAME='htmlContent' AND COLUMN_NAME='flg_options' AND IS_NULLABLE='YES')
	ALTER TABLE [report].[htmlContent] ALTER COLUMN [flg_options] [int]	NULL
GO




