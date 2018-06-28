

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
/* patch module: health-check																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20180628-patch-upgrade-from-v2018_5-to-v2018_6-hc.sql', 10, 1) WITH NOWAIT


IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='health-check' AND TABLE_NAME='statsErrorlogDetails' AND COLUMN_NAME='log_date_utc' AND DATA_TYPE='datetime')
	ALTER TABLE [health-check].[statsErrorlogDetails] ADD [log_date_utc] [datetime] NULL
GO

IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='health-check' AND TABLE_NAME='statsSQLAgentJobsHistory' AND COLUMN_NAME='last_execution_utc' AND DATA_TYPE='datetime')
	ALTER TABLE [health-check].[statsSQLAgentJobsHistory] ADD [last_execution_utc] [datetime] NULL
GO

IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='health-check' AND TABLE_NAME='statsOSEventLogs' AND COLUMN_NAME='time_created_utc' AND DATA_TYPE='datetime')
	ALTER TABLE [health-check].[statsOSEventLogs] ADD [time_created_utc] [datetime] NULL
GO


ALTER TABLE [report].[htmlContent] ALTER COLUMN [project_id] [smallint]	NULL
ALTER TABLE [report].[htmlContent] ALTER COLUMN [flg_actions] [int]	NULL
ALTER TABLE [report].[htmlContent] ALTER COLUMN [flg_options] [int]	NULL
