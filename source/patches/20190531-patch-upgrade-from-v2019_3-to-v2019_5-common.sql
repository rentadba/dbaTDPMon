SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2019.3 to 2019.5 (2019.05.31)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: common																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20190531-patch-upgrade-from-v2019_3-to-v2019_5-common.sql', 10, 1) WITH NOWAIT

IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='jobExecutionQueue' AND COLUMN_NAME='job_id')
begin
	EXEC ('ALTER TABLE [dbo].[jobExecutionQueue] ADD [job_id] [uniqueidentifier] NULL');
end

IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='jobExecutionHistory' AND COLUMN_NAME='job_id')
begin
	EXEC ('ALTER TABLE [dbo].[jobExecutionHistory] ADD [job_id] [uniqueidentifier] NULL');
end
GO
