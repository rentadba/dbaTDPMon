/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2018.5 to 2018.6 (2018.06.05)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: common																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20180605-patch-upgrade-from-v2018_5-to-v2018_6-common.sql', 10, 1) WITH NOWAIT

IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='jobExecutionQueue' AND COLUMN_NAME='priority')
begin
	EXEC ('ALTER TABLE [dbo].[jobExecutionQueue] ADD [priority] [bit] NULL');
	EXEC ('ALTER TABLE [dbo].[jobExecutionQueue] ADD CONSTRAINT [DF_jobExecutionQueue_Priority] DEFAULT (1) FOR [priority]');
	EXEC ('UPDATE [dbo].[jobExecutionQueue] SET [priority] = 1 WHERE [priority] IS NULL');
	EXEC ('ALTER TABLE [dbo].[jobExecutionQueue] ALTER COLUMN [priority] [bit] NOT NULL');
end
GO
