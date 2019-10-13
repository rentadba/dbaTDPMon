SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2019.9 to 2019.10 (2019.10.13)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: monitoring																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20191013-patch-upgrade-from-v2019_9-to-v2019_10-mon.sql', 10, 1) WITH NOWAIT
IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='monitoring' AND TABLE_NAME='statsTransactionsStatus' AND COLUMN_NAME='request_id')
	begin
		EXEC ('ALTER TABLE [monitoring].[statsTransactionsStatus] ADD [request_id] [int] NULL');
	end
GO
IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='monitoring' AND TABLE_NAME='statsTransactionsStatus' AND COLUMN_NAME='sql_text')
	begin
		EXEC ('ALTER TABLE [monitoring].[statsTransactionsStatus] ADD [sql_text] [nvarchar](max) NULL');
	end
GO
