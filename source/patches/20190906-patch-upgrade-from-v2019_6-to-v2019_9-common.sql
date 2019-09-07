SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2019.6 to 2019.9 (2019.09.06)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: common																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20190906-patch-upgrade-from-v2019_6-to-v2019_9-common.sql', 10, 1) WITH NOWAIT
IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='catalogInstanceNames' AND COLUMN_NAME='engine')
	begin
		EXEC ('ALTER TABLE [dbo].[catalogInstanceNames] ADD [engine] [int] NULL');
	end
GO
