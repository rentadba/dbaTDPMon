SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2017.12 to 2018.1 (2018.01.31)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: commons																							   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20180131-patch-upgrade-from-v2017_12-to-v2018_1-common.sql', 10, 1) WITH NOWAIT

IF EXISTS(SELECT * FROM [report].[htmlGraphics] WHERE [name] = 'Logo' and [reference_url] IS NULL)
	UPDATE [report].[htmlGraphics]
		SET [reference_url] = 'https://github.com/rentadba/dbaTDPMon'
	WHERE [name] = 'Logo'
GO
