SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2016.6 to 2016.9 (2016.08.25)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: commons																							   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20160825-patch-upgrade-from-v2016_6-to-v2016_9-common.sql', 10, 1) WITH NOWAIT

IF EXISTS(SELECT * FROM [dbo].[appConfigurations] WHERE [name] = 'Parallel Data Collecting Jobs' and [module] = 'common')
	UPDATE [dbo].[appConfigurations] 
		SET [name] = 'Parallel Execution Jobs' 
	WHERE [name] = 'Parallel Data Collecting Jobs'
GO
