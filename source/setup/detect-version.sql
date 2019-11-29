SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*/
DECLARE @appVersion [sysname]
SELECT @appVersion = [value] FROM [dbo].[appConfigurations] WHERE [module] = 'common' AND [name] = 'Application Version'
PRINT 'Detected dbaTDPMon version: ' + @appVersion

IF CONVERT([datetime], @appVersion, 102) < '2019-07-12'
	RAISERROR('ERROR: You must upgrade to dbaTDPMon version 2019.7 before upgrading to last version.', 16, 1) WITH NOWAIT
GO
