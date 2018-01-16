SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*/
DECLARE @appVersion [sysname]
SELECT @appVersion = [value] FROM [dbo].[appConfigurations] WHERE [module] = 'common' AND [name] = 'Application Version'
PRINT 'Detected dbaTDPMon version: ' + @appVersion
GO
