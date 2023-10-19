/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
*/
SET NOCOUNT ON
GO
IF NOT EXISTS(SELECT * FROM [dbo].[catalogProjects] WHERE [code] = '$(projectCode)')
	INSERT	INTO [dbo].[catalogProjects]([id], [code], [name], [description], [active], [solution_id], [is_production], [db_filter])
			SELECT 0, '$(projectCode)', '$(projectCode)', '', 1, cs.[id], 1, '%'
			FROM [dbo].[catalogSolutions] cs
			WHERE cs.[name]='Default'
GO

UPDATE [dbo].[appConfigurations] 
	SET [value] = '$(projectCode)'
WHERE	[module] = 'common'
	AND [name] = 'Default project code'
GO

UPDATE [dbo].[appConfigurations] SET [value] = [dbo].[ufn_formatPlatformSpecificPath](@@SERVERNAME, [value]) 
WHERE	[module] = 'common'
	AND [name] = 'Local storage path for HTML reports';
GO

UPDATE [dbo].[appConfigurations] SET [value] = [dbo].[ufn_formatPlatformSpecificPath](@@SERVERNAME, [value]) 
WHERE	[module] = 'common'
	AND [name] = 'Default folder for logs';
GO

UPDATE [dbo].[appConfigurations] SET [value] = [dbo].[ufn_formatPlatformSpecificPath](@@SERVERNAME, [value]) 
WHERE	[module] = 'common'
	AND [name] = 'Default backup location';
GO

EXEC dbo.usp_refreshMachineCatalogs DEFAULT, @@SERVERNAME;
GO
