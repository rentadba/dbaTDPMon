SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2019.11 to 2019.12 (2019.12.10)				  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: common																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20191210-patch-upgrade-from-v2019_11-to-v2019_12-common.sql', 10, 1) WITH NOWAIT

IF EXISTS (SELECT * FROM sys.indexes WHERE [object_id] = OBJECT_ID('dbo.logAnalysisMessages') AND [name] = 'IX_logAnalysisMessages_ProjecteID') 
begin
	EXEC ('DROP INDEX [IX_logAnalysisMessages_ProjecteID] ON [dbo].[logAnalysisMessages]');
end
GO

IF EXISTS(SELECT * FROM sys.foreign_keys WHERE [name]='FK_logAnalysisMessages_catalogInstanceNames' AND [parent_object_id] = OBJECT_ID('dbo.logAnalysisMessages'))
begin
	EXEC ('ALTER TABLE [dbo].[logAnalysisMessages] DROP CONSTRAINT [FK_logAnalysisMessages_catalogInstanceNames]');
end
GO

IF EXISTS(SELECT * FROM sys.foreign_keys WHERE [name]='FK_logAnalysisMessages_catalogProjects' AND [parent_object_id] = OBJECT_ID('dbo.logAnalysisMessages'))
begin
	EXEC ('ALTER TABLE [dbo].[logAnalysisMessages] DROP CONSTRAINT [FK_logAnalysisMessages_catalogProjects]');
end
GO
