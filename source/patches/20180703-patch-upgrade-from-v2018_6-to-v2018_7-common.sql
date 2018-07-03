SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2018.6 to 2018.7 (2018.07.03)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: common																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20180703-patch-upgrade-from-v2018_6-to-v2018_7-common.sql', 10, 1) WITH NOWAIT

IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='appInternalTasks' AND COLUMN_NAME='is_resource_intensive')
begin
	EXEC ('ALTER TABLE [dbo].[appInternalTasks] ADD [is_resource_intensive] [bit] NULL');
	EXEC ('ALTER TABLE [dbo].[appInternalTasks] ADD CONSTRAINT [DF_appInternalTasks_flg_resource_intensive] DEFAULT (0) FOR [is_resource_intensive]');
	EXEC ('UPDATE [dbo].[appInternalTasks] SET [is_resource_intensive] = 0 WHERE [is_resource_intensive] IS NULL');
	EXEC ('ALTER TABLE [dbo].[appInternalTasks] ALTER COLUMN [is_resource_intensive] [bit] NOT NULL');
	EXEC ('UPDATE [dbo].[appInternalTasks]
				SET [is_resource_intensive] = 1
			WHERE [task_name] IN (  ''Database Consistency Check''
								  , ''Tables Consistency Check''
								  , ''Reference Consistency Check''
								  , ''Perform Correction to Space Usage''
								  , ''Rebuild Heap Tables''
								  , ''User Databases (diff)''
								  , ''User Databases (full)''
								 )');
end
GO
