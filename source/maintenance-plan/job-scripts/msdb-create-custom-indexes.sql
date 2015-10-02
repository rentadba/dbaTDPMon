-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 01.10.2015
-- Module			 : SQL Server 2000/2005/2008/2008R2/2012/2014+
-- Description		 : add indexes to msdb database in order to improve system maintenance execution times
-------------------------------------------------------------------------------
-- Change date		 : 
-- Description		 : 
-------------------------------------------------------------------------------
RAISERROR('Create additional indexes on msdb database, if required...', 10, 1) WITH NOWAIT
GO
USE [msdb]
GO
--  backupset
IF NOT EXISTS(	SELECT *
				FROM [msdb]..sysindexes si
				INNER JOIN [msdb]..sysindexkeys sik ON si.[id] = sik.[id] AND si.[indid] = sik.[indid] 
				INNER JOIN [msdb]..syscolumns sc ON sik.[id] = sc.[id] AND sik.[colid] = sc.[colid]
				WHERE si.[id] = OBJECT_ID('dbo.backupset')
						AND sc.[name] = 'backup_set_uuid'
						and sik.[keyno] = 1
				)
	AND NOT EXISTS(SELECT * FROM [msdb]..sysindexes si WHERE [name]='IX_backupset_backup_set_uuid' AND [id]=OBJECT_ID('dbo.backupset'))
begin   
	RAISERROR('--Creating index => [IX_backupset_backup_set_uuid] ON [dbo].[backupset]', 10, 1) WITH NOWAIT
    CREATE INDEX [IX_backupset_backup_set_uuid] ON [dbo].[backupset]([backup_set_uuid])
end 

IF NOT EXISTS(	SELECT *
				FROM [msdb]..sysindexes si
				INNER JOIN [msdb]..sysindexkeys sik ON si.[id] = sik.[id] AND si.[indid] = sik.[indid] 
				INNER JOIN [msdb]..syscolumns sc ON sik.[id] = sc.[id] AND sik.[colid] = sc.[colid]
				WHERE si.[id] = OBJECT_ID('dbo.backupset')
						AND sc.[name] = 'media_set_id'
						and sik.[keyno] = 1
				)
	AND NOT EXISTS(SELECT * FROM [msdb]..sysindexes si WHERE [name]='IX_backupset_media_set_id' AND [id]=OBJECT_ID('dbo.backupset'))
begin   
	RAISERROR('--Creating index => [IX_backupset_media_set_id] ON [dbo].[backupset]', 10, 1) WITH NOWAIT
    CREATE INDEX [IX_backupset_media_set_id] ON [dbo].[backupset]([media_set_id])
end

IF NOT EXISTS(	SELECT *
				FROM [msdb]..sysindexes si
				INNER JOIN [msdb]..sysindexkeys sik ON si.[id] = sik.[id] AND si.[indid] = sik.[indid] 
				INNER JOIN [msdb]..syscolumns sc ON sik.[id] = sc.[id] AND sik.[colid] = sc.[colid]
				WHERE si.[id] = OBJECT_ID('dbo.backupset')
						AND sc.[name] = 'backup_finish_date'
						and sik.[keyno] = 1
				)
	AND NOT EXISTS(SELECT * FROM [msdb]..sysindexes si WHERE [name]='IX_backupset_backup_finish_date' AND [id]=OBJECT_ID('dbo.backupset'))
begin  
	RAISERROR('--Creating index => [IX_backupset_backup_finish_date] ON [dbo].[backupset]', 10, 1) WITH NOWAIT
    CREATE INDEX [IX_backupset_backup_finish_date] ON [dbo].[backupset]([backup_finish_date])
end 

IF NOT EXISTS(	SELECT *
				FROM [msdb]..sysindexes si
				INNER JOIN [msdb]..sysindexkeys sik ON si.[id] = sik.[id] AND si.[indid] = sik.[indid] 
				INNER JOIN [msdb]..syscolumns sc ON sik.[id] = sc.[id] AND sik.[colid] = sc.[colid]
				WHERE si.[id] = OBJECT_ID('dbo.backupset')
						AND sc.[name] = 'backup_start_date'
						and sik.[keyno] = 1
				)
	AND NOT EXISTS(SELECT * FROM [msdb]..sysindexes si WHERE [name]='IX_backupset_backup_start_date' AND [id]=OBJECT_ID('dbo.backupset'))
begin
	RAISERROR('--Creating index => [IX_backupset_backup_start_date] ON [dbo].[backupset]', 10, 1) WITH NOWAIT
    CREATE INDEX [IX_backupset_backup_start_date] ON [dbo].[backupset]([backup_start_date])
end


--  backupfile
IF NOT EXISTS(	SELECT *
				FROM [msdb]..sysindexes si
				INNER JOIN [msdb]..sysindexkeys sik ON si.[id] = sik.[id] AND si.[indid] = sik.[indid] 
				INNER JOIN [msdb]..syscolumns sc ON sik.[id] = sc.[id] AND sik.[colid] = sc.[colid]
				WHERE si.[id] = OBJECT_ID('dbo.backupfile')
						AND sc.[name] = 'backup_set_id'
						and sik.[keyno] = 1
				)
	AND NOT EXISTS(SELECT * FROM [msdb]..sysindexes si WHERE [name]='IX_backupfile_backup_set_id' AND [id]=OBJECT_ID('dbo.backupfile'))
begin
	RAISERROR('--Creating index => [IX_backupfile_backup_set_id] ON [dbo].[backupfile]', 10, 1) WITH NOWAIT
    CREATE INDEX [IX_backupfile_backup_set_id] ON [dbo].[backupfile]([backup_set_id])
end

--  backupmediafamily
IF NOT EXISTS(	SELECT *
				FROM [msdb]..sysindexes si
				INNER JOIN [msdb]..sysindexkeys sik ON si.[id] = sik.[id] AND si.[indid] = sik.[indid] 
				INNER JOIN [msdb]..syscolumns sc ON sik.[id] = sc.[id] AND sik.[colid] = sc.[colid]
				WHERE si.[id] = OBJECT_ID('dbo.backupmediafamily')
						AND sc.[name] = 'media_set_id'
						and sik.[keyno] = 1
				)
	AND NOT EXISTS(SELECT * FROM [msdb]..sysindexes si WHERE [name]='IX_backupmediafamily_media_set_id' AND [id]=OBJECT_ID('dbo.backupmediafamily'))
begin
	RAISERROR('--Creating index => [IX_backupmediafamily_media_set_id] ON [dbo].[backupmediafamily]', 10, 1) WITH NOWAIT
    CREATE INDEX [IX_backupmediafamily_media_set_id] ON [dbo].[backupmediafamily]([media_set_id])
end

--  backupfilegroup
IF NOT EXISTS(	SELECT *
				FROM [msdb]..sysindexes si
				INNER JOIN [msdb]..sysindexkeys sik ON si.[id] = sik.[id] AND si.[indid] = sik.[indid] 
				INNER JOIN [msdb]..syscolumns sc ON sik.[id] = sc.[id] AND sik.[colid] = sc.[colid]
				WHERE si.[id] = OBJECT_ID('dbo.backupfilegroup')
						AND sc.[name] = 'backup_set_id'
						and sik.[keyno] = 1
				)
	AND NOT EXISTS(SELECT * FROM [msdb]..sysindexes si WHERE [name]='IX_backupfilegroup_backup_set_id' AND [id]=OBJECT_ID('dbo.backupfilegroup'))
begin
    RAISERROR('--Creating index => [IX_backupfilegroup_backup_set_id] ON [dbo].[backupfilegroup]', 10, 1) WITH NOWAIT
	CREATE INDEX [IX_backupfilegroup_backup_set_id] ON [dbo].[backupfilegroup]([backup_set_id])
end

--  restorehistory
IF NOT EXISTS(	SELECT *
				FROM [msdb]..sysindexes si
				INNER JOIN [msdb]..sysindexkeys sik ON si.[id] = sik.[id] AND si.[indid] = sik.[indid] 
				INNER JOIN [msdb]..syscolumns sc ON sik.[id] = sc.[id] AND sik.[colid] = sc.[colid]
				WHERE si.[id] = OBJECT_ID('dbo.restorehistory')
						AND sc.[name] = 'restore_history_id'
						and sik.[keyno] = 1
				)
	AND NOT EXISTS(SELECT * FROM [msdb]..sysindexes si WHERE [name]='IX_restorehistory_restore_history_id' AND [id]=OBJECT_ID('dbo.restorehistory'))
begin
	RAISERROR('--Creating index => [IX_restorehistory_restore_history_id] ON [dbo].[restorehistory]', 10, 1) WITH NOWAIT
    CREATE INDEX [IX_restorehistory_restore_history_id] ON [dbo].[restorehistory]([restore_history_id])
end

IF NOT EXISTS(	SELECT *
				FROM [msdb]..sysindexes si
				INNER JOIN [msdb]..sysindexkeys sik ON si.[id] = sik.[id] AND si.[indid] = sik.[indid] 
				INNER JOIN [msdb]..syscolumns sc ON sik.[id] = sc.[id] AND sik.[colid] = sc.[colid]
				WHERE si.[id] = OBJECT_ID('dbo.restorehistory')
						AND sc.[name] = 'backup_set_id'
						and sik.[keyno] = 1
				)
	AND NOT EXISTS(SELECT * FROM [msdb]..sysindexes si WHERE [name]='IX_restorehistory_backup_set_id' AND [id]=OBJECT_ID('dbo.restorehistory'))
begin
	RAISERROR('--Creating index => [IX_restorehistory_backup_set_id] ON [dbo].[restorehistory]', 10, 1) WITH NOWAIT
    CREATE INDEX [IX_restorehistory_backup_set_id] ON [dbo].[restorehistory]([backup_set_id])
end

--  restorefile
IF NOT EXISTS(	SELECT *
				FROM [msdb]..sysindexes si
				INNER JOIN [msdb]..sysindexkeys sik ON si.[id] = sik.[id] AND si.[indid] = sik.[indid] 
				INNER JOIN [msdb]..syscolumns sc ON sik.[id] = sc.[id] AND sik.[colid] = sc.[colid]
				WHERE si.[id] = OBJECT_ID('dbo.restorefile')
						AND sc.[name] = 'restore_history_id'
						and sik.[keyno] = 1
				)
	AND NOT EXISTS(SELECT * FROM [msdb]..sysindexes si WHERE [name]='IX_restorefile_restore_history_id' AND [id]=OBJECT_ID('dbo.restorefile'))
begin
	RAISERROR('--Creating index => [IX_restorefile_restore_history_id] ON [dbo].[restorefile]', 10, 1) WITH NOWAIT
    CREATE INDEX [IX_restorefile_restore_history_id] ON [dbo].[restorefile]([restore_history_id])
end

--  restorefilegroup
IF NOT EXISTS(	SELECT *
				FROM [msdb]..sysindexes si
				INNER JOIN [msdb]..sysindexkeys sik ON si.[id] = sik.[id] AND si.[indid] = sik.[indid] 
				INNER JOIN [msdb]..syscolumns sc ON sik.[id] = sc.[id] AND sik.[colid] = sc.[colid]
				WHERE si.[id] = OBJECT_ID('dbo.restorefilegroup')
						AND sc.[name] = 'restore_history_id'
						and sik.[keyno] = 1
				)
	AND NOT EXISTS(SELECT * FROM [msdb]..sysindexes si WHERE [name]='IX_restorefilegroup_restore_history_id' AND [id]=OBJECT_ID('dbo.restorefilegroup'))
begin
	RAISERROR('--Creating index => [IX_restorefilegroup_restore_history_id] ON [dbo].[restorefilegroup]', 10, 1) WITH NOWAIT
    CREATE INDEX [IX_restorefilegroup_restore_history_id] ON [dbo].[restorefilegroup]([restore_history_id])
end
