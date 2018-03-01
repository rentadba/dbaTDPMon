SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2018.1 to 2018.3 (2018.03.01)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: maintenance-plan																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20180301-patch-upgrade-from-v2018_1-to-v2018_3-mp.sql', 10, 1) WITH NOWAIT

UPDATE [maintenance-plan].[internalScheduler] SET [active] = 0 WHERE [task_id] IN (256, 512)
GO 

DECLARE   @queryToRun	[nvarchar](max)
		, @job_id		[uniqueidentifier]

SELECT @job_id = [job_id]
FROM msdb.dbo.sysjobs 
WHERE [name]  = (DB_NAME() + ' - Database Maintenance - System DBs')

SET @queryToRun=N'DECLARE @databaseName [sysname]
/* only once a week on Monday */
IF DATENAME(weekday, GETDATE()) = ''Monday''
	begin
		DECLARE crsDatabases CURSOR LOCAL FAST_FORWARD FOR	SELECT [name] 
									FROM master.dbo.sysdatabases
									WHERE [name] IN (''master'', ''msdb'', ''distribution'')
											AND [status] <> 0
											AND CASE WHEN [status] & 32 = 32 THEN ''LOADING''
													 WHEN [status] & 64 = 64 THEN ''PRE RECOVERY''
													 WHEN [status] & 128 = 128 THEN ''RECOVERING''
													 WHEN [status] & 256 = 256 THEN ''NOT RECOVERED''
													 WHEN [status] & 512 = 512 THEN ''OFFLINE''
													 WHEN [status] & 2097152 = 2097152 THEN ''STANDBY''
													 WHEN [status] & 1024 = 1024 THEN ''READ ONLY''
													 WHEN [status] & 2048 = 2048 THEN ''DBO USE ONLY''
													 WHEN [status] & 4096 = 4096 THEN ''SINGLE USER''
													 WHEN [status] & 32768 = 32768 THEN ''EMERGENCY MODE''
													 WHEN [status] & 4194584 = 4194584 THEN ''SUSPECT''
													 ELSE ''ONLINE''
												END = ''ONLINE''
		OPEN crsDatabases
		FETCH NEXT FROM crsDatabases INTO @databaseName
		WHILE @@FETCH_STATUS=0
			begin
				EXEC [dbo].[usp_mpDatabaseShrink]	@sqlServerName		= @@SERVERNAME,
													@dbName				= @databaseName,
													@flgActions			= 2,	
													@flgOptions			= 1,
													@executionLevel		= DEFAULT,
													@debugMode			= DEFAULT
				
				FETCH NEXT FROM crsDatabases INTO @databaseName
			end
		CLOSE crsDatabases
		DEALLOCATE crsDatabases
	end
	'

IF @job_id IS NOT NULL AND EXISTS (	SELECT * FROM msdb..sysjobsteps 
									WHERE	[job_id] = @job_id AND [step_id] = 15
											AND REPLACE(REPLACE(REPLACE([command], '	', ' '), '  ', ' '), CHAR(13), '') <> REPLACE(REPLACE(REPLACE(@queryToRun, '	', ' '), '  ', ' '), CHAR(13), '')
									)
	begin
		RAISERROR('	update job: Database Maintenance - System DBs (step 15)', 10, 1) WITH NOWAIT

		EXEC msdb.dbo.sp_update_jobstep   @job_id = @job_id
										, @step_id = 15
										, @command = @queryToRun
	end
GO

DECLARE   @queryToRun	[nvarchar](max)
		, @job_id		[uniqueidentifier]

SELECT @job_id = [job_id]
FROM msdb.dbo.sysjobs 
WHERE [name]  = (DB_NAME() + ' - Database Maintenance - System DBs')

SET @queryToRun=N'DECLARE @databaseName [sysname]
/* on the first Saturday of the month */
IF DATENAME(weekday, GETDATE()) = ''Saturday'' AND DATEPART(dd, GETDATE())<=7
	begin
		DECLARE crsDatabases CURSOR LOCAL FAST_FORWARD FOR	SELECT [name] 
									FROM master.dbo.sysdatabases
									WHERE [name] IN (''master'', ''msdb'', ''tempdb'', ''distribution'')
											AND [status] <> 0
											AND CASE WHEN [status] & 32 = 32 THEN ''LOADING''
													 WHEN [status] & 64 = 64 THEN ''PRE RECOVERY''
													 WHEN [status] & 128 = 128 THEN ''RECOVERING''
													 WHEN [status] & 256 = 256 THEN ''NOT RECOVERED''
													 WHEN [status] & 512 = 512 THEN ''OFFLINE''
													 WHEN [status] & 2097152 = 2097152 THEN ''STANDBY''
													 WHEN [status] & 1024 = 1024 THEN ''READ ONLY''
													 WHEN [status] & 2048 = 2048 THEN ''DBO USE ONLY''
													 WHEN [status] & 4096 = 4096 THEN ''SINGLE USER''
													 WHEN [status] & 32768 = 32768 THEN ''EMERGENCY MODE''
													 WHEN [status] & 4194584 = 4194584 THEN ''SUSPECT''
													 ELSE ''ONLINE''
												END = ''ONLINE''
		OPEN crsDatabases
		FETCH NEXT FROM crsDatabases INTO @databaseName
		WHILE @@FETCH_STATUS=0
			begin
				EXEC [dbo].[usp_mpDatabaseShrink]	@sqlServerName		= @@SERVERNAME,
													@dbName				= @databaseName,
													@flgActions			= 1,	
													@flgOptions			= 0,
													@executionLevel		= DEFAULT,
													@debugMode			= DEFAULT
				
				FETCH NEXT FROM crsDatabases INTO @databaseName
			end
		CLOSE crsDatabases
		DEALLOCATE crsDatabases
	end
	'

IF @job_id IS NOT NULL AND EXISTS (	SELECT * FROM msdb..sysjobsteps 
									WHERE	[job_id] = @job_id AND [step_id] = 15
											AND REPLACE(REPLACE(REPLACE([command], '	', ' '), '  ', ' '), CHAR(13), '') <> REPLACE(REPLACE(REPLACE(@queryToRun, '	', ' '), '  ', ' '), CHAR(13), '')
									)
	begin
		RAISERROR('	update job: Database Maintenance - System DBs (step 16)', 10, 1) WITH NOWAIT

		EXEC msdb.dbo.sp_update_jobstep   @job_id = @job_id
										, @step_id = 16
										, @command = @queryToRun
	end
GO