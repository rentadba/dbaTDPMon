SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2017.4 to 2017.6 (2017.05.20)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: maintenance-plan																					   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20170520-patch-upgrade-from-v2017_4-to-v2017_6-mp.sql', 10, 1) WITH NOWAIT

DECLARE   @queryToRun	[nvarchar](max)
		, @job_id		[uniqueidentifier]

SELECT @job_id = [job_id]
FROM msdb.dbo.sysjobs 
WHERE [name]  = (DB_NAME() + ' - Database Maintenance - User DBs - Parallel')

SET @queryToRun=N'EXEC [dbo].[usp_mpJobQueueCreate]	@projectCode		= DEFAULT,
													@module				= ''maintenance-plan'',
													@sqlServerNameFilter = @@SERVERNAME,
													@jobDescriptor		=''dbo.usp_mpDatabaseConsistencyCheck;dbo.usp_mpDatabaseOptimize;dbo.usp_mpDatabaseShrink'',
													@flgActions			= DEFAULT,
													@recreateMode		= DEFAULT,
													@debugMode			= DEFAULT'

IF @job_id IS NOT NULL AND EXISTS (	SELECT * FROM msdb..sysjobsteps 
									WHERE	[job_id] = @job_id AND [step_id] = 1 
											AND REPLACE(REPLACE(REPLACE([command], '	', ' '), '  ', ' '), CHAR(13), '') <> REPLACE(REPLACE(REPLACE(@queryToRun, '	', ' '), '  ', ' '), CHAR(13), '')
									)
	begin
		RAISERROR('	update job: Database Maintenance - User DBs - Parallel', 10, 1) WITH NOWAIT

		EXEC msdb.dbo.sp_update_jobstep   @job_id = @job_id
										, @step_id = 1
										, @command = @queryToRun
	end
GO


DECLARE   @queryToRun	[nvarchar](max)
		, @job_id		[uniqueidentifier]

SELECT @job_id = [job_id]
FROM msdb.dbo.sysjobs 
WHERE [name] = (DB_NAME() + ' - Database Backup - Log - Parallel')

SET @queryToRun=N'EXEC [dbo].[usp_mpJobQueueCreate]	@projectCode		= DEFAULT,
													@module				= ''maintenance-plan'',
													@sqlServerNameFilter= @@SERVERNAME,
													@jobDescriptor		=''dbo.usp_mpDatabaseBackup(Log)'',
													@flgActions			= DEFAULT,
													@recreateMode		= DEFAULT,
													@debugMode			= DEFAULT'

IF @job_id IS NOT NULL AND EXISTS (	SELECT * FROM msdb..sysjobsteps 
									WHERE	[job_id] = @job_id AND [step_id] = 1 
											AND REPLACE(REPLACE(REPLACE([command], '	', ' '), '  ', ' '), CHAR(13), '') <> REPLACE(REPLACE(REPLACE(@queryToRun, '	', ' '), '  ', ' '), CHAR(13), '')
									)
	begin
		RAISERROR('	update job: Database Backup - Log - Parallel', 10, 1) WITH NOWAIT
		
		EXEC msdb.dbo.sp_update_jobstep   @job_id = @job_id
										, @step_id = 1
										, @command = @queryToRun
	end
GO


GO
DECLARE   @queryToRun	[nvarchar](max)
		, @job_id		[uniqueidentifier]

SELECT @job_id = [job_id]
FROM msdb.dbo.sysjobs 
WHERE [name]  = (DB_NAME() + ' - Database Backup - Full and Diff - Parallel')

SET @queryToRun=N'EXEC [dbo].[usp_mpJobQueueCreate]	@projectCode		= DEFAULT,
													@module				= ''maintenance-plan'',
													@sqlServerNameFilter= @@SERVERNAME,
													@jobDescriptor		=''dbo.usp_mpDatabaseBackup(Data)'',
													@flgActions			= DEFAULT,
													@recreateMode		= DEFAULT,
													@debugMode			= DEFAULT'

IF @job_id IS NOT NULL AND EXISTS (	SELECT * FROM msdb..sysjobsteps 
									WHERE	[job_id] = @job_id AND [step_id] = 1 
											AND REPLACE(REPLACE(REPLACE([command], '	', ' '), '  ', ' '), CHAR(13), '') <> REPLACE(REPLACE(REPLACE(@queryToRun, '	', ' '), '  ', ' '), CHAR(13), '')
									)
	begin
		RAISERROR('	update job: Database Backup - Full and Diff - Parallel', 10, 1) WITH NOWAIT
		
		EXEC msdb.dbo.sp_update_jobstep   @job_id = @job_id
										, @step_id = 1
										, @command = @queryToRun
	end
GO
