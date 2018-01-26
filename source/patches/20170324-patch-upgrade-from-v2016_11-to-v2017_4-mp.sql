SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2016.11 to 2017.4 (2017.03.24)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: maintenance-plan																							   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20170324-patch-upgrade-from-v2016_11-to-v2017_4-mp.sql', 10, 1) WITH NOWAIT

DECLARE   @queryToRun	[nvarchar](max)
		, @job_id		[uniqueidentifier]

SELECT @job_id = [job_id]
FROM msdb.dbo.sysjobs 
WHERE [name] LIKE '% - Database Maintenance - System DBs%'

SET @queryToRun = 'EXEC dbo.usp_purgeHistoryData'

IF @job_id IS NOT NULL AND EXISTS (	SELECT * FROM msdb..sysjobsteps 
									WHERE	[job_id] = @job_id AND [step_id] = 17 
											AND REPLACE(REPLACE(REPLACE([command], '	', ' '), '  ', ' '), CHAR(13), '') <> REPLACE(REPLACE(REPLACE(@queryToRun, '	', ' '), '  ', ' '), CHAR(13), '')
									)
	begin
		RAISERROR('	update job: Database Maintenance - System DBs', 10, 1) WITH NOWAIT

		EXEC msdb.dbo.sp_update_jobstep   @job_id = @job_id
										, @step_id = 17
										, @command = @queryToRun
	end
GO
