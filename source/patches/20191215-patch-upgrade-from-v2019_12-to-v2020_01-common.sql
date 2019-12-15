SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2019.12 to 2020.01 (2019.12.15)				  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: common																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20191215-patch-upgrade-from-v2019_12-to-v2020_01-common.sql', 10, 1) WITH NOWAIT
USE [msdb]
GO
DECLARE @jobID		[uniqueidentifier],
		@strMessage	[nvarchar](1024)

DECLARE crsJobsToUpdate CURSOR LOCAL FAST_FORWARD FOR	SELECT	sjs.[job_id], 
																'* updating SQL Agent job: "' + sj.[name] + '"' AS [message]
														FROM msdb.dbo.sysjobsteps sjs
														INNER JOIN msdb.dbo.sysjobs sj ON sjs.[job_id] = sj.[job_id]
														WHERE sjs.[step_id] = 1
															AND sjs.[step_name] = 'Generate Job Queue'
															AND sj.[description] LIKE '%dbaTDPMon%'
															AND (   sjs.[retry_attempts] < 3 
																 OR sjs.[retry_interval] = 0
																)
OPEN crsJobsToUpdate
FETCH NEXT FROM crsJobsToUpdate INTO @jobID, @strMessage
WHILE @@FETCH_STATUS = 0
	begin
		RAISERROR(@strMessage, 10, 1) WITH NOWAIT

		EXEC msdb.dbo.sp_update_jobstep   @job_id = @jobID
										, @step_id = 1
										, @retry_attempts = 3
										, @retry_interval = 1

		FETCH NEXT FROM crsJobsToUpdate INTO @jobID, @strMessage
	end
CLOSE crsJobsToUpdate
DEALLOCATE crsJobsToUpdate


								