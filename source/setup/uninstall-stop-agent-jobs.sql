-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 17.01.2011
-- Module			 : SQL Server 2000/2005/2008/2008R2/2012+
-- Description		 : stop and delete SQL Server Agent jobs
-- ============================================================================

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

SET NOCOUNT ON

-----------------------------------------------------------------------------------------
RAISERROR('Stop and delete SQL Server Agent jobs', 10, 1) WITH NOWAIT
GO

DECLARE   @jobName		[sysname]
		, @strMessage	[nvarchar](1024)
		
DECLARE crtSQLServerAgentJobs CURSOR LOCAL FAST_FORWARD FOR	SELECT DISTINCT sj.[name]
															FROM [msdb].[dbo].[sysjobs] sj
															INNER JOIN [msdb].[dbo].[sysjobsteps] sjs ON sjs.[job_id]=sj.[job_id]
															WHERE sjs.[database_name] = '$(dbName)'
																OR sj.[name] LIKE ('$(dbName)' + '%')
OPEN crtSQLServerAgentJobs
FETCH NEXT FROM crtSQLServerAgentJobs INTO @jobName
WHILE @@FETCH_STATUS=0	
	begin
		IF  EXISTS (SELECT job_id FROM msdb.dbo.sysjobs WHERE name = @jobName)
			AND EXISTS(
						--check if the job is running
						SELECT * 
						FROM (	
						SELECT B [step_id], SUBSTRING(A, 7, 2) + SUBSTRING(A, 5, 2) + SUBSTRING(A, 3, 2) + LEFT(A, 2) + '-' + SUBSTRING(A, 11, 2) + SUBSTRING(A, 9, 2) + '-' + SUBSTRING(A, 15, 2) + SUBSTRING(A, 13, 2) + '-' + SUBSTRING(A, 17, 4) + '-' + RIGHT(A , 12) [job_id] 
						FROM	(
								 SELECT SUBSTRING([program_name], CHARINDEX(': Step', [program_name]) + 7, LEN([program_name]) - CHARINDEX(': Step', [program_name]) - 7) B, SUBSTRING([program_name], CHARINDEX('(Job 0x', [program_name]) + 7, CHARINDEX(' : Step ', [program_name]) - CHARINDEX('(Job 0x', [program_name]) - 7) A
	 							 FROM [master].[dbo].[sysprocesses] 
	 							 WHERE [program_name] LIKE 'SQLAgent - %JobStep%') A
								) A 
						WHERE [job_id] IN (
											SELECT DISTINCT [job_id] 
											FROM [msdb].[dbo].[sysjobs] 
											WHERE [name]= @jobName
										  )
						  )
			begin
				SET @strMessage = 'Stop job: ' + @jobName
				PRINT @strMessage
				EXEC msdb.dbo.sp_stop_job   @job_name = @jobName
			end
	
		SET @strMessage = 'Delete job: ' + @jobName
		PRINT @strMessage

		EXEC msdb.dbo.sp_delete_job   @job_name=@jobName

		FETCH NEXT FROM crtSQLServerAgentJobs INTO @jobName
	end
CLOSE crtSQLServerAgentJobs
DEALLOCATE crtSQLServerAgentJobs
GO
