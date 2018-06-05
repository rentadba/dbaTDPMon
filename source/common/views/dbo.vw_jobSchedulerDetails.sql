-----------------------------------------------------------------------------------------------------
--
-----------------------------------------------------------------------------------------------------
RAISERROR('Create view : [dbo].[vw_jobSchedulerDetails]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[dbo].[vw_jobSchedulerDetails]'))
DROP VIEW [dbo].[vw_jobSchedulerDetails]
GO

CREATE VIEW [dbo].[vw_jobSchedulerDetails]
AS
SELECT    sj.[job_id]
		, sj.[name] AS [job_name]
		, CASE sj.[enabled] WHEN 1 THEN 'Yes' WHEN 0 THEN 'No' END AS [job_is_enabled]
		, sjsch.[next_run_date]
		, RIGHT('0' + CAST(CAST(sjsch.[next_run_time] AS [varchar](10)) AS [varchar](3)),4) AS [next_run_time]
		, CASE	WHEN [next_run_time] > 0 AND [next_run_date] IS NOT NULL AND [next_run_time] IS NOT NULL 
				THEN CONVERT([datetime], CONVERT([nvarchar](4), [next_run_date] / 10000) + N'-' + 
										 CONVERT([nvarchar](2),([next_run_date]  % 10000)/100)  + N'-' +
										 CONVERT([nvarchar](2), [next_run_date]  % 100) + N' ' +        
										 CONVERT([nvarchar](2), [next_run_time] / 10000) + N':' +        
										 CONVERT([nvarchar](2),([next_run_time] % 10000)/100) + N':' +        
										 CONVERT([nvarchar](2), [next_run_time] % 100), 120) 
				ELSE NULL 
		  END AS [next_run_date_time]
		, CASE	WHEN [freq_type] = 64 THEN 'Start automatically when SQL Server Agent starts' 
				WHEN [freq_type] = 128 THEN 'Start whenever the CPUs become idle' 
				WHEN [freq_type] IN (4, 8, 16, 32) THEN 'Recurring' 
				WHEN [freq_type] = 1 THEN 'One Time' 
		  END AS [schedule_type]
		, CASE [freq_type]	
				WHEN 1 THEN 'One Time' 
				WHEN 4 THEN 'Daily' 
				WHEN 8 THEN 'Weekly' 
				WHEN 16 THEN 'Monthly' 
				WHEN 32 THEN 'Monthly - Relative to Frequency Interval' 
				WHEN 64 THEN 'Start automatically when SQL Server Agent starts'
				WHEN 128 THEN 'Start whenever the CPUs become idle' 
		  END AS [occurrence]
		, CASE [freq_type]	
				WHEN 4 THEN 'Occurs every ' + CAST([freq_interval] AS [varchar](3)) + ' day(s)' 
				WHEN 8 THEN 'Occurs every ' + CAST([freq_recurrence_factor] AS [varchar](3)) + ' week(s) on ' + 
						CASE WHEN [freq_interval] & 1 = 1 THEN 'Sunday' ELSE '' END + 
						CASE WHEN [freq_interval] & 2 = 2 THEN ', Monday' ELSE '' END + 
						CASE WHEN [freq_interval] & 4 = 4 THEN ', Tuesday' ELSE '' END + 
						CASE WHEN [freq_interval] & 8 = 8 THEN ', Wednesday' ELSE '' END + 
						CASE WHEN [freq_interval] & 16 = 16 THEN ', Thursday' ELSE '' END + 
						CASE WHEN [freq_interval] & 32 = 32 THEN ', Friday' ELSE '' END +
						CASE WHEN [freq_interval] & 64 = 64 THEN ', Saturday' ELSE '' END 
				WHEN 16 THEN 'Occurs on Day ' + CAST([freq_interval] AS VARCHAR(3)) + ' of every ' + CAST([freq_recurrence_factor] AS VARCHAR(3)) + ' month(s)' 
				WHEN 32 THEN 'Occurs on ' + 
						CASE [freq_relative_interval]	WHEN 1 THEN 'First' 
														WHEN 2 THEN 'Second' 
														WHEN 4 THEN 'Third' 
														WHEN 8 THEN 'Fourth' 
														WHEN 16 THEN 'Last' 
						END + ' ' + 
						CASE [freq_interval]	WHEN 1 THEN 'Sunday' 
												WHEN 2 THEN 'Monday' 
												WHEN 3 THEN 'Tuesday' 
												WHEN 4 THEN 'Wednesday' 
												WHEN 5 THEN 'Thursday' 
												WHEN 6 THEN 'Friday' 
												WHEN 7 THEN 'Saturday' 
												WHEN 8 THEN 'Day' 
												WHEN 9 THEN 'Weekday' 
												WHEN 10 THEN 'Weekend day' 
						END + ' of every ' + CAST([freq_recurrence_factor] AS [varchar](3)) + ' month(s)' 
		  END AS [recurrence]
		, CASE [freq_subday_type]	
				WHEN 1 THEN 'Occurs once at ' + STUFF(STUFF(RIGHT('000000' + CAST([active_start_time] AS [varchar](6)), 6), 3, 0, ':'), 6, 0, ':') 
				WHEN 2 THEN 'Occurs every ' + CAST([freq_subday_interval] AS [varchar](3)) + ' Second(s) between ' + STUFF(STUFF(RIGHT('000000' + CAST([active_start_time] AS [varchar](6)), 6), 3, 0, ':'), 6, 0, ':') + ' & ' + STUFF(STUFF(RIGHT('000000' + CAST([active_end_time] AS [varchar](6)), 6), 3, 0, ':'), 6, 0, ':') 
				WHEN 4 THEN 'Occurs every ' + CAST([freq_subday_interval] AS [varchar](3)) + ' Minute(s) between ' + STUFF(STUFF(RIGHT('000000' + CAST([active_start_time] AS [varchar](6)), 6), 3, 0, ':'), 6, 0, ':') + ' & ' + STUFF(STUFF(RIGHT('000000' + CAST([active_end_time] AS [varchar](6)), 6), 3, 0, ':'), 6, 0, ':') 
				WHEN 8 THEN 'Occurs every ' + CAST([freq_subday_interval] AS [varchar](3)) + ' Hour(s) between '   + STUFF(STUFF(RIGHT('000000' + CAST([active_start_time] AS [varchar](6)), 6), 3, 0, ':'), 6, 0, ':') + ' & ' + STUFF(STUFF(RIGHT('000000' + CAST([active_end_time] AS [varchar](6)), 6), 3, 0, ':'), 6, 0, ':') 
		  END AS [frequency]
		, STUFF(STUFF(CAST(ssch.[active_start_date] AS [varchar](8)), 5, 0, '-'), 8, 0, '-') AS [schedule_usage_start_date]
		, STUFF(STUFF(CAST(ssch.[active_end_date] AS [varchar](8)), 5, 0, '-'), 8, 0, '-') AS [schedule_usage_end_date]
		, CASE [freq_subday_type] 
				WHEN 2 THEN [freq_subday_interval] 
				WHEN 4 THEN [freq_subday_interval] * 60 
				WHEN 8 THEN [freq_subday_interval] * 60 * 60 
				ELSE 0 
		  END AS [freq_seconds]
FROM [msdb].[dbo].[sysjobs] AS sj
LEFT JOIN [msdb].[dbo].[sysjobschedules] AS sjsch ON sj.[job_id] = sjsch.[job_id]
LEFT JOIN [msdb].[dbo].[sysschedules] AS ssch ON sjsch.[schedule_id] = ssch.[schedule_id]
INNER JOIN
	(	
		SELECT	DISTINCT [job_id], NULL AS [job_name]
		FROM	[msdb].[dbo].[sysjobsteps]
		WHERE	[database_name] = DB_NAME()
		UNION ALL
		SELECT	NULL AS [job_id], [job_name]
		FROM	[dbo].[vw_jobExecutionQueue]
	) as jeq ON	   (jeq.[job_name] = sj.[name] AND jeq.[job_name] IS NOT NULL)
				OR (jeq.[job_id] = sj.[job_id] AND jeq.[job_id] IS NOT NULL)
GO
