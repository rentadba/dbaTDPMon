SET NOCOUNT ON
/*
*-----------------------------------------------------------------------------*
* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
*-----------------------------------------------------------------------------*
* Patch script: from version 2018.3 to 2018.4 (2018.04.11)					  *
*-----------------------------------------------------------------------------*
*/

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: common																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('* Patch: 20180411-patch-upgrade-from-v2018_3-to-v2018_4-common.sql', 10, 1) WITH NOWAIT

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[jobExecutionStatisticsHistory]') AND type in (N'U'))
	begin
		--internal jobs execution statistics
		RAISERROR('	Create table: [dbo].[jobExecutionStatisticsHistory]', 10, 1) WITH NOWAIT;

		CREATE TABLE [dbo].[jobExecutionStatisticsHistory]
		(
			[id]						[int]	 IDENTITY (1, 1)	NOT NULL,
			[project_id]				[smallint]		NOT NULL,
			[task_id]					[smallint]		NULL,
			[start_date]				[datetime]		NOT NULL,
			[module]					[varchar](32)	NOT NULL,
			[descriptor]				[varchar](256)	NOT NULL,
			[duration_minutes_parallel] [int]			NOT NULL CONSTRAINT [DF_jobExecutionStatisticsHistory_DurationMinutesParallel]  DEFAULT ((0)),
			[duration_minutes_serial]	[int]			NOT NULL CONSTRAINT [DF_jobExecutionStatisticsHistory_DurationMinutesSerial]  DEFAULT ((0)),
			[status]					[varchar](256)	NULL,
			CONSTRAINT [PK_jobExecutionStatisticsHistory] PRIMARY KEY CLUSTERED 
			(
				[id] ASC
			) ON [FG_Statistics_Data],
			CONSTRAINT [FK_jobExecutionStatisticsHistory_catalogProjects] FOREIGN KEY 
			(
				[project_id]
			) 
			REFERENCES [dbo].[catalogProjects] 
			(
				[id]
			)
		) ON [FG_Statistics_Data];
		
		CREATE INDEX [IX_jobExecutionStatisticsHistory_ProjectID_TaskID] ON [dbo].[jobExecutionStatisticsHistory]([project_id], [task_id]) ON [FG_Statistics_Index];
	end
GO
