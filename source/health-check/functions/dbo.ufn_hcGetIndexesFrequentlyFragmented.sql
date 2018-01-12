RAISERROR('Create function: [dbo].[ufn_hcGetIndexesFrequentlyFragmented]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sysobjects WHERE id = OBJECT_ID(N'[dbo].[ufn_hcGetIndexesFrequentlyFragmented]') AND type in (N'FN', N'IF', N'TF', N'FS', N'FT'))
DROP FUNCTION [dbo].[ufn_hcGetIndexesFrequentlyFragmented]
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE FUNCTION [dbo].[ufn_hcGetIndexesFrequentlyFragmented]
(		
	@projectCode							[varchar](32)=NULL,
	@minimumIndexMaintenanceFrequencyDays	[tinyint] = 2,
	@analyzeOnlyMessagesFromTheLastHours	[tinyint] = 24 ,
	@analyzeIndexMaintenanceOperation		[nvarchar](128) = 'REBUILD'
)
RETURNS @fragmentedIndexes TABLE
	(
		[instance_name]				[sysname],
		[event_date_utc]			[datetime],
		[database_name]				[sysname],
		[object_name]				[nvarchar](256),
		[index_name]				[sysname],
		[interval_days]				[tinyint],
		[index_type]				[sysname],
		[fragmentation]				[numeric](38,2),
		[page_count]				[int],
		[fill_factor]				[int],
		[page_density_deviation]	[numeric](38,2),
		[last_action_made]			[nvarchar](128)
	)
/* WITH ENCRYPTION */
AS
-- ============================================================================
-- Copyright (c) 2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 17.08.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================
begin
	DECLARE	@projectID					[int],
			@maxEventDateUTCToAnalyze	[datetime]

	-----------------------------------------------------------------------------------------------------
	--get default project code
	IF @projectCode IS NULL
		SELECT	@projectCode = [value]
		FROM	[dbo].[appConfigurations]
		WHERE	[name] = 'Default project code'
				AND [module] = 'common'

	SELECT    @projectID = [id]
	FROM [dbo].[catalogProjects]
	WHERE [code] = @projectCode 

	SET @maxEventDateUTCToAnalyze = DATEADD(hh, -@analyzeOnlyMessagesFromTheLastHours, GETUTCDATE())

	-----------------------------------------------------------------------------------------------------
	;WITH fillfactorCandidateIndexes AS
	(
		SELECT	  i.[event_message_id], i.[event_date_utc]
				, i.[instance_name], i.[database_name], i.[object_name], i.[child_object_name]
				, i.[message_xml] AS [info_xml], a.[message_xml] AS [action_xml]
		FROM (
				SELECT	  [event_message_id], [event_date_utc]
						, ISNULL([instance_name], @@SERVERNAME) AS [instance_name], [database_name], [object_name], [child_object_name]
						, [message_xml]
				FROM	[dbo].[vw_logEventMessages]
				WHERE	(   
							(   [event_name] = 'database maintenance - rebuilding index' 
							 AND CHARINDEX('REBUILD', @analyzeIndexMaintenanceOperation) <> 0
							)
						 OR
							(	[event_name] = 'database maintenance - reorganize index' 
							 AND CHARINDEX('REORGANIZE', @analyzeIndexMaintenanceOperation) <> 0
							)
						)
						AND [event_type] = 0 --info
						AND [project_id] = @projectID
						AND [event_date_utc] >= @maxEventDateUTCToAnalyze
			)i
		INNER JOIN
			(
				SELECT	  [event_message_id], [event_date_utc]
						, ISNULL([instance_name], @@SERVERNAME) AS [instance_name], [database_name], [object_name], [child_object_name]
						, [message_xml]
				FROM	[dbo].[vw_logEventMessages]
				WHERE	(   
							(   [event_name] = 'database maintenance - rebuilding index' 
							 AND CHARINDEX('REBUILD', @analyzeIndexMaintenanceOperation) <> 0
							)
						 OR
							(	[event_name] = 'database maintenance - reorganize index' 
							 AND CHARINDEX('REORGANIZE', @analyzeIndexMaintenanceOperation) <> 0
							)
						)
						AND [event_type] = 4 --action
						AND [project_id] = @projectID
						AND [event_date_utc] >= @maxEventDateUTCToAnalyze
			)a ON	a.[instance_name] = i.[instance_name]
					AND a.[database_name] = i.[database_name] 
					AND a.[object_name] = i.[object_name] 
					AND a.[child_object_name] = i.[child_object_name]
					AND (  a.[event_message_id] = i.[event_message_id] + 1
						OR DATEDIFF(ss, i.[event_date_utc], a.[event_date_utc]) BETWEEN 0 AND 60 /* 60 seconds delay between info and action messages */
						)
		),
	fragmentedIndexesInfo AS
	(
		SELECT	  [event_message_id], [event_date_utc], [instance_name], [database_name], [object_name], [child_object_name]
				, [info_xml], [action_xml]
				, ROW_NUMBER() OVER (PARTITION BY [instance_name], [database_name], [object_name], [child_object_name] ORDER BY [event_date_utc] DESC) AS [sequence_id]
		FROM fillfactorCandidateIndexes
	)

	INSERT	INTO @fragmentedIndexes(  [instance_name], [event_date_utc], [database_name], [object_name], [index_name]
									, [interval_days], [index_type], [fragmentation], [page_count], [fill_factor], [page_density_deviation], [last_action_made])
			SELECT    [instance_name], [event_date_utc], [database_name], [object_name], [child_object_name] AS [index_name]
					, [interval_days]
					, info.value ('index_type[1]', 'sysname') as [index_type]
					, info.value ('fragmentation[1]', 'numeric(38,2)') as [fragmentation]
					, info.value ('page_count[1]', 'int') as [page_count]
					, info.value ('fill_factor[1]', 'int') as [fill_factor]
					, info.value ('page_density_deviation[1]', 'numeric(38,2)') as [page_density_deviation]
					, REPLACE(REPLACE(act.value ('event_name[1]', 'sysname'), 'database maintenance - ', ''), ' index', '') as [action_made]
			FROM (		
					SELECT    A.[event_message_id], A.[event_date_utc]
							, A.[instance_name], A.[database_name], A.[object_name], A.[child_object_name]
							, A.[info_xml], A.[action_xml]
							, A.[sequence_id], CEILING(DATEDIFF(hh, B.[event_date_utc], A.[event_date_utc]) / 24.) AS [interval_days]
					FROM fragmentedIndexesInfo A
					INNER JOIN fragmentedIndexesInfo B ON	A.[instance_name] = B.[instance_name]
															AND A.[database_name] = B.[database_name] 
															AND A.[object_name] = B.[object_name] 
															AND A.[child_object_name] = B.[child_object_name]
															AND A.sequence_id = B.sequence_id - 1
					WHERE CEILING(DATEDIFF(hh, B.[event_date_utc], A.[event_date_utc]) / 24.) <= @minimumIndexMaintenanceFrequencyDays
						AND A.[sequence_id] = 1
				)X
			CROSS APPLY [info_xml].nodes ('//index-fragmentation/detail') I(info)
			CROSS APPLY [action_xml].nodes ('//action/detail') A(act)
		
	RETURN
end
GO
