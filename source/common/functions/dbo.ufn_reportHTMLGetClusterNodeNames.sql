RAISERROR('Create function: [dbo].[ufn_reportHTMLGetClusterNodeNames]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ufn_reportHTMLGetClusterNodeNames]') AND type in (N'FN', N'IF', N'TF', N'FS', N'FT'))
DROP FUNCTION [dbo].[ufn_reportHTMLGetClusterNodeNames]
GO

CREATE FUNCTION [dbo].[ufn_reportHTMLGetClusterNodeNames]
(		
	  @projectID		[smallint]
	, @instanceName		[sysname]
)
RETURNS [nvarchar](max)
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 13.04.2011
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-- { sql_statement | statement_block }
begin
	DECLARE   @clusterNodes		[nvarchar](max)
			, @nodeName			[sysname]

	SET @clusterNodes = N''
	DECLARE crsClusterNodes CURSOR LOCAL READ_ONLY FOR	SELECT	cin.[machine_name]
														FROM	[dbo].[vw_catalogInstanceNames] cin
														WHERE	cin.[instance_name] = @instanceName
																AND cin.[project_id] = @projectID
																AND cin.[is_clustered] = 1
	OPEN crsClusterNodes
	FETCH NEXT FROM crsClusterNodes INTO @nodeName
	WHILE @@FETCH_STATUS=0
		begin
			IF @clusterNodes = N'' 
				SET @clusterNodes = @nodeName
			ELSE
				SET @clusterNodes = @clusterNodes + N'<BR>' + @nodeName

			FETCH NEXT FROM crsClusterNodes INTO @nodeName
		end
	CLOSE crsClusterNodes
	DEALLOCATE crsClusterNodes

	RETURN @clusterNodes
end

GO
