RAISERROR('Create function: [dbo].[ufn_getMilisecondsBetweenDates]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sysobjects WHERE id = OBJECT_ID(N'[dbo].[ufn_getMilisecondsBetweenDates]') AND type in (N'FN', N'IF', N'TF', N'FS', N'FT'))
DROP FUNCTION [dbo].[ufn_getMilisecondsBetweenDates]
GO

CREATE FUNCTION [dbo].[ufn_getMilisecondsBetweenDates]
(		
	@startDate	[datetime],
	@endDate	[datetime]
)
RETURNS [bigint]
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 10.01.2011
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-- { sql_statement | statement_block }
begin
	DECLARE @NumberOfMiliseconds [bigint]
	
	SET @NumberOfMiliseconds = CONVERT([bigint], CAST(DATEDIFF(minute, @startDate, @endDate) AS [bigint]) * 60000 + 
											     DATEDIFF(millisecond, DATEADD(minute, DATEDIFF(minute, @startDate, @endDate), @startDate), @endDate)
									  )
	
	RETURN @NumberOfMiliseconds
end

GO
