RAISERROR('Drop procedure: [dbo].[usp_mpUpdateStatisticsBasedOnStrategy]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_mpUpdateStatisticsBasedOnStrategy]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpUpdateStatisticsBasedOnStrategy]
GO
