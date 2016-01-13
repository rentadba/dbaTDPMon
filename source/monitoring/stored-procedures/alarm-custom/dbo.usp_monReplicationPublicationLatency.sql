RAISERROR('Drop procedure: [dbo].[usp_monReplicationPublicationLatency]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_monReplicationPublicationLatency]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_monReplicationPublicationLatency]
GO
