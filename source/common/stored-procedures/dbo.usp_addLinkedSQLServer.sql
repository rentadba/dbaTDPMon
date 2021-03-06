RAISERROR('Create procedure: [dbo].[usp_addLinkedSQLServer]', 10, 1) WITH NOWAIT
GO
SET QUOTED_IDENTIFIER ON 
GO
SET ANSI_NULLS ON 
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[usp_addLinkedSQLServer]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [dbo].[usp_addLinkedSQLServer]
GO

CREATE PROCEDURE dbo.usp_addLinkedSQLServer
	@ServerName 	varchar(255)
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 2004-2006
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SET NOCOUNT ON
/*
IF (SELECT count(*) FROM master.dbo.sysservers WHERE SrvName=@ServerName)<>0
	EXEC master.dbo.sp_dropserver @ServerName, 'droplogins'
*/

IF (SELECT count(*) FROM master.dbo.sysservers WHERE srvname=@ServerName)=0
	begin
		EXEC master.dbo.sp_addlinkedserver 	@server	   	= @ServerName, 
							@srvproduct	= 'SQL Server'
	
		EXEC master.dbo.sp_addlinkedsrvlogin	@rmtsrvname	= @ServerName, 
							@useself   	= 'true'

		EXEC master.dbo.sp_serveroption 	@server 	= @ServerName,
							@optname 	= 'data access',
							@optvalue 	= 'True'

		EXEC master.dbo.sp_serveroption 	@server 	= @ServerName,
							@optname 	= 'rpc',
							@optvalue 	= 'True'

		EXEC master.dbo.sp_serveroption 	@server 	= @ServerName,
							@optname 	= 'rpc out',
							@optvalue 	= 'True'

		EXEC master.dbo.sp_serveroption 	@server 	= @ServerName,
							@optname 	= 'use remote collation',
							@optvalue 	= 'False'
	end

GO
SET QUOTED_IDENTIFIER OFF 
GO
SET ANSI_NULLS ON 
GO

