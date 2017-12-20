RAISERROR('Create function: [dbo].[ufn_formatPlatformSpecificPath]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sysobjects WHERE [id] = OBJECT_ID(N'[dbo].[ufn_formatPlatformSpecificPath]') AND type in (N'FN', N'IF', N'TF', N'FS', N'FT'))
DROP FUNCTION [dbo].[ufn_formatPlatformSpecificPath]
GO

CREATE FUNCTION [dbo].[ufn_formatPlatformSpecificPath]
(		
	@sqlServerName		[sysname],
	@filePath			[nvarchar] (4000)
)
RETURNS [nvarchar](4000)
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 18.12.2017
-- Module			 : Database Analysis & Performance Monitoring
--					 : SQL Server 2000/2005/2008/2008R2/2012+
-- ============================================================================
-----------------------------------------------------------------------------------------
-- Input Parameters:
--		@sqlServerName	- name of SQL Server instance
--		@filePath		- path to convert to platform specific
-----------------------------------------------------------------------------------------
-- Return : 
--		SQL statement formated to be executed over linked server using OPENQUERY or locally
-----------------------------------------------------------------------------------------
-- { sql_statement | statement_block }

begin
	DECLARE @hostPlatform				[sysname],
			@filePathPlatformSpecific	[nvarchar](4000)

	SET @filePathPlatformSpecific = @filePath

	SELECT @hostPlatform = [host_platform]
	FROM [dbo].[vw_catalogInstanceNames]
	WHERE [instance_name] = @sqlServerName

	IF @hostPlatform='linux'
		begin
			SET @filePathPlatformSpecific = REPLACE(REPLACE(@filePathPlatformSpecific, '/\', '/'), '\', '/')
		end
	
	RETURN @filePathPlatformSpecific
end

GO
