RAISERROR('Create function: [dbo].[ufn_formatSQLQueryForLinkedServer]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sysobjects WHERE [id] = OBJECT_ID(N'[dbo].[ufn_formatSQLQueryForLinkedServer]') AND type in (N'FN', N'IF', N'TF', N'FS', N'FT'))
DROP FUNCTION [dbo].[ufn_formatSQLQueryForLinkedServer]
GO

CREATE FUNCTION [dbo].[ufn_formatSQLQueryForLinkedServer]
(		
	@sqlServerName		[sysname],
	@sqlText			[nvarchar] (4000)
)
RETURNS [nvarchar](4000)
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 06.01.2010
-- Module			 : Database Analysis & Performance Monitoring
--					 : SQL Server 2000/2005/2008/2008R2/2012+
-- ============================================================================
-----------------------------------------------------------------------------------------
-- Input Parameters:
--		@sqlServerName	- name of SQL Server instance
--		@sqlText		- initial SQL statement to be executed.
--						  this string is formated as to be executed on local server
-----------------------------------------------------------------------------------------
-- Return : 
--		SQL statement formated to be executed over linked server using OPENQUERY or locally
-----------------------------------------------------------------------------------------
-- { sql_statement | statement_block }

begin
	DECLARE @SQLStatement [nvarchar] (4000)

	SET @SQLStatement = N''

	IF @sqlServerName=@@SERVERNAME
		SET @SQLStatement = @sqlText
	ELSE
		begin
			SET @SQLStatement = @SQLStatement + 
								N'SELECT x.* FROM OPENQUERY([' + @sqlServerName + '], ''' + 
								REPLACE(@sqlText, '''', '''''') + 
								''')x'
		end
	RETURN @SQLStatement
end

GO
